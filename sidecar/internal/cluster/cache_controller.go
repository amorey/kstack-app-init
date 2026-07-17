// Copyright 2026 The Kstack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/rest"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/engine"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

const (
	// syncRecheckInterval paces the steady-state re-reconcile of a syncing
	// cluster (for credential-rotation detection).
	syncRecheckInterval = 30 * time.Second

	// engineStopTimeout bounds one engine teardown.
	engineStopTimeout = 10 * time.Second
)

// EngineHandle is the controller's view of one running sync engine. The seam
// allows tests to inject a fake without touching the network.
type EngineHandle interface {
	Start()
	Stop(ctx context.Context) error
}

// NewEngineFunc constructs one sync engine. ref locates the on-disk cache by
// beehive ObjectID (parent Cluster + ClusterCache); clusterID is the domain id,
// kept for log labels. Production uses a engine.NewEngine wrapper; tests inject a
// factory that returns fake handles.
type NewEngineFunc func(cfg *rest.Config, clusterID ClusterID, ref store.CacheRef, sink engine.Sink) EngineHandle

// engineEntry is the controller's runtime state for one running engine. The
// pointer guards the sink: reports from a stopped or replaced engine are dropped
// by comparing pointer identity.
type engineEntry struct {
	handle EngineHandle
	// restCfg is the connection config the engine was started with, kept so a
	// poke-driven restart can respawn the engine without re-resolving it.
	restCfg     *rest.Config
	fingerprint string
	// clusterObjID is the beehive ObjectID of the parent Cluster object; it names
	// the on-disk cache directory (clusters/<clusterObjID>/). Stored so a
	// poke-driven restart can rebuild the store.CacheRef without re-reading.
	clusterObjID beehive.ObjectID
	// cacheObjID is the beehive ObjectID of the ClusterCache object this engine
	// reports into. It names the on-disk cache file (<cacheObjID>.db) and lets the
	// sink call UpdateStatus without a lookup.
	cacheObjID beehive.ObjectID
	// cacheGen is the ClusterCache object's own generation, used as the
	// observedGeneration when the sink calls UpdateStatus. It must be this
	// object's generation (not the parent's) or beehive rejects the write as a
	// future generation.
	cacheGen int64
	// parentGen is the parent Cluster's generation stamp recorded in the Synced
	// condition's ObservedGeneration (the condition observes the parent's spec).
	parentGen int64

	// lastSyncReason is the reason of the most recent sync event actually recorded
	// for this engine, or "" before the first (syncEvent never returns an empty
	// reason, and each reason maps 1:1 to its EventType, so the reason alone keys
	// the transition). recordSyncEvent records only on a transition — a change in
	// this reason — so the engine's steady-state freshness heartbeat (a repeated
	// Watching report) doesn't append a redundant "Watching ×N" run. Mutated only
	// under writeMu (the sink path holds it), so no extra guard is needed.
	lastSyncReason string
}

// cacheFilesFinalizer gates a ClusterCache's deletion on this controller deleting
// its on-disk cache file. It is set at creation (ensureClusterCache) and cleared on
// the deletion reconcile once the file is gone, so GC can't collect the row — and
// orphan the file — before the cleanup runs.
const cacheFilesFinalizer = "kstack.io/cache-files"

// ClusterCacheController reconciles ClusterCache beehive objects: it manages
// the sync engine lifecycle for each cluster cache, folding engine reports
// back into ClusterCacheStatus as the Synced condition.
//
// The controller reads the parent Cluster (via ClusterClient) to determine
// eligibility (connection-eligible + SyncEnabled), and adds a DependsOn edge
// (ClusterCache depends on Cluster) so beehive re-queues this cache when the
// parent Cluster spec changes (e.g. SyncEnabled toggled).
//
// Resync pokes (OS resume / network-on / wall-clock gap) arrive out-of-band on
// the poke bus, not through beehive: the controller subscribes in Start and, on
// each signal, restarts its live engines in place (dropping stale watch streams;
// each driver re-resumes cheaply from its persisted resourceVersion). The
// engines are in-memory runtime state the controller already owns, so a poke
// needs no durable spec write — see restartLiveEngines.
type ClusterCacheController struct {
	cfgSource    KubeConfigSource
	coreClient   beehive.Client[ClusterSpec, ClusterStatus]
	cacheManager *store.Manager
	connMgr      *ConnectionManager
	ctrlClient   beehive.ControllerClient[ClusterCacheStatus]

	// pokeSvc is the resync bus; nil disables poke-driven restarts (tests).
	pokeSvc *poke.Service
	pokeSub *pokeSubscription

	// newEngine constructs one sync engine. Overridable for tests.
	newEngine NewEngineFunc

	// writeMu serializes read-modify-write status updates from the reconcile
	// worker and from the engines' sink goroutines.
	writeMu sync.Mutex
	mu      sync.Mutex
	// engines is keyed by the ClusterCache object's own ObjectID, not its parent
	// ClusterID: a cluster can own several ClusterCache records (one per physical
	// identity it has mirrored), and the controller reconciles each independently —
	// only the active one (UID == the parent's last-probed Server.UID) runs an engine,
	// but keying by cache id keeps a migration's old/new caches from racing on a shared
	// per-cluster slot during the hand-over.
	engines map[beehive.ObjectID]*engineEntry
}

// NewClusterCacheController builds the controller. manager owns the per-cluster
// SQLite cache files; it is shared with the resolver so both see the same open
// DBs. connMgr may be nil (credentials are then resolved from the kubeconfig).
func NewClusterCacheController(
	cfgSource KubeConfigSource,
	coreClient beehive.Client[ClusterSpec, ClusterStatus],
	manager *store.Manager,
	connMgr *ConnectionManager,
	pokeSvc *poke.Service,
) *ClusterCacheController {
	c := &ClusterCacheController{
		cfgSource:    cfgSource,
		coreClient:   coreClient,
		cacheManager: manager,
		connMgr:      connMgr,
		pokeSvc:      pokeSvc,
		engines:      make(map[beehive.ObjectID]*engineEntry),
	}
	cm := manager
	c.newEngine = func(cfg *rest.Config, id ClusterID, ref store.CacheRef, sink engine.Sink) EngineHandle {
		cdb, err := cm.Open(context.Background(), ref)
		if err != nil {
			slog.Warn("clustercachecontroller: open cache db", "cluster", id, "err", err)
			return nil
		}
		return engine.NewEngine(cfg, cdb, sink)
	}
	return c
}

// SetNewEngine replaces the engine factory — for tests.
func (c *ClusterCacheController) SetNewEngine(f NewEngineFunc) {
	c.newEngine = f
}

// SetControllerClient injects the status-write client obtained from
// beehive.Register. It backs the out-of-band engine sink (applyEngineReport),
// which writes status from engine goroutines; the reconcile path uses the client
// beehive passes into Reconcile instead. Call once, before the control plane
// starts — an engine spawned by a startup reconcile may report immediately.
func (c *ClusterCacheController) SetControllerClient(cl beehive.ControllerClient[ClusterCacheStatus]) {
	c.ctrlClient = cl
}

// StartPoke subscribes to the resync poke bus, restarting every live engine on
// each signal. Call after the control plane has started; pair with StopPoke.
func (c *ClusterCacheController) StartPoke() {
	c.pokeSub = startPokeSubscription(c.pokeSvc, func(context.Context) { c.restartLiveEngines() })
}

// StopPoke halts the poke subscription and joins its goroutine. Call before
// draining the reconcile loops so a resync restart can't race teardown.
func (c *ClusterCacheController) StopPoke() {
	c.pokeSub.stop()
}

// StopEngines tears down every running sync engine. Call after the reconcile
// loops have drained (so no reconcile can spawn a fresh engine) and before the
// per-cluster cache the engines write into is shut down.
func (c *ClusterCacheController) StopEngines() error {
	c.mu.Lock()
	entries := c.engines
	c.engines = make(map[beehive.ObjectID]*engineEntry)
	c.mu.Unlock()
	for cacheID, entry := range entries {
		stopCtx, cancel := context.WithTimeout(context.Background(), engineStopTimeout)
		if err := entry.handle.Stop(stopCtx); err != nil {
			slog.Warn("clustercachecontroller: engine stop", "cluster", entry.clusterObjID, "cache", cacheID, "err", err)
		}
		cancel()
	}
	return nil
}

// Reconcile converges one ClusterCache object toward its parent Cluster's spec.
func (c *ClusterCacheController) Reconcile(ctx context.Context, client beehive.ControllerClient[ClusterCacheStatus], obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) (beehive.Result, error) {
	// The parent ClusterID is the ClusterCache's owner (its owned_by edge), read
	// from beehive's object graph rather than re-parsed out of the slug.
	owner, ok, err := client.GetOwner(ctx, obj.ID)
	if err != nil {
		return beehive.Result{}, err
	}

	// Deletion (a UID-switch prune or a cluster-delete cascade): stop the engine,
	// delete the on-disk cache file, then clear the finalizer so GC can collect the
	// row. The locator is the owner's id (the per-cluster dir) + this cache's id (the
	// file); if the owner is already gone we can't form that path, so we clean up the
	// engine and clear the finalizer best-effort rather than wedging GC forever. A
	// file-delete error returns without clearing the finalizer, so the row lingers and
	// the next reconcile retries — the file can't be orphaned.
	if obj.DeletionRequestedAt != nil {
		c.stopEngine(obj.ID)
		if ok {
			if err := c.cacheManager.DeleteCacheFiles(ctx, newCacheRef(owner.ID, obj.ID)); err != nil {
				return beehive.Result{}, err
			}
		}
		return beehive.Result{}, c.clearCacheFilesFinalizer(ctx, client, obj)
	}

	if !ok {
		// No owner and not being deleted — the parent was GC'd; our object is being
		// cleaned up too.
		return beehive.Result{}, nil
	}
	clusterID := ClusterID(owner.ID)

	// Read the parent Cluster to determine eligibility.
	clusterObj, err := c.coreClient.Get(ctx, owner.ID)
	if err != nil {
		if errors.Is(err, beehive.ErrNotFound) {
			// Parent gone (GC race); our object will be cleaned up too.
			return beehive.Result{}, nil
		}
		return beehive.Result{}, err
	}

	// Add the DependsOn edge so beehive re-queues us when the parent Cluster
	// changes — spec edits (e.g. SyncEnabled toggled) AND status writes (the
	// ClusterCoreController's live source observation, which drives presence-based
	// eligibility). Then re-read the parent: establishing the edge before relying on
	// the parent's status closes the race where the parent is stamped between our
	// first read and AddDependency (which would wake nothing, leaving us stuck on a
	// stale 'not yet observed' view).
	if err := client.AddDependency(ctx, obj.ID, clusterObj.ID); err != nil {
		return beehive.Result{}, err
	}
	if fresh, err := c.coreClient.Get(ctx, beehive.ObjectID(clusterID)); err == nil {
		clusterObj = fresh
	}

	// This cache is "active" when it mirrors the cluster's currently-connected
	// physical identity (its UID matches the parent's last-probed kube-system UID).
	// Only the active cache runs a sync engine — a cache left behind by a physical
	// migration (its UID no longer matches) is paused, so the engine never writes
	// fresh data from the new cluster into the old identity's file.
	active := cacheIsActive(clusterObj, obj.Spec.ServerUID)

	// Load the working status copy.
	var loaded ClusterCacheStatus
	if obj.Status != nil {
		loaded = *obj.Status
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	working := ClusterCacheStatus{
		Conditions:   slices.Clone(loaded.Conditions),
		LastSyncedAt: loaded.LastSyncedAt,
	}

	requeueAfter := c.converge(clusterID, obj.ID, obj.Generation, active, clusterObj, &working)

	if ClusterCacheStatusEqual(loaded, working) {
		return beehive.Result{RequeueAfter: requeueAfter}, nil
	}
	return beehive.Result{RequeueAfter: requeueAfter},
		client.UpdateStatus(ctx, obj.ID, obj.Generation, working)
}

// clearCacheFilesFinalizer removes cacheFilesFinalizer so GC can collect the row.
// It is a no-op when the finalizer is absent — a ClusterCache created before the
// finalizer was introduced, or a double reconcile of the deletion — so it never errors
// on a missing finalizer.
func (c *ClusterCacheController) clearCacheFilesFinalizer(ctx context.Context, client beehive.ControllerClient[ClusterCacheStatus], obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) error {
	if !slices.Contains(obj.Finalizers, cacheFilesFinalizer) {
		return nil
	}
	return client.DeleteFinalizer(ctx, obj.ID, cacheFilesFinalizer)
}

// converge manages this ClusterCache's sync engine toward the parent Cluster's
// spec. active reports whether this cache mirrors the cluster's currently-connected
// identity; an inactive cache (a physical migration left it behind) is paused like
// a sync-ineligible one, so the engine never writes the new cluster's data into a
// stale identity's file.
func (c *ClusterCacheController) converge(
	clusterID ClusterID,
	cacheObjID beehive.ObjectID,
	cacheGen int64,
	active bool,
	clusterObj *beehive.Object[ClusterSpec, ClusterStatus],
	working *ClusterCacheStatus,
) time.Duration {
	gen := clusterObj.Generation
	conds := &working.Conditions

	if !syncEligible(clusterObj) || !active {
		stopped := c.stopEngine(cacheObjID)
		// Record SyncStopped only for a user-facing pause/ineligibility (sync
		// disabled, cluster disabled, context departed) — not for a migration
		// prune of a superseded cache (!active), which is an internal hand-over —
		// and only on the running→stopped transition (stopped).
		if stopped && !syncEligible(clusterObj) {
			c.recordSyncStopped(context.Background(), cacheObjID)
		}
		SetCondition(conds, Condition{
			Type: ConditionSynced, Status: ConditionFalse,
			Reason: ReasonPaused, ObservedGeneration: gen,
		})
		return 0
	}

	contextName := clusterObj.Spec.Source.Kubeconfig.Context
	var restCfg *rest.Config
	if c.connMgr != nil {
		restCfg = c.connMgr.Get(clusterID)
	}
	if restCfg == nil {
		var err error
		restCfg, err = ResolveRESTConfig(c.cfgSource.Get(), contextName)
		if err != nil {
			c.stopEngine(cacheObjID)
			SetCondition(conds, Condition{
				Type: ConditionSynced, Status: ConditionFalse,
				Reason: ReasonSyncFailed, Message: err.Error(), ObservedGeneration: gen,
			})
			return syncRecheckInterval
		}
	}
	fingerprint := engine.ConfigFingerprint(restCfg, engine.ContextProxyURL(c.cfgSource.Get(), contextName))

	c.mu.Lock()
	entry, running := c.engines[cacheObjID]
	c.mu.Unlock()

	if running && entry.fingerprint == fingerprint {
		return syncRecheckInterval
	}

	// Stop any running engine before starting a new one (credential change).
	c.stopEngine(cacheObjID)
	SetCondition(conds, Condition{
		Type: ConditionSynced, Status: ConditionFalse,
		Reason: ReasonSyncing, ObservedGeneration: gen,
	})
	c.spawnEngine(clusterID, restCfg, fingerprint, clusterObj.ID, cacheObjID, cacheGen, gen)
	return syncRecheckInterval
}

// spawnEngine constructs, registers, and starts a sync engine for clusterID.
// The caller is responsible for stopping any prior engine first and for holding
// writeMu (so it serializes with Reconcile and the sink). A nil engine (cache
// open failure) leaves no entry registered.
func (c *ClusterCacheController) spawnEngine(
	clusterID ClusterID,
	restCfg *rest.Config,
	fingerprint string,
	clusterObjID, cacheObjID beehive.ObjectID,
	cacheGen, parentGen int64,
) {
	newEntry := &engineEntry{
		restCfg:      restCfg,
		fingerprint:  fingerprint,
		clusterObjID: clusterObjID,
		cacheObjID:   cacheObjID,
		cacheGen:     cacheGen,
		parentGen:    parentGen,
	}
	sink := &engineSink{c: c, clusterID: clusterID, cacheID: cacheObjID, entry: newEntry}
	ref := newCacheRef(clusterObjID, cacheObjID)
	handle := c.newEngine(restCfg, clusterID, ref, sink)
	if handle == nil {
		return
	}
	newEntry.handle = handle
	c.mu.Lock()
	c.engines[cacheObjID] = newEntry
	c.mu.Unlock()
	handle.Start()
}

// restartLiveEngines stops and respawns every running engine, reusing each
// engine's stored connection config. Driven by the poke bus on OS resume /
// network-on: a restart drops watch streams that may have gone stale while the
// process was frozen, and each driver re-resumes from its persisted
// resourceVersion (cheap — no full re-list unless the RV expired). It holds
// writeMu for the whole pass so it serializes with Reconcile and the engine
// sinks, exactly as a converge-driven engine swap does.
func (c *ClusterCacheController) restartLiveEngines() {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.mu.Lock()
	ids := make([]beehive.ObjectID, 0, len(c.engines))
	for cacheID := range c.engines {
		ids = append(ids, cacheID)
	}
	c.mu.Unlock()

	for _, cacheID := range ids {
		c.restartEngineLocked(cacheID)
	}
}

// RestartEngine stops and respawns the running engine(s) for clusterID, reusing
// the stored config. Used by Service.ClearCache to rebuild the cache after deleting
// it on disk. A no-op when no engine is running for the cluster. A cluster has at
// most one active (engine-running) cache, but this restarts every live engine owned
// by it, so a clear during a migration hand-over rebuilds whichever is running.
func (c *ClusterCacheController) RestartEngine(clusterID ClusterID) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.Lock()
	var cacheIDs []beehive.ObjectID
	for cacheID, entry := range c.engines {
		if ClusterID(entry.clusterObjID) == clusterID {
			cacheIDs = append(cacheIDs, cacheID)
		}
	}
	c.mu.Unlock()
	for _, cacheID := range cacheIDs {
		c.restartEngineLocked(cacheID)
	}
}

// restartEngineLocked stops the engine for cacheID and respawns it with the same
// config and bookkeeping. Caller must hold writeMu.
func (c *ClusterCacheController) restartEngineLocked(cacheID beehive.ObjectID) {
	c.mu.Lock()
	entry, ok := c.engines[cacheID]
	c.mu.Unlock()
	if !ok {
		return
	}
	clusterID := ClusterID(entry.clusterObjID)
	restCfg, fingerprint := entry.restCfg, entry.fingerprint
	clusterObjID, cacheObjID := entry.clusterObjID, entry.cacheObjID
	cacheGen, parentGen := entry.cacheGen, entry.parentGen
	c.stopEngine(cacheID)
	c.spawnEngine(clusterID, restCfg, fingerprint, clusterObjID, cacheObjID, cacheGen, parentGen)
}

// stopEngine tears down a cache's engine if one is running, keyed by the
// ClusterCache ObjectID. Returns whether an engine was actually running — a
// running→stopped transition — so a caller can record a one-shot SyncStopped
// event without re-recording it on every reconcile of an already-stopped cache.
func (c *ClusterCacheController) stopEngine(cacheID beehive.ObjectID) bool {
	c.mu.Lock()
	entry, ok := c.engines[cacheID]
	delete(c.engines, cacheID)
	c.mu.Unlock()
	if !ok {
		return false
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), engineStopTimeout)
	defer cancel()
	if err := entry.handle.Stop(stopCtx); err != nil {
		slog.Warn("clustercachecontroller: engine stop", "cluster", entry.clusterObjID, "cache", cacheID, "err", err)
	}
	return true
}

// recordSyncStopped records the terminal SyncStopped event when a running engine
// is torn down because the cluster became sync-ineligible (paused/disabled/
// departed). Recorded directly rather than through recordSyncEvent's per-entry
// transition dedup: the caller invokes it only on an actual running→stopped
// transition (stopEngine returned true), so it's already once-per-stop.
// Best-effort — a write failure is logged and swallowed. Must hold writeMu.
func (c *ClusterCacheController) recordSyncStopped(ctx context.Context, cacheID beehive.ObjectID) {
	err := c.ctrlClient.RecordEvent(ctx, cacheID, beehive.EventSpec{
		Category: SyncEventCategory,
		Type:     beehive.EventNormal,
		Reason:   ReasonSyncStopped,
		Message:  "Sync stopped",
	})
	if err != nil && ctx.Err() == nil {
		slog.Warn("clustercachecontroller: record sync stopped", "cache", cacheID, "err", err)
	}
}

// syncEligible reports whether a cluster should have a running sync engine.
func syncEligible(obj *beehive.Object[ClusterSpec, ClusterStatus]) bool {
	return ConnectionEligible(obj) && obj.Spec.SyncEnabled
}

// engineSink delivers one engine's status reports into the ClusterCacheStatus
// via the controller's ControllerClient. It holds the entry pointer (keyed in the
// map by the ClusterCache ObjectID) so reports from a stopped or replaced engine
// are silently dropped; clusterID is kept only for log labels.
type engineSink struct {
	c         *ClusterCacheController
	clusterID ClusterID
	cacheID   beehive.ObjectID
	entry     *engineEntry
}

func (s *engineSink) Report(st engine.EngineStatus) {
	s.c.mu.Lock()
	current := s.c.engines[s.cacheID] == s.entry
	s.c.mu.Unlock()
	if !current {
		return
	}

	ctx := context.Background()
	s.c.writeMu.Lock()
	defer s.c.writeMu.Unlock()

	// Re-check under writeMu to avoid a race between the engine lock release
	// above and acquiring writeMu.
	s.c.mu.Lock()
	current = s.c.engines[s.cacheID] == s.entry
	s.c.mu.Unlock()
	if !current {
		return
	}

	if err := s.c.applyEngineReport(ctx, s.entry, st); err != nil {
		slog.Warn("clustercachecontroller: fold engine report", "cluster", s.clusterID, "cache", s.cacheID, "err", err)
	}
}

// applyEngineReport performs the read-modify-write for one engine status report.
// Must be called with writeMu held.
func (c *ClusterCacheController) applyEngineReport(ctx context.Context, entry *engineEntry, st engine.EngineStatus) error {
	cond := syncedCondition(st, entry.parentGen)
	lastSyncedAt := st.LastSyncedAt

	c.recordSyncEvent(ctx, entry, st)

	status := ClusterCacheStatus{
		Conditions:   []Condition{cond},
		LastSyncedAt: lastSyncedAt,
	}
	return c.ctrlClient.UpdateStatus(ctx, entry.cacheObjID, entry.cacheGen, status)
}

// recordSyncEvent appends one engine status report to the ClusterCache's beehive
// event log (category SyncEventCategory) — the sync-side parallel of the core
// controller's recordAttempt. It records only on a *transition*: a report whose
// (type, reason) matches the last recorded one is dropped. The engine re-reports
// its current state on a coarse (~30s) freshness heartbeat, so recording every
// report would grow the steady-state Watching run into a meaningless "Watching
// ×27" — the heartbeat carries no new information, only a fresh LastSyncedAt,
// which lands on the status write instead. A genuine flap still reads as a
// compact transition timeline. Best-effort — a write failure is logged and
// swallowed (and does not advance the last-recorded state, so the next report
// retries) so it neither disturbs the status write nor the engine. Must hold
// writeMu (which the sink path does; entry's last-recorded fields are mutated
// here under it).
func (c *ClusterCacheController) recordSyncEvent(ctx context.Context, entry *engineEntry, st engine.EngineStatus) {
	typ, reason, message := syncEvent(st)
	if entry.lastSyncReason == reason {
		return
	}
	err := c.ctrlClient.RecordEvent(ctx, entry.cacheObjID, beehive.EventSpec{
		Category: SyncEventCategory,
		Type:     typ,
		Reason:   reason,
		Message:  truncateMessage(message),
	})
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("clustercachecontroller: record sync event", "cache", entry.cacheObjID, "reason", reason, "err", err)
		}
		return
	}
	entry.lastSyncReason = reason
}

// syncEvent maps one engine status report onto a beehive event's (type, reason,
// message) — the transition vocabulary the event log speaks (distinct from
// syncedCondition, which names the current state). Every phase splits on
// ColdStart into a symmetric start/complete pair: an in-progress Syncing report
// is SyncStart (cold) or ResyncStart (warm), and the caught-up milestone is
// SyncComplete (cold) or ResyncComplete (warm) — each carrying a "…N objects
// across M kinds…" message. A failure is SyncDegraded (the engine's error) and a
// wedged watch is SyncStale.
func syncEvent(st engine.EngineStatus) (beehive.EventType, string, string) {
	switch st.State {
	case engine.EngineWatching:
		if st.ColdStart {
			return beehive.EventNormal, ReasonSyncComplete, syncCompleteMessage(st)
		}
		return beehive.EventNormal, ReasonResyncComplete, resyncCompleteMessage(st)
	case engine.EngineErrored:
		return beehive.EventWarning, ReasonSyncDegraded, st.LastError
	case engine.EngineStale:
		return beehive.EventWarning, ReasonSyncStale, staleMessage(st)
	default: // EngineSyncing
		if st.ColdStart {
			return beehive.EventNormal, ReasonSyncStart, "Starting initial sync"
		}
		return beehive.EventNormal, ReasonResyncStart, resyncStartMessage(st)
	}
}

// syncCompleteMessage describes a finished cold build: what it cached and how
// long it took.
func syncCompleteMessage(st engine.EngineStatus) string {
	return fmt.Sprintf("Initial sync complete — cached %d objects across %d kinds in %s",
		st.SyncedObjects, st.SyncedKinds, roundSyncDuration(st.CaughtUpIn))
}

// resyncStartMessage describes a warm resume as it begins: the size of the cache
// it's resuming from. The per-kind resume cookies aren't a single
// resource-version (one cache holds ~one cookie per kind), so the resume point
// is described qualitatively rather than as a bogus single number.
func resyncStartMessage(st engine.EngineStatus) string {
	return fmt.Sprintf("Starting re-sync from warm cache — %d objects across %d kinds, resuming watches from saved positions",
		st.SyncedObjects, st.SyncedKinds)
}

// resyncCompleteMessage describes a finished warm resume. The message carries the
// disambiguation that keeps ResyncComplete from being overloaded: a bare liveness
// recovery (a stale watch resuming) caught up zero kinds — the engine's
// livenessMonitor clears the catch-up facts, whereas a real catch-up always
// reports the discovered-kind count (≥ 1) — so SyncedKinds == 0 uniquely marks a
// recovery, and it's described as such rather than a misleading "0 objects".
// (SyncedKinds is the stable discriminator here; the object count is not, since a
// real resume of an empty cluster can legitimately re-pull zero objects.)
//
// Crucially it does NOT report the cache's object total: a resume re-establishes
// each kind's watch from its saved position and does not re-fetch the objects
// (they stream in as deltas afterward), so a "N objects … in 0s" line would
// misread as "processed N objects instantly". The honest completion fact is how
// many watches resumed and how long that took — near-zero on a clean reconnect,
// real seconds when an expired resume cookie forces a re-list. When some kinds DID
// have to re-list, it names the bodies actually re-pulled (the real delta, scoped
// to that work — not the whole-cache total) and how many kinds did it.
func resyncCompleteMessage(st engine.EngineStatus) string {
	if st.SyncedKinds == 0 {
		return "Re-sync complete — watch recovered, streaming updates again"
	}
	// Most resumes just reopen every watch from its saved position (no bodies
	// re-fetched). When some kinds couldn't — an expired or missing resume cookie
	// forced a full re-list — name that real work: how many bodies were re-pulled
	// and how many of the kinds did it.
	if st.ResyncedKinds > 0 {
		return fmt.Sprintf("Re-sync complete — resumed watches for %d kinds — re-synced %d objects in %d of them — in %s",
			st.SyncedKinds, st.ResyncedObjects, st.ResyncedKinds, roundSyncDuration(st.CaughtUpIn))
	}
	return fmt.Sprintf("Re-sync complete — resumed watches for %d kinds in %s",
		st.SyncedKinds, roundSyncDuration(st.CaughtUpIn))
}

// roundSyncDuration rounds a catch-up duration to a tenth of a second so the
// event message stays stable and readable (e.g. "in 4.2s").
func roundSyncDuration(d time.Duration) time.Duration {
	return d.Round(100 * time.Millisecond)
}

// staleMessage names the kinds whose watch went quiet, for the SyncStale event.
func staleMessage(st engine.EngineStatus) string {
	if len(st.StaleKinds) == 0 {
		return "Watch stopped delivering updates — cache may be behind"
	}
	return fmt.Sprintf("No watch heartbeat for %s — cache may be behind", strings.Join(st.StaleKinds, ", "))
}

// syncedCondition maps one engine status report onto the Synced condition.
func syncedCondition(st engine.EngineStatus, gen int64) Condition {
	cond := Condition{Type: ConditionSynced, ObservedGeneration: gen}
	switch st.State {
	case engine.EngineWatching:
		cond.Status, cond.Reason = ConditionTrue, ReasonWatching
	case engine.EngineErrored:
		cond.Status, cond.Reason, cond.Message = ConditionFalse, ReasonSyncFailed, st.LastError
	case engine.EngineStale:
		cond.Status, cond.Reason, cond.Message = ConditionFalse, ReasonStale, staleMessage(st)
	default:
		cond.Status, cond.Reason = ConditionFalse, ReasonSyncing
	}
	return cond
}
