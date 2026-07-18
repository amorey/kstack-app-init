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
// kept for log labels. Tests inject a factory returning fake handles.
type NewEngineFunc func(cfg *rest.Config, clusterID ClusterID, ref store.CacheRef, sink engine.Sink) EngineHandle

// engineEntry is the controller's runtime state for one running engine. The
// pointer guards the sink: reports from a stopped or replaced engine are dropped
// by comparing pointer identity.
type engineEntry struct {
	handle EngineHandle
	// restCfg is the config the engine was started with, kept so a poke-driven
	// restart can respawn without re-resolving it.
	restCfg     *rest.Config
	fingerprint string
	// clusterObjID is the parent Cluster's ObjectID; it names the on-disk cache
	// directory (clusters/<clusterObjID>/).
	clusterObjID beehive.ObjectID
	// cacheObjID is this ClusterCache's ObjectID; it names the on-disk cache file
	// (<cacheObjID>.db) and lets the sink call UpdateStatus without a lookup.
	cacheObjID beehive.ObjectID
	// cacheGen is the ClusterCache object's own generation, used as the
	// observedGeneration in UpdateStatus — must be this object's, not the parent's,
	// or beehive rejects the write as a future generation.
	cacheGen int64
	// parentGen is the parent Cluster's generation, recorded in the Synced
	// condition's ObservedGeneration (the condition observes the parent's spec).
	parentGen int64

	// lastSyncReason is the reason of the most recent sync event recorded for this
	// engine ("" before the first). recordSyncEvent records only on a change in it,
	// so the engine's steady-state freshness heartbeat (a repeated Watching report)
	// doesn't append a redundant "Watching ×N" run. Mutated only under writeMu.
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
// It reads the parent Cluster to determine eligibility (connection-eligible +
// SyncEnabled) and adds a DependsOn edge so beehive re-queues this cache when the
// parent's spec changes (e.g. SyncEnabled toggled).
//
// Resync pokes arrive out-of-band on the poke bus, not through beehive: the
// controller subscribes in Start and, on each signal, restarts its live engines
// in place (dropping stale watch streams; each driver re-resumes cheaply from its
// persisted resourceVersion) — no spec write needed, since the engines are
// in-memory state it already owns. See restartLiveEngines.
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
	// engines is keyed by the ClusterCache's own ObjectID, not its parent ClusterID:
	// a cluster can own several ClusterCache records and the controller reconciles
	// each independently. Only the active one (UID == parent's last-probed Server.UID)
	// runs an engine, but keying by cache id keeps a migration's old/new caches from
	// racing on a shared per-cluster slot during the hand-over.
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

// SetControllerClient injects the status-write client from beehive.Register. It
// backs the out-of-band engine sink (applyEngineReport), which writes status from
// engine goroutines; the reconcile path uses the client beehive passes into
// Reconcile. Call once, before the control plane starts — an engine spawned by a
// startup reconcile may report immediately.
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
	// row. The locator needs the owner's id (per-cluster dir) + this cache's id
	// (file); if the owner is already gone we can't form that path, so clean up the
	// engine and clear the finalizer best-effort rather than wedging GC forever. A
	// file-delete error returns without clearing the finalizer, so the next reconcile
	// retries — the file can't be orphaned.
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

	// Add the DependsOn edge so beehive re-queues us on parent changes — spec edits
	// (e.g. SyncEnabled toggled) AND status writes (the core controller's live source
	// observation, which drives presence-based eligibility). Then re-read the parent:
	// establishing the edge first closes the race where the parent is stamped between
	// our initial read and AddDependency, which would wake nothing and leave us stuck
	// on a stale 'not yet observed' view.
	if err := client.AddDependency(ctx, obj.ID, clusterObj.ID); err != nil {
		return beehive.Result{}, err
	}
	if fresh, err := c.coreClient.Get(ctx, beehive.ObjectID(clusterID)); err == nil {
		clusterObj = fresh
	}

	// This cache is "active" when its UID matches the parent's last-probed kube-system
	// UID. Only the active cache runs an engine — a cache left behind by a migration is
	// paused, so the engine never writes the new cluster's data into the old file.
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
// A no-op when the finalizer is absent (e.g. a double reconcile of the deletion).
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
		// Record SyncStopped only for a user-facing pause/ineligibility — not for a
		// migration prune of a superseded cache (!active), an internal hand-over — and
		// only on the running→stopped transition (stopped).
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
// engine's stored config. Driven by the poke bus on OS resume / network-on: the
// restart drops watch streams that may have gone stale while the process was
// frozen, and each driver re-resumes from its persisted resourceVersion (cheap
// unless the RV expired). Holds writeMu for the whole pass so it serializes with
// Reconcile and the engine sinks, like a converge-driven engine swap.
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
// it on disk; a no-op when no engine is running. A cluster has at most one active
// cache, but this restarts every live engine it owns, so a clear during a migration
// hand-over rebuilds whichever is running.
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
// is torn down because the cluster became sync-ineligible. Recorded directly rather
// than through recordSyncEvent's dedup — the caller invokes it only on an actual
// running→stopped transition. Best-effort; a write failure is logged. Must hold
// writeMu.
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

// engineSink delivers one engine's status reports into ClusterCacheStatus via the
// controller's ControllerClient. It holds the entry pointer so reports from a
// stopped or replaced engine are dropped; clusterID is kept only for log labels.
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
// event log — the sync-side parallel of recordAttempt. It records only on a
// transition (a change in (type, reason)); a report matching the last recorded one
// is dropped. The engine re-reports its state on a ~30s freshness heartbeat, so
// recording every report would grow a steady Watching run into a meaningless
// "Watching ×27" — the heartbeat's only new info is LastSyncedAt, which lands on
// the status write instead. Best-effort: a write failure is logged and does not
// advance the last-recorded state (so the next report retries). Must hold writeMu.
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
// message) — the event log's transition vocabulary, distinct from syncedCondition
// (which names the current state). Every phase splits on ColdStart into a
// start/complete pair: a Syncing report is SyncStart (cold) or ResyncStart (warm),
// and the caught-up milestone is SyncComplete (cold) or ResyncComplete (warm). A
// failure is SyncDegraded and a wedged watch is SyncStale.
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
// it's resuming from. The per-kind resume cookies aren't a single resource-version,
// so the resume point is described qualitatively, not as a bogus single number.
func resyncStartMessage(st engine.EngineStatus) string {
	return fmt.Sprintf("Starting re-sync from warm cache — %d objects across %d kinds, resuming watches from saved positions",
		st.SyncedObjects, st.SyncedKinds)
}

// resyncCompleteMessage describes a finished warm resume. SyncedKinds == 0 uniquely
// marks a bare liveness recovery (a stale watch resuming — the livenessMonitor
// clears the catch-up facts, whereas a real catch-up reports a kind count ≥ 1), so
// it reads as a recovery rather than a misleading "0 objects". The object count is
// not a valid discriminator, since a real resume of an empty cluster can re-pull
// zero objects.
//
// It deliberately does NOT report the cache's object total: a resume re-opens each
// kind's watch and does not re-fetch objects (they stream in as deltas), so an
// "N objects … in 0s" line would misread as "processed N objects instantly". The
// honest fact is how many watches resumed and how long — near-zero on a clean
// reconnect, real seconds when an expired resume cookie forces a re-list. When some
// kinds DID re-list, it names the bodies actually re-pulled (scoped to that work,
// not the whole-cache total) and how many kinds did it.
func resyncCompleteMessage(st engine.EngineStatus) string {
	if st.SyncedKinds == 0 {
		return "Re-sync complete — watch recovered, streaming updates again"
	}
	// Most resumes just reopen every watch (no bodies re-fetched). When some kinds
	// couldn't — an expired/missing resume cookie forced a re-list — name that work.
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
