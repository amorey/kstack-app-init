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
	"log/slog"
	"slices"
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

// NewEngineFunc constructs one sync engine. Production uses a engine.NewEngine
// wrapper; tests inject a factory that returns fake handles.
type NewEngineFunc func(cfg *rest.Config, clusterID ClusterID, sink engine.Sink) EngineHandle

// engineEntry is the controller's runtime state for one running engine. The
// pointer guards the sink: reports from a stopped or replaced engine are dropped
// by comparing pointer identity.
type engineEntry struct {
	handle EngineHandle
	// restCfg is the connection config the engine was started with, kept so a
	// poke-driven restart can respawn the engine without re-resolving it.
	restCfg     *rest.Config
	fingerprint string
	// cacheObjID is the beehive ObjectID of the ClusterCache object this engine
	// reports into. Stored so the sink can call UpdateStatus without a lookup.
	cacheObjID beehive.ObjectID
	// cacheGen is the ClusterCache object's own generation, used as the
	// observedGeneration when the sink calls UpdateStatus. It must be this
	// object's generation (not the parent's) or beehive rejects the write as a
	// future generation.
	cacheGen int64
	// parentGen is the parent Cluster's generation stamp recorded in the Synced
	// condition's ObservedGeneration (the condition observes the parent's spec).
	parentGen int64
}

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
	engines map[ClusterID]*engineEntry
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
		engines:      make(map[ClusterID]*engineEntry),
	}
	cm := manager
	c.newEngine = func(cfg *rest.Config, id ClusterID, sink engine.Sink) EngineHandle {
		cdb, err := cm.Open(context.Background(), string(id))
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
	c.engines = make(map[ClusterID]*engineEntry)
	c.mu.Unlock()
	for id, entry := range entries {
		stopCtx, cancel := context.WithTimeout(context.Background(), engineStopTimeout)
		if err := entry.handle.Stop(stopCtx); err != nil {
			slog.Warn("clustercachecontroller: engine stop", "cluster", id, "err", err)
		}
		cancel()
	}
	return nil
}

// Reconcile converges one ClusterCache object toward its parent Cluster's spec.
func (c *ClusterCacheController) Reconcile(ctx context.Context, client beehive.ControllerClient[ClusterCacheStatus], obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) (beehive.Result, error) {
	if obj.Slug == nil {
		return beehive.Result{}, errors.New("clustercachecontroller: object has no slug")
	}
	clusterID := ClusterIDFromSlug(*obj.Slug)

	// Read the parent Cluster to determine eligibility.
	clusterObj, err := c.coreClient.GetBySlug(ctx, ClusterSlug(clusterID))
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
	if fresh, err := c.coreClient.GetBySlug(ctx, ClusterSlug(clusterID)); err == nil {
		clusterObj = fresh
	}

	if obj.DeletionRequestedAt != nil {
		c.stopEngine(clusterID)
		return beehive.Result{}, nil
	}

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

	requeueAfter := c.converge(clusterID, obj.ID, obj.Generation, clusterObj, &working)

	if ClusterCacheStatusEqual(loaded, working) {
		return beehive.Result{RequeueAfter: requeueAfter}, nil
	}
	return beehive.Result{RequeueAfter: requeueAfter},
		client.UpdateStatus(ctx, obj.ID, obj.Generation, working)
}

// converge manages the sync engine toward the parent Cluster's spec.
func (c *ClusterCacheController) converge(
	clusterID ClusterID,
	cacheObjID beehive.ObjectID,
	cacheGen int64,
	clusterObj *beehive.Object[ClusterSpec, ClusterStatus],
	working *ClusterCacheStatus,
) time.Duration {
	gen := clusterObj.Generation
	conds := &working.Conditions

	if !syncEligible(clusterObj) {
		c.stopEngine(clusterID)
		SetCondition(conds, ClusterCondition{
			Type: ClusterConditionSynced, Status: ConditionFalse,
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
			c.stopEngine(clusterID)
			SetCondition(conds, ClusterCondition{
				Type: ClusterConditionSynced, Status: ConditionFalse,
				Reason: ReasonSyncFailed, Message: err.Error(), ObservedGeneration: gen,
			})
			return syncRecheckInterval
		}
	}
	fingerprint := engine.ConfigFingerprint(restCfg, engine.ContextProxyURL(c.cfgSource.Get(), contextName))

	c.mu.Lock()
	entry, running := c.engines[clusterID]
	c.mu.Unlock()

	if running && entry.fingerprint == fingerprint {
		return syncRecheckInterval
	}

	// Stop any running engine before starting a new one (credential change).
	c.stopEngine(clusterID)
	SetCondition(conds, ClusterCondition{
		Type: ClusterConditionSynced, Status: ConditionFalse,
		Reason: ReasonSyncing, ObservedGeneration: gen,
	})
	c.spawnEngine(clusterID, restCfg, fingerprint, cacheObjID, cacheGen, gen)
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
	cacheObjID beehive.ObjectID,
	cacheGen, parentGen int64,
) {
	newEntry := &engineEntry{
		restCfg:     restCfg,
		fingerprint: fingerprint,
		cacheObjID:  cacheObjID,
		cacheGen:    cacheGen,
		parentGen:   parentGen,
	}
	sink := &engineSink{c: c, id: clusterID, entry: newEntry}
	handle := c.newEngine(restCfg, clusterID, sink)
	if handle == nil {
		return
	}
	newEntry.handle = handle
	c.mu.Lock()
	c.engines[clusterID] = newEntry
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
	ids := make([]ClusterID, 0, len(c.engines))
	for id := range c.engines {
		ids = append(ids, id)
	}
	c.mu.Unlock()

	for _, id := range ids {
		c.restartEngineLocked(id)
	}
}

// RestartEngine stops and respawns the engine for id (if one is running),
// reusing its stored config. Used by Service.ClearCache to rebuild the cache
// after deleting it on disk. A no-op when no engine is running for id.
func (c *ClusterCacheController) RestartEngine(id ClusterID) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.restartEngineLocked(id)
}

// restartEngineLocked stops id's engine and respawns it with the same config and
// bookkeeping. Caller must hold writeMu.
func (c *ClusterCacheController) restartEngineLocked(id ClusterID) {
	c.mu.Lock()
	entry, ok := c.engines[id]
	c.mu.Unlock()
	if !ok {
		return
	}
	restCfg, fingerprint := entry.restCfg, entry.fingerprint
	cacheObjID, cacheGen, parentGen := entry.cacheObjID, entry.cacheGen, entry.parentGen
	c.stopEngine(id)
	c.spawnEngine(id, restCfg, fingerprint, cacheObjID, cacheGen, parentGen)
}

// stopEngine tears down a cluster's engine if one is running.
func (c *ClusterCacheController) stopEngine(id ClusterID) {
	c.mu.Lock()
	entry, ok := c.engines[id]
	delete(c.engines, id)
	c.mu.Unlock()
	if !ok {
		return
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), engineStopTimeout)
	defer cancel()
	if err := entry.handle.Stop(stopCtx); err != nil {
		slog.Warn("clustercachecontroller: engine stop", "cluster", id, "err", err)
	}
}

// syncEligible reports whether a cluster should have a running sync engine.
func syncEligible(obj *beehive.Object[ClusterSpec, ClusterStatus]) bool {
	return ConnectionEligible(obj) && obj.Spec.SyncEnabled
}

// engineSink delivers one engine's status reports into the ClusterCacheStatus
// via the controller's ControllerClient. It holds the entry pointer so reports
// from a stopped or replaced engine are silently dropped.
type engineSink struct {
	c     *ClusterCacheController
	id    ClusterID
	entry *engineEntry
}

func (s *engineSink) Report(st engine.EngineStatus) {
	s.c.mu.Lock()
	current := s.c.engines[s.id] == s.entry
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
	current = s.c.engines[s.id] == s.entry
	s.c.mu.Unlock()
	if !current {
		return
	}

	if err := s.c.applyEngineReport(ctx, s.entry, st); err != nil {
		slog.Warn("clustercachecontroller: fold engine report", "cluster", s.id, "err", err)
	}
}

// applyEngineReport performs the read-modify-write for one engine status report.
// Must be called with writeMu held.
func (c *ClusterCacheController) applyEngineReport(ctx context.Context, entry *engineEntry, st engine.EngineStatus) error {
	cond := syncedCondition(st, entry.parentGen)
	lastSyncedAt := st.LastSyncedAt

	status := ClusterCacheStatus{
		Conditions:   []ClusterCondition{cond},
		LastSyncedAt: lastSyncedAt,
	}
	return c.ctrlClient.UpdateStatus(ctx, entry.cacheObjID, entry.cacheGen, status)
}

// syncedCondition maps one engine status report onto the Synced condition.
func syncedCondition(st engine.EngineStatus, gen int64) ClusterCondition {
	cond := ClusterCondition{Type: ClusterConditionSynced, ObservedGeneration: gen}
	switch st.State {
	case engine.EngineWatching:
		cond.Status, cond.Reason = ConditionTrue, ReasonWatching
	case engine.EngineErrored:
		cond.Status, cond.Reason, cond.Message = ConditionFalse, ReasonSyncFailed, st.LastError
	default:
		cond.Status, cond.Reason = ConditionFalse, ReasonSyncing
	}
	return cond
}
