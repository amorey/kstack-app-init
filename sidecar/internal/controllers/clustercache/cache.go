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

package clustercache

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"k8s.io/client-go/rest"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers/clustercache/store"
	cachesync "github.com/kubetail-org/kstack-app/sidecar/internal/controllers/clustercache/sync"
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

// NewEngineFunc constructs one sync engine. Production uses a cachesync.NewEngine
// wrapper; tests inject a factory that returns fake handles.
type NewEngineFunc func(cfg *rest.Config, clusterID controllers.ClusterID, sink cachesync.Sink) EngineHandle

// engineEntry is the controller's runtime state for one running engine. The
// pointer guards the sink: reports from a stopped or replaced engine are dropped
// by comparing pointer identity.
type engineEntry struct {
	handle      EngineHandle
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
// eligibility (connection-eligible + IsSyncEnabled), and detects poke signals
// via the DependsOn edge (ClusterCache depends on Cluster): when
// PokeSyncGeneration in ClusterSpec increases, beehive re-queues this cache,
// and the controller bounces the running engine.
type ClusterCacheController struct {
	cfgSource     controllers.KubeConfigSource
	clusterClient beehive.Client[controllers.ClusterSpec, controllers.ClusterConnectionStatus]
	cacheManager  *store.Manager
	ctrlClient    beehive.ControllerClient[controllers.ClusterCacheStatus]

	// newEngine constructs one sync engine. Overridable for tests.
	newEngine NewEngineFunc

	// writeMu serializes read-modify-write status updates from the reconcile
	// worker and from the engines' sink goroutines.
	writeMu sync.Mutex
	mu      sync.Mutex
	engines map[controllers.ClusterID]*engineEntry
}

// NewClusterCacheController builds the controller. manager owns the per-cluster
// SQLite cache files; it is shared with the resolver so both see the same open DBs.
func NewClusterCacheController(
	cfgSource controllers.KubeConfigSource,
	clusterClient beehive.Client[controllers.ClusterSpec, controllers.ClusterConnectionStatus],
	manager *store.Manager,
) *ClusterCacheController {
	c := &ClusterCacheController{
		cfgSource:     cfgSource,
		clusterClient: clusterClient,
		cacheManager:  manager,
		engines:       make(map[controllers.ClusterID]*engineEntry),
	}
	cm := manager
	c.newEngine = func(cfg *rest.Config, id controllers.ClusterID, sink cachesync.Sink) EngineHandle {
		cdb, err := cm.Open(context.Background(), string(id))
		if err != nil {
			slog.Warn("clustercachecontroller: open cache db", "cluster", id, "err", err)
			return nil
		}
		return cachesync.NewEngine(cfg, cdb, sink)
	}
	return c
}

// SetNewEngine replaces the engine factory — for tests.
func (c *ClusterCacheController) SetNewEngine(f NewEngineFunc) {
	c.newEngine = f
}

// Start stores the ControllerClient.
func (c *ClusterCacheController) Start(cl beehive.ControllerClient[controllers.ClusterCacheStatus]) error {
	c.ctrlClient = cl
	return nil
}

// Stop tears down every running engine.
func (c *ClusterCacheController) Stop(_ context.Context) error {
	c.mu.Lock()
	entries := c.engines
	c.engines = make(map[controllers.ClusterID]*engineEntry)
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
func (c *ClusterCacheController) Reconcile(ctx context.Context, obj *beehive.Object[controllers.ClusterCacheSpec, controllers.ClusterCacheStatus]) (beehive.Result, error) {
	if obj.Slug == nil {
		return beehive.Result{}, errors.New("clustercachecontroller: object has no slug")
	}
	clusterID := controllers.ClusterIDFromSlug(*obj.Slug)

	// Read the parent Cluster to determine eligibility.
	clusterObj, err := c.clusterClient.GetBySlug(ctx, controllers.ClusterSlug(clusterID))
	if err != nil {
		if errors.Is(err, beehive.ErrNotFound) {
			// Parent gone (GC race); our object will be cleaned up too.
			return beehive.Result{}, nil
		}
		return beehive.Result{}, err
	}

	// Add DependsOn edge so beehive re-queues us when the parent Cluster changes
	// (enabling poke signal detection and spec change propagation).
	if err := c.ctrlClient.AddDependency(ctx, obj.ID, clusterObj.ID); err != nil {
		return beehive.Result{}, err
	}

	if obj.DeletionRequestedAt != nil {
		c.stopEngine(clusterID)
		return beehive.Result{}, nil
	}

	// Load the working status copy.
	var loaded controllers.ClusterCacheStatus
	if obj.Status != nil {
		loaded = *obj.Status
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	working := controllers.ClusterCacheStatus{
		Conditions:                 slices.Clone(loaded.Conditions),
		LastSyncedAt:               loaded.LastSyncedAt,
		ObservedPokeSyncGeneration: loaded.ObservedPokeSyncGeneration,
	}

	requeueAfter := c.converge(ctx, clusterID, obj.ID, obj.Generation, clusterObj, &working)

	if controllers.ClusterCacheStatusEqual(loaded, working) {
		return beehive.Result{RequeueAfter: requeueAfter}, nil
	}
	return beehive.Result{RequeueAfter: requeueAfter},
		c.ctrlClient.UpdateStatus(ctx, obj.ID, obj.Generation, working)
}

// converge manages the sync engine toward the parent Cluster's spec.
func (c *ClusterCacheController) converge(
	ctx context.Context,
	clusterID controllers.ClusterID,
	cacheObjID beehive.ObjectID,
	cacheGen int64,
	clusterObj *beehive.Object[controllers.ClusterSpec, controllers.ClusterConnectionStatus],
	working *controllers.ClusterCacheStatus,
) time.Duration {
	gen := clusterObj.Generation
	conds := &working.Conditions

	if !syncEligible(clusterObj) {
		c.stopEngine(clusterID)
		controllers.SetCondition(conds, controllers.ClusterCondition{
			Type: controllers.ClusterConditionSynced, Status: controllers.ConditionFalse,
			Reason: controllers.ReasonPaused, ObservedGeneration: gen,
		})
		return 0
	}

	contextName := clusterObj.Spec.Source.Kubeconfig.Context
	restCfg, err := controllers.ResolveRESTConfig(c.cfgSource.Get(), contextName)
	if err != nil {
		c.stopEngine(clusterID)
		controllers.SetCondition(conds, controllers.ClusterCondition{
			Type: controllers.ClusterConditionSynced, Status: controllers.ConditionFalse,
			Reason: controllers.ReasonSyncFailed, Message: err.Error(), ObservedGeneration: gen,
		})
		return syncRecheckInterval
	}
	fingerprint := cachesync.ConfigFingerprint(restCfg, cachesync.ContextProxyURL(c.cfgSource.Get(), contextName))

	// Detect poke signals via PokeSyncGeneration.
	bounce := false
	c.mu.Lock()
	entry, running := c.engines[clusterID]
	if clusterObj.Spec.PokeSyncGeneration != working.ObservedPokeSyncGeneration {
		bounce = true
		working.ObservedPokeSyncGeneration = clusterObj.Spec.PokeSyncGeneration
	}
	c.mu.Unlock()

	if running && entry.fingerprint == fingerprint && !bounce {
		return syncRecheckInterval
	}

	// Stop any running engine before starting a new one.
	c.stopEngine(clusterID)
	controllers.SetCondition(conds, controllers.ClusterCondition{
		Type: controllers.ClusterConditionSynced, Status: controllers.ConditionFalse,
		Reason: controllers.ReasonSyncing, ObservedGeneration: gen,
	})

	newEntry := &engineEntry{
		fingerprint: fingerprint,
		cacheObjID:  cacheObjID,
		cacheGen:    cacheGen,
		parentGen:   gen,
	}
	sink := &engineSink{c: c, id: clusterID, entry: newEntry}
	handle := c.newEngine(restCfg, clusterID, sink)
	if handle == nil {
		return syncRecheckInterval
	}
	newEntry.handle = handle
	c.mu.Lock()
	c.engines[clusterID] = newEntry
	c.mu.Unlock()
	handle.Start()
	return syncRecheckInterval
}

// stopEngine tears down a cluster's engine if one is running.
func (c *ClusterCacheController) stopEngine(id controllers.ClusterID) {
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
func syncEligible(obj *beehive.Object[controllers.ClusterSpec, controllers.ClusterConnectionStatus]) bool {
	return controllers.ConnectionEligible(obj) && obj.Spec.IsSyncEnabled
}

// engineSink delivers one engine's status reports into the ClusterCacheStatus
// via the controller's ControllerClient. It holds the entry pointer so reports
// from a stopped or replaced engine are silently dropped.
type engineSink struct {
	c     *ClusterCacheController
	id    controllers.ClusterID
	entry *engineEntry
}

func (s *engineSink) Report(st cachesync.EngineStatus) {
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
func (c *ClusterCacheController) applyEngineReport(ctx context.Context, entry *engineEntry, st cachesync.EngineStatus) error {
	cond := syncedCondition(st, entry.parentGen)
	lastSyncedAt := st.LastSyncedAt

	// Build status from the engine report. ObservedPokeSyncGeneration is reset to
	// zero here; the next Reconcile restores it. This is acceptable because the
	// poke bounce uses the Reconcile loop, not the sink.
	status := controllers.ClusterCacheStatus{
		Conditions:   []controllers.ClusterCondition{cond},
		LastSyncedAt: lastSyncedAt,
	}
	return c.ctrlClient.UpdateStatus(ctx, entry.cacheObjID, entry.cacheGen, status)
}

// syncedCondition maps one engine status report onto the Synced condition.
func syncedCondition(st cachesync.EngineStatus, gen int64) controllers.ClusterCondition {
	cond := controllers.ClusterCondition{Type: controllers.ClusterConditionSynced, ObservedGeneration: gen}
	switch st.State {
	case cachesync.EngineWatching:
		cond.Status, cond.Reason = controllers.ConditionTrue, controllers.ReasonWatching
	case cachesync.EngineErrored:
		cond.Status, cond.Reason, cond.Message = controllers.ConditionFalse, controllers.ReasonSyncFailed, st.LastError
	default:
		cond.Status, cond.Reason = controllers.ConditionFalse, controllers.ReasonSyncing
	}
	return cond
}
