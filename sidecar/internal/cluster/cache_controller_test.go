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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/engine"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// fakeEngine is a test sync engine that records Start/Stop calls. The flags are
// atomic because the controller sets them from its own goroutines while tests
// read them.
type fakeEngine struct {
	started atomic.Bool
	stopped atomic.Bool
	sink    engine.Sink
}

func (f *fakeEngine) Start() {
	f.started.Store(true)
	// Report asynchronously — Start is called while the controller holds writeMu,
	// and Report acquires writeMu too, so a synchronous call would deadlock. Model
	// a realistic cold first sync (ColdStart + catch-up counts) so the recorded
	// event is InitialSyncComplete, not a bare Watching.
	go f.sink.Report(engine.EngineStatus{
		State: engine.EngineWatching, ColdStart: true,
		SyncedObjects: 5, SyncedKinds: 3, CaughtUpIn: 2 * time.Second,
	})
}

func (f *fakeEngine) Stop(_ context.Context) error {
	f.stopped.Store(true)
	return nil
}

// newCacheTestBeehive builds a beehive with the real ClusterCacheController
// using a fake engine factory plus NoopControllers for the other kinds.
// Returns the clients, the factory's engine slot (populated on first call),
// and a pointer to a slot that holds the REST config passed to the engine factory.
func newCacheTestBeehive(t *testing.T, connMgr *ConnectionManager) (
	beehive.Client[ClusterSpec, ClusterStatus],
	beehive.Client[ClusterCacheSpec, ClusterCacheStatus],
	*fakeEngine,
	*capturedCfgSlot,
) {
	t.Helper()
	bh := NewTestBeehiveUnstarted(t)

	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)

	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	mgr := store.NewManager(t.TempDir())

	ctrl := NewClusterCacheController(w, coreClient, mgr, connMgr, nil)
	fakeEng := &fakeEngine{}
	slot := &capturedCfgSlot{}
	ctrl.SetNewEngine(func(cfg *rest.Config, id ClusterID, ref store.CacheRef, sink engine.Sink) EngineHandle {
		slot.cfg = cfg
		slot.ref = ref
		fakeEng.sink = sink
		return fakeEng
	})

	_, err := beehive.Register(bh, ClusterGroupKind, &presenceController{})
	require.NoError(t, err)
	cc, err := beehive.Register(bh, ClusterCacheGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()); _ = ctrl.StopEngines() })

	return coreClient, cacheClient, fakeEng, slot
}

// capturedCfgSlot holds the REST config and cache ref passed to the engine factory.
type capturedCfgSlot struct {
	cfg *rest.Config
	ref store.CacheRef
}

// awaitCacheSyncedStatus blocks on the ClusterCache object's beehive watch until
// its Synced condition reaches the wanted status, then returns that condition.
// beehive's Watch is current-on-subscribe (a snapshot Added event, then live
// Modified events), so this is fully event-driven — no polling.
//
// Waiting for a specific status matters because converge commits a transient
// Synced=Syncing (ConditionFalse) synchronously, then the engine's async
// Watching report flips it to ConditionTrue; a test that wants the settled value
// must wait for it, not the first write, or it races the async report.
func awaitCacheSyncedStatus(t *testing.T, cl beehive.Client[ClusterCacheSpec, ClusterCacheStatus], id beehive.ObjectID, want ConditionStatus) ClusterCondition {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := cl.Watch(ctx, id)
	require.NoError(t, err)

	timeout := time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("watch closed before Synced=%s on ClusterCache", want)
			}
			if ev.Object == nil || ev.Object.Status == nil {
				continue
			}
			if c := findCacheConditionOK(ev.Object.Status.Conditions, ClusterConditionSynced); c != nil && c.Status == want {
				return *c
			}
		case <-timeout:
			t.Fatalf("timed out waiting for Synced=%s on ClusterCache", want)
		}
	}
}

func eligibleClusterSpec(contextName string) ClusterSpec {
	return ClusterSpec{
		Enabled:     true,
		SyncEnabled: true,
		Source: ClusterSpecSource{
			Kubeconfig: &ClusterSpecSourceKubeconfig{Context: contextName},
		},
	}
}

// testCacheUID is the kube-system UID the cache tests' parent Cluster reports as
// its connected identity. A ClusterCache is the parent's *active* cache (and so
// runs an engine) only when its spec UID matches the parent's Status.Server.UID,
// so the tests create their cache with this UID and presenceController stamps it.
const testCacheUID = "kube-system-uid"

// presenceController is the test stand-in for the Cluster kind's controller in
// the cache tests. The cache controller gates on the parent's *observed* presence
// (ClusterStatus.Source.Kubeconfig.IsPresent) AND on the cache being the active
// identity (its UID == ClusterStatus.Server.UID) — both written by the real
// ClusterCoreController after a probe. This minimal controller stamps both (and the
// status write wakes the ClusterCache dependent, exercising the real trigger path)
// without the probing machinery.
type presenceController struct{}

func (presenceController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterStatus],
	obj *beehive.Object[ClusterSpec, ClusterStatus],
) (beehive.Result, error) {
	kc := obj.Spec.Source.Kubeconfig
	if kc == nil {
		return beehive.Result{}, nil
	}
	wantSrc := ClusterStatusSourceKubeconfig{
		Cluster:   kc.Context + "-cluster",
		User:      kc.Context + "-user",
		IsPresent: true,
	}
	uid := testCacheUID
	if obj.Status != nil && obj.Status.Source.Kubeconfig != nil &&
		*obj.Status.Source.Kubeconfig == wantSrc &&
		obj.Status.Server.UID != nil && *obj.Status.Server.UID == uid {
		return beehive.Result{}, nil // already stamped: no rewrite
	}
	status := ClusterStatus{
		Source: ClusterStatusSource{Kubeconfig: &wantSrc},
		Server: ClusterServer{UID: &uid},
	}
	return beehive.Result{}, client.UpdateStatus(ctx, obj.ID, obj.Generation, status)
}

func TestCacheControllerEligibleClusterStartsEngine(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, fakeEng, _ := newCacheTestBeehive(t, nil)

	// Create parent Cluster.
	clusterObj, err := coreClient.Create(ctx, eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := ClusterID(clusterObj.ID)

	// Create ClusterCache child.
	cacheObj, err := cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithSlug(ClusterCacheSlug(id, testCacheUID)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	synced := awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, ConditionTrue)
	assert.Equal(t, ConditionTrue, synced.Status,
		"engine started and reported Watching → Synced=True")
	assert.True(t, fakeEng.started.Load())
}

// TestCacheControllerDeletionStopsEngineAndClearsFinalizer verifies the cache
// teardown path used by a UID-switch prune (and a cluster-delete cascade): when a
// ClusterCache carrying the file-cleanup finalizer is deleted, the controller stops
// its engine, flushes the on-disk file, then clears the finalizer so GC collects the
// row. Without clearing the finalizer the deletion-pending row would linger forever.
func TestCacheControllerDeletionStopsEngineAndClearsFinalizer(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, fakeEng, _ := newCacheTestBeehive(t, nil)

	clusterObj, err := coreClient.Create(ctx, eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := ClusterID(clusterObj.ID)

	// Create the cache exactly as ensureClusterCache does in production: owned,
	// slugged, and carrying the file-cleanup finalizer (the literal must match the
	// package's cacheFilesFinalizer — see the core controller test, which pins it
	// against the production create path).
	cacheObj, err := cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithSlug(ClusterCacheSlug(id, testCacheUID)),
		beehive.WithOwner(clusterObj.ID),
		beehive.WithFinalizers("kstack.io/cache-files"))
	require.NoError(t, err)

	// Let the engine spin up so its stop is observable.
	awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, ConditionTrue)
	require.True(t, fakeEng.started.Load())

	// Delete → controller flushes files + clears the finalizer → GC removes the row.
	require.NoError(t, cacheClient.Delete(ctx, cacheObj.ID))

	require.Eventually(t, func() bool {
		_, err := cacheClient.Get(ctx, cacheObj.ID)
		return errors.Is(err, beehive.ErrNotFound)
	}, 2*time.Second, 10*time.Millisecond, "cache row must be GC'd once its finalizer is cleared")
	assert.True(t, fakeEng.stopped.Load(), "engine must be stopped on deletion")
}

// TestCacheControllerRecordsSyncEvents verifies the cache controller records each
// engine status report into the ClusterCache's beehive event log under the "sync"
// category — the parallel of the connection controller's probe-outcome history,
// exposed generically to the frontend. Watching → Normal/Watching, Errored →
// Warning/SyncFailed (carrying the engine's LastError), Syncing → Normal/Syncing.
// It records only on a *transition* — a change in (type, reason) — so the
// engine's steady-state freshness heartbeat (a repeated catch-up report that only
// bumps LastSyncedAt) does NOT open or extend an event run. This is what keeps a
// healthy cluster from reading as a meaningless "Watching ×27"; the heartbeat
// still lands on LastSyncedAt via the status write. The reasons are the
// transition vocabulary: a cold catch-up is InitialSyncComplete (with a "Cached N
// objects across M kinds" message), an engine failure is SyncDegraded.
func TestCacheControllerRecordsSyncEvents(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, fakeEng, _ := newCacheTestBeehive(t, nil)

	clusterObj, err := coreClient.Create(ctx, eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := ClusterID(clusterObj.ID)

	cacheObj, err := cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithSlug(ClusterCacheSlug(id, testCacheUID)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	// Sync-point on the engine's first (async) catch-up report, so the reports
	// below run strictly after it. Once the entry is live, sink.Report is
	// synchronous from this goroutine (it just takes writeMu), so no further
	// awaits are needed to observe each recorded event.
	awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, ConditionTrue)

	// A second catch-up report (the engine's freshness heartbeat, only bumping
	// LastSyncedAt) is NOT a transition — it must not open/extend an event run.
	now := time.Now()
	fakeEng.sink.Report(engine.EngineStatus{
		State: engine.EngineWatching, ColdStart: true,
		SyncedObjects: 5, SyncedKinds: 3, CaughtUpIn: 2 * time.Second, LastSyncedAt: &now,
	})
	// Transition to Errored, then to a (warm) Syncing.
	fakeEng.sink.Report(engine.EngineStatus{State: engine.EngineErrored, LastError: "boom"})
	fakeEng.sink.Report(engine.EngineStatus{State: engine.EngineSyncing})

	evs, err := cacheClient.ListEvents(ctx, cacheObj.ID, beehive.WithEventCategory(SyncEventCategory))
	require.NoError(t, err)
	require.Len(t, evs, 3, "one run per transition (the repeated catch-up heartbeat is not recorded)")

	// ListEvents is newest-run-first.
	assert.Equal(t, beehive.EventNormal, evs[0].Type)
	assert.Equal(t, ReasonSyncing, evs[0].Reason)

	assert.Equal(t, beehive.EventWarning, evs[1].Type)
	assert.Equal(t, ReasonSyncDegraded, evs[1].Reason)
	assert.Equal(t, "boom", evs[1].Message)

	assert.Equal(t, beehive.EventNormal, evs[2].Type)
	assert.Equal(t, ReasonInitialSyncComplete, evs[2].Reason)
	assert.Equal(t, 1, evs[2].Count,
		"the steady-state catch-up heartbeat is not recorded, so the run stays count 1")
	assert.Equal(t, "Cached 5 objects across 3 kinds in 2s", evs[2].Message)
}

// A cold Syncing report reads as SyncStarted; a warm catch-up (an already-
// populated cache resuming) reads as Resynced, with a "Re-synced …" message — the
// pair that distinguishes a first-ever build from a reconnect.
func TestCacheControllerSyncEventVocabulary(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, fakeEng, _ := newCacheTestBeehive(t, nil)

	clusterObj, err := coreClient.Create(ctx, eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := ClusterID(clusterObj.ID)
	cacheObj, err := cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithSlug(ClusterCacheSlug(id, testCacheUID)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	// Wait for the initial cold catch-up (InitialSyncComplete) so the entry is live.
	awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, ConditionTrue)

	fakeEng.sink.Report(engine.EngineStatus{State: engine.EngineSyncing, ColdStart: true})
	fakeEng.sink.Report(engine.EngineStatus{
		State: engine.EngineWatching, ColdStart: false,
		SyncedObjects: 7, SyncedKinds: 4, CaughtUpIn: 1500 * time.Millisecond,
	})

	evs, err := cacheClient.ListEvents(ctx, cacheObj.ID, beehive.WithEventCategory(SyncEventCategory))
	require.NoError(t, err)
	require.Len(t, evs, 3) // newest-first: Resynced, SyncStarted, InitialSyncComplete

	assert.Equal(t, ReasonResynced, evs[0].Reason)
	assert.Equal(t, "Re-synced 7 objects across 4 kinds in 1.5s", evs[0].Message)
	assert.Equal(t, ReasonSyncStarted, evs[1].Reason)
	assert.Equal(t, ReasonInitialSyncComplete, evs[2].Reason)
}

// An EngineStale report flips the Synced condition to False/Stale and records a
// SyncStale warning naming the wedged kinds — a stalled watch surfaced as its own
// state, distinct from a hard SyncFailed.
func TestCacheControllerStaleReport(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, fakeEng, _ := newCacheTestBeehive(t, nil)

	clusterObj, err := coreClient.Create(ctx, eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := ClusterID(clusterObj.ID)
	cacheObj, err := cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithSlug(ClusterCacheSlug(id, testCacheUID)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	// Live (cold catch-up), then the liveness monitor reports the watch stale.
	awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, ConditionTrue)
	fakeEng.sink.Report(engine.EngineStatus{State: engine.EngineStale, StaleKinds: []string{"Pod", "Endpoints"}})

	synced := awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, ConditionFalse)
	assert.Equal(t, ReasonStale, synced.Reason)

	evs, err := cacheClient.ListEvents(ctx, cacheObj.ID, beehive.WithEventCategory(SyncEventCategory))
	require.NoError(t, err)
	require.NotEmpty(t, evs)
	assert.Equal(t, ReasonSyncStale, evs[0].Reason)
	assert.Equal(t, beehive.EventWarning, evs[0].Type)
	assert.Contains(t, evs[0].Message, "Pod")
}

// Pausing sync (SyncEnabled → false) stops the running engine and records a
// SyncStopped event — but only on the actual running→stopped transition and only
// for a user-facing pause (not a migration prune or a restart).
func TestCacheControllerRecordsSyncStopped(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, _, _ := newCacheTestBeehive(t, nil)

	spec := eligibleClusterSpec("alpha")
	clusterObj, err := coreClient.Create(ctx, spec)
	require.NoError(t, err)
	id := ClusterID(clusterObj.ID)
	cacheObj, err := cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithSlug(ClusterCacheSlug(id, testCacheUID)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	// Engine running (cold catch-up).
	awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, ConditionTrue)

	// Pause sync → the cache re-reconciles (DependsOn the parent), stops the
	// engine, and records SyncStopped before flipping the condition to Paused.
	spec.SyncEnabled = false
	_, err = coreClient.Update(ctx, clusterObj.ID, spec)
	require.NoError(t, err)
	synced := awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, ConditionFalse)
	require.Equal(t, ReasonPaused, synced.Reason)

	evs, err := cacheClient.ListEvents(ctx, cacheObj.ID, beehive.WithEventCategory(SyncEventCategory))
	require.NoError(t, err)
	require.NotEmpty(t, evs)
	assert.Equal(t, ReasonSyncStopped, evs[0].Reason, "the newest sync event is SyncStopped")
	assert.Equal(t, beehive.EventNormal, evs[0].Type)
}

func TestCacheControllerIneligibleClusterStopsEngine(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, _, _ := newCacheTestBeehive(t, nil)

	// SyncEnabled=false → ineligible for sync.
	spec := eligibleClusterSpec("alpha")
	spec.SyncEnabled = false
	clusterObj, err := coreClient.Create(ctx, spec)
	require.NoError(t, err)
	id := ClusterID(clusterObj.ID)

	cacheObj, err := cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithSlug(ClusterCacheSlug(id, testCacheUID)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	synced := awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, ConditionFalse)
	assert.Equal(t, ConditionFalse, synced.Status)
	assert.Equal(t, ReasonPaused, synced.Reason)
}

// TestCacheControllerReportWithParentGenerationAhead reproduces the engine-sink
// generation skew: the parent Cluster's generation runs ahead of the ClusterCache
// object's own generation (e.g. after spec edits or poke bumps). The sink must
// stamp UpdateStatus with the cache object's own generation, not the parent's, or
// beehive rejects the write as a future generation and the report is dropped.
func TestCacheControllerReportWithParentGenerationAhead(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, fakeEng, _ := newCacheTestBeehive(t, nil)

	// Create the parent Cluster, then advance its generation past 1 by editing
	// its spec, before the ClusterCache child exists.
	spec := eligibleClusterSpec("alpha")
	clusterObj, err := coreClient.Create(ctx, spec)
	require.NoError(t, err)
	id := ClusterID(clusterObj.ID)
	for _, name := range []string{"rename-1", "rename-2"} {
		n := name
		spec.Name = &n
		clusterObj, err = coreClient.Update(ctx, clusterObj.ID, spec)
		require.NoError(t, err)
	}
	require.Greater(t, clusterObj.Generation, int64(1),
		"parent generation must be ahead of the cache object's gen 1")

	cacheObj, err := cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithSlug(ClusterCacheSlug(id, testCacheUID)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	// The async engine report must land as Synced=True. With the parent's
	// generation wrongly used as observedGeneration this write is rejected and
	// the condition never flips past the synchronous Syncing state.
	awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, ConditionTrue)
	assert.True(t, fakeEng.started.Load())
}

// TestCacheControllerUsesConnectionManagerConfig verifies that when a
// ConnectionManager holds a REST config for a cluster, the cache controller
// passes that config (not a freshly resolved one) to the engine factory.
func TestCacheControllerUsesConnectionManagerConfig(t *testing.T) {
	ctx := context.Background()
	connMgr := NewConnectionManager()
	injected := &rest.Config{Host: "https://from-conn-mgr:6443"}

	coreClient, cacheClient, _, slot := newCacheTestBeehive(t, connMgr)

	clusterObj, err := coreClient.Create(ctx, eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := ClusterID(clusterObj.ID)
	connMgr.Set(id, injected)
	cacheObj, err := cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithSlug(ClusterCacheSlug(id, testCacheUID)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, ConditionTrue)

	assert.Equal(t, injected, slot.cfg,
		"engine must receive the REST config from ConnectionManager, not a freshly resolved one")
	assert.Equal(t, store.CacheRef{ClusterID: int64(clusterObj.ID), CacheID: int64(cacheObj.ID)}, slot.ref,
		"engine cache ref must be the parent Cluster + ClusterCache beehive ObjectIDs")
}

// TestCacheControllerFallsBackToKubeconfigWhenNoConnectionManager verifies that
// the cache controller still works when no ConnectionManager is provided.
func TestCacheControllerFallsBackToKubeconfigWhenNoConnectionManager(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, fakeEng, _ := newCacheTestBeehive(t, nil)

	clusterObj, err := coreClient.Create(ctx, eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := ClusterID(clusterObj.ID)
	cacheObj, err := cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithSlug(ClusterCacheSlug(id, testCacheUID)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	synced := awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, ConditionTrue)
	assert.Equal(t, ConditionTrue, synced.Status)
	assert.True(t, fakeEng.started.Load())
}

// TestCacheControllerPokeRestartsLiveEngine verifies the controller subscribes
// to the poke bus and, on a signal, stops each live engine and starts a fresh
// one (so stale watch streams are dropped and re-resumed).
func TestCacheControllerPokeRestartsLiveEngine(t *testing.T) {
	ctx := context.Background()
	pk := poke.New()

	bh := NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	mgr := store.NewManager(t.TempDir())

	ctrl := NewClusterCacheController(w, coreClient, mgr, nil, pk)

	// Factory records every engine it builds, so the test can see the restart
	// (old engine stopped, a second engine created and started).
	var mu sync.Mutex
	var created []*fakeEngine
	ctrl.SetNewEngine(func(_ *rest.Config, _ ClusterID, _ store.CacheRef, sink engine.Sink) EngineHandle {
		e := &fakeEngine{sink: sink}
		mu.Lock()
		created = append(created, e)
		mu.Unlock()
		return e
	})

	_, err := beehive.Register(bh, ClusterGroupKind, &presenceController{})
	require.NoError(t, err)
	cc, err := beehive.Register(bh, ClusterCacheGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	ctrl.StartPoke()
	t.Cleanup(func() { ctrl.StopPoke(); _ = stop(ctx); _ = ctrl.StopEngines() })

	clusterObj, err := coreClient.Create(ctx, eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := ClusterID(clusterObj.ID)
	_, err = cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithSlug(ClusterCacheSlug(id, testCacheUID)), beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	// The first engine starts for the eligible
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(created) == 1 && created[0].started.Load()
	}, 2*time.Second, 10*time.Millisecond, "engine should start for eligible cluster")

	// Poke → the live engine is stopped and a fresh one started.
	pk.Poke(poke.SourceHost)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(created) == 2 && created[0].stopped.Load() && created[1].started.Load()
	}, 2*time.Second, 10*time.Millisecond, "poke should restart the live engine")
}

func findCacheConditionOK(conds []ClusterCondition, typ ClusterConditionType) *ClusterCondition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}
	return nil
}
