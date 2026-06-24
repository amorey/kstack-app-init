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

package cluster_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/engine"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/testutil"
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
	// and Report acquires writeMu too, so a synchronous call would deadlock.
	go f.sink.Report(engine.EngineStatus{State: engine.EngineWatching})
}

func (f *fakeEngine) Stop(_ context.Context) error {
	f.stopped.Store(true)
	return nil
}

// newCacheTestBeehive builds a beehive with the real ClusterCacheController
// using a fake engine factory plus NoopControllers for the other kinds.
// Returns the clients, the factory's engine slot (populated on first call),
// and a pointer to a slot that holds the REST config passed to the engine factory.
func newCacheTestBeehive(t *testing.T, connMgr *cluster.ConnectionManager) (
	beehive.Client[cluster.ClusterSpec, cluster.ClusterStatus],
	beehive.Client[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus],
	*fakeEngine,
	*capturedCfgSlot,
) {
	t.Helper()
	bh := testutil.NewTestBeehiveUnstarted(t)

	coreClient := beehive.NewClient[cluster.ClusterSpec, cluster.ClusterStatus](bh, cluster.ClusterGroupKind)
	cacheClient := beehive.NewClient[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus](bh, cluster.ClusterCacheGroupKind)

	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	mgr := store.NewManager(t.TempDir())

	ctrl := cluster.NewClusterCacheController(w, coreClient, mgr, connMgr, nil)
	fakeEng := &fakeEngine{}
	slot := &capturedCfgSlot{}
	ctrl.SetNewEngine(func(cfg *rest.Config, id cluster.ClusterID, sink engine.Sink) cluster.EngineHandle {
		slot.cfg = cfg
		fakeEng.sink = sink
		return fakeEng
	})

	_, err := beehive.Register(bh, cluster.ClusterGroupKind, &testutil.NoopController[cluster.ClusterSpec, cluster.ClusterStatus]{})
	require.NoError(t, err)
	cc, err := beehive.Register(bh, cluster.ClusterCacheGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()); _ = ctrl.StopEngines() })

	return coreClient, cacheClient, fakeEng, slot
}

// capturedCfgSlot holds the REST config that was passed to the engine factory.
type capturedCfgSlot struct{ cfg *rest.Config }

// awaitCacheSyncedStatus blocks on the ClusterCache object's beehive watch until
// its Synced condition reaches the wanted status, then returns that condition.
// beehive's Watch is current-on-subscribe (a snapshot Added event, then live
// Modified events), so this is fully event-driven — no polling.
//
// Waiting for a specific status matters because converge commits a transient
// Synced=Syncing (ConditionFalse) synchronously, then the engine's async
// Watching report flips it to ConditionTrue; a test that wants the settled value
// must wait for it, not the first write, or it races the async report.
func awaitCacheSyncedStatus(t *testing.T, cl beehive.Client[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus], id beehive.ObjectID, want cluster.ConditionStatus) cluster.ClusterCondition {
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
			if c := findCacheConditionOK(ev.Object.Status.Conditions, cluster.ClusterConditionSynced); c != nil && c.Status == want {
				return *c
			}
		case <-timeout:
			t.Fatalf("timed out waiting for Synced=%s on ClusterCache", want)
		}
	}
}

func eligibleClusterSpec(contextName string) cluster.ClusterSpec {
	return cluster.ClusterSpec{
		Enabled:     true,
		SyncEnabled: true,
		Source: cluster.ClusterSource{
			Kubeconfig: &cluster.ClusterSourceKubeconfig{Context: contextName},
		},
		SourceObs: &cluster.ClusterKubeconfig{
			Cluster:   contextName + "-cluster",
			User:      contextName + "-user",
			IsPresent: true,
		},
	}
}

func TestCacheControllerEligibleClusterStartsEngine(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, fakeEng, _ := newCacheTestBeehive(t, nil)

	id := cluster.ClusterID("abc-uuid")

	// Create parent Cluster.
	clusterObj, err := coreClient.Create(ctx, eligibleClusterSpec("alpha"),
		beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)

	// Create ClusterCache child.
	cacheObj, err := cacheClient.Create(ctx, cluster.ClusterCacheSpec{},
		beehive.WithSlug(cluster.ClusterCacheSlug(id)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	synced := awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, cluster.ConditionTrue)
	assert.Equal(t, cluster.ConditionTrue, synced.Status,
		"engine started and reported Watching → Synced=True")
	assert.True(t, fakeEng.started.Load())
}

func TestCacheControllerIneligibleClusterStopsEngine(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, _, _ := newCacheTestBeehive(t, nil)

	id := cluster.ClusterID("paused-uuid")

	// SyncEnabled=false → ineligible for sync.
	spec := eligibleClusterSpec("alpha")
	spec.SyncEnabled = false
	clusterObj, err := coreClient.Create(ctx, spec,
		beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)

	cacheObj, err := cacheClient.Create(ctx, cluster.ClusterCacheSpec{},
		beehive.WithSlug(cluster.ClusterCacheSlug(id)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	synced := awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, cluster.ConditionFalse)
	assert.Equal(t, cluster.ConditionFalse, synced.Status)
	assert.Equal(t, cluster.ReasonPaused, synced.Reason)
}

// TestCacheControllerReportWithParentGenerationAhead reproduces the engine-sink
// generation skew: the parent Cluster's generation runs ahead of the ClusterCache
// object's own generation (e.g. after spec edits or poke bumps). The sink must
// stamp UpdateStatus with the cache object's own generation, not the parent's, or
// beehive rejects the write as a future generation and the report is dropped.
func TestCacheControllerReportWithParentGenerationAhead(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, fakeEng, _ := newCacheTestBeehive(t, nil)

	id := cluster.ClusterID("gen-skew-uuid")

	// Create the parent Cluster, then advance its generation past 1 by editing
	// its spec, before the ClusterCache child exists.
	spec := eligibleClusterSpec("alpha")
	clusterObj, err := coreClient.Create(ctx, spec, beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)
	for _, name := range []string{"rename-1", "rename-2"} {
		n := name
		spec.Name = &n
		clusterObj, err = coreClient.Update(ctx, clusterObj.ID, spec)
		require.NoError(t, err)
	}
	require.Greater(t, clusterObj.Generation, int64(1),
		"parent generation must be ahead of the cache object's gen 1")

	cacheObj, err := cacheClient.Create(ctx, cluster.ClusterCacheSpec{},
		beehive.WithSlug(cluster.ClusterCacheSlug(id)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	// The async engine report must land as Synced=True. With the parent's
	// generation wrongly used as observedGeneration this write is rejected and
	// the condition never flips past the synchronous Syncing state.
	awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, cluster.ConditionTrue)
	assert.True(t, fakeEng.started.Load())
}

// TestCacheControllerUsesConnectionManagerConfig verifies that when a
// ConnectionManager holds a REST config for a cluster, the cache controller
// passes that config (not a freshly resolved one) to the engine factory.
func TestCacheControllerUsesConnectionManagerConfig(t *testing.T) {
	ctx := context.Background()
	connMgr := cluster.NewConnectionManager()
	id := cluster.ClusterID("conn-cfg-uuid")
	injected := &rest.Config{Host: "https://from-conn-mgr:6443"}
	connMgr.Set(id, injected)

	coreClient, cacheClient, _, slot := newCacheTestBeehive(t, connMgr)

	clusterObj, err := coreClient.Create(ctx, eligibleClusterSpec("alpha"),
		beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)
	cacheObj, err := cacheClient.Create(ctx, cluster.ClusterCacheSpec{},
		beehive.WithSlug(cluster.ClusterCacheSlug(id)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, cluster.ConditionTrue)

	assert.Equal(t, injected, slot.cfg,
		"engine must receive the REST config from ConnectionManager, not a freshly resolved one")
}

// TestCacheControllerFallsBackToKubeconfigWhenNoConnectionManager verifies that
// the cache controller still works when no ConnectionManager is provided.
func TestCacheControllerFallsBackToKubeconfigWhenNoConnectionManager(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, fakeEng, _ := newCacheTestBeehive(t, nil)

	id := cluster.ClusterID("fallback-uuid")
	clusterObj, err := coreClient.Create(ctx, eligibleClusterSpec("alpha"),
		beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)
	cacheObj, err := cacheClient.Create(ctx, cluster.ClusterCacheSpec{},
		beehive.WithSlug(cluster.ClusterCacheSlug(id)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	synced := awaitCacheSyncedStatus(t, cacheClient, cacheObj.ID, cluster.ConditionTrue)
	assert.Equal(t, cluster.ConditionTrue, synced.Status)
	assert.True(t, fakeEng.started.Load())
}

// TestCacheControllerPokeRestartsLiveEngine verifies the controller subscribes
// to the poke bus and, on a signal, stops each live engine and starts a fresh
// one (so stale watch streams are dropped and re-resumed).
func TestCacheControllerPokeRestartsLiveEngine(t *testing.T) {
	ctx := context.Background()
	pk := poke.New()

	bh := testutil.NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[cluster.ClusterSpec, cluster.ClusterStatus](bh, cluster.ClusterGroupKind)
	cacheClient := beehive.NewClient[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus](bh, cluster.ClusterCacheGroupKind)
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	mgr := store.NewManager(t.TempDir())

	ctrl := cluster.NewClusterCacheController(w, coreClient, mgr, nil, pk)

	// Factory records every engine it builds, so the test can see the restart
	// (old engine stopped, a second engine created and started).
	var mu sync.Mutex
	var created []*fakeEngine
	ctrl.SetNewEngine(func(_ *rest.Config, _ cluster.ClusterID, sink engine.Sink) cluster.EngineHandle {
		e := &fakeEngine{sink: sink}
		mu.Lock()
		created = append(created, e)
		mu.Unlock()
		return e
	})

	_, err := beehive.Register(bh, cluster.ClusterGroupKind, &testutil.NoopController[cluster.ClusterSpec, cluster.ClusterStatus]{})
	require.NoError(t, err)
	cc, err := beehive.Register(bh, cluster.ClusterCacheGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	ctrl.StartPoke()
	t.Cleanup(func() { ctrl.StopPoke(); _ = stop(ctx); _ = ctrl.StopEngines() })

	id := cluster.ClusterID("poke-uuid")
	clusterObj, err := coreClient.Create(ctx, eligibleClusterSpec("alpha"),
		beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)
	_, err = cacheClient.Create(ctx, cluster.ClusterCacheSpec{},
		beehive.WithSlug(cluster.ClusterCacheSlug(id)), beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	// The first engine starts for the eligible cluster.
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

func findCacheConditionOK(conds []cluster.ClusterCondition, typ cluster.ClusterConditionType) *cluster.ClusterCondition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}
	return nil
}
