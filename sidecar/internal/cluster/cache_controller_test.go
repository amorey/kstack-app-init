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
)

// fakeEngine is a test sync engine that records Start/Stop calls.
type fakeEngine struct {
	started bool
	stopped bool
	sink    engine.Sink
}

func (f *fakeEngine) Start() {
	f.started = true
	// Report asynchronously — Start is called while the controller holds writeMu,
	// and Report acquires writeMu too, so a synchronous call would deadlock.
	go f.sink.Report(engine.EngineStatus{State: engine.EngineWatching})
}

func (f *fakeEngine) Stop(_ context.Context) error {
	f.stopped = true
	return nil
}

// newCacheTestBeehive builds a beehive with the real ClusterCacheController
// using a fake engine factory plus NoopControllers for the other kinds.
// Returns the clients, the factory's engine slot (populated on first call),
// and a pointer to a slot that holds the REST config passed to the engine factory.
func newCacheTestBeehive(t *testing.T, connMgr *cluster.ConnectionManager) (
	beehive.Client[cluster.ClusterSpec, cluster.ClusterConnectionStatus],
	beehive.Client[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus],
	*fakeEngine,
	*capturedCfgSlot,
) {
	t.Helper()
	bh := testutil.NewTestBeehiveUnstarted(t)

	clusterClient := beehive.NewClient[cluster.ClusterSpec, cluster.ClusterConnectionStatus](bh, cluster.ClusterGroupKind)
	cacheClient := beehive.NewClient[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus](bh, cluster.ClusterCacheGroupKind)

	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	mgr := store.NewManager(t.TempDir())

	ctrl := cluster.NewClusterCacheController(w, clusterClient, mgr, connMgr)
	fakeEng := &fakeEngine{}
	slot := &capturedCfgSlot{}
	ctrl.SetNewEngine(func(cfg *rest.Config, id cluster.ClusterID, sink engine.Sink) cluster.EngineHandle {
		slot.cfg = cfg
		fakeEng.sink = sink
		return fakeEng
	})

	require.NoError(t, beehive.Register(bh, cluster.ClusterGroupKind, &testutil.NoopController[cluster.ClusterSpec, cluster.ClusterConnectionStatus]{}))
	require.NoError(t, beehive.Register(bh, cluster.ClusterCacheGroupKind, ctrl))
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	return clusterClient, cacheClient, fakeEng, slot
}

// capturedCfgSlot holds the REST config that was passed to the engine factory.
type capturedCfgSlot struct{ cfg *rest.Config }

// waitCacheCondition polls until the ClusterCache object has the Synced condition.
func waitCacheCondition(t *testing.T, cl beehive.Client[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus], id beehive.ObjectID) *beehive.Object[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus] {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		obj, err := cl.Get(context.Background(), id)
		require.NoError(t, err)
		if obj.Status != nil {
			for _, c := range obj.Status.Conditions {
				if c.Type == cluster.ClusterConditionSynced {
					return obj
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for Synced condition on ClusterCache")
	return nil
}

func eligibleClusterSpec(contextName string) cluster.ClusterSpec {
	return cluster.ClusterSpec{
		IsActive:      true,
		IsSyncEnabled: true,
		Source: cluster.ClusterSource{
			Kubeconfig: &cluster.ClusterSourceKubeconfig{Context: contextName},
		},
		SourceObs: &cluster.KubeconfigStatus{
			Cluster:   contextName + "-cluster",
			User:      contextName + "-user",
			IsPresent: true,
		},
	}
}

func TestCacheControllerEligibleClusterStartsEngine(t *testing.T) {
	ctx := context.Background()
	clusterClient, cacheClient, fakeEng, _ := newCacheTestBeehive(t, nil)

	id := cluster.ClusterID("abc-uuid")

	// Create parent Cluster.
	clusterObj, err := clusterClient.Create(ctx, eligibleClusterSpec("alpha"),
		beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)

	// Create ClusterCache child.
	cacheObj, err := cacheClient.Create(ctx, cluster.ClusterCacheSpec{},
		beehive.WithSlug(cluster.ClusterCacheSlug(id)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	got := waitCacheCondition(t, cacheClient, cacheObj.ID)
	synced := findCacheCondition(t, got.Status.Conditions, cluster.ClusterConditionSynced)
	assert.Equal(t, cluster.ConditionTrue, synced.Status,
		"engine started and reported Watching → Synced=True")
	assert.True(t, fakeEng.started)
}

func TestCacheControllerIneligibleClusterStopsEngine(t *testing.T) {
	ctx := context.Background()
	clusterClient, cacheClient, _, _ := newCacheTestBeehive(t, nil)

	id := cluster.ClusterID("paused-uuid")

	// IsSyncEnabled=false → ineligible for sync.
	spec := eligibleClusterSpec("alpha")
	spec.IsSyncEnabled = false
	clusterObj, err := clusterClient.Create(ctx, spec,
		beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)

	cacheObj, err := cacheClient.Create(ctx, cluster.ClusterCacheSpec{},
		beehive.WithSlug(cluster.ClusterCacheSlug(id)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	got := waitCacheCondition(t, cacheClient, cacheObj.ID)
	synced := findCacheCondition(t, got.Status.Conditions, cluster.ClusterConditionSynced)
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
	clusterClient, cacheClient, fakeEng, _ := newCacheTestBeehive(t, nil)

	id := cluster.ClusterID("gen-skew-uuid")

	// Create the parent Cluster, then advance its generation past 1 by editing
	// its spec, before the ClusterCache child exists.
	spec := eligibleClusterSpec("alpha")
	clusterObj, err := clusterClient.Create(ctx, spec, beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)
	for _, name := range []string{"rename-1", "rename-2"} {
		n := name
		spec.Name = &n
		clusterObj, err = clusterClient.Update(ctx, clusterObj.ID, spec)
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
	deadline := time.Now().Add(2 * time.Second)
	for {
		obj, err := cacheClient.Get(ctx, cacheObj.ID)
		require.NoError(t, err)
		if obj.Status != nil {
			synced := findCacheConditionOK(obj.Status.Conditions, cluster.ClusterConditionSynced)
			if synced != nil && synced.Status == cluster.ConditionTrue {
				break
			}
		}
		if !time.Now().Before(deadline) {
			t.Fatal("timed out waiting for Synced=True from engine report")
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(t, fakeEng.started)
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

	clusterClient, cacheClient, _, slot := newCacheTestBeehive(t, connMgr)

	clusterObj, err := clusterClient.Create(ctx, eligibleClusterSpec("alpha"),
		beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)
	cacheObj, err := cacheClient.Create(ctx, cluster.ClusterCacheSpec{},
		beehive.WithSlug(cluster.ClusterCacheSlug(id)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	waitCacheCondition(t, cacheClient, cacheObj.ID)

	assert.Equal(t, injected, slot.cfg,
		"engine must receive the REST config from ConnectionManager, not a freshly resolved one")
}

// TestCacheControllerFallsBackToKubeconfigWhenNoConnectionManager verifies that
// the cache controller still works when no ConnectionManager is provided.
func TestCacheControllerFallsBackToKubeconfigWhenNoConnectionManager(t *testing.T) {
	ctx := context.Background()
	clusterClient, cacheClient, fakeEng, _ := newCacheTestBeehive(t, nil)

	id := cluster.ClusterID("fallback-uuid")
	clusterObj, err := clusterClient.Create(ctx, eligibleClusterSpec("alpha"),
		beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)
	cacheObj, err := cacheClient.Create(ctx, cluster.ClusterCacheSpec{},
		beehive.WithSlug(cluster.ClusterCacheSlug(id)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	got := waitCacheCondition(t, cacheClient, cacheObj.ID)
	synced := findCacheCondition(t, got.Status.Conditions, cluster.ClusterConditionSynced)
	assert.Equal(t, cluster.ConditionTrue, synced.Status)
	assert.True(t, fakeEng.started)
}

func findCacheConditionOK(conds []cluster.ClusterCondition, typ cluster.ClusterConditionType) *cluster.ClusterCondition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}
	return nil
}

func findCacheCondition(t *testing.T, conds []cluster.ClusterCondition, typ cluster.ClusterConditionType) cluster.ClusterCondition {
	t.Helper()
	for _, c := range conds {
		if c.Type == typ {
			return c
		}
	}
	t.Fatalf("condition %s not found", typ)
	return cluster.ClusterCondition{}
}
