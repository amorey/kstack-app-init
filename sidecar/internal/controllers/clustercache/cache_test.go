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

package clustercache_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers/clustercache"
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers/clustercache/store"
	cachesync "github.com/kubetail-org/kstack-app/sidecar/internal/controllers/clustercache/sync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers/testutil"
)

// fakeEngine is a test sync engine that records Start/Stop calls.
type fakeEngine struct {
	started bool
	stopped bool
	sink    cachesync.Sink
}

func (f *fakeEngine) Start() {
	f.started = true
	// Report asynchronously — Start is called while the controller holds writeMu,
	// and Report acquires writeMu too, so a synchronous call would deadlock.
	go f.sink.Report(cachesync.EngineStatus{State: cachesync.EngineWatching})
}

func (f *fakeEngine) Stop(_ context.Context) error {
	f.stopped = true
	return nil
}

// newCacheTestBeehive builds a beehive with the real ClusterCacheController
// using a fake engine factory plus NoopControllers for the other kinds.
// Returns the clients and the factory's engine slot (populated on first call).
func newCacheTestBeehive(t *testing.T) (
	beehive.Client[controllers.ClusterSpec, controllers.ClusterConnectionStatus],
	beehive.Client[controllers.ClusterCacheSpec, controllers.ClusterCacheStatus],
	*fakeEngine,
) {
	t.Helper()
	bh := testutil.NewTestBeehiveUnstarted(t)

	clusterClient := beehive.NewClient[controllers.ClusterSpec, controllers.ClusterConnectionStatus](bh, controllers.ClusterGroupKind)
	cacheClient := beehive.NewClient[controllers.ClusterCacheSpec, controllers.ClusterCacheStatus](bh, controllers.ClusterCacheGroupKind)

	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	mgr := store.NewManager(t.TempDir())

	ctrl := clustercache.NewClusterCacheController(w, clusterClient, mgr)
	engine := &fakeEngine{}
	ctrl.SetNewEngine(func(cfg *rest.Config, id controllers.ClusterID, sink cachesync.Sink) clustercache.EngineHandle {
		engine.sink = sink
		return engine
	})

	require.NoError(t, beehive.Register(bh, controllers.ClusterSourceGroupKind, &testutil.NoopController[controllers.ClusterSourceSpec, controllers.ClusterSourceObjStatus]{}))
	require.NoError(t, beehive.Register(bh, controllers.ClusterGroupKind, &testutil.NoopController[controllers.ClusterSpec, controllers.ClusterConnectionStatus]{}))
	require.NoError(t, beehive.Register(bh, controllers.ClusterCacheGroupKind, ctrl))
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	return clusterClient, cacheClient, engine
}

// waitCacheCondition polls until the ClusterCache object has the Synced condition.
func waitCacheCondition(t *testing.T, cl beehive.Client[controllers.ClusterCacheSpec, controllers.ClusterCacheStatus], id beehive.ObjectID) *beehive.Object[controllers.ClusterCacheSpec, controllers.ClusterCacheStatus] {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		obj, err := cl.Get(context.Background(), id)
		require.NoError(t, err)
		if obj.Status != nil {
			for _, c := range obj.Status.Conditions {
				if c.Type == controllers.ClusterConditionSynced {
					return obj
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for Synced condition on ClusterCache")
	return nil
}

func eligibleClusterSpec(contextName string) controllers.ClusterSpec {
	return controllers.ClusterSpec{
		IsActive:      true,
		IsSyncEnabled: true,
		Source: controllers.ClusterSource{
			Kubeconfig: &controllers.ClusterSourceKubeconfig{Context: contextName},
		},
		SourceObs: &controllers.KubeconfigStatus{
			Cluster:   contextName + "-cluster",
			User:      contextName + "-user",
			IsPresent: true,
		},
	}
}

func TestCacheControllerEligibleClusterStartsEngine(t *testing.T) {
	ctx := context.Background()
	clusterClient, cacheClient, engine := newCacheTestBeehive(t)

	id := controllers.ClusterID("abc-uuid")

	// Create parent Cluster.
	clusterObj, err := clusterClient.Create(ctx, eligibleClusterSpec("alpha"),
		beehive.WithSlug(controllers.ClusterSlug(id)))
	require.NoError(t, err)

	// Create ClusterCache child.
	cacheObj, err := cacheClient.Create(ctx, controllers.ClusterCacheSpec{},
		beehive.WithSlug(controllers.ClusterCacheSlug(id)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	got := waitCacheCondition(t, cacheClient, cacheObj.ID)
	synced := findCacheCondition(t, got.Status.Conditions, controllers.ClusterConditionSynced)
	assert.Equal(t, controllers.ConditionTrue, synced.Status,
		"engine started and reported Watching → Synced=True")
	assert.True(t, engine.started)
}

func TestCacheControllerIneligibleClusterStopsEngine(t *testing.T) {
	ctx := context.Background()
	clusterClient, cacheClient, _ := newCacheTestBeehive(t)

	id := controllers.ClusterID("paused-uuid")

	// IsSyncEnabled=false → ineligible for sync.
	spec := eligibleClusterSpec("alpha")
	spec.IsSyncEnabled = false
	clusterObj, err := clusterClient.Create(ctx, spec,
		beehive.WithSlug(controllers.ClusterSlug(id)))
	require.NoError(t, err)

	cacheObj, err := cacheClient.Create(ctx, controllers.ClusterCacheSpec{},
		beehive.WithSlug(controllers.ClusterCacheSlug(id)),
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	got := waitCacheCondition(t, cacheClient, cacheObj.ID)
	synced := findCacheCondition(t, got.Status.Conditions, controllers.ClusterConditionSynced)
	assert.Equal(t, controllers.ConditionFalse, synced.Status)
	assert.Equal(t, controllers.ReasonPaused, synced.Reason)
}

// TestCacheControllerReportWithParentGenerationAhead reproduces the engine-sink
// generation skew: the parent Cluster's generation runs ahead of the ClusterCache
// object's own generation (e.g. after spec edits or poke bumps). The sink must
// stamp UpdateStatus with the cache object's own generation, not the parent's, or
// beehive rejects the write as a future generation and the report is dropped.
func TestCacheControllerReportWithParentGenerationAhead(t *testing.T) {
	ctx := context.Background()
	clusterClient, cacheClient, engine := newCacheTestBeehive(t)

	id := controllers.ClusterID("gen-skew-uuid")

	// Create the parent Cluster, then advance its generation past 1 by editing
	// its spec, before the ClusterCache child exists.
	spec := eligibleClusterSpec("alpha")
	clusterObj, err := clusterClient.Create(ctx, spec, beehive.WithSlug(controllers.ClusterSlug(id)))
	require.NoError(t, err)
	for _, name := range []string{"rename-1", "rename-2"} {
		n := name
		spec.Name = &n
		clusterObj, err = clusterClient.Update(ctx, clusterObj.ID, spec)
		require.NoError(t, err)
	}
	require.Greater(t, clusterObj.Generation, int64(1),
		"parent generation must be ahead of the cache object's gen 1")

	cacheObj, err := cacheClient.Create(ctx, controllers.ClusterCacheSpec{},
		beehive.WithSlug(controllers.ClusterCacheSlug(id)),
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
			synced := findCacheConditionOK(obj.Status.Conditions, controllers.ClusterConditionSynced)
			if synced != nil && synced.Status == controllers.ConditionTrue {
				break
			}
		}
		if !time.Now().Before(deadline) {
			t.Fatal("timed out waiting for Synced=True from engine report")
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(t, engine.started)
}

func findCacheConditionOK(conds []controllers.ClusterCondition, typ controllers.ClusterConditionType) *controllers.ClusterCondition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}
	return nil
}

func findCacheCondition(t *testing.T, conds []controllers.ClusterCondition, typ controllers.ClusterConditionType) controllers.ClusterCondition {
	t.Helper()
	for _, c := range conds {
		if c.Type == typ {
			return c
		}
	}
	t.Fatalf("condition %s not found", typ)
	return controllers.ClusterCondition{}
}
