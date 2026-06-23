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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/testutil"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// staticProbe returns a ProbeFunc that always yields the given server/principal.
func staticProbe(server cluster.ClusterServer, principal cluster.ClusterPrincipal) cluster.ProbeFunc {
	return func(context.Context, *rest.Config) (cluster.ClusterServer, cluster.ClusterPrincipal, error) {
		return server, principal, nil
	}
}

// errProbe returns a ProbeFunc that always fails.
func errProbe(err error) cluster.ProbeFunc {
	return func(context.Context, *rest.Config) (cluster.ClusterServer, cluster.ClusterPrincipal, error) {
		return cluster.ClusterServer{}, cluster.ClusterPrincipal{}, err
	}
}

// staticCheck returns a CheckFunc that always yields phase.
func staticCheck(phase cluster.HealthPhase) cluster.CheckFunc {
	return func(context.Context, *rest.Config) (cluster.HealthPhase, *string) {
		return phase, nil
	}
}

// newClusterTestBeehive builds a beehive with the real ClusterController using
// the given probe/check fakes plus NoopControllers for the other kinds.
func newClusterTestBeehive(t *testing.T, w cluster.KubeConfigSource, probe cluster.ProbeFunc, check cluster.CheckFunc, connMgr *cluster.ConnectionManager) (beehive.Client[cluster.ClusterSpec, cluster.ClusterConnectionStatus], beehive.Client[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus]) {
	t.Helper()
	bh := testutil.NewTestBeehiveUnstarted(t)

	coreClient := beehive.NewClient[cluster.ClusterSpec, cluster.ClusterConnectionStatus](bh, cluster.ClusterGroupKind)
	cacheClient := beehive.NewClient[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus](bh, cluster.ClusterCacheGroupKind)

	ctrl := cluster.NewClusterController(w, coreClient, cacheClient, connMgr, nil, probe, check)
	cc, err := beehive.Register(bh, cluster.ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, cluster.ClusterCacheGroupKind, &testutil.NoopController[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	return coreClient, cacheClient
}

// waitCondition polls until the object has the named condition or the deadline.
func waitCondition(t *testing.T, cl beehive.Client[cluster.ClusterSpec, cluster.ClusterConnectionStatus], id beehive.ObjectID, condType cluster.ClusterConditionType) *beehive.Object[cluster.ClusterSpec, cluster.ClusterConnectionStatus] {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		obj, err := cl.Get(context.Background(), id)
		require.NoError(t, err)
		if obj.Status != nil {
			for _, c := range obj.Status.Conditions {
				if c.Type == condType {
					return obj
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for condition %s", condType)
	return nil
}

// eligibleSpec builds a Cluster spec that passes ConnectionEligible.
func eligibleSpec(contextName string) cluster.ClusterSpec {
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

func TestClusterControllerSuccessfulProbeWritesConditions(t *testing.T) {
	uid := "kube-system-uid"
	ver := "v1.31.0"
	user := "alice"
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))

	coreClient, cacheClient := newClusterTestBeehive(t, w,
		staticProbe(
			cluster.ClusterServer{UID: &uid, Version: &ver},
			cluster.ClusterPrincipal{Username: &user},
		),
		staticCheck(cluster.HealthPhaseHealthy),
		nil,
	)
	ctx := context.Background()

	id := cluster.ClusterID("test-uid")
	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"),
		beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)

	got := waitCondition(t, coreClient, obj.ID, cluster.ClusterConditionConnected)
	require.NotNil(t, got.Status)

	connected := findCondition(t, got.Status.Conditions, cluster.ClusterConditionConnected)
	assert.Equal(t, cluster.ConditionTrue, connected.Status)
	assert.Equal(t, cluster.ReasonConnected, connected.Reason)

	healthy := findCondition(t, got.Status.Conditions, cluster.ClusterConditionHealthy)
	assert.Equal(t, cluster.ConditionTrue, healthy.Status)

	assert.NotNil(t, got.Status.LastConnectedAt)

	// ClusterCache child must have been created.
	_, err = cacheClient.GetBySlug(ctx, cluster.ClusterCacheSlug(id))
	require.NoError(t, err, "ClusterCache child must exist after successful reconcile")
}

func TestClusterControllerProbeFailureSetsConnectedFalse(t *testing.T) {
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	coreClient, _ := newClusterTestBeehive(t, w,
		errProbe(errors.New("connection refused")),
		staticCheck(cluster.HealthPhaseHealthy),
		nil,
	)
	ctx := context.Background()

	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"),
		beehive.WithSlug(cluster.ClusterSlug("probe-fail-id")))
	require.NoError(t, err)

	got := waitCondition(t, coreClient, obj.ID, cluster.ClusterConditionConnected)
	connected := findCondition(t, got.Status.Conditions, cluster.ClusterConditionConnected)
	assert.Equal(t, cluster.ConditionFalse, connected.Status)
	assert.Equal(t, cluster.ReasonProbeFailed, connected.Reason)
}

func TestClusterControllerIneligibleClusterDoesNotProbe(t *testing.T) {
	probeCalled := false
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	probe := func(_ context.Context, _ *rest.Config) (cluster.ClusterServer, cluster.ClusterPrincipal, error) {
		probeCalled = true
		return cluster.ClusterServer{}, cluster.ClusterPrincipal{}, nil
	}
	coreClient, _ := newClusterTestBeehive(t, w, probe, staticCheck(cluster.HealthPhaseHealthy), nil)
	ctx := context.Background()

	// IsActive=false → ineligible.
	spec := eligibleSpec("alpha")
	spec.IsActive = false
	obj, err := coreClient.Create(ctx, spec,
		beehive.WithSlug(cluster.ClusterSlug("inactive-id")))
	require.NoError(t, err)

	got := waitCondition(t, coreClient, obj.ID, cluster.ClusterConditionConnected)
	connected := findCondition(t, got.Status.Conditions, cluster.ClusterConditionConnected)
	assert.Equal(t, cluster.ConditionFalse, connected.Status)
	assert.Equal(t, cluster.ReasonInactive, connected.Reason)
	assert.False(t, probeCalled, "probe must not be called for ineligible cluster")
}

func TestClusterControllerSuccessfulProbePopulatesConnectionManager(t *testing.T) {
	uid := "kube-system-uid"
	ver := "v1.31.0"
	user := "alice"
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	connMgr := cluster.NewConnectionManager()

	coreClient, _ := newClusterTestBeehive(t, w,
		staticProbe(
			cluster.ClusterServer{UID: &uid, Version: &ver},
			cluster.ClusterPrincipal{Username: &user},
		),
		staticCheck(cluster.HealthPhaseHealthy),
		connMgr,
	)

	id := cluster.ClusterID("conn-mgr-set-id")
	obj, err := coreClient.Create(context.Background(), eligibleSpec("alpha"),
		beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)

	waitCondition(t, coreClient, obj.ID, cluster.ClusterConditionConnected)

	got := connMgr.Get(id)
	assert.NotNil(t, got, "ConnectionManager must have a REST config after successful probe")
}

func TestClusterControllerProbeFailureDeletesFromConnectionManager(t *testing.T) {
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	connMgr := cluster.NewConnectionManager()

	id := cluster.ClusterID("conn-mgr-del-id")
	// Pre-seed a stale entry so we can confirm it gets cleared.
	connMgr.Set(id, &rest.Config{Host: "https://stale"})

	coreClient, _ := newClusterTestBeehive(t, w,
		errProbe(errors.New("dial failed")),
		staticCheck(cluster.HealthPhaseHealthy),
		connMgr,
	)

	obj, err := coreClient.Create(context.Background(), eligibleSpec("alpha"),
		beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)

	waitCondition(t, coreClient, obj.ID, cluster.ClusterConditionConnected)

	got := connMgr.Get(id)
	assert.Nil(t, got, "ConnectionManager must not hold a config after probe failure")
}

func TestClusterControllerIneligibleDeletesFromConnectionManager(t *testing.T) {
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	connMgr := cluster.NewConnectionManager()

	id := cluster.ClusterID("conn-mgr-ineligible-id")
	connMgr.Set(id, &rest.Config{Host: "https://stale"})

	coreClient, _ := newClusterTestBeehive(t, w,
		staticProbe(cluster.ClusterServer{}, cluster.ClusterPrincipal{}),
		staticCheck(cluster.HealthPhaseHealthy),
		connMgr,
	)

	spec := eligibleSpec("alpha")
	spec.IsActive = false
	obj, err := coreClient.Create(context.Background(), spec,
		beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)

	waitCondition(t, coreClient, obj.ID, cluster.ClusterConditionConnected)

	got := connMgr.Get(id)
	assert.Nil(t, got, "ConnectionManager must not hold a config for an ineligible cluster")
}

func findCondition(t *testing.T, conds []cluster.ClusterCondition, typ cluster.ClusterConditionType) cluster.ClusterCondition {
	t.Helper()
	for _, c := range conds {
		if c.Type == typ {
			return c
		}
	}
	t.Fatalf("condition %s not found in %v", typ, conds)
	return cluster.ClusterCondition{}
}

// TestClusterControllerPokeReprobes verifies the controller subscribes to the
// poke bus and forces an immediate re-probe of every eligible cluster, rather
// than waiting for the next scheduled (30s) reconcile.
func TestClusterControllerPokeReprobes(t *testing.T) {
	ctx := context.Background()
	pk := poke.New()

	var probeCount atomic.Int32
	probe := func(context.Context, *rest.Config) (cluster.ClusterServer, cluster.ClusterPrincipal, error) {
		probeCount.Add(1)
		uid := "uid"
		return cluster.ClusterServer{UID: &uid}, cluster.ClusterPrincipal{}, nil
	}

	bh := testutil.NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[cluster.ClusterSpec, cluster.ClusterConnectionStatus](bh, cluster.ClusterGroupKind)
	cacheClient := beehive.NewClient[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus](bh, cluster.ClusterCacheGroupKind)
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))

	ctrl := cluster.NewClusterController(w, coreClient, cacheClient, nil, pk, probe, staticCheck(cluster.HealthPhaseHealthy))
	cc, err := beehive.Register(bh, cluster.ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, cluster.ClusterCacheGroupKind, &testutil.NoopController[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	ctrl.StartPoke()
	t.Cleanup(func() { ctrl.StopPoke(); _ = stop(ctx) })

	id := cluster.ClusterID("reprobe-uuid")
	_, err = coreClient.Create(ctx, eligibleSpec("alpha"), beehive.WithSlug(cluster.ClusterSlug(id)))
	require.NoError(t, err)

	// The initial scheduled reconcile probes once (then requeues ~30s out).
	require.Eventually(t, func() bool { return probeCount.Load() >= 1 },
		2*time.Second, 10*time.Millisecond, "initial reconcile should probe once")
	before := probeCount.Load()

	// A poke forces an immediate re-probe without waiting for the 30s cadence.
	pk.Poke(poke.SourceHost)
	require.Eventually(t, func() bool { return probeCount.Load() > before },
		2*time.Second, 10*time.Millisecond, "poke should force an immediate re-probe")
}
