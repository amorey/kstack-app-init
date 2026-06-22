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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers/cluster"
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers/testutil"
)

// staticProbe returns a ProbeFunc that always yields the given server/principal.
func staticProbe(server controllers.ClusterServer, principal controllers.ClusterPrincipal) cluster.ProbeFunc {
	return func(context.Context, *rest.Config) (controllers.ClusterServer, controllers.ClusterPrincipal, error) {
		return server, principal, nil
	}
}

// errProbe returns a ProbeFunc that always fails.
func errProbe(err error) cluster.ProbeFunc {
	return func(context.Context, *rest.Config) (controllers.ClusterServer, controllers.ClusterPrincipal, error) {
		return controllers.ClusterServer{}, controllers.ClusterPrincipal{}, err
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
func newClusterTestBeehive(t *testing.T, w controllers.KubeConfigSource, probe cluster.ProbeFunc, check cluster.CheckFunc) (beehive.Client[controllers.ClusterSpec, controllers.ClusterConnectionStatus], beehive.Client[controllers.ClusterCacheSpec, controllers.ClusterCacheStatus]) {
	t.Helper()
	bh := testutil.NewTestBeehiveUnstarted(t)

	clusterClient := beehive.NewClient[controllers.ClusterSpec, controllers.ClusterConnectionStatus](bh, controllers.ClusterGroupKind)
	cacheClient := beehive.NewClient[controllers.ClusterCacheSpec, controllers.ClusterCacheStatus](bh, controllers.ClusterCacheGroupKind)

	ctrl := cluster.NewClusterController(w, cacheClient, probe, check)
	require.NoError(t, beehive.Register(bh, controllers.ClusterSourceGroupKind, &testutil.NoopController[controllers.ClusterSourceSpec, controllers.ClusterSourceObjStatus]{}))
	require.NoError(t, beehive.Register(bh, controllers.ClusterGroupKind, ctrl))
	require.NoError(t, beehive.Register(bh, controllers.ClusterCacheGroupKind, &testutil.NoopController[controllers.ClusterCacheSpec, controllers.ClusterCacheStatus]{}))
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	return clusterClient, cacheClient
}

// waitCondition polls until the object has the named condition or the deadline.
func waitCondition(t *testing.T, cl beehive.Client[controllers.ClusterSpec, controllers.ClusterConnectionStatus], id beehive.ObjectID, condType controllers.ClusterConditionType) *beehive.Object[controllers.ClusterSpec, controllers.ClusterConnectionStatus] {
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
func eligibleSpec(contextName string) controllers.ClusterSpec {
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

func TestClusterControllerSuccessfulProbeWritesConditions(t *testing.T) {
	uid := "kube-system-uid"
	ver := "v1.31.0"
	user := "alice"
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))

	clusterClient, cacheClient := newClusterTestBeehive(t, w,
		staticProbe(
			controllers.ClusterServer{UID: &uid, Version: &ver},
			controllers.ClusterPrincipal{Username: &user},
		),
		staticCheck(cluster.HealthPhaseHealthy),
	)
	ctx := context.Background()

	id := controllers.ClusterID("test-uid")
	obj, err := clusterClient.Create(ctx, eligibleSpec("alpha"),
		beehive.WithSlug(controllers.ClusterSlug(id)))
	require.NoError(t, err)

	got := waitCondition(t, clusterClient, obj.ID, controllers.ClusterConditionConnected)
	require.NotNil(t, got.Status)

	connected := findCondition(t, got.Status.Conditions, controllers.ClusterConditionConnected)
	assert.Equal(t, controllers.ConditionTrue, connected.Status)
	assert.Equal(t, controllers.ReasonConnected, connected.Reason)

	healthy := findCondition(t, got.Status.Conditions, controllers.ClusterConditionHealthy)
	assert.Equal(t, controllers.ConditionTrue, healthy.Status)

	assert.NotNil(t, got.Status.LastConnectedAt)

	// ClusterCache child must have been created.
	_, err = cacheClient.GetBySlug(ctx, controllers.ClusterCacheSlug(id))
	require.NoError(t, err, "ClusterCache child must exist after successful reconcile")
}

func TestClusterControllerProbeFailureSetsConnectedFalse(t *testing.T) {
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	clusterClient, _ := newClusterTestBeehive(t, w,
		errProbe(errors.New("connection refused")),
		staticCheck(cluster.HealthPhaseHealthy),
	)
	ctx := context.Background()

	obj, err := clusterClient.Create(ctx, eligibleSpec("alpha"),
		beehive.WithSlug(controllers.ClusterSlug("probe-fail-id")))
	require.NoError(t, err)

	got := waitCondition(t, clusterClient, obj.ID, controllers.ClusterConditionConnected)
	connected := findCondition(t, got.Status.Conditions, controllers.ClusterConditionConnected)
	assert.Equal(t, controllers.ConditionFalse, connected.Status)
	assert.Equal(t, controllers.ReasonProbeFailed, connected.Reason)
}

func TestClusterControllerIneligibleClusterDoesNotProbe(t *testing.T) {
	probeCalled := false
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	probe := func(_ context.Context, _ *rest.Config) (controllers.ClusterServer, controllers.ClusterPrincipal, error) {
		probeCalled = true
		return controllers.ClusterServer{}, controllers.ClusterPrincipal{}, nil
	}
	clusterClient, _ := newClusterTestBeehive(t, w, probe, staticCheck(cluster.HealthPhaseHealthy))
	ctx := context.Background()

	// IsActive=false → ineligible.
	spec := eligibleSpec("alpha")
	spec.IsActive = false
	obj, err := clusterClient.Create(ctx, spec,
		beehive.WithSlug(controllers.ClusterSlug("inactive-id")))
	require.NoError(t, err)

	got := waitCondition(t, clusterClient, obj.ID, controllers.ClusterConditionConnected)
	connected := findCondition(t, got.Status.Conditions, controllers.ClusterConditionConnected)
	assert.Equal(t, controllers.ConditionFalse, connected.Status)
	assert.Equal(t, controllers.ReasonInactive, connected.Reason)
	assert.False(t, probeCalled, "probe must not be called for ineligible cluster")
}

func findCondition(t *testing.T, conds []controllers.ClusterCondition, typ controllers.ClusterConditionType) controllers.ClusterCondition {
	t.Helper()
	for _, c := range conds {
		if c.Type == typ {
			return c
		}
	}
	t.Fatalf("condition %s not found in %v", typ, conds)
	return controllers.ClusterCondition{}
}
