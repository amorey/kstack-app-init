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

package clustersource_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers/clustersource"
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers/testutil"
)

// waitClusterSourceStatus polls until the ClusterSource status is populated or
// the deadline passes. Returns the final object.
func waitClusterSourceStatus(t *testing.T, cl beehive.Client[controllers.ClusterSourceSpec, controllers.ClusterSourceObjStatus], id beehive.ObjectID) *beehive.Object[controllers.ClusterSourceSpec, controllers.ClusterSourceObjStatus] {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		obj, err := cl.Get(context.Background(), id)
		require.NoError(t, err)
		if obj.Status != nil && obj.Status.ClusterID != nil {
			return obj
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for ClusterSource status")
	return nil
}

// newSourceTestBeehive builds a beehive with the real ClusterSourceController
// plus NoopControllers for the downstream kinds.
func newSourceTestBeehive(t *testing.T) (*beehive.Beehive, beehive.Client[controllers.ClusterSourceSpec, controllers.ClusterSourceObjStatus], beehive.Client[controllers.ClusterSpec, controllers.ClusterConnectionStatus]) {
	t.Helper()
	bh := testutil.NewTestBeehiveUnstarted(t)

	clusterClient := beehive.NewClient[controllers.ClusterSpec, controllers.ClusterConnectionStatus](bh, controllers.ClusterGroupKind)
	srcClient := beehive.NewClient[controllers.ClusterSourceSpec, controllers.ClusterSourceObjStatus](bh, controllers.ClusterSourceGroupKind)

	srcCtrl := clustersource.NewClusterSourceController(clusterClient)
	require.NoError(t, beehive.Register(bh, controllers.ClusterSourceGroupKind, srcCtrl))
	require.NoError(t, beehive.Register(bh, controllers.ClusterGroupKind, &testutil.NoopController[controllers.ClusterSpec, controllers.ClusterConnectionStatus]{}))
	require.NoError(t, beehive.Register(bh, controllers.ClusterCacheGroupKind, &testutil.NoopController[controllers.ClusterCacheSpec, controllers.ClusterCacheStatus]{}))
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	return bh, srcClient, clusterClient
}

func TestSourceControllerCreatesClusterChild(t *testing.T) {
	ctx := context.Background()
	_, srcClient, clusterClient := newSourceTestBeehive(t)

	src, err := srcClient.Create(ctx, controllers.ClusterSourceSpec{
		ContextName: "alpha",
		ClusterName: "alpha-cluster",
		UserName:    "alpha-user",
		IsPresent:   true,
	}, beehive.WithSlug(controllers.ClusterSourceSlug("alpha")))
	require.NoError(t, err)

	// Wait for reconcile to set ClusterID in status.
	srcObj := waitClusterSourceStatus(t, srcClient, src.ID)
	require.NotNil(t, srcObj.Status.ClusterID)

	// Verify the Cluster child exists.
	clusterObj, err := clusterClient.GetBySlug(ctx, controllers.ClusterSlug(*srcObj.Status.ClusterID))
	require.NoError(t, err)
	assert.Equal(t, "alpha", clusterObj.Spec.Source.Kubeconfig.Context)
	assert.NotNil(t, clusterObj.Spec.SourceObs)
	assert.Equal(t, "alpha-cluster", clusterObj.Spec.SourceObs.Cluster)
}

func TestSourceControllerIdempotentSecondReconcile(t *testing.T) {
	ctx := context.Background()
	_, srcClient, clusterClient := newSourceTestBeehive(t)

	src, err := srcClient.Create(ctx, controllers.ClusterSourceSpec{
		ContextName: "alpha",
		IsPresent:   true,
	}, beehive.WithSlug(controllers.ClusterSourceSlug("alpha")))
	require.NoError(t, err)

	srcObj := waitClusterSourceStatus(t, srcClient, src.ID)
	clusterID := *srcObj.Status.ClusterID

	// Update the source spec (changes observation).
	_, err = srcClient.Update(ctx, src.ID, controllers.ClusterSourceSpec{
		ContextName: "alpha",
		ClusterName: "new-cluster",
		IsPresent:   true,
	})
	require.NoError(t, err)

	// Wait for updated SourceObs to propagate.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		clusterObj, err := clusterClient.GetBySlug(ctx, controllers.ClusterSlug(clusterID))
		require.NoError(t, err)
		if clusterObj.Spec.SourceObs != nil && clusterObj.Spec.SourceObs.Cluster == "new-cluster" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	clusterObj, err := clusterClient.GetBySlug(ctx, controllers.ClusterSlug(clusterID))
	require.NoError(t, err)
	assert.Equal(t, "new-cluster", clusterObj.Spec.SourceObs.Cluster)
}

func TestSourceControllerDeletionNoOp(t *testing.T) {
	ctx := context.Background()
	_, srcClient, _ := newSourceTestBeehive(t)

	src, err := srcClient.Create(ctx, controllers.ClusterSourceSpec{
		ContextName: "alpha",
		IsPresent:   true,
	}, beehive.WithSlug(controllers.ClusterSourceSlug("alpha")))
	require.NoError(t, err)
	waitClusterSourceStatus(t, srcClient, src.ID)

	// Request deletion.
	require.NoError(t, srcClient.Delete(ctx, src.ID))

	// Beehive GC handles cascade; just verify no panic/error.
	time.Sleep(50 * time.Millisecond)
}
