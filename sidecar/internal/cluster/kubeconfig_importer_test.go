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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/testutil"
)

// newTestImporter builds a KubeconfigImporter against a fresh beehive
// (no-op controllers — the importer only writes Cluster specs).
func newTestImporter(t *testing.T, cfg *api.Config) *cluster.KubeconfigImporter {
	t.Helper()
	bh := testutil.NewTestBeehive(t)
	coreClient := beehive.NewClient[cluster.ClusterCoreSpec, cluster.ClusterCoreStatus](bh, cluster.ClusterGroupKind)
	w := testutil.NewStaticWatcher(t, cfg)
	return cluster.NewKubeconfigImporter(w, coreClient)
}

func TestImporterNewContextCreatesCluster(t *testing.T) {
	ctx := context.Background()
	cfg := testutil.TestKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))

	obj := mustSingleObj(t, im.ClusterClient())
	require.NotNil(t, obj.Spec.Source.Kubeconfig)
	assert.Equal(t, "alpha", obj.Spec.Source.Kubeconfig.Context)
	assert.True(t, obj.Spec.IsActive)
	assert.True(t, obj.Spec.IsSyncEnabled)
	require.NotNil(t, obj.Spec.SourceObs)
	assert.Equal(t, "alpha-cluster", obj.Spec.SourceObs.Cluster)
	assert.Equal(t, "alpha-user", obj.Spec.SourceObs.User)
	assert.True(t, obj.Spec.SourceObs.IsPresent)
	assert.True(t, obj.Spec.SourceObs.IsDefault)
}

func TestImporterDepartedContextSetsNotPresent(t *testing.T) {
	ctx := context.Background()
	cfg := testutil.TestKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))

	// Remove context from config.
	empty := &api.Config{Contexts: map[string]*api.Context{}}
	require.NoError(t, im.ReconcileClusterSet(ctx, empty))

	obj := mustSingleObj(t, im.ClusterClient())
	require.NotNil(t, obj.Spec.SourceObs, "departed context must not be deleted, only orphaned")
	assert.False(t, obj.Spec.SourceObs.IsPresent)
}

func TestImporterReturningContextRevivesInPlace(t *testing.T) {
	ctx := context.Background()
	cfg := testutil.TestKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	origID := mustSingleObj(t, im.ClusterClient()).ID

	// Orphan it.
	require.NoError(t, im.ReconcileClusterSet(ctx, &api.Config{Contexts: map[string]*api.Context{}}))

	// Revive.
	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	obj := mustSingleObj(t, im.ClusterClient())

	assert.Equal(t, origID, obj.ID, "same object ID: revived in-place, no new cluster")
	require.NotNil(t, obj.Spec.SourceObs)
	assert.True(t, obj.Spec.SourceObs.IsPresent)
}

func TestImporterUnchangedSnapshotWritesNothing(t *testing.T) {
	ctx := context.Background()
	cfg := testutil.TestKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	obj1 := mustSingleObj(t, im.ClusterClient())

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	obj2 := mustSingleObj(t, im.ClusterClient())

	assert.Equal(t, obj1.Generation, obj2.Generation, "unchanged snapshot must not bump generation")
}

func mustSingleObj(t *testing.T, cl beehive.Client[cluster.ClusterCoreSpec, cluster.ClusterCoreStatus]) *beehive.Object[cluster.ClusterCoreSpec, cluster.ClusterCoreStatus] {
	t.Helper()
	objs, err := cl.List(context.Background())
	require.NoError(t, err)
	require.Len(t, objs, 1)
	return objs[0]
}
