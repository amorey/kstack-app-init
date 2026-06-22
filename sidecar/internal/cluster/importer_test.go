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
// (no controllers registered — the importer only calls srcClient).
func newTestImporter(t *testing.T, cfg *api.Config) *cluster.KubeconfigImporter {
	t.Helper()
	bh := testutil.NewTestBeehive(t)
	srcClient := beehive.NewClient[cluster.ClusterSourceSpec, cluster.ClusterSourceObjStatus](bh, cluster.ClusterSourceGroupKind)
	w := testutil.NewStaticWatcher(t, cfg)
	return cluster.NewKubeconfigImporter(w, srcClient)
}

func TestImporterNewContextCreatesClusterSource(t *testing.T) {
	ctx := context.Background()
	cfg := testutil.TestKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))

	objs, err := im.SourceClient().List(ctx)
	require.NoError(t, err)
	require.Len(t, objs, 1)
	assert.Equal(t, "alpha", objs[0].Spec.ContextName)
	assert.Equal(t, "alpha-cluster", objs[0].Spec.ClusterName)
	assert.Equal(t, "alpha-user", objs[0].Spec.UserName)
	assert.True(t, objs[0].Spec.IsPresent)
	assert.True(t, objs[0].Spec.IsDefault)
}

func TestImporterDepartedContextSetsNotPresent(t *testing.T) {
	ctx := context.Background()
	cfg := testutil.TestKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))

	// Remove context from config.
	empty := &api.Config{Contexts: map[string]*api.Context{}}
	require.NoError(t, im.ReconcileClusterSet(ctx, empty))

	objs, err := im.SourceClient().List(ctx)
	require.NoError(t, err)
	require.Len(t, objs, 1, "departed context must not be deleted, only orphaned")
	assert.False(t, objs[0].Spec.IsPresent)
}

func TestImporterReturningContextRevivesInPlace(t *testing.T) {
	ctx := context.Background()
	cfg := testutil.TestKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	origID := mustSingleObj(t, im.SourceClient()).ID

	// Orphan it.
	require.NoError(t, im.ReconcileClusterSet(ctx, &api.Config{Contexts: map[string]*api.Context{}}))

	// Revive.
	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	obj := mustSingleObj(t, im.SourceClient())

	assert.Equal(t, origID, obj.ID, "same object ID: revived in-place, no new row")
	assert.True(t, obj.Spec.IsPresent)
}

func TestImporterUnchangedSnapshotWritesNothing(t *testing.T) {
	ctx := context.Background()
	cfg := testutil.TestKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	obj1 := mustSingleObj(t, im.SourceClient())

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	obj2 := mustSingleObj(t, im.SourceClient())

	assert.Equal(t, obj1.Generation, obj2.Generation, "unchanged snapshot must not bump generation")
}

func mustSingleObj(t *testing.T, cl beehive.Client[cluster.ClusterSourceSpec, cluster.ClusterSourceObjStatus]) *beehive.Object[cluster.ClusterSourceSpec, cluster.ClusterSourceObjStatus] {
	t.Helper()
	objs, err := cl.List(context.Background())
	require.NoError(t, err)
	require.Len(t, objs, 1)
	return objs[0]
}
