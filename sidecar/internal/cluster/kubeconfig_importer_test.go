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
	coreClient := beehive.NewClient[cluster.ClusterSpec, cluster.ClusterStatus](bh, cluster.ClusterGroupKind)
	w := testutil.NewStaticWatcher(t, cfg)
	return cluster.NewKubeconfigImporter(w, coreClient)
}

// The importer creates a Cluster carrying only the source reference; the
// observation (cluster/user names, presence, isDefault) is the controller's job
// and is not written by the importer.
func TestImporterNewContextCreatesCluster(t *testing.T) {
	ctx := context.Background()
	cfg := testutil.TestKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))

	obj := mustSingleObj(t, im.ClusterClient())
	require.NotNil(t, obj.Spec.Source.Kubeconfig)
	assert.Equal(t, "alpha", obj.Spec.Source.Kubeconfig.Context)
	assert.True(t, obj.Spec.Enabled)
	assert.True(t, obj.Spec.SyncEnabled)
}

// A departed context is never deleted by the importer — the Cluster (and its
// owned cache) survives. Flipping it to not-present is the controller's job.
func TestImporterDepartedContextKeepsCluster(t *testing.T) {
	ctx := context.Background()
	cfg := testutil.TestKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	origID := mustSingleObj(t, im.ClusterClient()).ID

	// Remove context from config: the importer must not delete the record.
	empty := &api.Config{Contexts: map[string]*api.Context{}}
	require.NoError(t, im.ReconcileClusterSet(ctx, empty))

	obj := mustSingleObj(t, im.ClusterClient())
	assert.Equal(t, origID, obj.ID, "departed context must not be deleted")
}

// A returning context reuses its (never-deleted) Cluster — the importer finds it
// by context and creates no duplicate.
func TestImporterReturningContextCreatesNoDuplicate(t *testing.T) {
	ctx := context.Background()
	cfg := testutil.TestKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	origID := mustSingleObj(t, im.ClusterClient()).ID

	// Depart, then return.
	require.NoError(t, im.ReconcileClusterSet(ctx, &api.Config{Contexts: map[string]*api.Context{}}))
	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))

	obj := mustSingleObj(t, im.ClusterClient())
	assert.Equal(t, origID, obj.ID, "same object ID: reused in-place, no new cluster")
}

// An already-tracked context creates nothing on a repeat snapshot (no duplicate,
// no spec write — the importer only ever creates).
func TestImporterRepeatSnapshotCreatesNothing(t *testing.T) {
	ctx := context.Background()
	cfg := testutil.TestKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	obj1 := mustSingleObj(t, im.ClusterClient())

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	obj2 := mustSingleObj(t, im.ClusterClient())

	assert.Equal(t, obj1.ID, obj2.ID, "repeat snapshot must not create a second cluster")
	assert.Equal(t, obj1.Generation, obj2.Generation, "repeat snapshot must not bump generation")
}

func mustSingleObj(t *testing.T, cl beehive.Client[cluster.ClusterSpec, cluster.ClusterStatus]) *beehive.Object[cluster.ClusterSpec, cluster.ClusterStatus] {
	t.Helper()
	objs, err := cl.List(context.Background())
	require.NoError(t, err)
	require.Len(t, objs, 1)
	return objs[0]
}
