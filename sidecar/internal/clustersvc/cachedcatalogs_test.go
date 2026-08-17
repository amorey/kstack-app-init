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

package clustersvc

import (
	"context"
	"testing"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Exactly one discovery anchor per cache, so creation is idempotent under
// name-uniqueness dedup.
func TestClusterCachedCatalogName(t *testing.T) {
	assert.Equal(t, "cachedcatalog/7", ClusterCachedCatalogName(7))
	assert.Equal(t, ClusterCachedCatalogName(7), ClusterCachedCatalogName(7))
}

// --- ClusterCachedCatalog creation ---

// catalogs returns every stored catalog, owner edge loaded — what a write is read
// back through.
func catalogs(t *testing.T, client beehive.Client[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]) []*beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus] {
	t.Helper()
	objs, err := client.List(context.Background(), beehive.LoadOwner())
	require.NoError(t, err)
	return objs
}

// A cache's catalog hangs off the cache, which is the join key its consumers have and
// the edge beehive's GC cascades on.
func TestEnsureClusterCachedCatalogCreatesOnePerCache(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	require.NoError(t, ensureClusterCachedCatalog(context.Background(), d.catalogClient, ClusterCacheID(cache.ID), true))

	objs := catalogs(t, d.catalogClient)
	require.Len(t, objs, 1)
	assert.Equal(t, ClusterCachedCatalogName(cache.ID), objs[0].Name)
	assert.True(t, objs[0].Spec.Enabled, "the pause switch is relayed in at creation")

	owner, ok, err := objs[0].Owner()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, cache.ID, owner.ID)
}

// Every cache pass ensures the catalog, so the second call is the common case.
func TestEnsureClusterCachedCatalogCreatesItOnlyOnce(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	for range 2 {
		require.NoError(t, ensureClusterCachedCatalog(context.Background(), d.catalogClient, ClusterCacheID(cache.ID), true))
	}

	assert.Len(t, catalogs(t, d.catalogClient), 1)
}

// The pause switch is relayed from above on every pass, so a flip has to reach the
// stored spec — the anchor outlives the pause, and the children read Enabled off it.
func TestEnsureClusterCachedCatalogRelaysAFlip(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	ctx := context.Background()
	require.NoError(t, ensureClusterCachedCatalog(ctx, d.catalogClient, ClusterCacheID(cache.ID), true))
	created := catalogs(t, d.catalogClient)[0]

	require.NoError(t, ensureClusterCachedCatalog(ctx, d.catalogClient, ClusterCacheID(cache.ID), false))

	objs := catalogs(t, d.catalogClient)
	require.Len(t, objs, 1)
	assert.Equal(t, created.ID, objs[0].ID, "the anchor survives the pause")
	assert.False(t, objs[0].Spec.Enabled)
}

// A placeholder until the kind is rebuilt: it must settle the object rather than
// requeue it, or beehive's owed pass would re-dispatch every catalog forever.
func TestCachedCatalogControllerReconcilesToANoOp(t *testing.T) {
	client := &settleRecorder[ClusterCachedCatalogStatus]{}
	obj := &beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]{ID: 1, Generation: 3}

	res, err := (&clusterCachedCatalogController{}).Reconcile(context.Background(), client, obj)

	require.NoError(t, err)
	assert.Equal(t, beehive.Result{}, res)
	require.NotNil(t, client.observed)
	assert.Equal(t, obj.Generation, *client.observed)
}
