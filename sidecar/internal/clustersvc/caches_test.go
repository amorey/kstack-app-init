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

// One cache per identity per cluster: the name is the creation/dedup key beehive's
// name-uniqueness enforces, so a UID migration must yield a second, distinct name
// rather than colliding with the cache it supersedes.
func TestClusterCacheName(t *testing.T) {
	assert.Equal(t, "7/uid-1", ClusterCacheName(7, "uid-1"))
	assert.NotEqual(t, ClusterCacheName(7, "uid-1"), ClusterCacheName(7, "uid-2"))
	assert.NotEqual(t, ClusterCacheName(7, "uid-1"), ClusterCacheName(8, "uid-1"))
}

// --- Caches() reads ---

// createCache stores a cache for clusterID under uid, through the same write the
// cluster controller makes — a fixture that hand-rolled the name, spec and owner edge
// could drift from what production actually stores.
func createCache(t *testing.T, caches beehive.Client[ClusterCacheSpec, ClusterCacheStatus], clusterID ClusterID, uid string) *beehive.Object[ClusterCacheSpec, ClusterCacheStatus] {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, ensureClusterCache(ctx, caches, clusterID, uid))

	obj, err := caches.GetByName(ctx, ClusterCacheName(clusterID, uid))
	require.NoError(t, err)
	return obj
}

// uidsOf reads the identities out of a read, which is what the list tests assert on.
func uidsOf(caches []*ClusterCache) []string {
	uids := make([]string, 0, len(caches))
	for _, cache := range caches {
		uids = append(uids, cache.Spec.ServerUID)
	}
	return uids
}

func TestCachesGet(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	obj := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	got, err := serviceOver(t, d).Caches().Get(context.Background(), ClusterCacheID(obj.ID))

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, ClusterCacheID(obj.ID), got.ID)
	assert.Equal(t, "uid-1", got.Spec.ServerUID)
	assert.Equal(t, ObjectRef{ID: ObjectID(cluster.ID), Kind: "Cluster"}, got.Owner, "the join comes off the owner edge, not the name")
}

// An unknown id is not an error: a caller holds ids from watch frames, and a cache
// collected in between is an ordinary race rather than a bad request.
func TestCachesGetUnknownIsNotAnError(t *testing.T) {
	d := newTestDeps(t)

	got, err := serviceOver(t, d).Caches().Get(context.Background(), 404)

	require.NoError(t, err)
	assert.Nil(t, got)
}

// twoClustersThreeCaches stores two clusters and three caches, the first cluster owning
// two of them — enough for one fixture to prove both a list's order and its scoping. It
// returns that cluster's id.
func twoClustersThreeCaches(t *testing.T) (deps, ClusterID) {
	t.Helper()
	d := newTestDeps(t)
	one := createCluster(t, d.clusterClient, "prod")
	two := createCluster(t, d.clusterClient, "staging")
	createCache(t, d.cacheClient, ClusterID(one.ID), "uid-1")
	createCache(t, d.cacheClient, ClusterID(two.ID), "uid-2")
	createCache(t, d.cacheClient, ClusterID(one.ID), "uid-3")
	return d, ClusterID(one.ID)
}

// Creation order, which is what a UID migration needs: the superseded cache reads
// ahead of the one that replaced it, so a consumer walking the list sees the turnover
// in the order it happened.
func TestCachesList(t *testing.T) {
	d, _ := twoClustersThreeCaches(t)

	got, err := serviceOver(t, d).Caches().List(context.Background())

	require.NoError(t, err)
	assert.Equal(t, []string{"uid-1", "uid-2", "uid-3"}, uidsOf(got))
}

// One cluster's caches, in the same order — what Cluster.caches serves, and what a
// migration turns into a two-element list.
func TestCachesListByCluster(t *testing.T) {
	d, one := twoClustersThreeCaches(t)

	got, err := serviceOver(t, d).Caches().ListByCluster(context.Background(), one)

	require.NoError(t, err)
	assert.Equal(t, []string{"uid-1", "uid-3"}, uidsOf(got))
}

// A cluster that has never been probed owns none, which is empty rather than an error.
func TestCachesListByClusterWithNone(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")

	got, err := serviceOver(t, d).Caches().ListByCluster(context.Background(), ClusterID(cluster.ID))

	require.NoError(t, err)
	assert.Empty(t, got)
}

// A placeholder until the kind is rebuilt: it must settle the object rather than
// requeue it, or beehive would spin on a kind nothing reconciles yet.
func TestCacheControllerReconcilesToANoOp(t *testing.T) {
	res, err := (&clusterCacheController{}).Reconcile(context.Background(), nil, &beehive.Object[ClusterCacheSpec, ClusterCacheStatus]{ID: 1})

	require.NoError(t, err)
	assert.Equal(t, beehive.Result{}, res)
}
