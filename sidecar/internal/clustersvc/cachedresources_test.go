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

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// Deterministic per (anchor, apiVersion, resource), which is what lets a discovery
// pass reconcile its children as a set with no per-child bookkeeping. Keyed on the
// plural, not the Kind: a CRD may reuse a built-in's plural under another group, so
// the group-version has to be in the name too.
func TestClusterCachedResourceName(t *testing.T) {
	assert.Equal(t, "cachedresource/3/apps/v1/deployments", ClusterCachedResourceName(3, "apps/v1", "deployments"))
	assert.NotEqual(t,
		ClusterCachedResourceName(3, "apps/v1", "deployments"),
		ClusterCachedResourceName(3, "example.com/v1", "deployments"),
		"the same plural under another group is a different kind")
	assert.NotEqual(t,
		ClusterCachedResourceName(3, "apps/v1", "deployments"),
		ClusterCachedResourceName(4, "apps/v1", "deployments"),
		"the same kind under another cache's anchor")
}

func createResource(t *testing.T, d deps, catalogID beehive.ObjectID, spec ClusterCachedResourceSpec) *beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus] {
	t.Helper()
	ctx := context.Background()
	name := ClusterCachedResourceName(catalogID, spec.APIVersion, spec.Resource)
	obj, _, err := d.resourceClient.CreateOrUpdate(ctx, name, spec, beehive.WithOwner(catalogID))
	require.NoError(t, err)
	return obj
}

// deploymentsSpec and podsSpec are two served kinds, enough to prove ordering and scoping.
var (
	deploymentsSpec = ClusterCachedResourceSpec{Enabled: true, APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Namespaced: true}
	podsSpec        = ClusterCachedResourceSpec{Enabled: false, APIVersion: "v1", Kind: "Pod", Resource: "pods", Namespaced: true}
)

// twoCachesTwoResources stores a kind under each of two caches' catalogs, and returns the
// first cache's id — enough for one fixture to prove both a read's contents and its scoping.
func twoCachesTwoResources(t *testing.T) (deps, ClusterCacheID) {
	t.Helper()
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	one := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	two := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-2")
	createResource(t, d, createCatalog(t, d, ClusterCacheID(one.ID), true).ID, deploymentsSpec)
	createResource(t, d, createCatalog(t, d, ClusterCacheID(two.ID), true).ID, podsSpec)
	return d, ClusterCacheID(one.ID)
}

func TestCachedResourcesGet(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	catalog := createCatalog(t, d, ClusterCacheID(cache.ID), true)
	obj := createResource(t, d, catalog.ID, deploymentsSpec)

	got, err := serviceOver(t, d).CachedResources().Get(context.Background(), ClusterCachedResourceID(obj.ID))

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, ClusterCachedResourceID(obj.ID), got.ID)
	assert.Equal(t, deploymentsSpec, got.Spec)
	assert.Equal(t, ObjectRef{ID: ObjectID(catalog.ID), Kind: "ClusterCachedCatalog"}, got.Owner,
		"the owner is the catalog, not the cache — the join key a client has from the discovery stream")
}

// An unknown id is not an error: a caller holds ids from watch frames, and a record
// collected in between is an ordinary race rather than a bad request.
func TestCachedResourcesGetUnknownIsNotAnError(t *testing.T) {
	d := newTestDeps(t)

	got, err := serviceOver(t, d).CachedResources().Get(context.Background(), 404)

	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestCachedResourcesList(t *testing.T) {
	d, _ := twoCachesTwoResources(t)

	got, err := serviceOver(t, d).CachedResources().List(context.Background())

	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []string{"deployments", "pods"}, []string{got[0].Spec.Resource, got[1].Spec.Resource},
		"creation order, the same order every family's list promises")
}

// watchResources opens the unscoped stream for the test's life.
func watchResources(t *testing.T, d deps) *Stream[ClusterCachedResourceWatchFrame] {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream, err := serviceOver(t, d).CachedResources().WatchList(ctx)
	require.NoError(t, err)
	return stream
}

func TestCachedResourcesWatchListEmitsTheSnapshotThenABookmark(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	catalog := createCatalog(t, d, ClusterCacheID(cache.ID), true)
	createResource(t, d, catalog.ID, deploymentsSpec)

	stream := watchResources(t, d)

	f := testutil.Recv(t, stream.Frames, "the snapshot frame")
	require.Equal(t, DeltaFrameAdded, f.Type)
	require.NotNil(t, f.Resource)
	assert.Equal(t, "deployments", f.Resource.Spec.Resource)
	assert.Equal(t, ObjectRef{ID: ObjectID(catalog.ID), Kind: "ClusterCachedCatalog"}, f.Resource.Owner)
	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// A catalog that has discovered nothing yet bookmarks an empty collection rather than
// holding the snapshot back: the wait is the consumer's to render.
func TestCachedResourcesWatchListBookmarksAnEmptyCollection(t *testing.T) {
	stream := watchResources(t, newRunningDeps(t))

	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// A departure is a collected row, not the soft delete that precedes it — a prune marks the
// record and that arrives as an ordinary Modified. The frame carries no owner, because
// beehive loads no edges for a collected row and a frame that failed over that would kill
// the stream and strand the record in the client's map.
func TestCachedResourcesWatchListReportsADeparture(t *testing.T) {
	frames := pumpFrames(t, resourceWatch, nil,
		beehive.ObjectChange[ClusterCachedResourceSpec, ClusterCachedResourceStatus]{Type: beehive.Deleted, ID: 7, Object: nil},
	)

	require.Len(t, frames, 2)
	assert.Equal(t, DeltaFrameDeleted, frames[1].Type)
	require.NotNil(t, frames[1].Resource)
	assert.Equal(t, ClusterCachedResourceID(7), frames[1].Resource.ID)
}

// A prune is beehive's soft delete, so the row lingers holding its name and the frame is an
// ordinary Modified — which is what makes the missing tombstone field on this record matter:
// a consumer cannot tell the mark from any other spec write.
func TestCachedResourcesWatchListReportsAPruneAsAModify(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	catalog := createCatalog(t, d, ClusterCacheID(cache.ID), true)
	obj := createResource(t, d, catalog.ID, deploymentsSpec)

	stream := watchResources(t, d)
	require.Equal(t, DeltaFrameAdded, testutil.Recv(t, stream.Frames, "the snapshot frame").Type)
	require.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)

	require.NoError(t, d.resourceClient.Delete(context.Background(), obj.ID))

	f := testutil.Recv(t, stream.Frames, "the prune")
	assert.Equal(t, DeltaFrameModified, f.Type)
	require.NotNil(t, f.Resource)
	assert.Equal(t, ClusterCachedResourceID(obj.ID), f.Resource.ID)
}

// One record's own stream, for a consumer holding an id from a frame.
func TestCachedResourcesWatchStreamsOneRecord(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	catalog := createCatalog(t, d, ClusterCacheID(cache.ID), true)
	obj := createResource(t, d, catalog.ID, deploymentsSpec)
	createResource(t, d, catalog.ID, podsSpec)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := serviceOver(t, d).CachedResources().Watch(ctx, ClusterCachedResourceID(obj.ID))
	require.NoError(t, err)

	f := testutil.Recv(t, stream.Frames, "the snapshot frame")
	require.NotNil(t, f.Resource)
	assert.Equal(t, "deployments", f.Resource.Spec.Resource, "the other kind is not this record")
	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// The scope a caller passes is a *cache*, but the records hang off that cache's catalog —
// so the service resolves the anchor, because a caller only ever holds a cache id.
func TestCachedResourcesListByCache(t *testing.T) {
	d, one := twoCachesTwoResources(t)

	got, err := serviceOver(t, d).CachedResources().ListByCache(context.Background(), one)

	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "deployments", got[0].Spec.Resource, "the other cache's kinds are not this cache's")
}

// A cache whose catalog has not been written yet has no anchor to cross, which reads empty
// rather than failing — the same wait an unreconciled cache shows everywhere else.
func TestCachedResourcesListByCacheWithNoCatalogIsEmpty(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	got, err := serviceOver(t, d).CachedResources().ListByCache(context.Background(), ClusterCacheID(cache.ID))

	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestCachedResourcesWatchByCache(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	one := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	two := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-2")
	createResource(t, d, createCatalog(t, d, ClusterCacheID(one.ID), true).ID, deploymentsSpec)
	createResource(t, d, createCatalog(t, d, ClusterCacheID(two.ID), true).ID, podsSpec)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := serviceOver(t, d).CachedResources().WatchByCache(ctx, ClusterCacheID(one.ID))
	require.NoError(t, err)

	f := testutil.Recv(t, stream.Frames, "the snapshot frame")
	require.NotNil(t, f.Resource)
	assert.Equal(t, "deployments", f.Resource.Spec.Resource, "the other cache's kinds are not on this stream")
	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// A cache with no catalog yet bookmarks an empty collection: an unopened cache is
// definitively empty, not pending, and holding the bookmark back would render a populated
// table as loading for as long as the cache takes to reconcile.
func TestCachedResourcesWatchByCacheWithNoCatalogBookmarksEmpty(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := serviceOver(t, d).CachedResources().WatchByCache(ctx, ClusterCacheID(cache.ID))
	require.NoError(t, err)

	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// clearKindRows is what both teardown paths go through, and it must reach exactly the
// kind it was handed: the cookie is the observable half — a kind whose position is gone
// cold-syncs, and one whose survived does not.
func TestClearKindRowsClearsOnlyThatKind(t *testing.T) {
	ctx := context.Background()
	stores := newFakeKubestore(t)
	store, _, err := stores.OpenExisting(1)
	require.NoError(t, err)
	require.NoError(t, store.SetCookie(ctx, "apps/v1", "deployments", "10"))
	require.NoError(t, store.SetCookie(ctx, "v1", "pods", "20"))
	store.Release()

	require.NoError(t, clearKindRows(ctx, stores, 1, deploymentsSpec))

	store, _, err = stores.OpenExisting(1)
	require.NoError(t, err)
	defer store.Release()
	_, ok, err := store.Cookie(ctx, "apps/v1", "deployments")
	require.NoError(t, err)
	assert.False(t, ok, "the kind's own position survived its clear")
	_, ok, err = store.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	assert.True(t, ok, "another kind's position went with it")
}

// A cache with no file has nothing to clear, and finding that out must not create one.
func TestClearKindRowsSkipsACacheWithNoFile(t *testing.T) {
	stores := newFakeKubestore(t)
	stores.noFile = true

	require.NoError(t, clearKindRows(context.Background(), stores, 1, deploymentsSpec))
}

// The per-kind clear owes the same re-arm as the cache-wide one: the worker is stopped
// before the rows are touched, so a clear that fails has to ask the record to reconcile
// anyway — its pass is what arms the worker again, and the rows it failed to remove are
// still there being served.

// A cache's catalog is created by its own pass, so a client that subscribes on the frame
// announcing the cache arrives before the anchor exists. That is a wait, not an empty
// collection: the bookmark closes a snapshot of none, and the kinds are reported as they
// land. Binding to nothing here would leave the view at "no kinds" for the life of the
// subscription.
func TestCachedResourcesWatchByCacheReportsKindsThatArriveLater(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := serviceOver(t, d).CachedResources().WatchByCache(ctx, ClusterCacheID(cache.ID))
	require.NoError(t, err)
	require.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)

	// The cache's pass, arriving after the subscription.
	catalog := createCatalog(t, d, ClusterCacheID(cache.ID), true)
	obj := createResource(t, d, catalog.ID, deploymentsSpec)

	got := testutil.Recv(t, stream.Frames, "the kind that landed after the anchor")
	assert.Equal(t, DeltaFrameAdded, got.Type)
	require.NotNil(t, got.Resource)
	assert.Equal(t, ClusterCachedResourceID(obj.ID), got.Resource.ID)
}
