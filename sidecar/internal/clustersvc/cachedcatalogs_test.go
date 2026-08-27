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

// A record the GC is coming for keeps the spec it has: rewriting it would land the relay
// on an incarnation about to go, and the replacement cannot be created until the name is
// released with it. The same rule ensureClusterCache keeps.
func TestEnsureClusterCachedCatalogRewritesNoDrainingRecord(t *testing.T) {
	d := newTestDeps(t)
	ctx := context.Background()
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	require.NoError(t, ensureClusterCachedCatalog(ctx, d.catalogClient, ClusterCacheID(cache.ID), true))

	created := catalogs(t, d.catalogClient)[0]
	require.NoError(t, d.catalogClient.Delete(ctx, created.ID))

	require.NoError(t, ensureClusterCachedCatalog(ctx, d.catalogClient, ClusterCacheID(cache.ID), false))

	draining, err := d.catalogClient.GetByName(ctx, ClusterCachedCatalogName(cache.ID))
	require.NoError(t, err)
	require.NotNil(t, draining.DeletionRequestedAt, "still awaiting collection")
	assert.True(t, draining.Spec.Enabled, "left as it was")
}

// --- the boundary ---

// createCatalog stores one cache's anchor and hands it back.
func createCatalog(t *testing.T, d deps, cacheID ClusterCacheID, enabled bool) *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus] {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, ensureClusterCachedCatalog(ctx, d.catalogClient, cacheID, enabled))

	obj, err := d.catalogClient.GetByName(ctx, ClusterCachedCatalogName(beehive.ObjectID(cacheID)))
	require.NoError(t, err)
	return obj
}

// twoCachesTwoCatalogs stores two caches under one cluster, each with its catalog, and
// returns the first cache's id — enough for one fixture to prove both a list's contents
// and its scoping.
func twoCachesTwoCatalogs(t *testing.T) (deps, ClusterCacheID) {
	t.Helper()
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	one := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	two := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-2")
	createCatalog(t, d, ClusterCacheID(one.ID), true)
	createCatalog(t, d, ClusterCacheID(two.ID), false)
	return d, ClusterCacheID(one.ID)
}

func TestCachedCatalogsGet(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	obj := createCatalog(t, d, ClusterCacheID(cache.ID), true)

	got, err := serviceOver(t, d).CachedCatalogs().Get(context.Background(), ClusterCachedCatalogID(obj.ID))

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, ClusterCachedCatalogID(obj.ID), got.ID)
	assert.True(t, got.Spec.Enabled)
	assert.Equal(t, ObjectRef{ID: ObjectID(cache.ID), Kind: "ClusterCache"}, got.Owner,
		"the join comes off the owner edge, not the derived name")
}

// An unknown id is not an error: a caller holds ids from watch frames, and a record
// collected in between is an ordinary race rather than a bad request.
func TestCachedCatalogsGetUnknownIsNotAnError(t *testing.T) {
	d := newTestDeps(t)

	got, err := serviceOver(t, d).CachedCatalogs().Get(context.Background(), 404)

	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestCachedCatalogsList(t *testing.T) {
	d, _ := twoCachesTwoCatalogs(t)

	got, err := serviceOver(t, d).CachedCatalogs().List(context.Background())

	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []bool{true, false}, []bool{got[0].Spec.Enabled, got[1].Spec.Enabled},
		"creation order, the same order every family's list promises")
}

// At most one record, as a slice so the read reads like its siblings.
func TestCachedCatalogsListByCache(t *testing.T) {
	d, one := twoCachesTwoCatalogs(t)

	got, err := serviceOver(t, d).CachedCatalogs().ListByCache(context.Background(), one)

	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.True(t, got[0].Spec.Enabled, "the other cache's catalog is not this cache's")
}

// A cache that has not reconciled owns none, which is empty rather than an error —
// beehive does not existence-check the owner, so an unknown id reads the same way.
func TestCachedCatalogsListByCacheWithNone(t *testing.T) {
	d := newTestDeps(t)

	got, err := serviceOver(t, d).CachedCatalogs().ListByCache(context.Background(), 404)

	require.NoError(t, err)
	assert.Empty(t, got)
}

// --- CachedCatalogs() watches ---

// watchCatalogs opens a catalog list watch bounded by the test.
func watchCatalogs(t *testing.T, d deps) *Stream[ClusterCachedCatalogWatchFrame] {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream, err := serviceOver(t, d).CachedCatalogs().WatchList(ctx)
	require.NoError(t, err)
	return stream
}

// awaitCatalogBookmark drains the snapshot up to and including the bookmark.
func awaitCatalogBookmark(t *testing.T, stream *Stream[ClusterCachedCatalogWatchFrame]) {
	t.Helper()
	for {
		if testutil.Recv(t, stream.Frames, "the bookmark").Type == DeltaFrameBookmark {
			return
		}
	}
}

// The snapshot arrives as Added frames closed by exactly one Bookmark, each carrying the
// owner edge the client joins onto its cache.
func TestCachedCatalogsWatchListEmitsTheSnapshotThenABookmark(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	createCatalog(t, d, ClusterCacheID(cache.ID), true)

	stream := watchCatalogs(t, d)

	f := testutil.Recv(t, stream.Frames, "the snapshot frame")
	require.Equal(t, DeltaFrameAdded, f.Type)
	require.NotNil(t, f.Catalog)
	assert.Equal(t, ObjectRef{ID: ObjectID(cache.ID), Kind: "ClusterCache"}, f.Catalog.Owner)
	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// A cache whose catalog has not been written yet bookmarks an empty collection rather
// than holding the snapshot back: the wait is the consumer's to render.
func TestCachedCatalogsWatchListBookmarksAnEmptyCollection(t *testing.T) {
	stream := watchCatalogs(t, newRunningDeps(t))

	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// The pause switch is what moves on a catalog after creation, and it must reach the
// subtree's watchers without the record being re-created.
func TestCachedCatalogsWatchListReportsAFlip(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	createCatalog(t, d, ClusterCacheID(cache.ID), true)
	stream := watchCatalogs(t, d)
	awaitCatalogBookmark(t, stream)

	createCatalog(t, d, ClusterCacheID(cache.ID), false)

	f := testutil.Recv(t, stream.Frames, "the flip")
	assert.Equal(t, DeltaFrameModified, f.Type)
	require.NotNil(t, f.Catalog)
	assert.False(t, f.Catalog.Spec.Enabled)
}

// Scoped to one cache: the other cache's catalog must not reach this stream, or a
// per-cache view would fold a record it never asked for.
func TestCachedCatalogsWatchByCacheIsScopedToItsCache(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	mine := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	theirs := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-2")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream, err := serviceOver(t, d).CachedCatalogs().WatchByCache(ctx, ClusterCacheID(mine.ID))
	require.NoError(t, err)
	awaitCatalogBookmark(t, stream)

	createCatalog(t, d, ClusterCacheID(theirs.ID), true)
	createCatalog(t, d, ClusterCacheID(mine.ID), true)

	f := testutil.Recv(t, stream.Frames, "this cache's catalog")
	require.NotNil(t, f.Catalog)
	assert.Equal(t, ObjectRef{ID: ObjectID(mine.ID), Kind: "ClusterCache"}, f.Catalog.Owner)
}

// Bookmark-only while the id names nothing: the record may still arrive, and this
// subscription is what reports it.
func TestCachedCatalogsWatchBookmarksAnUnknownID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream, err := serviceOver(t, newRunningDeps(t)).CachedCatalogs().Watch(ctx, 404)
	require.NoError(t, err)

	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// The single-record watch is scoped to its id, and reports the flip its cache relays.
func TestCachedCatalogsWatchReportsChangesToItsRecord(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	obj := createCatalog(t, d, ClusterCacheID(cache.ID), true)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream, err := serviceOver(t, d).CachedCatalogs().Watch(ctx, ClusterCachedCatalogID(obj.ID))
	require.NoError(t, err)
	awaitCatalogBookmark(t, stream)

	createCatalog(t, d, ClusterCacheID(cache.ID), false)

	f := testutil.Recv(t, stream.Frames, "the flip")
	require.NotNil(t, f.Catalog)
	assert.False(t, f.Catalog.Spec.Enabled)
}

// A removal reaches the subscriber carrying the row's final state and no owner: beehive
// loads no edges for a collected row, and a frame that failed over that would kill the
// stream and strand the record in the client's map.
func TestCachedCatalogsWatchListReportsADeparture(t *testing.T) {
	frames := pumpFrames(t, catalogWatch, nil,
		beehive.ObjectChange[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]{Type: beehive.Deleted, ID: 7, Object: nil},
	)

	require.Len(t, frames, 2)
	assert.Equal(t, DeltaFrameDeleted, frames[1].Type)
	require.NotNil(t, frames[1].Catalog, "the bookmark is the only frame that carries no entity")
	assert.Equal(t, ClusterCachedCatalogID(7), frames[1].Catalog.ID)
}

// A cache whose context no longer reaches its cluster is neither connecting nor failing at
// discovery: nothing was asked, and nothing is retrying. Reporting the wait as Connecting
// would show a permanent state as a dial in progress; reporting it as DiscoveryFailed would
// point a reader at the API server when what moved is which cluster the context reaches.
