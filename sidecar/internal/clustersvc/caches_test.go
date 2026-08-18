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
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// One cache per identity per cluster: the name is the creation/dedup key beehive's
// name-uniqueness enforces, so a UID migration must yield a second, distinct name
// rather than colliding with the cache it supersedes.
func TestClusterCacheName(t *testing.T) {
	assert.Equal(t, "7/uid-1", ClusterCacheName(7, "uid-1"))
	assert.NotEqual(t, ClusterCacheName(7, "uid-1"), ClusterCacheName(7, "uid-2"))
	assert.NotEqual(t, ClusterCacheName(7, "uid-1"), ClusterCacheName(8, "uid-1"))
}

// Sync runs only where all three hold: the cluster is tracked, syncing is on, and this
// cache mirrors the identity the cluster is currently probed at. An inactive cache is
// the trap — a UID migration leaves one behind, and it must not keep syncing.
func TestCacheSyncEnabled(t *testing.T) {
	cluster := func(enabled, sync bool, uid string) *beehive.Object[ClusterSpec, ClusterStatus] {
		return &beehive.Object[ClusterSpec, ClusterStatus]{
			Spec:   ClusterSpec{Enabled: enabled, SyncEnabled: sync},
			Status: &ClusterStatus{Server: ClusterServer{UID: &uid}},
		}
	}

	assert.True(t, cacheSyncEnabled(cluster(true, true, "uid-1"), "uid-1"))
	assert.False(t, cacheSyncEnabled(cluster(false, true, "uid-1"), "uid-1"), "the cluster is not tracked")
	assert.False(t, cacheSyncEnabled(cluster(true, false, "uid-1"), "uid-1"), "syncing is paused")
	assert.False(t, cacheSyncEnabled(cluster(true, true, "uid-2"), "uid-1"), "the identity moved on")
	assert.False(t, cacheSyncEnabled(&beehive.Object[ClusterSpec, ClusterStatus]{Spec: ClusterSpec{Enabled: true, SyncEnabled: true}}, "uid-1"), "never probed")
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

// --- Caches() watches ---

// watchCaches opens a cache list watch bounded by the test.
func watchCaches(t *testing.T, d deps) *Stream[ClusterCacheWatchFrame] {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream, err := serviceOver(t, d).Caches().WatchList(ctx)
	require.NoError(t, err)
	return stream
}

// awaitCacheBookmark drains the snapshot up to and including the bookmark.
func awaitCacheBookmark(t *testing.T, stream *Stream[ClusterCacheWatchFrame]) {
	t.Helper()
	for {
		if testutil.Recv(t, stream.Frames, "the bookmark").Type == DeltaFrameBookmark {
			return
		}
	}
}

// The snapshot arrives as Added frames closed by exactly one Bookmark — the frame a
// consumer renders its empty state on, and never before. Each frame carries the owner
// edge the client joins onto its cluster.
func TestCachesWatchListEmitsTheSnapshotThenABookmark(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-2")

	stream := watchCaches(t, d)

	// The snapshot's order is the store's, which no consumer relies on.
	var uids []string
	for range 2 {
		f := testutil.Recv(t, stream.Frames, "a snapshot frame")
		require.Equal(t, DeltaFrameAdded, f.Type)
		require.NotNil(t, f.Cache)
		assert.Equal(t, ObjectRef{ID: ObjectID(cluster.ID), Kind: "Cluster"}, f.Cache.Owner)
		uids = append(uids, f.Cache.Spec.ServerUID)
	}
	assert.ElementsMatch(t, []string{"uid-1", "uid-2"}, uids)

	bookmark := testutil.Recv(t, stream.Frames, "the bookmark closing the snapshot")
	assert.Equal(t, DeltaFrameBookmark, bookmark.Type)
	assert.Nil(t, bookmark.Cache, "the bookmark carries no entity")
}

// An empty collection is definitively empty rather than pending, so the bookmark
// still lands: without it a populated table and an empty one look alike.
func TestCachesWatchListBookmarksAnEmptyCollection(t *testing.T) {
	stream := watchCaches(t, newRunningDeps(t))

	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// A cache created after the snapshot — how a client learns of the identity a probe
// just recorded.
func TestCachesWatchListReportsACreate(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	stream := watchCaches(t, d)
	awaitCacheBookmark(t, stream)

	createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	f := testutil.Recv(t, stream.Frames, "the create")
	assert.Equal(t, DeltaFrameAdded, f.Type)
	require.NotNil(t, f.Cache)
	assert.Equal(t, "uid-1", f.Cache.Spec.ServerUID)
	assert.Equal(t, ObjectRef{ID: ObjectID(cluster.ID), Kind: "Cluster"}, f.Cache.Owner)
}

// pumpCacheFrames runs the watch pump over a snapshot and a hand-driven change stream,
// and collects what it produced. A departure spans two log entries whose grouping is
// beehive's to decide, so driving the pump directly is the only way to pin both shapes.
func pumpCacheFrames(t *testing.T, snapshot []*beehive.Object[ClusterCacheSpec, ClusterCacheStatus], changes ...beehive.ObjectChange[ClusterCacheSpec, ClusterCacheStatus]) []ClusterCacheWatchFrame {
	t.Helper()
	src := make(chan beehive.ObjectChange[ClusterCacheSpec, ClusterCacheStatus], len(changes))
	for _, c := range changes {
		src <- c
	}
	close(src)

	out := make(chan ClusterCacheWatchFrame, len(snapshot)+len(changes)+1)
	require.NoError(t, pumpCacheWatch(context.Background(), out, snapshot, src, func() error { return nil }))
	close(out)

	var frames []ClusterCacheWatchFrame
	for f := range out {
		frames = append(frames, f)
	}
	return frames
}

// A removal whose final row beehive could not decode carries no object, and nothing
// later in the log mentions the id. The frame still has to name it: a consumer drops a
// change with no entity, so the record would sit in its map until the subscription ends.
func TestCachesWatchListReportsAnUndecodableDeparture(t *testing.T) {
	frames := pumpCacheFrames(t, nil,
		beehive.ObjectChange[ClusterCacheSpec, ClusterCacheStatus]{Type: beehive.Deleted, ID: 7, Object: nil},
	)

	require.Len(t, frames, 2)
	assert.Equal(t, DeltaFrameDeleted, frames[1].Type)
	require.NotNil(t, frames[1].Cache, "the bookmark is the only frame that carries no entity")
	assert.Equal(t, ClusterCacheID(7), frames[1].Cache.ID)
}

// A removal reaches the subscriber carrying the row's final state and no owner: beehive
// loads no edges for a collected row, and a frame that failed over that would kill the
// stream and strand the record in the client's map. Driven through the store because
// that load is beehive's to do — a hand-built object proves nothing about it.
//
// The GC interval is shrunk rather than waited out: a cache is client-only here (no
// controller is registered), so its removal costs one sweep.
func TestCachesWatchListReportsADeparture(t *testing.T) {
	d := newRunningDeps(t, beehive.WithGCInterval(time.Millisecond))
	cluster := createCluster(t, d.clusterClient, "prod")
	obj := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	stream := watchCaches(t, d)
	awaitCacheBookmark(t, stream)

	require.NoError(t, d.cacheClient.Delete(context.Background(), obj.ID))

	// The mark and the removal are separate frames unless beehive folds them into one
	// Deleted, which it does when both land in a single tail page.
	for {
		f := testutil.Recv(t, stream.Frames, "the departure")
		require.NotNil(t, f.Cache)
		assert.Equal(t, ClusterCacheID(obj.ID), f.Cache.ID)
		if f.Type == DeltaFrameDeleted {
			assert.Equal(t, "uid-1", f.Cache.Spec.ServerUID, "the removal carries the row's final state")
			assert.Equal(t, ObjectRef{}, f.Cache.Owner, "the owner edge went with the collected row")
			return
		}
	}
}

// watchCache opens a single-record watch bounded by the test.
func watchCache(t *testing.T, d deps, id ClusterCacheID) *Stream[ClusterCacheWatchFrame] {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream, err := serviceOver(t, d).Caches().Watch(ctx, id)
	require.NoError(t, err)
	return stream
}

func TestCachesWatchEmitsTheRecordThenABookmark(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	obj := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	stream := watchCache(t, d, ClusterCacheID(obj.ID))

	first := testutil.Recv(t, stream.Frames, "the record")
	assert.Equal(t, DeltaFrameAdded, first.Type)
	require.NotNil(t, first.Cache)
	assert.Equal(t, ClusterCacheID(obj.ID), first.Cache.ID)
	assert.Equal(t, ObjectRef{ID: ObjectID(cluster.ID), Kind: "Cluster"}, first.Cache.Owner)

	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// An id naming nothing is bookmark-only rather than an error: the record may not
// exist yet, and the same subscription reports it arriving.
func TestCachesWatchBookmarksAnUnknownID(t *testing.T) {
	stream := watchCache(t, newRunningDeps(t), 404)

	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

func TestCachesWatchReportsChangesToItsRecord(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	obj := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	stream := watchCache(t, d, ClusterCacheID(obj.ID))
	awaitCacheBookmark(t, stream)

	require.NoError(t, d.cacheClient.Delete(context.Background(), obj.ID))

	// The type is unpinned: GC can collect the row before this reads, and beehive folds
	// the mark and the removal into one Deleted when both land in a single tail page.
	f := testutil.Recv(t, stream.Frames, "the deletion")
	require.NotNil(t, f.Cache)
	assert.Equal(t, ClusterCacheID(obj.ID), f.Cache.ID)
}

// Cancellation is an ordinary teardown, so Frames closes with nothing to report: a
// consumer reads Err on close, and a reason there is rendered as a dead watch.
func TestCachesWatchListCancellationIsQuiet(t *testing.T) {
	d := newRunningDeps(t)
	ctx, cancel := context.WithCancel(context.Background())

	stream, err := serviceOver(t, d).Caches().WatchList(ctx)
	require.NoError(t, err)
	awaitCacheBookmark(t, stream)

	cancel()

	testutil.WaitClosed(t, stream.Frames, "the frames to close on cancellation")
	assert.NoError(t, stream.Err())
}

// --- ClusterCachedCatalog creation ---

// storedCluster writes a tracked cluster with syncing set as asked, probed at uid.
// Both halves are stored, because the reconcile under test re-reads them.
func storedCluster(t *testing.T, d deps, status *clusterStatusWriter, syncEnabled bool, uid string) *beehive.Object[ClusterSpec, ClusterStatus] {
	t.Helper()
	ctx := context.Background()
	obj := createCluster(t, d.clusterClient, "prod")

	obj, err := d.clusterClient.Update(ctx, obj.ID, ClusterSpec{Enabled: true, SyncEnabled: syncEnabled, Source: obj.Spec.Source})
	require.NoError(t, err)
	status.Set(t, obj.ID, ClusterStatus{Server: ClusterServer{UID: &uid}})
	return obj
}

// cacheControllerClient answers the owner lookup a cache reconcile makes, from the
// store, so the edge a fixture wrote is the one the reconcile reads. ownerID overrides
// it, for a race the store cannot be held in. The embedded interface is nil: Reconcile
// calls nothing else on it.
type cacheControllerClient struct {
	beehive.ControllerClient[ClusterCacheStatus]
	caches       beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	ownerID      *beehive.ObjectID
	dependencies []dependency
}

// dependency is one edge a reconcile declared.
type dependency struct{ from, to beehive.ObjectID }

func (c *cacheControllerClient) AddDependency(_ context.Context, fromID, toID beehive.ObjectID) error {
	c.dependencies = append(c.dependencies, dependency{from: fromID, to: toID})
	return nil
}

func (c *cacheControllerClient) GetOwner(ctx context.Context, id beehive.ObjectID) (beehive.ObjectRef, bool, error) {
	if c.ownerID != nil {
		return beehive.ObjectRef{ID: *c.ownerID, Kind: ClusterGroupKind.Kind}, true, nil
	}
	return c.caches.GetOwner(ctx, id)
}

// reconcileCache runs one cache pass the way beehive would.
func reconcileCache(t *testing.T, d deps, obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) {
	t.Helper()
	res := (&clusterCacheController{deps: d}).Reconcile(context.Background(), &cacheControllerClient{caches: d.cacheClient}, obj)
	require.Equal(t, beehive.Settled(0), res)
}

// A cache owns the discovery anchor beneath it, so the pass that knows the cluster's
// toggles is the one that creates it.
func TestCacheReconcileCreatesTheCatalog(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	reconcileCache(t, d, cache)

	objs := catalogs(t, d.catalogClient)
	require.Len(t, objs, 1)
	assert.Equal(t, ClusterCachedCatalogName(cache.ID), objs[0].Name)
	assert.True(t, objs[0].Spec.Enabled)
}

// The pass writes no status, so the result it returns is the only thing that reports
// it converged. Unsettled, the store keeps every cache owed and beehive's pass re-runs
// this reconcile, store reads and all, every interval forever.
func TestCacheReconcileSettlesTheGeneration(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	client := &cacheControllerClient{caches: d.cacheClient}

	res := (&clusterCacheController{deps: d}).Reconcile(context.Background(), client, cache)

	assert.Equal(t, beehive.Settled(0), res)
}

// The relayed switch is the cluster's, and a write there wakes nothing here on its
// own — so the pass declares the dependency that makes the next relay certain.
func TestCacheReconcileDependsOnItsCluster(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	client := &cacheControllerClient{caches: d.cacheClient}

	(&clusterCacheController{deps: d}).Reconcile(context.Background(), client, cache)

	assert.Equal(t, []dependency{{from: cache.ID, to: cluster.ID}}, client.dependencies)
}

// A cache on its way out is about to be collected with everything it owns, so a pass
// that recreated its anchor would only make work for the GC.
func TestCacheReconcileWritesNothingForADyingCache(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	require.NoError(t, d.cacheClient.Delete(context.Background(), cache.ID))

	obj, err := d.cacheClient.Get(context.Background(), cache.ID)
	require.NoError(t, err)
	require.NotNil(t, obj.DeletionRequestedAt)
	reconcileCache(t, d, obj)

	assert.Empty(t, catalogs(t, d.catalogClient))
}

// A cluster on its way out cascades to this cache next, so its subtree is not worth
// growing.
func TestCacheReconcileWritesNothingForADyingCluster(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	require.NoError(t, d.clusterClient.Delete(context.Background(), cluster.ID))

	reconcileCache(t, d, cache)

	assert.Empty(t, catalogs(t, d.catalogClient))
}

// The cascade can also finish first, collecting the cluster between the owner lookup
// and the read of it — an ordinary race, not a failure to retry under backoff.
func TestCacheReconcileWritesNothingWhenItsClusterIsCollected(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	gone := beehive.ObjectID(9999)

	res := (&clusterCacheController{deps: d}).Reconcile(
		context.Background(),
		&cacheControllerClient{caches: d.cacheClient, ownerID: &gone},
		cache,
	)

	require.Equal(t, beehive.Settled(0), res)
	assert.Empty(t, catalogs(t, d.catalogClient))
}

// An owner edge is what a cache is reconciled against, so one without it has nothing
// to relay.
func TestCacheReconcileWritesNothingWithoutAnOwner(t *testing.T) {
	d, _ := newClusterStatusDeps(t)

	reconcileCache(t, d, &beehive.Object[ClusterCacheSpec, ClusterCacheStatus]{ID: 1})

	assert.Empty(t, catalogs(t, d.catalogClient))
}

// The switch is the cluster's, read off the record rather than the cache's own spec —
// which is why the pass reads its owner at all.
func TestCacheReconcileRelaysThePauseFromItsCluster(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, false, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	reconcileCache(t, d, cache)

	objs := catalogs(t, d.catalogClient)
	require.Len(t, objs, 1)
	assert.False(t, objs[0].Spec.Enabled)
}
