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
	"errors"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubesync"
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

// A removal whose final row beehive could not decode carries no object, and nothing
// later in the log mentions the id. The frame still has to name it: a consumer drops a
// change with no entity, so the record would sit in its map until the subscription ends.
func TestCachesWatchListReportsAnUndecodableDeparture(t *testing.T) {
	frames := pumpFrames(t, cacheWatch, nil,
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
func storedCluster(t *testing.T, d deps, status *beehive.AdminClient[ClusterStatus], syncEnabled bool, uid string) *beehive.Object[ClusterSpec, ClusterStatus] {
	t.Helper()
	ctx := context.Background()
	obj := createCluster(t, d.clusterClient, "prod")

	obj, err := d.clusterClient.Update(ctx, obj.ID, ClusterSpec{Enabled: true, SyncEnabled: syncEnabled, Source: obj.Spec.Source})
	require.NoError(t, err)
	require.NoError(t, status.UpdateStatus(ctx, obj.ID, ClusterStatus{Server: ClusterServer{UID: &uid}}))
	return obj
}

// cacheControllerClient answers the owner lookup a cache reconcile makes, from the
// store, so the edge a fixture wrote is the one the reconcile reads. It is bound to
// the cache being reconciled, the way beehive binds the real one. ownerID overrides
// the lookup, for a race the store cannot be held in. The embedded interface is nil:
// Reconcile calls nothing else on it.
type cacheControllerClient struct {
	beehive.ControllerClient[ClusterCacheStatus]
	caches    beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	id        beehive.ObjectID
	ownerID   *beehive.ObjectID
	dependsOn []beehive.ObjectID
}

func (c *cacheControllerClient) AddDependency(_ context.Context, toID beehive.ObjectID) error {
	c.dependsOn = append(c.dependsOn, toID)
	return nil
}

func (c *cacheControllerClient) GetOwner(ctx context.Context) (beehive.ObjectRef, bool, error) {
	if c.ownerID != nil {
		return beehive.ObjectRef{ID: *c.ownerID, Kind: ClusterGroupKind.Kind}, true, nil
	}
	return c.caches.GetOwner(ctx, c.id)
}

// reconcileCache runs one cache pass the way beehive would.
func reconcileCache(t *testing.T, d deps, obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) {
	t.Helper()
	client := &cacheControllerClient{caches: d.cacheClient, id: obj.ID}
	res := (&clusterCacheController{deps: d}).Reconcile(context.Background(), client, obj)
	require.Equal(t, beehive.Settled(), res)
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
	client := &cacheControllerClient{caches: d.cacheClient, id: cache.ID}

	res := (&clusterCacheController{deps: d}).Reconcile(context.Background(), client, cache)

	assert.Equal(t, beehive.Settled(), res)
}

// The relayed switch is the cluster's, and a write there wakes nothing here on its
// own — so the pass declares the dependency that makes the next relay certain.
func TestCacheReconcileDependsOnItsCluster(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	client := &cacheControllerClient{caches: d.cacheClient, id: cache.ID}

	(&clusterCacheController{deps: d}).Reconcile(context.Background(), client, cache)

	assert.Equal(t, []beehive.ObjectID{cluster.ID}, client.dependsOn)
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

// The cache's file is named for an id that dies with the record, so nothing could
// find it afterwards: the teardown pass is where it is deleted.
func TestCacheReconcileDeletesTheStoreForADyingCache(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	require.NoError(t, d.cacheClient.Delete(context.Background(), cache.ID))

	obj, err := d.cacheClient.Get(context.Background(), cache.ID)
	require.NoError(t, err)
	reconcileCache(t, d, obj)

	assert.Equal(t, []int64{int64(cache.ID)}, kubestoreFake(d).removed)
}

// The kinds below disarm their workers on their own passes, so the cache's teardown
// disarms them itself — a worker outliving the file would be writing through a store
// this pass has closed.
func TestCacheReconcileForgetsItsWorkersBeforeDeletingTheStore(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	require.NoError(t, d.cacheClient.Delete(context.Background(), cache.ID))

	var forgottenFirst bool
	kubestoreFake(d).onRemove = func(int64) {
		forgottenFirst = len(syncFleet(d).forgottenCaches) == 1
	}

	obj, err := d.cacheClient.Get(context.Background(), cache.ID)
	require.NoError(t, err)
	reconcileCache(t, d, obj)

	assert.Equal(t, []int64{int64(cache.ID)}, syncFleet(d).forgottenCaches)
	assert.True(t, forgottenFirst, "the store was deleted with the cache's workers still armed")
}

// A cache that is staying keeps its file — deleting on any other pass would wipe a
// live mirror.
func TestCacheReconcileLeavesTheStoreAloneForALiveCache(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	reconcileCache(t, d, cache)

	assert.Empty(t, kubestoreFake(d).removed)
}

// A store that will not delete fails the pass: the file is the pass's whole job here,
// and settling would leave it on disk with nothing left to name it.
func TestCacheReconcileFailsWhenTheStoreWillNotDelete(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	require.NoError(t, d.cacheClient.Delete(context.Background(), cache.ID))
	kubestoreFake(d).err = errors.New("boom")

	obj, err := d.cacheClient.Get(context.Background(), cache.ID)
	require.NoError(t, err)
	client := &cacheControllerClient{caches: d.cacheClient, id: obj.ID}
	res := (&clusterCacheController{deps: d}).Reconcile(context.Background(), client, obj)

	assert.NotEqual(t, beehive.Settled(), res)
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
		&cacheControllerClient{caches: d.cacheClient, id: cache.ID, ownerID: &gone},
		cache,
	)

	require.Equal(t, beehive.Settled(), res)
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

// Scoped to one cluster: another cluster's cache must not reach this stream, or a
// per-cluster view would fold a record it never asked for.
func TestCachesWatchByClusterIsScopedToItsCluster(t *testing.T) {
	d := newRunningDeps(t)
	mine := createCluster(t, d.clusterClient, "prod")
	theirs := createCluster(t, d.clusterClient, "staging")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream, err := serviceOver(t, d).Caches().WatchByCluster(ctx, ClusterID(mine.ID))
	require.NoError(t, err)
	awaitCacheBookmark(t, stream)

	createCache(t, d.cacheClient, ClusterID(theirs.ID), "uid-2")
	createCache(t, d.cacheClient, ClusterID(mine.ID), "uid-1")

	f := testutil.Recv(t, stream.Frames, "this cluster's cache")
	require.NotNil(t, f.Cache)
	assert.Equal(t, "uid-1", f.Cache.Spec.ServerUID)
	assert.Equal(t, ObjectRef{ID: ObjectID(mine.ID), Kind: "Cluster"}, f.Cache.Owner)
}

// A UID migration leaves the superseded cache in place, so the scoped watch carries
// both — which is what makes "the set, never the cache" true on this stream too.
func TestCachesWatchByClusterCarriesEveryIdentity(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-2")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream, err := serviceOver(t, d).Caches().WatchByCluster(ctx, ClusterID(cluster.ID))
	require.NoError(t, err)

	var uids []string
	for range 2 {
		f := testutil.Recv(t, stream.Frames, "a snapshot frame")
		require.Equal(t, DeltaFrameAdded, f.Type)
		require.NotNil(t, f.Cache)
		uids = append(uids, f.Cache.Spec.ServerUID)
	}
	assert.ElementsMatch(t, []string{"uid-1", "uid-2"}, uids)
	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// A cluster owning none bookmarks an empty collection: beehive does not existence-check
// the owner, so an unprobed cluster and an unknown id behave alike, and either may still
// gain a cache this stream reports.
func TestCachesWatchByClusterBookmarksAClusterWithNone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream, err := serviceOver(t, newRunningDeps(t)).Caches().WatchByCluster(ctx, 404)
	require.NoError(t, err)

	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// --- the gauges ---

// A gauge is current-on-subscribe and emits nothing before its first measurement, so
// the first frame is what the cache holds now.
func TestCachesWatchStatsEmitsTheCurrentMeasurement(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	store := kubestoreFake(d)
	store.stats = kubestore.Stats{Exists: true, Bytes: 4096, Counts: kubestore.Counts{ObjectCount: 12, KindCount: 3}}

	stream, err := serviceOver(t, d).Caches().WatchStats(context.Background(), ClusterID(cluster.ID), ClusterCacheID(cache.ID))

	require.NoError(t, err)
	got := testutil.Recv(t, stream.Frames, "the first measurement")
	assert.Equal(t, ClusterCacheStats{Exists: true, Bytes: 4096, ObjectCount: 12, KindCount: 3}, got)
}

// The gauge borrows its change feed and never claims the file. One runs per cache row, so
// a list of twenty caches would otherwise pin twenty idle files open — and the measurement
// needs no open file anyway, since Manager.Stats reads a closed one directly.
func TestCachesWatchStatsDoesNotHoldAnIdleCacheOpen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	mgr := kubestoreFake(d).mgr

	// A cache that was synced and is now idle: the file exists, nothing holds it.
	writer, err := mgr.OpenOrCreate(int64(cache.ID))
	require.NoError(t, err)
	writer.Release()

	stream, err := serviceOver(t, d).Caches().WatchStats(ctx, ClusterID(cluster.ID), ClusterCacheID(cache.ID))
	require.NoError(t, err)
	testutil.Recv(t, stream.Frames, "the first measurement")

	assert.False(t, cacheIsOpen(mgr, int64(cache.ID)), "the gauge opened an idle cache's file")
}

// A gauge carries no bookmark, so an id pair that names nothing holds silent rather
// than claiming an answer — a caller holding a bad id got it from a watch frame and
// drops the subscription itself.
func TestCachesWatchStatsHoldsSilentForAMismatchedPair(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	other := createCluster(t, d.clusterClient, "staging")
	cache := createCache(t, d.cacheClient, ClusterID(other.ID), "uid-2")
	kubestoreFake(d).stats = kubestore.Stats{Exists: true, Bytes: 4096}

	stream, err := serviceOver(t, d).Caches().WatchStats(context.Background(), ClusterID(cluster.ID), ClusterCacheID(cache.ID))

	require.NoError(t, err)
	testutil.NoRecv(t, stream.Frames, testutil.Timeout/50, "a frame for another cluster's cache")
}

// The gauge re-emits only when the measurement moved: it is re-read on a cadence, and
// a cache at rest must not push a frame per tick to every watcher.
func TestCachesWatchStatsEmitsOnlyOnAChange(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	store := kubestoreFake(d)
	store.stats = kubestore.Stats{Exists: true, Bytes: 4096}

	svc := serviceOver(t, d)
	svc.gaugeCadence = time.Millisecond
	stream, err := svc.Caches().WatchStats(context.Background(), ClusterID(cluster.ID), ClusterCacheID(cache.ID))
	require.NoError(t, err)
	testutil.Recv(t, stream.Frames, "the first measurement")

	testutil.NoRecv(t, stream.Frames, testutil.Timeout/50, "a frame for an unchanged cache")

	store.setStats(kubestore.Stats{Exists: true, Bytes: 8192})

	got := testutil.Recv(t, stream.Frames, "the changed measurement")
	assert.Equal(t, int64(8192), got.Bytes)
}

// The health gauge is a read-side fold over the fleet, grouped by cache: one frame per
// cache, and a cache is only healthy when every kind it syncs is.
func TestCachesWatchHealthFoldsTheFleetByCache(t *testing.T) {
	d := newTestDeps(t)
	fleet := syncFleet(d)
	fleet.observations = []kubesync.SubjectObservation{
		{ID: "a", Params: kubesync.Params{CacheID: 1, APIVersion: "v1", Resource: "pods"},
			Observation: kubesync.Observation{Reason: kubesync.ReasonWatching}, Known: true},
		{ID: "b", Params: kubesync.Params{CacheID: 1, APIVersion: "apps/v1", Resource: "deployments"},
			Observation: kubesync.Observation{Reason: kubesync.ReasonSyncFailed}, Known: true},
		{ID: "c", Params: kubesync.Params{CacheID: 2, APIVersion: "v1", Resource: "pods"},
			Observation: kubesync.Observation{Reason: kubesync.ReasonWatching}, Known: true},
	}

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	byCache := map[ClusterCacheID]ClusterCacheHealth{}
	for range 2 {
		frame := testutil.Recv(t, stream.Frames, "a cache's verdict")
		byCache[frame.CacheID] = frame
	}
	unhealthy := byCache[1]
	assert.Equal(t, ConditionFalse, unhealthy.Status)
	assert.Equal(t, ReasonSyncFailed, unhealthy.Reason)
	assert.Equal(t, 2, unhealthy.TotalKinds)
	assert.Equal(t, 1, unhealthy.UnhealthyKinds)
	assert.Equal(t, []SyncedKindRef{{APIVersion: "apps/v1", Resource: "deployments"}}, unhealthy.UnhealthyKindRefs)
	assert.Equal(t, ConditionTrue, byCache[2].Status)
	assert.Equal(t, ReasonWatching, byCache[2].Reason)
	assert.Empty(t, byCache[2].UnhealthyKindRefs)
}

// The verdict is the most severe reason present: ninety-nine healthy kinds and one
// unreachable is not a cache that is nearly fine.
func TestCachesWatchHealthReportsTheMostSevereReason(t *testing.T) {
	tests := map[string]struct {
		reasons []string
		want    string
	}{
		"identity beats everything": {[]string{kubesync.ReasonStale, kubesync.ReasonIdentityMismatch}, ReasonIdentityMismatch},
		"an outage beats a failure": {[]string{kubesync.ReasonSyncFailed, kubesync.ReasonNoConnection}, ReasonNoConnection},
		"a failure beats staleness": {[]string{kubesync.ReasonStale, kubesync.ReasonSyncFailed}, ReasonSyncFailed},
		"staleness beats building":  {[]string{kubesync.ReasonSyncing, kubesync.ReasonStale}, ReasonStale},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			d := newTestDeps(t)
			fleet := syncFleet(d)
			for i, reason := range tt.reasons {
				fleet.observations = append(fleet.observations, kubesync.SubjectObservation{
					ID:          string(rune('a' + i)),
					Params:      kubesync.Params{CacheID: 1, APIVersion: "v1", Resource: "pods"},
					Observation: kubesync.Observation{Reason: reason},
					Known:       true,
				})
			}

			stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
			require.NoError(t, err)

			assert.Equal(t, tt.want, testutil.Recv(t, stream.Frames, "the cache's verdict").Reason)
		})
	}
}

// A tracked kind with no answer yet is a cache still connecting, not a healthy one.
func TestCachesWatchHealthReportsAnUnansweredKindAsConnecting(t *testing.T) {
	d := newTestDeps(t)
	syncFleet(d).observations = []kubesync.SubjectObservation{
		{ID: "a", Params: kubesync.Params{CacheID: 1, APIVersion: "v1", Resource: "pods"}},
	}

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	frame := testutil.Recv(t, stream.Frames, "the cache's verdict")
	assert.Equal(t, ConditionFalse, frame.Status)
	assert.Equal(t, ReasonConnecting, frame.Reason)
}

// LastLiveAt is the weakest link — a cache is only as verified as its least recently
// proven watch — while LastUpdateAt is the most recent write anywhere in it.
func TestCachesWatchHealthTakesTheOldestProofAndTheNewestWrite(t *testing.T) {
	older := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)
	d := newTestDeps(t)
	syncFleet(d).observations = []kubesync.SubjectObservation{
		{ID: "a", Params: kubesync.Params{CacheID: 1, APIVersion: "v1", Resource: "pods"}, Known: true,
			Observation: kubesync.Observation{Reason: kubesync.ReasonWatching, LastUpdateAt: older, LastLiveAt: newer}},
		{ID: "b", Params: kubesync.Params{CacheID: 1, APIVersion: "apps/v1", Resource: "deployments"}, Known: true,
			Observation: kubesync.Observation{Reason: kubesync.ReasonWatching, LastUpdateAt: newer, LastLiveAt: older}},
	}

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	frame := testutil.Recv(t, stream.Frames, "the cache's verdict")
	require.NotNil(t, frame.LastUpdateAt)
	require.NotNil(t, frame.LastLiveAt)
	assert.Equal(t, newer, *frame.LastUpdateAt)
	assert.Equal(t, older, *frame.LastLiveAt)
}

// --- the clears ---

// Clearing a cache stops its workers before the file goes — a worker still running
// would be writing through a store about to close — and requeues the kinds, whose own
// passes are what re-arm them.
func TestCachesClearStopsTheWorkersThenClearsThenRequeues(t *testing.T) {
	d, cacheID := twoCachesTwoResources(t)
	store := kubestoreFake(d)
	var orderAtClear []string
	store.onClear = func(int64) { orderAtClear = append(orderAtClear, "clear") }
	fleet := syncFleet(d)
	fleet.onForgetCache = func(int64) { orderAtClear = append(orderAtClear, "forget") }

	got, err := serviceOver(t, d).Caches().Clear(context.Background(), cacheID)

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, cacheID, got.ID)
	assert.Equal(t, []int64{int64(cacheID)}, fleet.forgottenCaches)
	assert.Equal(t, []int64{int64(cacheID)}, store.clearedCaches)
	assert.Equal(t, []string{"forget", "clear"}, orderAtClear, "the file went while a worker could still write")
	// Inside the hold, not beside it: a pass arming a worker between the two would leave
	// one watching into the file being emptied.
	assert.Equal(t, []int64{int64(cacheID)}, fleet.heldCaches, "the clear ran outside the hold")
}

// A clear empties the catalog rows too, and nothing else would put them back before the
// sweep's own interval — so it asks for the sweep. After the clear, never before: a wake
// ahead of it would have the sweep write the rows the clear then deletes, leaving the
// table empty for a full interval.
func TestCachesClearWakesTheSweeperAfterTheClear(t *testing.T) {
	d, cacheID := twoCachesTwoResources(t)
	store := kubestoreFake(d)
	var order []string
	store.onClear = func(int64) { order = append(order, "clear") }
	f := sweeper(d)
	f.onWake = func(string) { order = append(order, "wake") }

	_, err := serviceOver(t, d).Caches().Clear(context.Background(), cacheID)
	require.NoError(t, err)

	assert.Equal(t, []string{ClusterCachedCatalogName(beehive.ObjectID(cacheID))}, f.woken)
	assert.Equal(t, []string{"clear", "wake"}, order, "the sweeper was woken into the clear")
}

// The kinds' own passes are what re-arm their workers, so a clear asks for them — and
// a requeue that cannot be delivered costs latency rather than the clear: the kind's
// resync runs the same pass.
func TestCachesClearSurvivesAnUndeliverableRequeue(t *testing.T) {
	d, cacheID := twoCachesTwoResources(t)

	got, err := serviceOver(t, d).Caches().Clear(context.Background(), cacheID)

	require.NoError(t, err, "a requeue with no controller behind it failed the clear")
	assert.NotNil(t, got)
}

func TestCachesClearOfAnUnknownCacheIsNotAnError(t *testing.T) {
	d := newTestDeps(t)

	got, err := serviceOver(t, d).Caches().Clear(context.Background(), 999)

	require.NoError(t, err)
	assert.Nil(t, got)
}

// A clear that fails leaves the cache's workers stopped, so it has to ask the kinds to
// reconcile anyway: their passes are what re-arm them, and without that the cache sits
// unsynced until the resync notices — ten minutes of a cache that looks fine and is not
// being written to.
func TestCachesClearRearmsTheWorkersWhenTheStoreFails(t *testing.T) {
	d, status := newReconcilingDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	catalog := createCatalog(t, d, ClusterCacheID(cache.ID), true)
	createResource(t, d, catalog.ID, deploymentsSpec)
	fleet := syncFleet(d)
	armed := fleet.settle(t)
	kubestoreFake(d).err = errors.New("disk full")

	_, err := serviceOver(t, d).Caches().Clear(context.Background(), ClusterCacheID(cache.ID))

	require.Error(t, err, "the failed clear was reported as a success")
	require.Eventually(t, func() bool {
		return fleet.arms() > armed
	}, testutil.Timeout, time.Millisecond, "the cache's kinds were left disarmed by a failed clear")
}

// storedCacheWithWorker stores a cache record with one healthy kind syncing into it —
// the shape the fold sees in production, where every observation belongs to a record.
func storedCacheWithWorker(t *testing.T, d deps) ClusterCacheID {
	t.Helper()
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	syncFleet(d).observations = []kubesync.SubjectObservation{
		{ID: "a", Params: kubesync.Params{CacheID: int64(cache.ID), APIVersion: "v1", Resource: "pods"},
			Observation: kubesync.Observation{Reason: kubesync.ReasonWatching}, Known: true},
	}
	return ClusterCacheID(cache.ID)
}

// A cache whose kinds are all disarmed has to say so. The gauge is latest-value with no
// departure frame, so falling silent leaves the consumer rendering the last verdict it
// was given — a paused cache reading Watching for as long as its record lives.
func TestCachesWatchHealthReportsACacheThatStoppedSyncing(t *testing.T) {
	d := newTestDeps(t)
	cache := storedCacheWithWorker(t, d)
	fleet := syncFleet(d)
	svc := serviceOver(t, d)
	svc.gaugeCadence = time.Millisecond
	stream, err := svc.Caches().WatchHealth(context.Background())
	require.NoError(t, err)
	require.Equal(t, ConditionTrue, testutil.Recv(t, stream.Frames, "the healthy verdict").Status)

	// Everything under the cache is disarmed — a paused cluster, or a superseded cache.
	fleet.setObservations(nil)

	got := testutil.Recv(t, stream.Frames, "the stopped cache's verdict")
	assert.Equal(t, cache, got.CacheID)
	assert.Equal(t, ConditionFalse, got.Status)
	assert.Equal(t, ReasonPaused, got.Reason)
	assert.Zero(t, got.TotalKinds)
	assert.Empty(t, got.UnhealthyKindRefs)
}

// And it says so once: a cache nobody syncs is not news every tick.
func TestCachesWatchHealthReportsAStoppedCacheOnlyOnce(t *testing.T) {
	d := newTestDeps(t)
	storedCacheWithWorker(t, d)
	fleet := syncFleet(d)
	svc := serviceOver(t, d)
	svc.gaugeCadence = time.Millisecond
	stream, err := svc.Caches().WatchHealth(context.Background())
	require.NoError(t, err)
	testutil.Recv(t, stream.Frames, "the healthy verdict")

	fleet.setObservations(nil)
	testutil.Recv(t, stream.Frames, "the stopped cache's verdict")

	// A negative assertion needs a bounded window, sized against the shrunk cadence.
	testutil.NoRecv(t, stream.Frames, 50*time.Millisecond, "a repeat of the stopped verdict")
}

// A clear stops the cache's workers on its way through, and the gauge must not report
// that as the cache having stopped syncing: a user clearing a cache would watch it flip
// to Paused and back for no reason they took. The hold is what tells the two apart.
func TestCachesWatchHealthHoldsItsVerdictThroughAClear(t *testing.T) {
	d := newTestDeps(t)
	fleet := syncFleet(d)
	fleet.observations = []kubesync.SubjectObservation{
		{ID: "a", Params: kubesync.Params{CacheID: 1, APIVersion: "v1", Resource: "pods"},
			Observation: kubesync.Observation{Reason: kubesync.ReasonWatching}, Known: true},
	}
	svc := serviceOver(t, d)
	svc.gaugeCadence = time.Millisecond
	stream, err := svc.Caches().WatchHealth(context.Background())
	require.NoError(t, err)
	require.Equal(t, ConditionTrue, testutil.Recv(t, stream.Frames, "the healthy verdict").Status)

	// Mid-clear: the workers are stopped and the cache is held.
	fleet.holdCache(1)
	fleet.setObservations(nil)

	// A negative assertion needs a bounded window, sized against the shrunk cadence.
	testutil.NoRecv(t, stream.Frames, 50*time.Millisecond, "a verdict reported mid-clear")
}

// The requeue is what arms the workers a clear stopped, so it cannot ride the request's
// context: a client that hangs up as the clear lands would leave its own cache disarmed
// until the resync notices.
func TestCachesClearRearmsTheWorkersWhenTheCallerHangsUp(t *testing.T) {
	d, status := newReconcilingDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	catalog := createCatalog(t, d, ClusterCacheID(cache.ID), true)
	createResource(t, d, catalog.ID, deploymentsSpec)
	fleet := syncFleet(d)
	armed := fleet.settle(t)

	ctx, cancel := context.WithCancel(context.Background())
	// The caller gives up while the store work is under way.
	kubestoreFake(d).onClear = func(int64) { cancel() }

	_, err := serviceOver(t, d).Caches().Clear(ctx, ClusterCacheID(cache.ID))

	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return fleet.arms() > armed
	}, testutil.Timeout, time.Millisecond, "the cache's kinds were left disarmed by a cancelled caller")
}

// The fleet's hub closes with the process, and a closed channel selects forever: a gauge
// that read it as a wake would fold at full speed until its own context ended.
func TestCachesWatchHealthEndsWhenTheFleetCloses(t *testing.T) {
	d := newTestDeps(t)
	fleet := syncFleet(d)
	fleet.observations = []kubesync.SubjectObservation{
		{ID: "a", Params: kubesync.Params{CacheID: 1, APIVersion: "v1", Resource: "pods"},
			Observation: kubesync.Observation{Reason: kubesync.ReasonWatching}, Known: true},
	}
	svc := serviceOver(t, d)
	svc.gaugeCadence = time.Hour // only the fleet's signal can wake this fold
	stream, err := svc.Caches().WatchHealth(context.Background())
	require.NoError(t, err)
	testutil.Recv(t, stream.Frames, "the first verdict")

	fleet.closeHub()

	testutil.WaitClosed(t, stream.Frames, "the stream to end with the fleet")
}

// The weakest link is a kind with no proof at all: another kind's stamp cannot vouch for
// it. A cache holding one unproven watch is a cache nothing has verified, whatever its
// neighbours have seen.
func TestCachesWatchHealthReportsNoProofWhenAKindHasNone(t *testing.T) {
	proven := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	d := newTestDeps(t)
	syncFleet(d).observations = []kubesync.SubjectObservation{
		{ID: "a", Params: kubesync.Params{CacheID: 1, APIVersion: "v1", Resource: "pods"}, Known: true,
			Observation: kubesync.Observation{Reason: kubesync.ReasonWatching, LastUpdateAt: proven, LastLiveAt: proven}},
		{ID: "b", Params: kubesync.Params{CacheID: 1, APIVersion: "apps/v1", Resource: "deployments"}, Known: true,
			Observation: kubesync.Observation{Reason: kubesync.ReasonWatching}},
	}

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	frame := testutil.Recv(t, stream.Frames, "the cache's verdict")
	assert.Nil(t, frame.LastLiveAt, "an unproven watch was vouched for by its neighbour")
	// A write is a write, though: that one is the newest across the cache, not a claim
	// about any watch.
	require.NotNil(t, frame.LastUpdateAt)
	assert.Equal(t, proven, *frame.LastUpdateAt)
}

// A subscriber that arrives after a cache was paused has to hear about it: the fold
// covers only caches with workers, and a verdict this subscription never witnessed the
// transition into is still the verdict.
func TestCachesWatchHealthReportsACacheThatWasAlreadyPaused(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the paused cache's verdict")
	assert.Equal(t, ClusterCacheID(cache.ID), got.CacheID)
	assert.Equal(t, ConditionFalse, got.Status)
	assert.Equal(t, ReasonPaused, got.Reason)
	assert.Zero(t, got.TotalKinds)
}

// A cache being collected is not a cache that stopped syncing: its record says it is
// going, and a verdict for it would be noise a consumer has to unpick.
func TestCachesWatchHealthSkipsACacheOnItsWayOut(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	require.NoError(t, d.cacheClient.Delete(context.Background(), cache.ID))

	svc := serviceOver(t, d)
	svc.gaugeCadence = time.Millisecond
	stream, err := svc.Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	// A negative assertion needs a bounded window, sized against the shrunk cadence.
	testutil.NoRecv(t, stream.Frames, 50*time.Millisecond, "a verdict for a cache being collected")
}

// A cache with no workers is not necessarily a paused one: an enabled cluster's cache
// has none until its catalog pass has run, and one that serves no kinds never will.
// Calling that Paused would show a user their own enabled cluster as switched off.
func TestCachesWatchHealthTellsAnInitializingCacheFromAPausedOne(t *testing.T) {
	tests := map[string]struct {
		syncEnabled bool
		want        string
	}{
		"still initializing": {syncEnabled: true, want: ReasonConnecting},
		"switched off":       {syncEnabled: false, want: ReasonPaused},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			d, status := newClusterStatusDeps(t)
			cluster := storedCluster(t, d, status, tt.syncEnabled, "uid-1")
			createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

			stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
			require.NoError(t, err)

			got := testutil.Recv(t, stream.Frames, "the workerless cache's verdict")
			assert.Equal(t, ConditionFalse, got.Status)
			assert.Equal(t, tt.want, got.Reason)
		})
	}
}
