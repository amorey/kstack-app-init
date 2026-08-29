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
	"slices"
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

// --- clusterCacheController ---

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

// cacheControllerClient stands in for the client beehive binds to the object being
// reconciled. The embedded interface is nil: nothing the pass calls goes unimplemented here,
// so a method it grows shows up as a panic rather than as silence.
type cacheControllerClient struct {
	beehive.ControllerClient[ClusterCacheStatus]
	events       []beehive.EventSpec
	dependencies []beehive.ObjectID
}

func (c *cacheControllerClient) AddDependency(_ context.Context, toID beehive.ObjectID) error {
	c.dependencies = append(c.dependencies, toID)
	return nil
}

func (c *cacheControllerClient) AddEvent(_ context.Context, event beehive.EventSpec) error {
	c.events = append(c.events, event)
	return nil
}

// reconcileCache runs one cache pass the way beehive would.
func reconcileCache(t *testing.T, d deps, obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) *cacheControllerClient {
	t.Helper()
	client := &cacheControllerClient{}
	res := (&clusterCacheController{deps: d}).Reconcile(context.Background(), client, obj)
	require.Equal(t, beehive.Settled(), res)
	return client
}

// The pass writes no status, so the result it returns is the only thing that reports
// it converged. Unsettled, the store keeps every cache owed and beehive's pass re-runs
// this reconcile, store reads and all, every interval forever.
func TestCacheReconcileSettlesTheGeneration(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	res := (&clusterCacheController{deps: d}).Reconcile(context.Background(), &cacheControllerClient{}, cache)

	assert.Equal(t, beehive.Settled(), res)
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
	res := (&clusterCacheController{deps: d}).Reconcile(context.Background(), &cacheControllerClient{}, obj)

	assert.NotEqual(t, beehive.Settled(), res)
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

// --- the clears ---

func TestCachesClearOfAnUnknownCacheIsNotAnError(t *testing.T) {
	d := newTestDeps(t)

	got, err := serviceOver(t, d).Caches().Clear(context.Background(), 999)

	require.NoError(t, err)
	assert.Nil(t, got)
}

// A subscriber that arrives after a cache was paused has to hear about it: a verdict
// this subscription never witnessed the transition into is still the verdict.
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
// has none until its discovery pass has run, and one that serves no kinds never will.
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

// --- arming the sync ---

// syncFake is the sync seam a fixture wired, for a test that drives or reads it.
func syncFake(d deps) *fakeKubesync { return d.kubesyncSvc.(*fakeKubesync) }

// writeCatalog is a sweep having landed: the kinds a cluster serves, on disk in the cache's
// own file, which is where the mirror pass reads its desired set.
func writeCatalog(t *testing.T, d deps, cacheID ClusterCacheID, rows ...kubestore.KindRow) {
	t.Helper()
	store, ok, err := d.kubestoreMgr.OpenExisting(int64(cacheID))
	require.NoError(t, err)
	require.True(t, ok)
	defer store.Release()
	require.NoError(t, store.SyncKinds(context.Background(), rows, true, uint64(len(rows)+1)))
}

// kindRow is one served kind as the sweep writes it.
func kindRow(apiVersion, kind, resource string) kubestore.KindRow {
	return kubestore.KindRow{APIVersion: apiVersion, Kind: kind, Resource: resource, Scope: "Namespaced"}
}

// recordedKinds is the set of kind records under a cache, by name.
func recordedKinds(t *testing.T, d deps, cacheID ClusterCacheID) []string {
	t.Helper()
	objs, err := d.kindClient.ListOwnedObjects(context.Background(), beehive.ObjectID(cacheID))
	require.NoError(t, err)
	names := make([]string, 0, len(objs))
	for _, obj := range objs {
		if obj.DeletionRequestedAt == nil {
			names = append(names, obj.Name)
		}
	}
	slices.Sort(names)
	return names
}

// Arming is the cache pass's, off the switch it already computes. Nothing a reader does starts
// a sync, and the params are what every worker under the cache dials over.
func TestCachePassArmsTheSyncWhileTheSwitchHolds(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	reconcileCache(t, d, cache)

	params, armed := syncFake(d).armedDiscovery(int64(cache.ID))
	require.True(t, armed, "a cache whose switch holds is armed")
	assert.Equal(t, kubesync.Params{ContextName: "prod", ServerUID: "uid-1"}, params)
}

// Pausing is one call and no record written: the kinds stay registered under kubesync, so a
// resume starts every one of them again with nothing requeued.
func TestCachePassDisarmsTheSyncWhenTheSwitchIsOff(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	reconcileCache(t, d, cache)

	_, err := d.clusterClient.Update(context.Background(), cluster.ID,
		ClusterSpec{Enabled: true, SyncEnabled: false, Source: cluster.Spec.Source})
	require.NoError(t, err)
	reconcileCache(t, d, cache)

	_, armed := syncFake(d).armedDiscovery(int64(cache.ID))
	assert.False(t, armed, "a paused cache syncs nothing")
}

// A cache whose identity the cluster has moved off is not the one being probed, so nothing
// under it may write.
func TestCachePassDisarmsACacheTheClusterNoLongerAnswersAs(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	reconcileCache(t, d, cache)

	other := "uid-2"
	require.NoError(t, status.UpdateStatus(context.Background(), cluster.ID, ClusterStatus{Server: ClusterServer{UID: &other}}))
	reconcileCache(t, d, cache)

	_, armed := syncFake(d).armedDiscovery(int64(cache.ID))
	assert.False(t, armed, "a cache the cluster has migrated off syncs nothing")
}

// Forgetting returns only once nothing can still write through the cache's store, which is
// exactly what the file being deleted needs.
func TestCachePassForgetsTheSyncBeforeItRemovesTheStore(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	reconcileCache(t, d, cache)

	var armedAtRemoval bool
	kubestoreFake(d).onRemove = func(int64) {
		_, armedAtRemoval = syncFake(d).armedDiscovery(int64(cache.ID))
	}
	require.NoError(t, d.cacheClient.Delete(context.Background(), cache.ID))
	obj, err := d.cacheClient.Get(context.Background(), cache.ID)
	require.NoError(t, err)
	reconcileCache(t, d, obj)

	assert.False(t, armedAtRemoval, "nothing is still syncing when the file goes")
}

// A cache on its way out is deleted, not paused. Pausing keeps every kind registered so a
// resume is one call; a teardown that kept them would leak a cache's worth on every delete.
func TestCachePassForgetsTheKindsWithTheCache(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	reconcileCache(t, d, cache)
	syncFake(d).TrackKind(int64(cache.ID), kubestore.Kind{APIVersion: "v1", Kind: "Pod", Resource: "pods"})

	require.NoError(t, d.cacheClient.Delete(context.Background(), cache.ID))
	obj, err := d.cacheClient.Get(context.Background(), cache.ID)
	require.NoError(t, err)
	reconcileCache(t, d, obj)

	assert.Empty(t, syncFake(d).armedKinds(int64(cache.ID)),
		"nothing under a deleted cache stays registered")
}

// --- mirroring the catalog ---

// The desired set is what the cluster serves, read back off disk; a record is what turns one
// of those into a kind that is actually mirrored.
func TestCachePassMirrorsTheCatalogIntoRecords(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	reconcileCache(t, d, cache)
	writeCatalog(t, d, ClusterCacheID(cache.ID),
		kindRow("v1", "Pod", "pods"), kindRow("apps/v1", "Deployment", "deployments"))

	reconcileCache(t, d, cache)

	assert.Equal(t, []string{
		ClusterCachedKindName(cache.ID, "apps/v1", "deployments"),
		ClusterCachedKindName(cache.ID, "v1", "pods"),
	}, recordedKinds(t, d, ClusterCacheID(cache.ID)))
}

// A kind's spec carries data outside its name — the singular, and whether it is namespaced —
// so a renamed or re-scoped kind converges in place rather than being recreated under a name
// it already holds.
func TestCachePassConvergesARenamedKindInPlace(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	reconcileCache(t, d, cache)
	writeCatalog(t, d, ClusterCacheID(cache.ID), kindRow("v1", "Pod", "pods"))
	reconcileCache(t, d, cache)

	writeCatalog(t, d, ClusterCacheID(cache.ID), kindRow("v1", "PodThing", "pods"))
	reconcileCache(t, d, cache)

	obj, err := d.kindClient.GetByName(context.Background(), ClusterCachedKindName(cache.ID, "v1", "pods"))
	require.NoError(t, err)
	assert.Equal(t, "PodThing", obj.Spec.Kind, "the singular converges under the name it already holds")
}

// A record with no row is a kind the cluster has stopped serving. Marked, not collected: the
// record's own pass is what clears the rows behind it.
func TestCachePassMarksARecordWithNoRow(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	reconcileCache(t, d, cache)
	writeCatalog(t, d, ClusterCacheID(cache.ID), kindRow("v1", "Pod", "pods"), kindRow("v1", "Node", "nodes"))
	reconcileCache(t, d, cache)

	writeCatalog(t, d, ClusterCacheID(cache.ID), kindRow("v1", "Pod", "pods"))
	reconcileCache(t, d, cache)

	assert.Equal(t, []string{ClusterCachedKindName(cache.ID, "v1", "pods")},
		recordedKinds(t, d, ClusterCacheID(cache.ID)))
}

// **"Never swept" is not "serves nothing."** A table with no fingerprint has never had an
// answer written to it, and only the first of those may delete records — otherwise a cluster
// that is merely unreachable loses its whole record set for the length of the outage.
func TestCachePassPrunesNothingBeforeAnySweep(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	reconcileCache(t, d, cache)
	writeCatalog(t, d, ClusterCacheID(cache.ID), kindRow("v1", "Pod", "pods"))
	reconcileCache(t, d, cache)

	// The file goes and comes back empty, which is what a clear leaves behind.
	require.NoError(t, d.kubestoreMgr.Clear(int64(cache.ID)))
	reconcileCache(t, d, cache)

	assert.Equal(t, []string{ClusterCachedKindName(cache.ID, "v1", "pods")},
		recordedKinds(t, d, ClusterCacheID(cache.ID)), "an unswept table deletes nothing")
}

// A pass that runs before any sweep finds no file rather than a fresh empty one, so it reads
// no rows and prunes nothing — and creates nothing the cache's teardown would have to undo.
func TestCachePassReadsNoCatalogWhenThereIsNoFile(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	kubestoreFake(d).noFile = true

	reconcileCache(t, d, cache)

	assert.Empty(t, recordedKinds(t, d, ClusterCacheID(cache.ID)))
}

// --- the discovery timeline ---

// A verdict that moved is an event on the cache's own timeline. Repeating a run's
// (Category, Type, Reason) extends that run rather than appending, so a flapping sweep costs
// one row per transition.
func TestCachePassLogsTheDiscoveryVerdict(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	syncFake(d).setDiscoveryState(int64(cache.ID), kubesync.DiscoveryState{
		Reason: kubesync.ReasonDiscoveryFailed, Message: "the /apis document would not load",
	})

	client := reconcileCache(t, d, cache)

	require.Len(t, client.events, 1)
	assert.Equal(t, categoryDiscovery, client.events[0].Category)
	assert.Equal(t, kubesync.ReasonDiscoveryFailed, client.events[0].Reason)
	assert.Equal(t, "the /apis document would not load", client.events[0].Message)
}

// A suspended session is the CLUSTER's fact, already on its own timeline; logging it per cache
// is the same news twice.
func TestCachePassLogsNoDiscoveryEventForALostConnection(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	syncFake(d).setDiscoveryState(int64(cache.ID), kubesync.DiscoveryState{Reason: kubesync.ReasonNoConnection})

	client := reconcileCache(t, d, cache)

	assert.Empty(t, client.events)
}

// Nothing has answered for a cache whose sweep has not run, and that is not a verdict to log.
func TestCachePassLogsNothingBeforeASweepAnswers(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	client := reconcileCache(t, d, cache)

	assert.Empty(t, client.events)
}

// A clear swaps the file under whoever holds it open, so the workers writing through it must
// be down for the whole swap. Only kubesync can stop them, so the clear runs inside its hold.
func TestCachesClearRunsInsideTheSyncsHold(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	var heldAtClear bool
	kubestoreFake(d).onClear = func(int64) {
		heldAtClear = slices.Contains(syncFake(d).stoppedCaches(), int64(cache.ID))
	}
	_, err := serviceOver(t, d).Caches().Clear(context.Background(), ClusterCacheID(cache.ID))

	require.NoError(t, err)
	assert.True(t, heldAtClear, "the cache's workers are stopped across the swap")
}

// --- the health fold ---

// syncingCacheWithKinds arms a cache with two kinds and hands back the ids, so a health test
// says only what it is about.
func syncingCacheWithKinds(t *testing.T) (deps, ClusterCacheID) {
	t.Helper()
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	createKind(t, d, cache.ID, podsSpec)
	createKind(t, d, cache.ID, deploymentsSpec)
	return d, ClusterCacheID(cache.ID)
}

// setKindReason is one kind's worker having settled somewhere.
func setKindReason(d deps, cacheID ClusterCacheID, spec ClusterCachedKindSpec, state kubesync.KindState) {
	syncFake(d).setKindState(int64(cacheID), toKubestoreKind(spec), state)
}

// The verdict a cache stands behind is every kind under it folded together: ninety-nine
// healthy kinds and one forbidden CRD is not healthy, which is why no single child can serve
// this and why the fold happens here.
func TestCachesWatchHealthFoldsEveryKindsVerdict(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	now := time.Now()
	setKindReason(d, cacheID, podsSpec, kubesync.KindState{
		Reason: kubesync.ReasonWatching, Live: true, LastUpdateAt: now, LastLiveAt: now,
	})
	setKindReason(d, cacheID, deploymentsSpec, kubesync.KindState{
		Reason: kubesync.ReasonSyncFailed, LastLiveAt: now.Add(-time.Hour),
	})

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the cache's folded verdict")
	assert.Equal(t, ConditionFalse, got.Status)
	assert.Equal(t, kubesync.ReasonSyncFailed, got.Reason)
	assert.Equal(t, 2, got.TotalKinds)
	assert.Equal(t, 1, got.UnhealthyKinds)
	assert.Equal(t, []SyncedKindRef{{APIVersion: "apps/v1", Resource: "deployments"}}, got.UnhealthyKindRefs)
}

// Every kind watching is the one verdict a cache can report as healthy.
func TestCachesWatchHealthReportsAFullySyncedCache(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	now := time.Now()
	for _, spec := range []ClusterCachedKindSpec{podsSpec, deploymentsSpec} {
		setKindReason(d, cacheID, spec, kubesync.KindState{
			Reason: kubesync.ReasonWatching, Live: true, LastUpdateAt: now, LastLiveAt: now,
		})
	}

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the healthy cache's verdict")
	assert.Equal(t, ConditionTrue, got.Status)
	assert.Equal(t, kubesync.ReasonWatching, got.Reason)
	assert.Empty(t, got.UnhealthyKindRefs)
	require.NotNil(t, got.LastUpdateAt)
	require.NotNil(t, got.LastLiveAt)
}

// LastUpdateAt is the most recent write anywhere in the cache; LastLiveAt the OLDEST proof
// across every kind, because a cache is only as verified as its least proven watch.
func TestCachesWatchHealthTakesTheNewestUpdateAndTheOldestProof(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	now := time.Now().Truncate(time.Second)
	older, newer := now.Add(-time.Hour), now
	setKindReason(d, cacheID, podsSpec, kubesync.KindState{
		Reason: kubesync.ReasonWatching, Live: true, LastUpdateAt: older, LastLiveAt: older,
	})
	setKindReason(d, cacheID, deploymentsSpec, kubesync.KindState{
		Reason: kubesync.ReasonWatching, Live: true, LastUpdateAt: newer, LastLiveAt: newer,
	})

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the cache's stamps")
	require.NotNil(t, got.LastUpdateAt)
	require.NotNil(t, got.LastLiveAt)
	assert.True(t, got.LastUpdateAt.Equal(newer), "the newest write anywhere in the cache")
	assert.True(t, got.LastLiveAt.Equal(older), "the least proven watch")
}

// A kind nothing has proven live is weaker than any stamp its neighbours carry, so the cache
// reports no proof at all rather than one kind's.
func TestCachesWatchHealthReportsNoProofWhileAKindHasNone(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	now := time.Now()
	setKindReason(d, cacheID, podsSpec, kubesync.KindState{
		Reason: kubesync.ReasonWatching, Live: true, LastUpdateAt: now, LastLiveAt: now,
	})
	setKindReason(d, cacheID, deploymentsSpec, kubesync.KindState{Reason: kubesync.ReasonSyncing})

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the cache's stamps")
	assert.Nil(t, got.LastLiveAt)
}

// The same, whichever kind the list reaches first: a missing proof is a fact about the cache,
// not a value the next kind's stamp can outrank.
func TestCachesWatchHealthReportsNoProofWhenTheUnprovenKindIsFirst(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	now := time.Now()
	setKindReason(d, cacheID, podsSpec, kubesync.KindState{Reason: kubesync.ReasonSyncing})
	setKindReason(d, cacheID, deploymentsSpec, kubesync.KindState{
		Reason: kubesync.ReasonWatching, Live: true, LastUpdateAt: now, LastLiveAt: now,
	})

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the cache's stamps")
	assert.Nil(t, got.LastLiveAt)
}

// A kind that has answered nothing has proved nothing either, so it withholds the cache's
// proof exactly as an answered-but-unproven one does.
func TestCachesWatchHealthReportsNoProofWhileAKindHasNotAnswered(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	now := time.Now()
	setKindReason(d, cacheID, podsSpec, kubesync.KindState{
		Reason: kubesync.ReasonWatching, Live: true, LastUpdateAt: now, LastLiveAt: now,
	})

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the cache's stamps")
	assert.Nil(t, got.LastLiveAt)
}

// **No answer is not an empty answer.** A kind whose worker has committed nothing — a clear
// in progress, a cache still starting — is not a kind that stopped syncing, so the cache
// reads as still connecting rather than as unhealthy.
func TestCachesWatchHealthReadsAKindWithNoAnswerAsConnecting(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	setKindReason(d, cacheID, podsSpec, kubesync.KindState{Reason: kubesync.ReasonWatching, Live: true})

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the cache's verdict")
	assert.Equal(t, ReasonConnecting, got.Reason)
	assert.Empty(t, got.UnhealthyKindRefs, "a kind that has not answered is not an offender")
}

// --- the per-cache sync detail ---

// The detail gauge is the only thing on the wire that can carry a per-kind verdict, so it
// serves the discovery reason and a row per mirrored kind together.
func TestCachesWatchSyncStatusServesTheDiscoveryReasonAndEveryKind(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	cluster, _, err := d.cacheClient.GetOwner(context.Background(), beehive.ObjectID(cacheID))
	require.NoError(t, err)
	syncFake(d).setDiscoveryState(int64(cacheID), kubesync.DiscoveryState{
		Reason: kubesync.ReasonPartial, Message: "metrics.k8s.io/v1beta1 would not load",
	})
	setKindReason(d, cacheID, podsSpec, kubesync.KindState{Reason: kubesync.ReasonWatching, Live: true})
	setKindReason(d, cacheID, deploymentsSpec, kubesync.KindState{
		Reason: kubesync.ReasonSyncFailed, Message: "forbidden", Restarts: 3,
	})

	stream, err := serviceOver(t, d).Caches().WatchSyncStatus(context.Background(), ClusterID(cluster.ID), cacheID)
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the cache's sync detail")
	assert.Equal(t, kubesync.ReasonPartial, got.Discovery.Reason)
	assert.Equal(t, "metrics.k8s.io/v1beta1 would not load", got.Discovery.Message)
	require.Len(t, got.Kinds, 2)
	assert.Equal(t, "apps/v1", got.Kinds[0].APIVersion)
	assert.Equal(t, kubesync.ReasonSyncFailed, got.Kinds[0].Reason)
	assert.Equal(t, 3, got.Kinds[0].Restarts)
	assert.Equal(t, "v1", got.Kinds[1].APIVersion)
	assert.Equal(t, kubesync.ReasonWatching, got.Kinds[1].Reason)
}

// The count comes off the store, never off the seam: kubesync knows only the caches it has
// armed, where kubestore answers for a paused one too.
// The gauge re-emits only when the detail moved: it is re-read on a cadence, and an idle
// cache pushing a frame per tick would re-render the panel over an answer nothing changed.
func TestCachesWatchSyncStatusEmitsOnlyOnAChange(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	cluster, _, err := d.cacheClient.GetOwner(context.Background(), beehive.ObjectID(cacheID))
	require.NoError(t, err)
	setKindReason(d, cacheID, podsSpec, kubesync.KindState{Reason: kubesync.ReasonWatching, Live: true})

	svc := serviceOver(t, d)
	svc.gaugeCadence = time.Millisecond
	// Ended before the temp dir goes: at this cadence the pump reopens the cache's file
	// every tick, and one landing mid-cleanup fails the test on a directory it refilled.
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := svc.Caches().WatchSyncStatus(ctx, ClusterID(cluster.ID), cacheID)
	require.NoError(t, err)
	defer func() {
		cancel()
		testutil.WaitClosed(t, stream.Frames, "the gauge to stop")
	}()
	testutil.Recv(t, stream.Frames, "the first detail")

	// A negative assertion has no event to wait for, so it takes a bounded window — many
	// cadences wide, and failing the moment a second identical frame arrives.
	testutil.NoRecv(t, stream.Frames, testutil.Timeout/50, "a frame for an unchanged cache")

	setKindReason(d, cacheID, deploymentsSpec, kubesync.KindState{
		Reason: kubesync.ReasonSyncFailed, Message: "forbidden",
	})

	got := testutil.Recv(t, stream.Frames, "the changed detail")
	assert.Equal(t, kubesync.ReasonSyncFailed, got.Kinds[0].Reason)
}

func TestCachesWatchSyncStatusCountsRowsFromTheStore(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	cluster, _, err := d.cacheClient.GetOwner(context.Background(), beehive.ObjectID(cacheID))
	require.NoError(t, err)
	writeCatalog(t, d, cacheID, kindRow("v1", "Pod", "pods"))

	stream, err := serviceOver(t, d).Caches().WatchSyncStatus(context.Background(), ClusterID(cluster.ID), cacheID)
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the cache's sync detail")
	require.Len(t, got.Kinds, 2)
	assert.Equal(t, 0, got.Kinds[1].ObjectCount, "a kind the catalog knows and nothing has cached")
}

// A pair naming no live cache holds silent rather than claiming an answer, the way every
// gauge in this family does: the caller held a bad id and drops the subscription itself.
func TestCachesWatchSyncStatusHoldsSilentForAMismatchedPair(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)

	stream, err := serviceOver(t, d).Caches().WatchSyncStatus(context.Background(), 999, cacheID)
	require.NoError(t, err)

	testutil.NoRecv(t, stream.Frames, 50*time.Millisecond, "a verdict for a cache that is not this cluster's")
}

// **A relayed value needs a depends_on edge; the owner edge is not one.** The switch this pass
// reads lives on the cluster, and owning a child wakes nothing — so without this a paused
// cluster's cache keeps syncing until something unrelated wakes it.
func TestCachePassDependsOnTheClusterWhoseSwitchItReads(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	client := reconcileCache(t, d, cache)

	assert.Equal(t, []beehive.ObjectID{cluster.ID}, client.dependencies)
}
