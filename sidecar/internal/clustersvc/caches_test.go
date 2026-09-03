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
	"strconv"
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
	conditions   []Condition
	deleted      []string
	// dependErr and eventErr fail the two writes a pass makes through this client, which
	// is the only way to reach what a pass does when one of them will not land.
	dependErr error
	eventErr  error
}

func (c *cacheControllerClient) AddDependency(_ context.Context, toID beehive.ObjectID) error {
	c.dependencies = append(c.dependencies, toID)
	return c.dependErr
}

func (c *cacheControllerClient) SetCondition(_ context.Context, cond Condition) error {
	c.conditions = append(c.conditions, cond)
	return nil
}

func (c *cacheControllerClient) DeleteCondition(_ context.Context, conditionType string) error {
	c.deleted = append(c.deleted, conditionType)
	return nil
}

func (c *cacheControllerClient) AddEvent(_ context.Context, event beehive.EventSpec) error {
	c.events = append(c.events, event)
	return c.eventErr
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
	store.stats = kubestore.Stats{
		Exists: true, DBBytes: 4096, WALBytes: 512, SHMBytes: 32,
		Counts: kubestore.Counts{ObjectCount: 12, KindCount: 3},
	}

	stream, err := serviceOver(t, d).Caches().WatchStats(context.Background(), ClusterID(cluster.ID), ClusterCacheID(cache.ID))

	require.NoError(t, err)
	got := testutil.Recv(t, stream.Frames, "the first measurement")
	// The parts ride along with the total: the headline is what the cache costs, and the split
	// is what says whether the WAL is being checkpointed.
	assert.Equal(t, ClusterCacheStats{
		Exists: true, Bytes: 4640, DBBytes: 4096, WALBytes: 512, SHMBytes: 32,
		ObjectCount: 12, KindCount: 3,
	}, got)
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
	kubestoreFake(d).stats = kubestore.Stats{Exists: true, DBBytes: 4096}

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
	store.stats = kubestore.Stats{Exists: true, DBBytes: 4096}

	svc := serviceOver(t, d)
	svc.gaugeCadence = time.Millisecond
	stream, err := svc.Caches().WatchStats(context.Background(), ClusterID(cluster.ID), ClusterCacheID(cache.ID))
	require.NoError(t, err)
	testutil.Recv(t, stream.Frames, "the first measurement")

	testutil.NoRecv(t, stream.Frames, testutil.Timeout/50, "a frame for an unchanged cache")

	store.setStats(kubestore.Stats{Exists: true, DBBytes: 8192})

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

// **The catalog owns four fields; the user owns one.** The sweep writes on a schedule, so
// a pass that wrote the whole desired spec would un-pause a kind within one discovery
// interval — and silently, since nothing else moves.
func TestCachePassLeavesAPausedKindPaused(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	reconcileCache(t, d, cache)
	writeCatalog(t, d, ClusterCacheID(cache.ID), kindRow("v1", "Pod", "pods"))
	reconcileCache(t, d, cache)
	pauseKind(t, d, cache.ID, "v1", "pods")

	reconcileCache(t, d, cache)

	assert.True(t, storedKind(t, d, cache.ID, "v1", "pods").Spec.Paused,
		"the sweep writes nothing when no catalog field moved")
}

// The other half of the ownership split: a catalog change is the sweep's to converge, and
// it converges the four fields it owns WITHOUT taking the fifth back.
func TestCachePassCarriesAPauseThroughACatalogChange(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	reconcileCache(t, d, cache)
	writeCatalog(t, d, ClusterCacheID(cache.ID), kindRow("v1", "Pod", "pods"))
	reconcileCache(t, d, cache)
	pauseKind(t, d, cache.ID, "v1", "pods")

	writeCatalog(t, d, ClusterCacheID(cache.ID), kindRow("v1", "PodThing", "pods"))
	reconcileCache(t, d, cache)

	obj := storedKind(t, d, cache.ID, "v1", "pods")
	assert.Equal(t, "PodThing", obj.Spec.Kind, "the singular converges")
	assert.True(t, obj.Spec.Paused, "and the user's switch rides through it")
}

// The pass lists the records BEFORE it takes the lock, so a pause landing in that window is
// already stored and missing from the snapshot. The carry-forward has to read the record
// rather than the snapshot, or a catalog change arriving alongside writes the user's
// mutation away — with both writers ostensibly sharing the mutex.
//
// Driven through upsertKinds directly, with the snapshot taken before the pause: that is
// the race's outcome, without a race to reproduce.
func TestCachePassCarriesAPauseItsSnapshotMissed(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	reconcileCache(t, d, cache)
	writeCatalog(t, d, ClusterCacheID(cache.ID), kindRow("v1", "Pod", "pods"))
	reconcileCache(t, d, cache)

	ctx := context.Background()
	c := &clusterCacheController{deps: d}
	stale, err := c.storedKinds(ctx, cache.ID)
	require.NoError(t, err)
	pauseKind(t, d, cache.ID, "v1", "pods")

	name := ClusterCachedKindName(cache.ID, "v1", "pods")
	desired := map[string]ClusterCachedKindSpec{
		name: {APIVersion: "v1", Kind: "PodThing", Resource: "pods", Namespaced: true},
	}
	require.NoError(t, c.upsertKinds(ctx, cache.ID, desired, stale))

	obj := storedKind(t, d, cache.ID, "v1", "pods")
	assert.Equal(t, "PodThing", obj.Spec.Kind, "the singular converges")
	assert.True(t, obj.Spec.Paused, "and the pause the snapshot missed rides through it")
}

// pauseKind sets one record's switch the way the mutation will, so the sweep tests can
// stand a pause up before any setter exists.
func pauseKind(t *testing.T, d deps, cacheID beehive.ObjectID, apiVersion, resource string) {
	t.Helper()
	obj := storedKind(t, d, cacheID, apiVersion, resource)
	spec := obj.Spec
	spec.Paused = true
	_, err := d.kindClient.Update(context.Background(), obj.ID, spec)
	require.NoError(t, err)
}

// storedKind reads one kind record back by the name the sweep gives it.
func storedKind(t *testing.T, d deps, cacheID beehive.ObjectID, apiVersion, resource string) *beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus] {
	t.Helper()
	obj, err := d.kindClient.GetByName(context.Background(), ClusterCachedKindName(cacheID, apiVersion, resource))
	require.NoError(t, err)
	return obj
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

// A store failure is THIS cache's, not the cluster's, so it passes the guard that holds back
// NoConnection and IdentityMismatch and lands on the timeline. A guard rather than a red test:
// logDiscoveryVerdict needs no change for it, and this is what keeps it that way.
func TestCachePassLogsAStoreFailure(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	syncFake(d).setDiscoveryState(int64(cache.ID), kubesync.DiscoveryState{
		Reason: kubesync.ReasonStoreFailed, Message: "disk full",
	})

	client := reconcileCache(t, d, cache)

	require.Len(t, client.events, 1)
	assert.Equal(t, kubesync.ReasonStoreFailed, client.events[0].Reason)
	assert.Equal(t, "disk full", client.events[0].Message)
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

// **A paused kind is skipped, not folded.** It never reaches GetKindState, so it can never
// be read as unanswered — and an unanswered kind pins the whole cache at Connecting, which
// is what would leave a user's own deliberate pause looking like a cache that never started.
//
// It still counts in totalKinds: that field has a documented meaning and three consumers, so
// the paused ones are tallied separately rather than subtracted out of the census.
func TestCachesWatchHealthSkipsAPausedKindWithoutLosingItFromTheCensus(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	setKindReason(d, cacheID, podsSpec, kubesync.KindState{
		Reason: kubesync.ReasonWatching, LastLiveAt: probedAt,
	})
	pauseKind(t, d, beehive.ObjectID(cacheID), deploymentsSpec.APIVersion, deploymentsSpec.Resource)

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the cache's verdict")
	assert.Equal(t, kubesync.ReasonWatching, got.Reason, "the paused kind withholds nothing")
	assert.Equal(t, 2, got.TotalKinds, "the census keeps it")
	assert.Equal(t, 1, got.PausedKinds)
	assert.Equal(t, 0, got.UnhealthyKinds, "a paused kind is not unhealthy")
}

// The gauge dedupes per cache against a hand-written comparison, so a field nothing
// compares is invisible by default — and pausing one kind on an otherwise-idle healthy
// cache moves NOTHING ELSE: the census holds, the paused kind is skipped so there are no
// offenders, and status and reason sit still. The frame would be suppressed and the count
// would never reach a client.
func TestCachesWatchHealthPublishesWhenAKindIsPaused(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	setKindReason(d, cacheID, podsSpec, kubesync.KindState{Reason: kubesync.ReasonWatching, LastLiveAt: probedAt})
	setKindReason(d, cacheID, deploymentsSpec, kubesync.KindState{Reason: kubesync.ReasonWatching, LastLiveAt: probedAt})

	svc := serviceOver(t, d)
	svc.gaugeCadence = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := svc.Caches().WatchHealth(ctx)
	require.NoError(t, err)
	defer func() {
		cancel()
		testutil.WaitClosed(t, stream.Frames, "the gauge to stop")
	}()
	require.Equal(t, kubesync.ReasonWatching, testutil.Recv(t, stream.Frames, "the healthy verdict").Reason)

	pauseKind(t, d, beehive.ObjectID(cacheID), deploymentsSpec.APIVersion, deploymentsSpec.Resource)

	assert.Equal(t, 1, testutil.Recv(t, stream.Frames, "the verdict after the pause").PausedKinds)
}

// The arm ordering, which is the easy thing to get wrong. Paused kinds are skipped, so a
// fully paused cache has no offenders and nothing unanswered — every later arm would call
// it healthy, and the badge would read Watching over a cache syncing nothing.
func TestCachesWatchHealthReadsAFullyPausedCacheAsPaused(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	pauseKind(t, d, beehive.ObjectID(cacheID), podsSpec.APIVersion, podsSpec.Resource)
	pauseKind(t, d, beehive.ObjectID(cacheID), deploymentsSpec.APIVersion, deploymentsSpec.Resource)

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the cache's verdict")
	assert.Equal(t, ConditionFalse, got.Status)
	assert.Equal(t, ReasonPaused, got.Reason)
	assert.Equal(t, 2, got.PausedKinds)
}

// A cache whose file will not open has no session behind it, so every kind reads as
// unanswered and the per-kind fold sits at Connecting for good — an amber badge with no
// reason anywhere. The store's verdict is the cache's own, and it decides above the fold.
func TestCachesWatchHealthReportsACacheWhoseStoreWillNotOpen(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	syncFake(d).setDiscoveryState(int64(cacheID), kubesync.DiscoveryState{
		Reason: kubesync.ReasonStoreFailed, Message: "disk full",
	})

	stream, err := serviceOver(t, d).Caches().WatchHealth(context.Background())
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the cache's verdict")
	assert.Equal(t, ConditionFalse, got.Status)
	assert.Equal(t, kubesync.ReasonStoreFailed, got.Reason)
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
// The panel's own report of the failure. A guard, like the timeline one: the projection is
// reason-agnostic, and this is what stops a future narrowing from dropping the one verdict
// that has no kind to speak through.
func TestCachesWatchSyncStatusCarriesAStoreFailure(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	cluster, _, err := d.cacheClient.GetOwner(context.Background(), beehive.ObjectID(cacheID))
	require.NoError(t, err)
	syncFake(d).setDiscoveryState(int64(cacheID), kubesync.DiscoveryState{
		Reason: kubesync.ReasonStoreFailed, Message: "disk full",
	})

	stream, err := serviceOver(t, d).Caches().WatchSyncStatus(context.Background(), ClusterID(cluster.ID), cacheID)
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the cache's sync detail")
	assert.Equal(t, kubesync.ReasonStoreFailed, got.Discovery.Reason)
	assert.Equal(t, "disk full", got.Discovery.Message)
}

// The panel's list is where a user looks for a per-kind reason, and a paused kind has no
// worker to ask. **ObjectCount still answers**, since it comes off the store's counts and
// not the sync seam — which is the point of keeping the rows.
func TestCachesWatchSyncStatusReadsAPausedKindOffItsRecord(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	cluster, _, err := d.cacheClient.GetOwner(context.Background(), beehive.ObjectID(cacheID))
	require.NoError(t, err)
	writeCatalog(t, d, cacheID, kindRow("v1", "Pod", "pods"))
	pauseKind(t, d, beehive.ObjectID(cacheID), "v1", "pods")

	stream, err := serviceOver(t, d).Caches().WatchSyncStatus(context.Background(), ClusterID(cluster.ID), cacheID)
	require.NoError(t, err)

	got := testutil.Recv(t, stream.Frames, "the cache's sync detail")
	require.Len(t, got.Kinds, 2)
	assert.Equal(t, ReasonPaused, got.Kinds[1].Reason, "the reason comes off the record, not the seam")
	assert.Equal(t, 0, got.Kinds[1].ObjectCount, "and the count still answers")
}

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

// Two stamps are the same when both are absent or both name the same instant — a pointer
// comparison would call two equal times different and re-send the gauge every tick.
func TestSameTimeComparesTheInstantNotThePointer(t *testing.T) {
	at := time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)
	same := at

	assert.True(t, sameTime(&at, &same))
	assert.True(t, sameTime(nil, nil))
	assert.False(t, sameTime(&at, nil))
}

// Kinds are ordered by apiVersion and then resource, so a rendered list holds still
// between ticks rather than reshuffling on whatever order the store returned.
func TestSyncedKindRefsSortByApiVersionThenResource(t *testing.T) {
	refs := []SyncedKindRef{
		{APIVersion: "v1", Resource: "pods"},
		{APIVersion: "apps/v1", Resource: "statefulsets"},
		{APIVersion: "apps/v1", Resource: "deployments"},
	}

	slices.SortFunc(refs, compareSyncedKindRefs)

	assert.Equal(t, []SyncedKindRef{
		{APIVersion: "apps/v1", Resource: "deployments"},
		{APIVersion: "apps/v1", Resource: "statefulsets"},
		{APIVersion: "v1", Resource: "pods"},
	}, refs)
}

// The three steps a live cache's pass makes, each failed in turn. A pass that swallowed
// one would settle the generation over work that did not happen, and beehive would not
// come back to it — the cache would sit unarmed until something else moved.
func TestCacheReconcileFailsOnAnyStepThatDoesNot(t *testing.T) {
	steps := map[string]func(d *deps, client *cacheControllerClient, cacheID int64){
		"arming the sync": func(_ *deps, client *cacheControllerClient, _ int64) {
			client.dependErr = assert.AnError
		},
		"mirroring the kinds": func(d *deps, _ *cacheControllerClient, _ int64) {
			d.kubestoreMgr.(*fakeKubestore).err = assert.AnError
		},
		"logging the verdict": func(d *deps, client *cacheControllerClient, cacheID int64) {
			// A sweep that has answered, so there is a verdict to write at all.
			d.kubesyncSvc.(*fakeKubesync).setDiscoveryState(cacheID, kubesync.DiscoveryState{
				Reason: kubesync.ReasonDiscovered,
			})
			client.eventErr = assert.AnError
		},
	}

	for name, breakStep := range steps {
		t.Run(name, func(t *testing.T) {
			d, status := newClusterStatusDeps(t)
			cluster := storedCluster(t, d, status, true, "uid-1")
			cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
			client := &cacheControllerClient{}
			breakStep(&d, client, int64(cache.ID))

			res := (&clusterCacheController{deps: d}).Reconcile(context.Background(), client, cache)

			assert.NotEqual(t, beehive.Settled(), res)
		})
	}
}

// A cache whose owner edge was never loaded is a programming error, not an unowned
// record: silently reading it as "no cluster" would drop the cache out of the fold, and
// the gauge would report a fleet short of one with nothing to say why.
func TestReadingACacheOwnerThatWasNotLoadedIsReported(t *testing.T) {
	ctx := context.Background()
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	unloaded, err := d.cacheClient.Get(ctx, cache.ID)
	require.NoError(t, err)
	a := cachesAPI{serviceOver(t, d)}

	_, frameErr := cacheWatch.frame(DeltaFrameAdded, unloaded)
	_, clusterErr := a.clusterFor(ctx, unloaded, map[beehive.ObjectID]*beehive.Object[ClusterSpec, ClusterStatus]{})

	assert.Error(t, frameErr)
	assert.ErrorContains(t, clusterErr, "owner")
}

// A cache with no cluster is one beehive's GC is about to take: it drops out of the fold
// rather than being reported, since its going is not a fault.
func TestTheHealthFoldSkipsACacheWithNoCluster(t *testing.T) {
	ctx := context.Background()
	d := newTestDeps(t)
	_, _, err := d.cacheClient.CreateOrUpdate(ctx, "orphan-cache", ClusterCacheSpec{ServerUID: "uid-1"})
	require.NoError(t, err)

	healths, err := cachesAPI{serviceOver(t, d)}.readAllCacheHealth(ctx)

	require.NoError(t, err)
	assert.Empty(t, healths)
}

// The cluster lookup is memoised across the fold — caches of one cluster share it, and
// this runs on the gauge's cadence. A cluster already found is not read again, and one
// already found to be gone is not looked for again either.
func TestTheClusterLookupIsMemoisedAcrossTheFold(t *testing.T) {
	ctx := context.Background()
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	first := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	second := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-2")
	a := cachesAPI{serviceOver(t, d)}
	seen := map[beehive.ObjectID]*beehive.Object[ClusterSpec, ClusterStatus]{}

	firstObj, err := d.cacheClient.Get(ctx, first.ID, beehive.LoadOwner())
	require.NoError(t, err)
	one, err := a.clusterFor(ctx, firstObj, seen)
	require.NoError(t, err)
	secondObj, err := d.cacheClient.Get(ctx, second.ID, beehive.LoadOwner())
	require.NoError(t, err)
	two, err := a.clusterFor(ctx, secondObj, seen)

	require.NoError(t, err)
	assert.Same(t, one, two)
}

// A cache whose cluster has been collected reads as no cluster rather than as a failure:
// the fold read the cache a moment before its cluster went, and the record is on its way
// out with it.
func TestTheClusterLookupAnswersACollectedClusterAsNone(t *testing.T) {
	ctx := context.Background()
	d, _ := newRunningRegisteredDeps(t, beehive.WithGCInterval(time.Millisecond))
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	cacheObj, err := d.cacheClient.Get(ctx, cache.ID, beehive.LoadOwner())
	require.NoError(t, err)
	a := cachesAPI{serviceOver(t, d)}

	require.NoError(t, d.clusterClient.Delete(ctx, cluster.ID))

	require.Eventually(t, func() bool {
		got, err := a.clusterFor(ctx, cacheObj, map[beehive.ObjectID]*beehive.Object[ClusterSpec, ClusterStatus]{})
		return err == nil && got == nil
	}, 5*time.Second, time.Millisecond, "the cluster to be collected out from under the fold")
}

// A cache with no cluster cannot be armed: every switch the pass reads lives on the
// cluster, and inventing one would arm a sync against defaults the user never set.
func TestArmingACacheWithNoClusterIsReported(t *testing.T) {
	ctx := context.Background()
	d := newTestDeps(t)
	orphan, _, err := d.cacheClient.CreateOrUpdate(ctx, "orphan-cache", ClusterCacheSpec{})
	require.NoError(t, err)

	_, err = (&clusterCacheController{deps: d}).loadCluster(ctx, orphan)

	assert.ErrorContains(t, err, "has no cluster")
}

// The catalog read is what the mirror is built from, so a cache whose file goes between
// the claim and the read fails the pass — carrying on would mirror an empty answer and
// prune every kind the cache holds.
func TestMirroringFailsWhenTheCatalogWillNotRead(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	store := d.kubestoreMgr.(*fakeKubestore)
	// Retired with the claim already handed out: the pass holds a store whose file is gone.
	store.afterOpen = func(cacheID int64) { require.NoError(t, store.mgr.Remove(cacheID)) }

	err := (&clusterCacheController{deps: d}).mirrorKinds(context.Background(), cache)

	assert.ErrorContains(t, err, "kind catalog")
}

// The records under a cache are read once and answer both the write and the prune, so a
// read that fails takes the pass with it rather than pruning against an empty set.
func TestMirroringFailsWhenTheStoredRecordsWillNotList(t *testing.T) {
	d, cache := cacheOverABrokenStore(t)

	err := (&clusterCacheController{deps: d}).mirrorKinds(context.Background(), cache)

	assert.ErrorContains(t, err, "cached kinds")
}

// A record that will not write fails the pass: settling over it would leave the cache
// mirroring a kind the cluster serves and nothing has a record for.
func TestUpsertingKindsFailsWhenARecordWillNotWrite(t *testing.T) {
	d, cache := cacheOverABrokenStore(t)
	desired := map[string]ClusterCachedKindSpec{
		ClusterCachedKindName(cache.ID, "apps/v1", "deployments"): deploymentsSpec,
	}

	err := (&clusterCacheController{deps: d}).upsertKinds(context.Background(), cache.ID, desired, nil)

	assert.ErrorContains(t, err, "mirror cached kind")
}

// The user's switch is re-read under the lock before a catalog change is written over it,
// and a read that fails must stop the write — carrying on would resume a paused kind
// because its singular was renamed.
func TestUpsertingKindsFailsWhenThePauseWillNotRead(t *testing.T) {
	d, cache := cacheOverABrokenStore(t)
	name := ClusterCachedKindName(cache.ID, "apps/v1", "deployments")
	renamed := deploymentsSpec
	renamed.Kind = "Deploy"
	stored := map[string]*beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus]{
		name: {ID: 404, Name: name, Spec: deploymentsSpec},
	}

	err := (&clusterCacheController{deps: d}).upsertKinds(context.Background(), cache.ID,
		map[string]ClusterCachedKindSpec{name: renamed}, stored)

	assert.ErrorContains(t, err, "read cached kind")
}

// A record collected since the pass listed it has no switch to carry, and the write that
// follows creates it afresh — a race, not a failure.
func TestUpsertingKindsTreatsACollectedRecordAsUnpaused(t *testing.T) {
	ctx := context.Background()
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	name := ClusterCachedKindName(cache.ID, "apps/v1", "deployments")
	renamed := deploymentsSpec
	renamed.Kind = "Deploy"
	// An id nothing names, standing in for a record collected between the list and here.
	stored := map[string]*beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus]{
		name: {ID: 404, Name: name, Spec: deploymentsSpec},
	}

	err := (&clusterCacheController{deps: d}).upsertKinds(ctx, cache.ID,
		map[string]ClusterCachedKindSpec{name: renamed}, stored)

	require.NoError(t, err)
	got, err := d.kindClient.GetByName(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, "Deploy", got.Spec.Kind)
}

// A record the catalog no longer names is marked, and a mark that will not land fails the
// pass — the kind would otherwise keep syncing with nothing left to say it should.
func TestPruningKindsFailsWhenTheMarkWillNotLand(t *testing.T) {
	d, cache := cacheOverABrokenStore(t)
	stored := map[string]*beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus]{
		"gone": {ID: beehive.ObjectID(cache.ID) + 1, Name: "gone", Spec: deploymentsSpec},
	}

	err := (&clusterCacheController{deps: d}).pruneKinds(context.Background(), nil, stored)

	assert.ErrorContains(t, err, "drop cached kind")
}

// cacheOverABrokenStore stores a cluster and a cache, then closes the store under them —
// so the records exist as Go values while every read and write through beehive fails.
func cacheOverABrokenStore(t *testing.T) (deps, *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) {
	t.Helper()
	d, closeStore := newTestDepsWithABreakableStore(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	closeStore()
	return d, cache
}

// The gauge binds to the store's change feed when one exists, so a write is a re-measure
// rather than a wait for the next tick. It binds late, because the file opens when a
// worker arms — after the gauge is already running.
func TestCachesWatchStatsRemeasuresOnAWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	store := kubestoreFake(d)
	writer, err := store.mgr.OpenOrCreate(int64(cache.ID))
	require.NoError(t, err)
	t.Cleanup(writer.Release)
	store.setStats(kubestore.Stats{Exists: true, DBBytes: 4096})

	stream, err := serviceOver(t, d).Caches().WatchStats(ctx, ClusterID(cluster.ID), ClusterCacheID(cache.ID))
	require.NoError(t, err)
	testutil.Recv(t, stream.Frames, "the first measurement")

	store.setStats(kubestore.Stats{Exists: true, DBBytes: 8192})
	require.NoError(t, writer.SyncKinds(ctx, nil, true, 7))

	assert.Equal(t, int64(8192), testutil.Recv(t, stream.Frames, "the re-measurement").Bytes)
}

// A measurement that fails ends the gauge with its reason: a cache whose file cannot be
// measured is not one of zero bytes, and rendering it as such would tell the user their
// cache is empty.
func TestCachesWatchStatsEndsOnAMeasurementItCannotTake(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	kubestoreFake(d).err = assert.AnError

	stream, err := serviceOver(t, d).Caches().WatchStats(context.Background(),
		ClusterID(cluster.ID), ClusterCacheID(cache.ID))

	require.NoError(t, err)
	testutil.WaitClosed(t, stream.Frames, "the gauge to end")
	assert.ErrorContains(t, stream.Err(), "measure cache")
}

// The store closing under the gauge — a clear, a shutdown — re-binds rather than ending:
// the cache is still the caller's, and the fresh file's pings come through a subscription
// the old one never carried.
func TestCachesWatchStatsRebindsWhenTheStoreClosesUnderIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	store := kubestoreFake(d)
	writer, err := store.mgr.OpenOrCreate(int64(cache.ID))
	require.NoError(t, err)
	store.setStats(kubestore.Stats{Exists: true, DBBytes: 4096})

	stream, err := serviceOver(t, d).Caches().WatchStats(ctx, ClusterID(cluster.ID), ClusterCacheID(cache.ID))
	require.NoError(t, err)
	testutil.Recv(t, stream.Frames, "the first measurement")

	// The clear's close, which ends every subscription on the old file.
	store.setStats(kubestore.Stats{Exists: true, DBBytes: 8192})
	require.NoError(t, store.mgr.Clear(int64(cache.ID)))
	t.Cleanup(writer.Release)

	assert.Equal(t, int64(8192), testutil.Recv(t, stream.Frames, "the re-measurement after the rebind").Bytes)
}

// A record already marked for deletion is not one of the cache's kinds any more: counting
// it would hold the cache at Connecting over a kind that is on its way out, and put a row
// in the detail for something the user has stopped syncing.
func TestTheSyncReadsSkipARecordAlreadyGoing(t *testing.T) {
	ctx := context.Background()
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	kind := createKind(t, d, cache.ID, deploymentsSpec)
	require.NoError(t, d.kindClient.Delete(ctx, kind.ID))
	a := cachesAPI{serviceOver(t, d)}

	health, err := a.readCacheHealth(ctx, cache)
	require.NoError(t, err)
	status, err := a.readSyncStatus(ctx, ClusterCacheID(cache.ID))
	require.NoError(t, err)

	assert.Zero(t, health.TotalKinds)
	assert.Empty(t, status.Kinds)
}

// Neither fold invents a verdict over records it could not read: a cache whose kinds will
// not list is unknown, not healthy, and the gauge above says so instead of rendering green.
func TestTheSyncReadsReportRecordsTheyCannotList(t *testing.T) {
	ctx := context.Background()
	d, cache := cacheOverABrokenStore(t)
	a := cachesAPI{serviceOver(t, d)}

	_, healthErr := a.readCacheHealth(ctx, cache)
	_, statusErr := a.readSyncStatus(ctx, ClusterCacheID(cache.ID))

	assert.ErrorContains(t, healthErr, "cached kinds")
	assert.ErrorContains(t, statusErr, "cached kinds")
}

// The per-kind counts come off the cache's file: a cache with none has no counts, which is
// a paused cache or one nothing has swept — while a file that will not read is a fault.
func TestTheKindCountsAnswerAMissingFileAndReportABrokenOne(t *testing.T) {
	ctx := context.Background()
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	a := cachesAPI{serviceOver(t, d)}
	store := kubestoreFake(d)

	store.noFile = true
	counts, err := a.readObjectCountsByKind(ctx, ClusterCacheID(cache.ID))
	require.NoError(t, err)
	assert.Empty(t, counts)

	store.noFile = false
	store.afterOpen = func(cacheID int64) { require.NoError(t, store.mgr.Remove(cacheID)) }
	_, err = a.readObjectCountsByKind(ctx, ClusterCacheID(cache.ID))
	assert.ErrorContains(t, err, "kinds")
}

// A clear that fails is reported: the file is still there, and answering with the record
// would tell the user the cache had been emptied when it had not.
func TestCacheClearReportsAStoreThatWillNotClear(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	kubestoreFake(d).err = assert.AnError

	_, err := serviceOver(t, d).Caches().Clear(context.Background(), ClusterCacheID(cache.ID))

	assert.ErrorContains(t, err, "clear cluster cache")
}

// The gauge sends only what moved: it re-reads on a cadence, and a cache whose verdict
// sits still would otherwise put a frame on the wire every tick, for every cache, forever.
func TestCachesWatchHealthSaysNothingWhileTheVerdictHoldsStill(t *testing.T) {
	d, cacheID := syncingCacheWithKinds(t)
	setKindReason(d, cacheID, podsSpec, kubesync.KindState{Reason: kubesync.ReasonWatching, LastLiveAt: probedAt})
	setKindReason(d, cacheID, deploymentsSpec, kubesync.KindState{Reason: kubesync.ReasonWatching, LastLiveAt: probedAt})
	svc := serviceOver(t, d)
	svc.gaugeCadence = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := svc.Caches().WatchHealth(ctx)
	require.NoError(t, err)
	require.Equal(t, kubesync.ReasonWatching, testutil.Recv(t, stream.Frames, "the verdict").Reason)

	testutil.NoRecv(t, stream.Frames, testutil.Timeout/50, "a repeat of a verdict that has not moved")
}

// The three gauges stop where they stand when the consumer goes: every send is checked,
// so a dropped subscription ends the loop rather than parking it on a channel forever.
// Each is driven until it is parked on a send — Frames buffers one, and nothing reads it —
// so the cancellation is the only thing that can free it.
func TestTheCacheGaugesStopForAConsumerThatIsGone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	d, cacheID := syncingCacheWithKinds(t)
	clusterID := ownerClusterOf(t, d, cacheID)
	// A second cache, so the health fold has more than one verdict to send.
	createCache(t, d.cacheClient, clusterID, "uid-2")
	setKindReason(d, cacheID, podsSpec, kubesync.KindState{Reason: kubesync.ReasonWatching, LastLiveAt: probedAt})
	store := kubestoreFake(d)
	sync := syncFake(d)
	svc := serviceOver(t, d)
	svc.gaugeCadence = time.Millisecond

	health, err := svc.Caches().WatchHealth(ctx)
	require.NoError(t, err)
	status, err := svc.Caches().WatchSyncStatus(ctx, clusterID, cacheID)
	require.NoError(t, err)
	stats, err := svc.Caches().WatchStats(ctx, clusterID, cacheID)
	require.NoError(t, err)

	// The two per-cache gauges re-send only what moved, so each needs a reading that keeps
	// moving to fill its buffer and then park on the next send.
	go func() {
		for i := int64(1); ctx.Err() == nil; i++ {
			store.setStats(kubestore.Stats{Exists: true, DBBytes: i})
			sync.setDiscoveryState(int64(cacheID), kubesync.DiscoveryState{
				Reason: kubesync.ReasonDiscovered, Message: strconv.FormatInt(i, 10),
			})
			time.Sleep(time.Millisecond)
		}
	}()
	require.Eventually(t, func() bool {
		return len(health.Frames) == 1 && len(status.Frames) == 1 && len(stats.Frames) == 1
	}, 5*time.Second, time.Millisecond, "each gauge to park on a send")

	cancel()

	testutil.WaitClosed(t, health.Frames, "the health gauge to stop")
	testutil.WaitClosed(t, status.Frames, "the sync-status gauge to stop")
	testutil.WaitClosed(t, stats.Frames, "the stats gauge to stop")
}

// ownerClusterOf is the cluster a cache hangs off, which the per-cache gauges are scoped by.
func ownerClusterOf(t *testing.T, d deps, cacheID ClusterCacheID) ClusterID {
	t.Helper()
	owner, ok, err := d.cacheClient.GetOwner(context.Background(), beehive.ObjectID(cacheID))
	require.NoError(t, err)
	require.True(t, ok)
	return ClusterID(owner.ID)
}

// Neither the fold nor the detail read invents an answer over a store it cannot reach: a
// cache whose records or file will not read is unknown, not healthy and not empty.
func TestTheCacheFoldsReportWhatTheyCannotRead(t *testing.T) {
	ctx := context.Background()

	t.Run("the fold's cluster lookup", func(t *testing.T) {
		d, closeStore := newTestDepsWithABreakableStore(t)
		cluster := createCluster(t, d.clusterClient, "prod")
		cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
		// Owner-loaded, so the lookup gets past the edge and fails on the cluster itself.
		loaded, err := d.cacheClient.Get(ctx, cache.ID, beehive.LoadOwner())
		require.NoError(t, err)
		closeStore()

		_, err = cachesAPI{serviceOver(t, d)}.clusterFor(ctx, loaded,
			map[beehive.ObjectID]*beehive.Object[ClusterSpec, ClusterStatus]{})

		assert.ErrorContains(t, err, "read cluster 1")
	})

	t.Run("the fold's cache list", func(t *testing.T) {
		d, _ := cacheOverABrokenStore(t)
		_, err := cachesAPI{serviceOver(t, d)}.readAllCacheHealth(ctx)
		assert.Error(t, err)
	})

	t.Run("the detail's kind counts", func(t *testing.T) {
		d := newTestDeps(t)
		cluster := createCluster(t, d.clusterClient, "prod")
		cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
		kubestoreFake(d).err = assert.AnError
		_, err := cachesAPI{serviceOver(t, d)}.readSyncStatus(ctx, ClusterCacheID(cache.ID))
		assert.ErrorContains(t, err, "open cluster cache")
	})
}

// The fold orders by cache id, so a fleet's verdicts hold still between ticks rather than
// reshuffling on whatever order the store returned.
func TestTheHealthFoldOrdersByCacheID(t *testing.T) {
	ctx := context.Background()
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	first := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	second := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-2")

	healths, err := cachesAPI{serviceOver(t, d)}.readAllCacheHealth(ctx)

	require.NoError(t, err)
	require.Len(t, healths, 2)
	assert.Equal(t, ClusterCacheID(first.ID), healths[0].CacheID)
	assert.Equal(t, ClusterCacheID(second.ID), healths[1].CacheID)
}

// A cache whose cluster cannot be read cannot be armed: every switch the pass reads lives
// there, and arming on a guess would sync against settings the user never chose.
func TestArmingReportsAClusterItCannotRead(t *testing.T) {
	d, cache := cacheOverABrokenStore(t)

	c := &clusterCacheController{deps: d}
	_, loadErr := c.loadCluster(context.Background(), cache)
	armErr := c.armSync(context.Background(), &cacheControllerClient{}, cache, false)

	assert.ErrorContains(t, loadErr, "owner")
	assert.ErrorContains(t, armErr, "owner")
}

// A whole read that carries one record it cannot project fails rather than serving the
// rest: a list silently short by one is a cache missing from the UI with nothing to say so.
func TestProjectingCachesReportsARecordItCannotRead(t *testing.T) {
	ctx := context.Background()
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	unloaded, err := d.cacheClient.Get(ctx, cache.ID)
	require.NoError(t, err)

	_, err = toClusterCaches([]*beehive.Object[ClusterCacheSpec, ClusterCacheStatus]{unloaded})

	assert.Error(t, err)
}

// A user navigating away ends the gauges cleanly. Their reads all take ctx, so each one
// ends with ctx's own error — reported, that reaches the webview as a watch failure and
// puts an error in front of someone who has already left the view.
func TestTheCacheGaugesEndCleanlyWhenTheConsumerLeaves(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	d, cacheID := syncingCacheWithKinds(t)
	clusterID := ownerClusterOf(t, d, cacheID)
	svc := serviceOver(t, d)
	svc.gaugeCadence = time.Millisecond

	status, err := svc.Caches().WatchSyncStatus(ctx, clusterID, cacheID)
	require.NoError(t, err)
	health, err := svc.Caches().WatchHealth(ctx)
	require.NoError(t, err)
	stats, err := svc.Caches().WatchStats(ctx, clusterID, cacheID)
	require.NoError(t, err)
	testutil.Recv(t, status.Frames, "the first sync-status reading")
	testutil.Recv(t, health.Frames, "the first verdict")
	testutil.Recv(t, stats.Frames, "the first measurement")

	cancel()

	testutil.WaitClosed(t, status.Frames, "the sync-status gauge to stop")
	testutil.WaitClosed(t, health.Frames, "the health gauge to stop")
	testutil.WaitClosed(t, stats.Frames, "the stats gauge to stop")
	assert.NoError(t, status.Err())
	assert.NoError(t, health.Err())
	assert.NoError(t, stats.Err())
}

// The ceiling reaches the UI on the gauge the size is already on, so a client renders how
// close a cache is without a second subscription — and without hardcoding our default.
func TestMeasureCacheCarriesTheSizeCeiling(t *testing.T) {
	deps := newTestDeps(t)
	deps.kubestoreMgr = &fakeKubestore{stats: kubestore.Stats{
		Exists: true, DBBytes: 100, OverSizeLimit: true, SizeLimitBytes: 512,
	}}
	svc := &service{deps: deps}

	got, err := cachesAPI{s: svc}.measureCache(context.Background(), 1)

	require.NoError(t, err)
	assert.True(t, got.OverSizeLimit)
	assert.Equal(t, int64(512), got.SizeLimitBytes)
}

// --- the size ceiling ---

// overLimit is a cache whose file has passed its ceiling, as the janitor reports it.
func overLimit() kubestore.Stats {
	return kubestore.Stats{Exists: true, DBBytes: 3 * gib, SizeLimitBytes: 2 * gib, OverSizeLimit: true}
}

// gib is the unit the ceiling is set in, so a fixture reads as a size.
const gib = 1 << 30

// A cache at its ceiling stops syncing, and the record is told why — the pause closes the
// file, so the condition is the only thing that will still remember.
func TestCachePassStopsACacheOverItsSizeLimit(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	kubestoreFake(d).setStats(overLimit())

	client := reconcileCache(t, d, cache)

	_, armed := syncFake(d).armedDiscovery(int64(cache.ID))
	assert.False(t, armed, "a cache at its ceiling syncs nothing")
	require.Len(t, client.conditions, 1)
	assert.Equal(t, string(ConditionSynced), client.conditions[0].Type)
	assert.Equal(t, ConditionFalse, client.conditions[0].Status)
	assert.Equal(t, ReasonSizeLimit, client.conditions[0].Reason)
	assert.Contains(t, client.conditions[0].Message, "3221225472")
	assert.Contains(t, client.conditions[0].Message, "2147483648")
}

// sizeStopped is the condition the pass writes when a cache hits its ceiling, as a record
// that has already been stopped carries it into the next pass.
func sizeStopped() []Condition {
	return []Condition{LiveCondition(ConditionSynced, ConditionFalse, ReasonSizeLimit, "over")}
}

// The pause closes the file, and a closed file reports no verdict at all — so a pass that
// read the verdict alone would restart the cache it had just stopped, refill it, and stop
// it again. The record is what remembers.
func TestCachePassKeepsACacheStoppedOnceItsFileIsClosed(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	cache.Conditions = sizeStopped()
	closed := overLimit()
	closed.OverSizeLimit = false
	kubestoreFake(d).setStats(closed)

	client := reconcileCache(t, d, cache)

	_, armed := syncFake(d).armedDiscovery(int64(cache.ID))
	assert.False(t, armed, "a stopped cache stays stopped while its bytes are over")
	assert.Empty(t, client.deleted, "nothing releases a cache still over its limit")
}

// The verdict is the only way in. Bytes over the limit with no verdict behind them are a
// file nobody has open, which cannot be growing — stopping it would stop caches nothing is
// filling.
func TestCachePassDoesNotStopACacheOnBytesAlone(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	closed := overLimit()
	closed.OverSizeLimit = false
	kubestoreFake(d).setStats(closed)

	client := reconcileCache(t, d, cache)

	_, armed := syncFake(d).armedDiscovery(int64(cache.ID))
	assert.True(t, armed, "a cache nobody has open is not one that is filling")
	assert.Empty(t, client.conditions)
}

// The release: a cleared or unbounded cache starts again, and the condition goes with it.
func TestCachePassRestartsACacheBackUnderItsLimit(t *testing.T) {
	tests := map[string]kubestore.Stats{
		"cleared":   {},
		"under":     {Exists: true, DBBytes: gib, SizeLimitBytes: 2 * gib},
		"unbounded": {Exists: true, DBBytes: 9 * gib},
	}

	for name, stats := range tests {
		t.Run(name, func(t *testing.T) {
			d, status := newClusterStatusDeps(t)
			cluster := storedCluster(t, d, status, true, "uid-1")
			cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
			cache.Conditions = sizeStopped()
			kubestoreFake(d).setStats(stats)

			client := reconcileCache(t, d, cache)

			_, armed := syncFake(d).armedDiscovery(int64(cache.ID))
			assert.True(t, armed, "a cache back under its ceiling syncs again")
			assert.Equal(t, []string{string(ConditionSynced)}, client.deleted)
		})
	}
}

// Every cache that was never over its limit is this case, on every pass: a condition write
// per pass per cache would be a transaction for a verdict nothing formed.
func TestCachePassWritesNoConditionForAnOrdinaryCache(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	kubestoreFake(d).setStats(kubestore.Stats{Exists: true, DBBytes: gib, SizeLimitBytes: 2 * gib})

	client := reconcileCache(t, d, cache)

	assert.Empty(t, client.conditions)
	assert.Empty(t, client.deleted)
}

// A measurement that failed decides nothing: answering under would restart a cache nobody
// measured, and answering over would stop one over an error.
func TestTheSizeCheckLeavesAFailedMeasurementAlone(t *testing.T) {
	tests := map[string]struct {
		conditions []Condition
		want       bool
	}{
		"a stopped cache stays stopped": {conditions: sizeStopped(), want: true},
		"a running cache keeps running": {want: false},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			d, status := newClusterStatusDeps(t)
			cluster := storedCluster(t, d, status, true, "uid-1")
			cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
			cache.Conditions = tt.conditions
			kubestoreFake(d).err = assert.AnError

			limited, measured := (&clusterCacheController{deps: d}).checkSizeLimit(context.Background(), cache)

			assert.Equal(t, tt.want, limited)
			assert.Nil(t, measured, "an unmeasured pass has no size to write")
		})
	}
}

// sizeStoppedCache is a synced cluster's cache whose record carries the ceiling's
// condition, as the pass that stopped it left the record.
func sizeStoppedCache(t *testing.T) (deps, ClusterCacheID) {
	t.Helper()
	d, bh := newTestDepsAndBeehive(t)
	cluster := storedCluster(t, d, beehive.NewAdminClient[ClusterStatus](bh, ClusterGroupKind), true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	admin := beehive.NewAdminClient[ClusterCacheStatus](bh, ClusterCacheGroupKind)
	require.NoError(t, admin.SetCondition(context.Background(), cache.ID, sizeStopped()[0]))
	return d, ClusterCacheID(cache.ID)
}

// The condition is where the ceiling's verdict lives, and the gauge is the only place the
// UI looks — so the fold reads it back. Nothing else stops a cache without arming a kind
// that could say so.
func TestCachesWatchHealthReportsACacheStoppedByItsCeiling(t *testing.T) {
	d, _ := sizeStoppedCache(t)

	healths, err := cachesAPI{serviceOver(t, d)}.readAllCacheHealth(context.Background())

	require.NoError(t, err)
	require.Len(t, healths, 1)
	assert.Equal(t, ConditionFalse, healths[0].Status)
	assert.Equal(t, ReasonSizeLimit, healths[0].Reason)
}

// The arm ordering, the other way round: a cache that is both at its ceiling and paused
// kind by kind is stopped by the ceiling, and reporting it as merely paused is the
// confusion the two reasons exist to keep apart.
func TestCachesWatchHealthReadsTheCeilingAboveAFullyPausedCache(t *testing.T) {
	d, cacheID := sizeStoppedCache(t)
	createKind(t, d, beehive.ObjectID(cacheID), podsSpec)
	pauseKind(t, d, beehive.ObjectID(cacheID), podsSpec.APIVersion, podsSpec.Resource)

	healths, err := cachesAPI{serviceOver(t, d)}.readAllCacheHealth(context.Background())

	require.NoError(t, err)
	require.Len(t, healths, 1)
	assert.Equal(t, ReasonSizeLimit, healths[0].Reason)
}

// The user's remedy has to reach the pass. A stopped cache holds no claim, so Manager.Clear
// reopens nothing: no file, no janitor, and no verdict to wake the record — the clear does
// the waking itself, or the cache stays stopped until the next start.
//
// Everything here is arranged so the requeue is the only wake left. The record is stopped
// before beehive starts, carrying the message the pass itself would write, so the startup
// pass changes nothing and wakes nothing. The file then empties as the clear runs, so no
// earlier pass could have released it either.
func TestClearingACacheReleasesItsSizeStop(t *testing.T) {
	ctx := context.Background()
	d, bh := newTestDepsAndBeehive(t)
	cluster := storedCluster(t, d, beehive.NewAdminClient[ClusterStatus](bh, ClusterGroupKind), true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	store := kubestoreFake(d)
	store.setStats(overLimit())
	store.onClear = func(int64) { store.setStats(kubestore.Stats{}) }
	measured := testutil.NewProbe[int64](8)
	store.onStats = measured.Fire
	admin := beehive.NewAdminClient[ClusterCacheStatus](bh, ClusterCacheGroupKind)
	require.NoError(t, admin.SetCondition(ctx, cache.ID, LiveCondition(ConditionSynced, ConditionFalse,
		ReasonSizeLimit, "cache is 3221225472 bytes, over its 2147483648-byte limit")))
	// The edge the pass declares, declared up front: a first pass that writes it would wake
	// the record again, and this test's whole point is that nothing else does.
	require.NoError(t, admin.AddDependency(ctx, cache.ID, cluster.ID))
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
	// Two passes, not one: settling the first is itself a write, and that write wakes the
	// record once more. Waiting for the second is waiting for the quiet in which the only
	// wake left is the clear's own.
	require.Equal(t, int64(cache.ID), measured.Await(t, "the startup pass to measure the cache"))
	require.Equal(t, int64(cache.ID), measured.Await(t, "the pass its settling woke"))

	_, clearErr := serviceOver(t, d).Caches().Clear(ctx, ClusterCacheID(cache.ID))

	require.NoError(t, clearErr)
	require.Eventually(t, func() bool {
		obj, err := d.cacheClient.Get(ctx, cache.ID)
		return err == nil && FindCondition(obj.Conditions, ConditionSynced) == nil
	}, 5*time.Second, time.Millisecond, "the clear to release the cache's size stop")
}
