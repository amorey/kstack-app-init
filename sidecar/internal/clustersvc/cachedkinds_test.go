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

// Deterministic per (anchor, apiVersion, resource), which is what lets a discovery
// pass reconcile its children as a set with no per-child bookkeeping. Keyed on the
// plural, not the Kind: a CRD may reuse a built-in's plural under another group, so
// the group-version has to be in the name too.
func TestClusterCachedKindName(t *testing.T) {
	assert.Equal(t, "cachedkind/3/apps/v1/deployments", ClusterCachedKindName(3, "apps/v1", "deployments"))
	assert.NotEqual(t,
		ClusterCachedKindName(3, "apps/v1", "deployments"),
		ClusterCachedKindName(3, "example.com/v1", "deployments"),
		"the same plural under another group is a different kind")
	assert.NotEqual(t,
		ClusterCachedKindName(3, "apps/v1", "deployments"),
		ClusterCachedKindName(4, "apps/v1", "deployments"),
		"the same kind under another cache")
}

func createKind(t *testing.T, d deps, cacheID beehive.ObjectID, spec ClusterCachedKindSpec) *beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus] {
	t.Helper()
	ctx := context.Background()
	name := ClusterCachedKindName(cacheID, spec.APIVersion, spec.Resource)
	obj, _, err := d.kindClient.CreateOrUpdate(ctx, name, spec, beehive.WithOwner(cacheID))
	require.NoError(t, err)
	return obj
}

// deploymentsSpec and podsSpec are two served kinds, enough to prove ordering and scoping.
var (
	deploymentsSpec = ClusterCachedKindSpec{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Namespaced: true}
	podsSpec        = ClusterCachedKindSpec{APIVersion: "v1", Kind: "Pod", Resource: "pods", Namespaced: true}
)

// twoCachesTwoResources stores a kind under each of two caches, and returns the first
// cache's id — enough for one fixture to prove both a read's contents and its scoping.
func twoCachesTwoResources(t *testing.T) (deps, ClusterCacheID) {
	t.Helper()
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	one := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	two := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-2")
	createKind(t, d, one.ID, deploymentsSpec)
	createKind(t, d, two.ID, podsSpec)
	return d, ClusterCacheID(one.ID)
}

func TestCachedKindsGet(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	obj := createKind(t, d, cache.ID, deploymentsSpec)

	got, err := serviceOver(t, d).CachedKinds().Get(context.Background(), ClusterCachedKindID(obj.ID))

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, ClusterCachedKindID(obj.ID), got.ID)
	assert.Equal(t, deploymentsSpec, got.Spec)
	assert.Equal(t, ObjectRef{ID: ObjectID(cache.ID), Kind: "ClusterCache"}, got.Owner,
		"the owner is the cache — the join key a client already holds from the cache stream")
}

// An unknown id is not an error: a caller holds ids from watch frames, and a record
// collected in between is an ordinary race rather than a bad request.
func TestCachedKindsGetUnknownIsNotAnError(t *testing.T) {
	d := newTestDeps(t)

	got, err := serviceOver(t, d).CachedKinds().Get(context.Background(), 404)

	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestCachedKindsList(t *testing.T) {
	d, _ := twoCachesTwoResources(t)

	got, err := serviceOver(t, d).CachedKinds().List(context.Background())

	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []string{"deployments", "pods"}, []string{got[0].Spec.Resource, got[1].Spec.Resource},
		"creation order, the same order every family's list promises")
}

// watchKinds opens the unscoped stream for the test's life.
func watchKinds(t *testing.T, d deps) *Stream[ClusterCachedKindWatchFrame] {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream, err := serviceOver(t, d).CachedKinds().WatchList(ctx)
	require.NoError(t, err)
	return stream
}

func TestCachedKindsWatchListEmitsTheSnapshotThenABookmark(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	createKind(t, d, cache.ID, deploymentsSpec)

	stream := watchKinds(t, d)

	f := testutil.Recv(t, stream.Frames, "the snapshot frame")
	require.Equal(t, DeltaFrameAdded, f.Type)
	require.NotNil(t, f.Kind)
	assert.Equal(t, "deployments", f.Kind.Spec.Resource)
	assert.Equal(t, ObjectRef{ID: ObjectID(cache.ID), Kind: "ClusterCache"}, f.Kind.Owner)
	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// A fleet with nothing discovered yet bookmarks an empty collection rather than holding
// the snapshot back: the wait is the consumer's to render.
func TestCachedKindsWatchListBookmarksAnEmptyCollection(t *testing.T) {
	stream := watchKinds(t, newRunningDeps(t))

	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// A departure is a collected row, not the soft delete that precedes it — a prune marks the
// record and that arrives as an ordinary Modified. The frame carries no owner, because
// beehive loads no edges for a collected row and a frame that failed over that would kill
// the stream and strand the record in the client's map.
func TestCachedKindsWatchListReportsADeparture(t *testing.T) {
	frames := pumpFrames(t, kindWatch, nil,
		beehive.ObjectChange[ClusterCachedKindSpec, ClusterCachedKindStatus]{Type: beehive.Deleted, ID: 7, Object: nil},
	)

	require.Len(t, frames, 2)
	assert.Equal(t, DeltaFrameDeleted, frames[1].Type)
	require.NotNil(t, frames[1].Kind)
	assert.Equal(t, ClusterCachedKindID(7), frames[1].Kind.ID)
}

// A prune is beehive's soft delete, so the row lingers holding its name and the frame is an
// ordinary Modified — which is what makes the missing tombstone field on this record matter:
// a consumer cannot tell the mark from any other spec write.
func TestCachedKindsWatchListReportsAPruneAsAModify(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	obj := createKind(t, d, cache.ID, deploymentsSpec)

	stream := watchKinds(t, d)
	require.Equal(t, DeltaFrameAdded, testutil.Recv(t, stream.Frames, "the snapshot frame").Type)
	require.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)

	require.NoError(t, d.kindClient.Delete(context.Background(), obj.ID))

	f := testutil.Recv(t, stream.Frames, "the prune")
	assert.Equal(t, DeltaFrameModified, f.Type)
	require.NotNil(t, f.Kind)
	assert.Equal(t, ClusterCachedKindID(obj.ID), f.Kind.ID)
}

// One record's own stream, for a consumer holding an id from a frame.
func TestCachedKindsWatchStreamsOneRecord(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	obj := createKind(t, d, cache.ID, deploymentsSpec)
	createKind(t, d, cache.ID, podsSpec)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := serviceOver(t, d).CachedKinds().Watch(ctx, ClusterCachedKindID(obj.ID))
	require.NoError(t, err)

	f := testutil.Recv(t, stream.Frames, "the snapshot frame")
	require.NotNil(t, f.Kind)
	assert.Equal(t, "deployments", f.Kind.Spec.Resource, "the other kind is not this record")
	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// Scoped to one cache: another cache's kinds must not reach this read.
func TestCachedKindsListByCache(t *testing.T) {
	d, one := twoCachesTwoResources(t)

	got, err := serviceOver(t, d).CachedKinds().ListByCache(context.Background(), one)

	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "deployments", got[0].Spec.Resource, "the other cache's kinds are not this cache's")
}

// A cache nothing has discovered kinds for owns none, which reads empty rather than
// failing — the same wait an unsynced cache shows everywhere else.
func TestCachedKindsListByCacheWithNoKindsIsEmpty(t *testing.T) {
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	got, err := serviceOver(t, d).CachedKinds().ListByCache(context.Background(), ClusterCacheID(cache.ID))

	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestCachedKindsWatchByCache(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	one := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	two := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-2")
	createKind(t, d, one.ID, deploymentsSpec)
	createKind(t, d, two.ID, podsSpec)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := serviceOver(t, d).CachedKinds().WatchByCache(ctx, ClusterCacheID(one.ID))
	require.NoError(t, err)

	f := testutil.Recv(t, stream.Frames, "the snapshot frame")
	require.NotNil(t, f.Kind)
	assert.Equal(t, "deployments", f.Kind.Spec.Resource, "the other cache's kinds are not on this stream")
	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// A cache with no kinds yet bookmarks an empty collection: an unsynced cache is
// definitively empty, not pending, and holding the bookmark back would render a populated
// table as loading for as long as discovery takes.
func TestCachedKindsWatchByCacheWithNoKindsBookmarksEmpty(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := serviceOver(t, d).CachedKinds().WatchByCache(ctx, ClusterCacheID(cache.ID))
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

// A client subscribes on the frame announcing the cache, which is before any kind has been
// discovered for it. That is a wait, not an empty collection: the bookmark closes a
// snapshot of none, and the kinds are reported as they land.
func TestCachedKindsWatchByCacheReportsKindsThatArriveLater(t *testing.T) {
	d := newRunningDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := serviceOver(t, d).CachedKinds().WatchByCache(ctx, ClusterCacheID(cache.ID))
	require.NoError(t, err)
	require.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)

	// Discovery, arriving after the subscription.
	obj := createKind(t, d, cache.ID, deploymentsSpec)

	got := testutil.Recv(t, stream.Frames, "the kind that landed after the snapshot")
	assert.Equal(t, DeltaFrameAdded, got.Type)
	require.NotNil(t, got.Kind)
	assert.Equal(t, ClusterCachedKindID(obj.ID), got.Kind.ID)
}

// kindControllerClient stands in for the client beehive binds to the object being reconciled.
// The embedded interface is nil: a method the pass grows shows up as a panic, not as silence.
type kindControllerClient struct {
	beehive.ControllerClient[ClusterCachedKindStatus]
	events []beehive.EventSpec
}

func (c *kindControllerClient) AddEvent(_ context.Context, event beehive.EventSpec) error {
	c.events = append(c.events, event)
	return nil
}

// reconcileKind runs one kind pass the way beehive would.
func reconcileKind(t *testing.T, d deps, obj *beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus]) *kindControllerClient {
	t.Helper()
	client := &kindControllerClient{}
	res := (&clusterCachedKindController{deps: d}).Reconcile(context.Background(), client, obj)
	require.Equal(t, beehive.Settled(), res)
	return client
}

// oneCachedKind stores a cache with one kind under it and hands back both.
func oneCachedKind(t *testing.T) (deps, ClusterCacheID, *beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus]) {
	t.Helper()
	d := newTestDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	return d, ClusterCacheID(cache.ID), createKind(t, d, cache.ID, deploymentsSpec)
}

// The record is what arms its own kind: kubesync decides what EXISTS, and a record decides
// what is MIRRORED.
func TestKindPassArmsItsOwnSync(t *testing.T) {
	d, cacheID, kind := oneCachedKind(t)

	reconcileKind(t, d, kind)

	assert.Equal(t, []kubesync.KindKey{{
		CacheID: int64(cacheID),
		Kind: kubestore.Kind{
			APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments",
		},
	}}, syncFake(d).armedKinds(int64(cacheID)))
}

// **Forget first, then clear.** Clearing ahead of the join would race a relist page landing
// behind it, leaving rows for a kind nothing syncs any more.
func TestKindPassForgetsTheSyncBeforeItClearsTheRows(t *testing.T) {
	d, cacheID, kind := oneCachedKind(t)
	reconcileKind(t, d, kind)

	ctx := context.Background()
	store, _, err := d.kubestoreMgr.OpenExisting(int64(cacheID))
	require.NoError(t, err)
	require.NoError(t, store.SetCookie(ctx, "apps/v1", "deployments", "10"))
	store.Release()

	require.NoError(t, d.kindClient.Delete(ctx, kind.ID))
	obj, err := d.kindClient.Get(ctx, kind.ID)
	require.NoError(t, err)
	reconcileKind(t, d, obj)

	assert.Empty(t, syncFake(d).armedKinds(int64(cacheID)), "nothing syncs the kind that is going")
	store, _, err = d.kubestoreMgr.OpenExisting(int64(cacheID))
	require.NoError(t, err)
	defer store.Release()
	_, ok, err := store.Cookie(ctx, "apps/v1", "deployments")
	require.NoError(t, err)
	assert.False(t, ok, "the rows and the position went with the record")
}

// A record whose cache is already gone skips both: the file went with the cache, and there
// is nothing left to stop or to clear.
func TestKindPassSkipsTheClearWhenTheCacheIsGone(t *testing.T) {
	d, _, kind := oneCachedKind(t)
	ctx := context.Background()
	require.NoError(t, d.kindClient.Delete(ctx, kind.ID))
	obj, err := d.kindClient.Get(ctx, kind.ID)
	require.NoError(t, err)
	kubestoreFake(d).noFile = true

	reconcileKind(t, d, obj)
}

// A record whose cache record is gone still withdraws its registration. Nothing else is left
// to: the cache's pass has been and gone, and the id survives only in the record's own name.
func TestKindPassForgetsTheSyncWhenItsCacheRecordIsGone(t *testing.T) {
	d := newTestDeps(t)
	cacheID := ClusterCacheID(7)
	syncFake(d).TrackKind(int64(cacheID), toKubestoreKind(deploymentsSpec))
	now := time.Now()

	reconcileKind(t, d, &beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus]{
		ID: 999,
		Name: ClusterCachedKindName(
			beehive.ObjectID(cacheID), deploymentsSpec.APIVersion, deploymentsSpec.Resource),
		Spec:                deploymentsSpec,
		DeletionRequestedAt: &now,
	})

	assert.Empty(t, syncFake(d).armedKinds(int64(cacheID)))
}

// One kind's transitions land on its own timeline, which is what makes a cache's hundred
// kinds legible: the parent's carries only what the cache itself records.
func TestKindPassLogsItsOwnVerdict(t *testing.T) {
	d, cacheID, kind := oneCachedKind(t)
	syncFake(d).setKindState(int64(cacheID), kubestore.Kind{
		APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments",
	}, kubesync.KindState{Reason: kubesync.ReasonSyncFailed, Message: "deployments is forbidden"})

	client := reconcileKind(t, d, kind)

	require.Len(t, client.events, 1)
	assert.Equal(t, categorySync, client.events[0].Category)
	assert.Equal(t, kubesync.ReasonSyncFailed, client.events[0].Reason)
	assert.Equal(t, beehive.EventWarning, client.events[0].Type)
}

// A kind whose worker has answered nothing has no verdict to log — a cache that is paused,
// or one whose sweep has not reached this kind yet.
func TestKindPassLogsNothingBeforeItsWorkerAnswers(t *testing.T) {
	d, _, kind := oneCachedKind(t)

	client := reconcileKind(t, d, kind)

	assert.Empty(t, client.events)
}

// The pass writes no condition: the verdict is the gauge's, and a stored one would serve a
// dead process's answer until the passes caught up.
func TestKindPassWritesNoCondition(t *testing.T) {
	d, cacheID, kind := oneCachedKind(t)
	syncFake(d).setKindState(int64(cacheID), kubestore.Kind{
		APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments",
	}, kubesync.KindState{Reason: kubesync.ReasonWatching})

	reconcileKind(t, d, kind)

	obj, err := d.kindClient.Get(context.Background(), kind.ID)
	require.NoError(t, err)
	assert.Empty(t, obj.Conditions)
}

// One kind's clear needs the same hold as the cache-wide one, scoped to the kind: a worker
// that resumed from its cookie afterwards would apply deltas to rows nothing cold-listed.
func TestCachedKindsClearRunsInsideTheSyncsHold(t *testing.T) {
	d, cacheID, kind := oneCachedKind(t)

	var heldAtClear bool
	kubestoreFake(d).onOpen = func(int64) {
		heldAtClear = slices.ContainsFunc(syncFake(d).stoppedKinds(), func(key kubesync.KindKey) bool {
			return key.CacheID == int64(cacheID) && key.Resource == "deployments"
		})
	}
	_, err := serviceOver(t, d).CachedKinds().Clear(context.Background(), ClusterCachedKindID(kind.ID))

	require.NoError(t, err)
	assert.True(t, heldAtClear, "the kind's worker is stopped before its rows go")
}
