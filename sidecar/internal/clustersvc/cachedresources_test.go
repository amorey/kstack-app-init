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

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubesync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
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

// --- the reconcile fold ---

// resourceControllerClient answers the owner lookup a resource reconcile makes, from
// the store, so the edge a fixture wrote is the one the reconcile reads. It records the
// conditions written rather than storing them, since the verdict is what the tests
// assert. The embedded interface is nil: Reconcile calls nothing else on it.
type resourceControllerClient struct {
	beehive.ControllerClient[ClusterCachedResourceStatus]
	resources beehive.Client[ClusterCachedResourceSpec, ClusterCachedResourceStatus]
	id        beehive.ObjectID
	// noOwner stands in for a resource whose owner edge is gone, which the store cannot
	// hold while the row is still there.
	noOwner    bool
	conditions []Condition
	events     []beehive.EventSpec
}

func (c *resourceControllerClient) GetOwner(ctx context.Context) (beehive.ObjectRef, bool, error) {
	if c.noOwner {
		return beehive.ObjectRef{}, false, nil
	}
	return c.resources.GetOwner(ctx, c.id)
}

func (c *resourceControllerClient) SetCondition(_ context.Context, cond Condition) error {
	c.conditions = append(c.conditions, cond)
	return nil
}

func (c *resourceControllerClient) AddEvent(_ context.Context, ev beehive.EventSpec) error {
	c.events = append(c.events, ev)
	return nil
}

// Within runs the group inline: the fold's condition and its event are one write, and
// what the tests assert is that both landed.
func (c *resourceControllerClient) Within(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

// synced is the verdict the pass recorded.
func (c *resourceControllerClient) synced(t *testing.T) Condition {
	t.Helper()
	require.Len(t, c.conditions, 1, "one pass writes one verdict")
	require.Equal(t, string(ConditionSynced), c.conditions[0].Type)
	return c.conditions[0]
}

// servingResource stores a tracked, syncing cluster with a cache, its catalog, and one
// served kind under it — deploymentsSpec, with Enabled overridden — and returns the
// deps, the cache (what Track's params and ClearKind's calls are checked against), and
// the resource the pass under test reconciles.
func servingResource(t *testing.T, enabled bool) (deps, *beehive.Object[ClusterCacheSpec, ClusterCacheStatus], *beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus]) {
	t.Helper()
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	catalog := createCatalog(t, d, ClusterCacheID(cache.ID), true)
	spec := deploymentsSpec
	spec.Enabled = enabled
	return d, cache, createResource(t, d, catalog.ID, spec)
}

// reconcileResource runs one resource pass the way beehive would, folding whatever the
// fake worker fleet and store hold.
func reconcileResource(
	t *testing.T,
	d deps,
	obj *beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus],
) (*resourceControllerClient, beehive.ReconcileResult) {
	t.Helper()
	client := &resourceControllerClient{resources: d.resourceClient, id: obj.ID}
	c := &clusterCachedResourceController{deps: d}
	return client, c.Reconcile(context.Background(), client, obj)
}

// syncFleet is the fake worker fleet behind the pass under test.
func syncFleet(d deps) *fakeKubesync { return d.kubesyncSvc.(*fakeKubesync) }

// kubestoreFake is the fake store registry behind the pass under test.
func kubestoreFake(d deps) *fakeKubestore { return d.kubestoreMgr.(*fakeKubestore) }

// A resource on its way out has its worker disarmed and its rows cleared — the other
// side of the handshake the catalog's DiscoveryDraining requeue is waiting on.
func TestResourceReconcileClearsOnDeletion(t *testing.T) {
	d, cache, obj := servingResource(t, true)
	now := time.Now()
	obj.DeletionRequestedAt = &now

	client, res := reconcileResource(t, d, obj)

	assert.Equal(t, beehive.Settled(), res)
	assert.Equal(t, []string{obj.Name}, syncFleet(d).forgotten)
	assert.Equal(t, []int64{int64(cache.ID)}, kubestoreFake(d).opened, "the kind's rows were cleared from the wrong cache")
	assert.Empty(t, client.conditions)
}

// A store that cannot clear its rows is retried under backoff — the tombstone alone
// (already forgotten) is not enough to say the teardown finished.
func TestResourceReconcileFailsWhenClearKindErrors(t *testing.T) {
	d, cache, obj := servingResource(t, true)
	now := time.Now()
	obj.DeletionRequestedAt = &now
	kubestoreFake(d).err = errors.New("disk full")

	client, res := reconcileResource(t, d, obj)

	require.Error(t, res.Err())
	assert.Equal(t, []string{obj.Name}, syncFleet(d).forgotten, "the worker is disarmed even though the clear failed")
	assert.Equal(t, []int64{int64(cache.ID)}, kubestoreFake(d).opened)
	assert.Empty(t, client.conditions)
}

// A cache on its own way out breaks the chain above a still-live resource: the cascade
// is coming for the whole subtree, so nothing here clears rows the cache's own teardown
// will take with it.
func TestResourceReconcileSettlesWhenOwnerChainIsBroken(t *testing.T) {
	d, cache, obj := servingResource(t, true)
	require.NoError(t, d.cacheClient.Delete(context.Background(), cache.ID))

	client, res := reconcileResource(t, d, obj)

	assert.Equal(t, beehive.Settled(), res)
	assert.Equal(t, []string{obj.Name}, syncFleet(d).forgotten)
	assert.Empty(t, kubestoreFake(d).opened)
	assert.Empty(t, client.conditions)
}

// A disabled kind keeps its data — pause is not clear — and only stops the worker.
func TestResourceReconcileDisabledForgetsAndPauses(t *testing.T) {
	d, _, obj := servingResource(t, false)

	client, res := reconcileResource(t, d, obj)

	require.NoError(t, res.Err())
	assert.Equal(t, beehive.Settled(), res)
	assert.Equal(t, []string{obj.Name}, syncFleet(d).forgotten)
	assert.Empty(t, syncFleet(d).tracked)
	cond := client.synced(t)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Equal(t, ReasonPaused, cond.Reason)
}

// An enabled kind with a healthy owner chain arms the worker with exactly the params it
// needs to sync, and reports the wait for its first observation.
func TestResourceReconcileArmsTheWorker(t *testing.T) {
	d, cache, obj := servingResource(t, true)

	client, res := reconcileResource(t, d, obj)

	require.NoError(t, res.Err())
	assert.Equal(t, beehive.Settled(), res)
	f := syncFleet(d)
	assert.Equal(t, []string{obj.Name}, f.tracked)
	assert.Equal(t, kubesync.Params{
		CacheID:     int64(cache.ID),
		ContextName: "prod",
		ServerUID:   "uid-1",
		APIVersion:  "apps/v1",
		Kind:        "Deployment",
		Resource:    "deployments",
	}, f.armedWith[obj.Name])
	cond := client.synced(t)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Equal(t, ReasonConnecting, cond.Reason)
}

// The worker's observation reasons map onto the record's own condition vocabulary — kept
// a separate switch on purpose even where the strings coincide, since the two are
// different kinds' words for their own state.
func TestResourceReconcileMapsTheObservation(t *testing.T) {
	tests := map[string]struct {
		reason     string
		wantStatus ConditionStatus
		wantReason string
	}{
		"watching":            {kubesync.ReasonWatching, ConditionTrue, ReasonWatching},
		"syncing":             {kubesync.ReasonSyncing, ConditionFalse, ReasonSyncing},
		"stale":               {kubesync.ReasonStale, ConditionFalse, ReasonStale},
		"sync failed":         {kubesync.ReasonSyncFailed, ConditionFalse, ReasonSyncFailed},
		"no connection":       {kubesync.ReasonNoConnection, ConditionFalse, ReasonNoConnection},
		"identity mismatch":   {kubesync.ReasonIdentityMismatch, ConditionFalse, ReasonIdentityMismatch},
		"an unrecognized one": {"SomethingElse", ConditionFalse, "SomethingElse"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			d, _, obj := servingResource(t, true)
			syncFleet(d).obs = map[string]kubesync.Observation{
				obj.Name: {Reason: tt.reason, Message: "detail"},
			}

			client, res := reconcileResource(t, d, obj)

			require.NoError(t, res.Err())
			cond := client.synced(t)
			assert.Equal(t, tt.wantStatus, cond.Status)
			assert.Equal(t, tt.wantReason, cond.Reason)
			assert.Equal(t, "detail", cond.Message)
		})
	}
}

// A cluster whose record cannot name a context — no kubeconfig credentials, here — is
// the record's own state, reported directly, the same as the catalog's identical branch.
func TestResourceReconcileReportsNoConnectionWhenTheClusterCannotBeReached(t *testing.T) {
	d := newTestDeps(t)
	cluster, err := d.clusterClient.Create(context.Background(), "adopted", ClusterSpec{Enabled: true})
	require.NoError(t, err)
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	catalog := createCatalog(t, d, ClusterCacheID(cache.ID), true)
	obj := createResource(t, d, catalog.ID, deploymentsSpec)

	client, res := reconcileResource(t, d, obj)

	require.NoError(t, res.Err())
	assert.Equal(t, beehive.Settled(), res)
	assert.Equal(t, []string{obj.Name}, syncFleet(d).forgotten)
	assert.Empty(t, syncFleet(d).tracked)
	cond := client.synced(t)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Equal(t, ReasonNoConnection, cond.Reason)
}

// --- the CachedResources family ---

// createResource stores one per-kind record under a catalog, the way the discovery fold
// writes them.
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

// --- the sync event timeline ---

// syncEvents is the sync-category timeline the pass recorded.
func (c *resourceControllerClient) syncEvents(t *testing.T) []beehive.EventSpec {
	t.Helper()
	var out []beehive.EventSpec
	for _, ev := range c.events {
		if ev.Category == SyncEventCategory {
			out = append(out, ev)
		}
	}
	return out
}

// storedSynced is the verdict a previous pass left on the record, which the fold
// compares this pass's against — only a move records an event.
func storedSynced(obj *beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus], reason string) {
	obj.Conditions = []Condition{LiveCondition(ConditionSynced, ConditionFalse, reason, "")}
}

// The transition into a verdict is what the timeline records — the worker cannot write
// one, since only a ControllerClient can, so the fold derives it from the move.
func TestResourceReconcileRecordsTheSyncTransition(t *testing.T) {
	tests := map[string]struct {
		stored     string
		obs        kubesync.Observation
		wantReason string
		wantType   beehive.EventType
	}{
		"a cold build starting": {
			obs:        kubesync.Observation{Reason: kubesync.ReasonSyncing},
			wantReason: ReasonSyncStart, wantType: beehive.EventNormal,
		},
		"a warm cache resuming": {
			obs:        kubesync.Observation{Reason: kubesync.ReasonSyncing, Resumed: true, ObjectCount: 12},
			wantReason: ReasonResyncStart, wantType: beehive.EventNormal,
		},
		"a cold build caught up": {
			stored:     ReasonSyncing,
			obs:        kubesync.Observation{Reason: kubesync.ReasonWatching, ObjectCount: 7},
			wantReason: ReasonSyncComplete, wantType: beehive.EventNormal,
		},
		"a resume caught up": {
			stored:     ReasonSyncing,
			obs:        kubesync.Observation{Reason: kubesync.ReasonWatching, Resumed: true, ObjectCount: 7},
			wantReason: ReasonResyncComplete, wantType: beehive.EventNormal,
		},
		"a watch that came back from stale": {
			stored:     ReasonStale,
			obs:        kubesync.Observation{Reason: kubesync.ReasonWatching, ObjectCount: 7},
			wantReason: ReasonResyncComplete, wantType: beehive.EventNormal,
		},
		"a worker failing": {
			stored:     ReasonWatching,
			obs:        kubesync.Observation{Reason: kubesync.ReasonSyncFailed, Message: "forbidden"},
			wantReason: ReasonSyncDegraded, wantType: beehive.EventWarning,
		},
		"a watch going quiet": {
			stored:     ReasonWatching,
			obs:        kubesync.Observation{Reason: kubesync.ReasonStale},
			wantReason: ReasonSyncStale, wantType: beehive.EventWarning,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			d, _, obj := servingResource(t, true)
			if tt.stored != "" {
				storedSynced(obj, tt.stored)
			}
			syncFleet(d).obs = map[string]kubesync.Observation{obj.Name: tt.obs}

			client, res := reconcileResource(t, d, obj)

			require.NoError(t, res.Err())
			events := client.syncEvents(t)
			require.Len(t, events, 1)
			assert.Equal(t, tt.wantReason, events[0].Reason)
			assert.Equal(t, tt.wantType, events[0].Type)
		})
	}
}

// A resume's message reports the warm size, which is what tells it from a cold build
// in a timeline read weeks later.
func TestResourceReconcileReportsTheWarmSizeOnAResume(t *testing.T) {
	d, _, obj := servingResource(t, true)
	syncFleet(d).obs = map[string]kubesync.Observation{
		obj.Name: {Reason: kubesync.ReasonSyncing, Resumed: true, ObjectCount: 12},
	}

	client, _ := reconcileResource(t, d, obj)

	require.Len(t, client.syncEvents(t), 1)
	assert.Contains(t, client.syncEvents(t)[0].Message, "12")
}

// A pass that observed the verdict already on the record records nothing: a healthy
// steady state is silent, and the fold runs on every resync.
func TestResourceReconcileRecordsNoEventWithoutAMove(t *testing.T) {
	d, _, obj := servingResource(t, true)
	storedSynced(obj, ReasonWatching)
	syncFleet(d).obs = map[string]kubesync.Observation{
		obj.Name: {Reason: kubesync.ReasonWatching, ObjectCount: 7},
	}

	client, res := reconcileResource(t, d, obj)

	require.NoError(t, res.Err())
	assert.Empty(t, client.syncEvents(t))
}

// The disarm branches say so on the timeline: a kind that stops syncing because it was
// paused, or because its cluster cannot be reached, is not a kind that failed.
func TestResourceReconcileRecordsTheDisarmBranches(t *testing.T) {
	t.Run("paused", func(t *testing.T) {
		d, _, obj := servingResource(t, false)
		storedSynced(obj, ReasonWatching)

		client, res := reconcileResource(t, d, obj)

		require.NoError(t, res.Err())
		require.Len(t, client.syncEvents(t), 1)
		assert.Equal(t, ReasonSyncStopped, client.syncEvents(t)[0].Reason)
	})

	t.Run("no connection", func(t *testing.T) {
		d := newTestDeps(t)
		cluster, err := d.clusterClient.Create(context.Background(), "adopted", ClusterSpec{Enabled: true})
		require.NoError(t, err)
		cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
		catalog := createCatalog(t, d, ClusterCacheID(cache.ID), true)
		obj := createResource(t, d, catalog.ID, deploymentsSpec)
		storedSynced(obj, ReasonWatching)

		client, res := reconcileResource(t, d, obj)

		require.NoError(t, res.Err())
		require.Len(t, client.syncEvents(t), 1)
		assert.Equal(t, ReasonSyncStopped, client.syncEvents(t)[0].Reason)
	})
}

// Clearing one kind is the cache-wide clear per kind: stop that worker, drop its rows,
// and requeue the record whose pass re-arms it.
func TestCachedResourcesClearStopsTheWorkerAndClearsTheKind(t *testing.T) {
	d, cache, obj := servingResource(t, true)

	got, err := serviceOver(t, d).CachedResources().Clear(context.Background(), ClusterCachedResourceID(obj.ID))

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, ClusterCachedResourceID(obj.ID), got.ID)
	assert.Equal(t, []string{obj.Name}, syncFleet(d).forgotten, "the record's name is the subject id")
	// Inside the hold: the rows and the cookie go together, so a worker armed between
	// them would resume its watch and never re-list the kind.
	assert.Equal(t, []string{obj.Name}, syncFleet(d).held, "the clear ran outside the hold")
	assert.Equal(t, []int64{int64(cache.ID)}, kubestoreFake(d).opened)
}

func TestCachedResourcesClearOfAnUnknownRecordIsNotAnError(t *testing.T) {
	d := newTestDeps(t)

	got, err := serviceOver(t, d).CachedResources().Clear(context.Background(), 999)

	require.NoError(t, err)
	assert.Nil(t, got)
}

// A poke is a resume, not a rebuild: every worker restarts in place off its cookie.
func TestResourceControllerRestartsEveryWorkerOnAPoke(t *testing.T) {
	pokeSvc := poke.New()
	d := newTestDeps(t)
	d.pokeSvc = pokeSvc
	c := &clusterCachedResourceController{deps: d}
	stop, err := c.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })

	pokeSvc.Poke(poke.SourceHost)

	require.Eventually(t, func() bool {
		return syncFleet(d).fleetRestartCount() > 0
	}, testutil.Timeout, time.Millisecond)
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
func TestCachedResourcesClearRearmsTheWorkerWhenTheStoreFails(t *testing.T) {
	d, status := newReconcilingDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	catalog := createCatalog(t, d, ClusterCacheID(cache.ID), true)
	obj := createResource(t, d, catalog.ID, deploymentsSpec)
	fleet := syncFleet(d)
	armed := fleet.settle(t)
	kubestoreFake(d).err = errors.New("disk full")

	_, err := serviceOver(t, d).CachedResources().Clear(context.Background(), ClusterCachedResourceID(obj.ID))

	require.Error(t, err, "the failed clear was reported as a success")
	require.Eventually(t, func() bool {
		return fleet.arms() > armed
	}, testutil.Timeout, time.Millisecond, "the kind was left disarmed by a failed clear")
}

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

// The per-kind clear owes the same: its requeue is what arms the worker again, and a
// caller that hangs up mid-clear must not cost the kind its sync.
func TestCachedResourcesClearRearmsTheWorkerWhenTheCallerHangsUp(t *testing.T) {
	d, status := newReconcilingDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	catalog := createCatalog(t, d, ClusterCacheID(cache.ID), true)
	obj := createResource(t, d, catalog.ID, deploymentsSpec)
	fleet := syncFleet(d)
	armed := fleet.settle(t)

	ctx, cancel := context.WithCancel(context.Background())
	kubestoreFake(d).onOpen = func(int64) { cancel() }

	_, err := serviceOver(t, d).CachedResources().Clear(ctx, ClusterCachedResourceID(obj.ID))

	// The clear itself goes with the caller — its statements ride that context — but the
	// worker it stopped is not the caller's to leave stopped.
	require.Error(t, err)
	require.Eventually(t, func() bool {
		return fleet.arms() > armed
	}, testutil.Timeout, time.Millisecond, "the kind was left disarmed by a cancelled caller")
}

// A kind the discovery pass has just written, on a cluster whose sync is off, has not
// stopped syncing — it never started. The timeline is for what happened.
func TestResourceReconcileRecordsNoStopBeforeAnythingStarted(t *testing.T) {
	d, _, obj := servingResource(t, false)

	client, res := reconcileResource(t, d, obj)

	require.NoError(t, res.Err())
	assert.Equal(t, ReasonPaused, client.synced(t).Reason)
	assert.Empty(t, client.syncEvents(t), "a kind that never started was recorded as stopping")
}
