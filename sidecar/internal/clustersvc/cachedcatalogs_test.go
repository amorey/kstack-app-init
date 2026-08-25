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

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubecatalog"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"

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

// --- the discovery pass ---

// catalogControllerClient answers the owner lookup a catalog reconcile makes, from the
// store, so the edge a fixture wrote is the one the reconcile reads. It records the
// conditions written rather than storing them, since the verdict is what the tests assert.
// The embedded interface is nil: Reconcile calls nothing else on it.
type catalogControllerClient struct {
	beehive.ControllerClient[ClusterCachedCatalogStatus]
	catalogs beehive.Client[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]
	id       beehive.ObjectID
	// noOwner stands in for a catalog whose owner edge is gone, which the store cannot
	// hold while the row is still there.
	noOwner    bool
	conditions []Condition
}

func (c *catalogControllerClient) GetOwner(ctx context.Context) (beehive.ObjectRef, bool, error) {
	if c.noOwner {
		return beehive.ObjectRef{}, false, nil
	}
	return c.catalogs.GetOwner(ctx, c.id)
}

func (c *catalogControllerClient) SetCondition(_ context.Context, cond Condition) error {
	c.conditions = append(c.conditions, cond)
	return nil
}

// discovered is the verdict the pass recorded.
func (c *catalogControllerClient) discovered(t *testing.T) Condition {
	t.Helper()
	require.Len(t, c.conditions, 1, "one pass writes one verdict")
	require.Equal(t, string(ConditionDiscovered), c.conditions[0].Type)
	return c.conditions[0]
}

// servingCatalog stores a tracked, syncing cluster with a cache and its catalog, and
// returns the deps plus the catalog the pass under test reconciles.
func servingCatalog(t *testing.T, enabled bool) (deps, *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]) {
	t.Helper()
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	return d, createCatalog(t, d, ClusterCacheID(cache.ID), enabled)
}

// sweeper is the fake behind the pass under test.
func sweeper(d deps) *fakeKubecatalog { return d.kubecatalogSvc.(*fakeKubecatalog) }

// sweepAnswered files the sweeper's standing answer for this catalog's subject.
func sweepAnswered(d deps, obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus], o kubecatalog.Observation) {
	f := sweeper(d)
	if f.obs == nil {
		f.obs = map[string]kubecatalog.Observation{}
	}
	f.obs[obj.Name] = o
}

// swept is a sweep that answered these kinds at probedAt.
func swept(kinds ...kubecatalog.Kind) kubecatalog.Observation {
	return kubecatalog.Observation{
		Value:    kubecatalog.Catalog{Kinds: kinds},
		LastSeen: probedAt,
		Attempts: kubeconn.Attempts{LastAttempt: finished(kubeconn.ReasonSucceeded, "")},
	}
}

// partialSwept is a sweep some groups failed to answer: the partial list is committed
// and the run failed, which is how the probe records it.
func partialSwept(kinds ...kubecatalog.Kind) kubecatalog.Observation {
	o := swept(kinds...)
	o.Value.Partial = true
	o.Attempts = kubeconn.Attempts{
		LastAttempt: finished(kubecatalog.ReasonSweepPartial, "a group failed to answer"),
		Failures:    1, FailingSince: probedAt,
	}
	return o
}

// deployments and pods are the two kinds the pass fixtures discover.
var (
	deployments = kubecatalog.Kind{GroupVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Namespaced: true}
	pods        = kubecatalog.Kind{GroupVersion: "v1", Kind: "Pod", Resource: "pods", Namespaced: true}
)

// reconcileCatalog runs one catalog pass the way beehive would, folding whatever the
// fake sweeper holds.
func reconcileCatalog(
	t *testing.T,
	d deps,
	obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus],
) (*catalogControllerClient, beehive.ReconcileResult) {
	t.Helper()
	client := &catalogControllerClient{catalogs: d.catalogClient, id: obj.ID}
	c := &clusterCachedCatalogController{deps: d}
	return client, c.Reconcile(context.Background(), client, obj)
}

// storedKinds reads the per-kind children back, keyed the way the pass names them.
func storedKinds(t *testing.T, d deps, catalogID beehive.ObjectID) map[SyncedKindRef]*beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus] {
	t.Helper()
	objs, err := d.resourceClient.ListOwnedObjects(context.Background(), catalogID)
	require.NoError(t, err)

	byRef := map[SyncedKindRef]*beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus]{}
	for _, obj := range objs {
		byRef[SyncedKindRef{APIVersion: obj.Spec.APIVersion, Resource: obj.Spec.Resource}] = obj
	}
	return byRef
}

// The pass's first job: the sweep is armed exactly while the record wants discovery,
// keyed by the record's own name and bound to its cluster's context. No answer has
// landed yet, so the verdict is the wait — and no requeue, since the sweeper's signal
// re-runs the fold and the kind's resync is the backstop.
//
// The server is named alongside the context because a context is not an identity: it can
// be re-pointed at another cluster, and this cache mirrors one server only.
func TestCatalogReconcileArmsTheSweeper(t *testing.T) {
	d, catalog := servingCatalog(t, true)

	client, res := reconcileCatalog(t, d, catalog)

	require.NoError(t, res.Err())
	assert.Equal(t, beehive.Settled(), res)
	f := sweeper(d)
	assert.Equal(t, []string{catalog.Name}, f.tracked)
	assert.Equal(t, armedSubject{contextName: "prod", serverUID: "uid-1"}, f.armedFor[catalog.Name])
	assert.Empty(t, storedKinds(t, d, catalog.ID))
	cond := client.discovered(t)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Equal(t, ReasonConnecting, cond.Reason)
}

// The pass's whole product: one child per served kind, owned by the catalog so beehive's
// GC cascades, carrying the identity a worker builds its REST path from.
func TestCatalogReconcileCreatesAChildPerServedKind(t *testing.T) {
	d, catalog := servingCatalog(t, true)
	sweepAnswered(d, catalog, swept(deployments, pods))

	client, res := reconcileCatalog(t, d, catalog)

	require.NoError(t, res.Err())
	kinds := storedKinds(t, d, catalog.ID)
	require.Len(t, kinds, 2)
	got := kinds[SyncedKindRef{APIVersion: "apps/v1", Resource: "deployments"}]
	require.NotNil(t, got)
	assert.Equal(t, "Deployment", got.Spec.Kind)
	assert.True(t, got.Spec.Namespaced)
	assert.True(t, got.Spec.Enabled, "the pause switch is relayed in")
	assert.Equal(t, ReasonDiscovered, client.discovered(t).Reason)
}

// Every pass rewrites the children, so a kind that changed shape converges without being
// recreated — which is what keeps its id, and any subscription keyed on it, alive.
func TestCatalogReconcileRefreshesAChangedKind(t *testing.T) {
	d, catalog := servingCatalog(t, true)
	sweepAnswered(d, catalog, swept(pods))
	_, res := reconcileCatalog(t, d, catalog)
	require.NoError(t, res.Err())
	before := storedKinds(t, d, catalog.ID)[SyncedKindRef{APIVersion: "v1", Resource: "pods"}]

	clusterScoped := pods
	clusterScoped.Namespaced = false
	sweepAnswered(d, catalog, swept(clusterScoped))
	_, res = reconcileCatalog(t, d, catalog)
	require.NoError(t, res.Err())

	after := storedKinds(t, d, catalog.ID)[SyncedKindRef{APIVersion: "v1", Resource: "pods"}]
	require.NotNil(t, after)
	assert.Equal(t, before.ID, after.ID, "the record converges rather than being replaced")
	assert.False(t, after.Spec.Namespaced)
}

// A kind the server stopped serving loses its child: nothing else would ever stop the
// worker behind it.
func TestCatalogReconcilePrunesAKindNoLongerServed(t *testing.T) {
	d, catalog := servingCatalog(t, true)
	sweepAnswered(d, catalog, swept(deployments, pods))
	_, res := reconcileCatalog(t, d, catalog)
	require.NoError(t, res.Err())

	sweepAnswered(d, catalog, swept(pods))
	_, res = reconcileCatalog(t, d, catalog)

	require.NoError(t, res.Err())
	gone := storedKinds(t, d, catalog.ID)[SyncedKindRef{APIVersion: "apps/v1", Resource: "deployments"}]
	require.NotNil(t, gone, "beehive's delete is a tombstone, so the row is still there")
	assert.NotNil(t, gone.DeletionRequestedAt)
}

// A group that failed to answer has not stopped being served, so a partial sweep adds
// without pruning: deleting its children would stop live workers over a transient outage
// of one aggregated API server.
func TestCatalogReconcilePrunesNothingOnAPartialSweep(t *testing.T) {
	d, catalog := servingCatalog(t, true)
	sweepAnswered(d, catalog, swept(deployments, pods))
	_, res := reconcileCatalog(t, d, catalog)
	require.NoError(t, res.Err())

	sweepAnswered(d, catalog, partialSwept(pods))
	client, res := reconcileCatalog(t, d, catalog)

	require.NoError(t, res.Err())
	kept := storedKinds(t, d, catalog.ID)[SyncedKindRef{APIVersion: "apps/v1", Resource: "deployments"}]
	require.NotNil(t, kept)
	assert.Nil(t, kept.DeletionRequestedAt, "the group that went quiet is not shown gone")
	assert.Equal(t, ReasonDiscoveryPartial, client.discovered(t).Reason)
}

// Nothing is known about the served kinds when the last sweep failed, and an empty
// answer is not the same as "serves nothing" — so the children converge from the
// standing answer while the verdict says it is not being re-confirmed. The fold
// settles: retrying the sweep is the probe's own ladder, not beehive's.
func TestCatalogReconcileKeepsItsChildrenWhenTheSweepFails(t *testing.T) {
	d, catalog := servingCatalog(t, true)
	sweepAnswered(d, catalog, swept(pods))
	_, res := reconcileCatalog(t, d, catalog)
	require.NoError(t, res.Err())

	failing := swept(pods)
	failing.Attempts = kubeconn.Attempts{
		LastAttempt: finished(kubecatalog.ReasonSweepFailed, "the server rejected our request"),
		Failures:    1, FailingSince: probedAt,
	}
	sweepAnswered(d, catalog, failing)
	client, res := reconcileCatalog(t, d, catalog)

	require.NoError(t, res.Err())
	assert.Equal(t, beehive.Settled(), res)
	kept := storedKinds(t, d, catalog.ID)[SyncedKindRef{APIVersion: "v1", Resource: "pods"}]
	require.NotNil(t, kept)
	assert.Nil(t, kept.DeletionRequestedAt)
	cond := client.discovered(t)
	assert.Equal(t, ReasonDiscoveryFailed, cond.Reason)
	assert.Equal(t, "the server rejected our request", cond.Message)
}

// The verdict is the last attempt's, not the retained value's: a sweep that failed
// outright after a partial answer must report that failure, not the standing partial
// flag it left behind.
func TestCatalogReconcileReportsAFailureOverAStalePartialFlag(t *testing.T) {
	d, catalog := servingCatalog(t, true)
	sweepAnswered(d, catalog, partialSwept(pods))
	_, res := reconcileCatalog(t, d, catalog)
	require.NoError(t, res.Err())

	failing := partialSwept(pods)
	failing.Attempts = kubeconn.Attempts{
		LastAttempt: finished(kubecatalog.ReasonSweepFailed, "the server rejected our request"),
		Failures:    2, FailingSince: probedAt,
	}
	sweepAnswered(d, catalog, failing)
	client, res := reconcileCatalog(t, d, catalog)

	require.NoError(t, res.Err())
	cond := client.discovered(t)
	assert.Equal(t, ReasonDiscoveryFailed, cond.Reason)
	assert.Equal(t, "the server rejected our request", cond.Message)
}

// A cluster nothing reached: the sweep is suspended on its claim, and the fold reports
// the wait without a requeue of its own — the connection bridge wakes the sweep the
// moment the pool reaches the server, and the sweep's signal re-runs this fold.
func TestCatalogReconcileWaitsForAConnection(t *testing.T) {
	d, catalog := servingCatalog(t, true)
	sweepAnswered(d, catalog, kubecatalog.Observation{
		Attempts: kubeconn.Attempts{LastAttempt: suspended(kubecatalog.ReasonNoConnection, "no connection for kube-context")},
	})

	client, res := reconcileCatalog(t, d, catalog)

	require.NoError(t, res.Err())
	assert.Equal(t, beehive.Settled(), res)
	assert.Empty(t, storedKinds(t, d, catalog.ID))
	assert.Equal(t, ReasonNoConnection, client.discovered(t).Reason)
}

// suspended is an attempt that parked the probe rather than failing it.
func suspended(reason kubeconn.Reason, msg string) kubeconn.Attempt {
	return kubeconn.Attempt{
		ScheduledAt: probedAt, StartedAt: probedAt, FinishedAt: probedAt,
		Verdict: kubeconn.VerdictSuspended, Reason: reason, Message: msg,
	}
}

// A paused catalog keeps its subtree and stops looking: pausing disarms the sweep —
// arming is policy, not interest — and the anchor lives as long as the cache, so
// resuming must not rebuild what pausing tore down. The switch still reaches the
// children, since relaying it is what pausing means.
func TestCatalogReconcileRelaysAPauseAndDisarmsTheSweep(t *testing.T) {
	d, catalog := servingCatalog(t, true)
	sweepAnswered(d, catalog, swept(pods))
	_, res := reconcileCatalog(t, d, catalog)
	require.NoError(t, res.Err())

	paused := createCatalog(t, d, ClusterCacheID(mustOwnerID(t, d, catalog)), false)
	client, res := reconcileCatalog(t, d, paused)

	require.NoError(t, res.Err())
	assert.Equal(t, []string{paused.Name}, sweeper(d).forgotten)
	kept := storedKinds(t, d, catalog.ID)[SyncedKindRef{APIVersion: "v1", Resource: "pods"}]
	require.NotNil(t, kept, "the subtree survives a pause")
	assert.False(t, kept.Spec.Enabled, "and hears about it")
	assert.Equal(t, ReasonPaused, client.discovered(t).Reason)
}

// mustOwnerID returns the cache a catalog hangs off.
func mustOwnerID(t *testing.T, d deps, catalog *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]) beehive.ObjectID {
	t.Helper()
	ref, ok, err := d.catalogClient.GetOwner(context.Background(), catalog.ID)
	require.NoError(t, err)
	require.True(t, ok)
	return ref.ID
}

// A catalog on its way out is about to be collected with everything it owns, so a pass
// that rebuilt its children would only make work for the GC. The sweep is disarmed with
// the record.
func TestCatalogReconcileSkipsARecordAwaitingDeletion(t *testing.T) {
	d, catalog := servingCatalog(t, true)
	now := time.Now()
	catalog.DeletionRequestedAt = &now

	client, res := reconcileCatalog(t, d, catalog)

	assert.Equal(t, beehive.Settled(), res)
	assert.Equal(t, []string{catalog.Name}, sweeper(d).forgotten)
	assert.Empty(t, storedKinds(t, d, catalog.ID))
	assert.Empty(t, client.conditions)
}

// The cascade that takes this catalog may have collected the cache above it already,
// which is a race rather than a failure worth retrying under backoff.
func TestCatalogReconcileSettlesWhenItsOwnerIsGone(t *testing.T) {
	d, catalog := servingCatalog(t, true)
	client := &catalogControllerClient{catalogs: d.catalogClient, id: catalog.ID, noOwner: true}

	res := (&clusterCachedCatalogController{deps: d}).
		Reconcile(context.Background(), client, catalog)

	assert.Equal(t, beehive.Settled(), res)
	assert.Equal(t, []string{catalog.Name}, sweeper(d).forgotten)
	assert.Empty(t, client.conditions)
}

// --- CachedCatalogs() reads ---

// createCatalog stores one cache's catalog through the same write the cache controller
// makes — a fixture that hand-rolled the name, spec and owner edge could drift from what
// production actually stores.
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
func TestCatalogReconcileReportsAnIdentityMismatchBeforeAnySweep(t *testing.T) {
	d, catalog := servingCatalog(t, true)
	sweepAnswered(d, catalog, kubecatalog.Observation{
		Attempts: kubeconn.Attempts{
			LastAttempt: suspended(kubecatalog.ReasonIdentityMismatch, "context \"prod\" reached uid-2, not uid-1"),
		},
	})

	client, res := reconcileCatalog(t, d, catalog)

	require.NoError(t, res.Err())
	assert.Equal(t, beehive.Settled(), res)
	cond := client.discovered(t)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Equal(t, ReasonIdentityMismatch, cond.Reason)
}

// The same after a cache has swept: its standing answer stands, and the verdict says the
// identity moved rather than blaming the discovery request that was never made.
func TestCatalogReconcileReportsAnIdentityMismatchOverAStandingAnswer(t *testing.T) {
	d, catalog := servingCatalog(t, true)
	o := swept(pods)
	o.Attempts = kubeconn.Attempts{
		LastAttempt: suspended(kubecatalog.ReasonIdentityMismatch, "the server behind \"prod\" was replaced"),
	}
	sweepAnswered(d, catalog, o)

	client, res := reconcileCatalog(t, d, catalog)

	require.NoError(t, res.Err())
	assert.Equal(t, ReasonIdentityMismatch, client.discovered(t).Reason)
	assert.NotEmpty(t, storedKinds(t, d, catalog.ID), "the answer from its own server still converges")
}
