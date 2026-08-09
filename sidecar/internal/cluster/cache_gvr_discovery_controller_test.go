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

package cluster

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/beehive"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/objectsync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// These tests cover the controller's own responsibility: turning one discovery answer into
// the right set of ClusterCacheGVRSync children. The API server is faked, so what's
// exercised is the filtering, the set reconcile (add / refresh / prune), and the prune
// safety a partial answer demands.

// fakeDiscovery is a swappable discovery answer. Its lists and error can be replaced
// mid-test to model a CRD appearing, a kind being uninstalled, or a group going down.
type fakeDiscovery struct {
	mu    sync.Mutex
	lists []*metav1.APIResourceList
	err   error
}

func (f *fakeDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lists, f.err
}

func (f *fakeDiscovery) set(lists []*metav1.APIResourceList, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists, f.err = lists, err
}

// listableVerbs is what a syncable resource advertises; a kind missing either verb can't be
// mirrored.
var listableVerbs = metav1.Verbs{"get", "list", "watch"}

// defaultDiscoveryLists is a small but representative answer: three syncable kinds
// (Events among them — they sync like any other kind, into their own table), plus the
// three shapes that must be filtered out: a subresource, a create-only endpoint, and the
// events.k8s.io spelling of Events, which is the same collection served twice.
func defaultDiscoveryLists() []*metav1.APIResourceList {
	return []*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Kind: "Pod", Namespaced: true, Verbs: listableVerbs},
				{Name: "pods/log", Kind: "Pod", Namespaced: true, Verbs: listableVerbs},
				{Name: "events", Kind: "Event", Namespaced: true, Verbs: listableVerbs},
			},
		},
		{
			GroupVersion: "apps/v1",
			APIResources: []metav1.APIResource{
				{Name: "deployments", Kind: "Deployment", Namespaced: true, Verbs: listableVerbs},
			},
		},
		{
			GroupVersion: "events.k8s.io/v1",
			APIResources: []metav1.APIResource{
				{Name: "events", Kind: "Event", Namespaced: true, Verbs: listableVerbs},
			},
		},
		{
			GroupVersion: "authorization.k8s.io/v1",
			APIResources: []metav1.APIResource{
				{Name: "subjectaccessreviews", Kind: "SubjectAccessReview", Verbs: metav1.Verbs{"create"}},
			},
		},
	}
}

// gvrDiscoveryFixture is a running control plane holding the owner chain Cluster →
// ClusterCache → ClusterCacheGVRDiscovery → ClusterCacheGVRSync, with the discovery
// controller real and the API server faked.
type gvrDiscoveryFixture struct {
	ctrl      *ClusterCacheGVRDiscoveryController
	client    beehive.Client[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus]
	syncs     beehive.Client[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus]
	api       *fakeDiscovery
	connMgr   *ConnectionManager
	clusterCC beehive.ControllerClient[ClusterStatus]
	// syncCC clears a child's drain finalizer the way the sync controller would — this
	// fixture runs a no-op in its place.
	syncCC beehive.ControllerClient[ClusterCacheGVRSyncStatus]
	// discoveryCC is the controller's own status-write client, kept so a test can drive one
	// reconcile directly and read the Result it asked for.
	discoveryCC beehive.ControllerClient[ClusterCacheGVRDiscoveryStatus]
	clusterID   ClusterID
	cacheID     beehive.ObjectID
}

// newGVRDiscoveryFixture wires the chain. The two parents and the per-GVR children get no-op
// controllers: only the owner edges matter here (they key the credentials), not what those
// controllers would do with the objects.
func newGVRDiscoveryFixture(t *testing.T) *gvrDiscoveryFixture {
	t.Helper()
	ctx := context.Background()
	bh := NewTestBeehiveUnstarted(t)

	connMgr := NewConnectionManager()
	// The pools must close before TempDir's RemoveAll: on Windows an open file can't
	// be unlinked, so a leaked cache handle fails the cleanup.
	cacheMgr := store.NewManager(t.TempDir())
	t.Cleanup(func() { _ = cacheMgr.Shutdown(context.Background()) })
	rt := &controllerRuntime{bh: bh, connMgr: connMgr, cacheManager: cacheMgr}

	api := &fakeDiscovery{lists: defaultDiscoveryLists()}
	ctrl := NewClusterCacheGVRDiscoveryController(rt)
	ctrl.newDiscovery = func(*rest.Config) (resourceLister, error) { return api, nil }

	clusterCC, err := beehive.Register(bh, ClusterGroupKind, &NoopController[ClusterSpec, ClusterStatus]{})
	require.NoError(t, err)
	_, err = beehive.Register(bh, ClusterCacheGroupKind, &NoopController[ClusterCacheSpec, ClusterCacheStatus]{})
	require.NoError(t, err)
	discoveryCC, err := beehive.Register(bh, ClusterCacheGVRDiscoveryGroupKind, ctrl)
	require.NoError(t, err)
	syncCC, err := beehive.Register(bh, ClusterCacheGVRSyncGroupKind, &NoopController[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus]{})
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)

	clusterObj, err := coreClient.Create(ctx, kubeconfigName("alpha"), eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	cacheObj, err := cacheClient.Create(ctx,
		ClusterCacheName(ClusterID(clusterObj.ID), testCacheUID),
		ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	return &gvrDiscoveryFixture{
		ctrl:        ctrl,
		client:      beehive.NewClient[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus](bh, ClusterCacheGVRDiscoveryGroupKind),
		syncs:       beehive.NewClient[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus](bh, ClusterCacheGVRSyncGroupKind),
		api:         api,
		connMgr:     connMgr,
		clusterCC:   clusterCC,
		syncCC:      syncCC,
		discoveryCC: discoveryCC,
		clusterID:   ClusterID(clusterObj.ID),
		cacheID:     cacheObj.ID,
	}
}

// connect gives the cluster credentials, as a successful probe would.
func (f *gvrDiscoveryFixture) connect() {
	f.connMgr.Set(f.clusterID, &rest.Config{Host: "https://example"}, "fp-1")
}

// repoke writes the parent Cluster's status, which is what a core-controller probe does —
// waking us through the DependsOn edge. The tests use it to drive a second discovery pass
// without waiting out gvrDiscoveryInterval.
func (f *gvrDiscoveryFixture) repoke(t *testing.T) {
	t.Helper()
	version := "v1.34.0" // arbitrary: nothing reads it, the write itself is the wake
	require.NoError(t, f.clusterCC.UpdateStatus(context.Background(), beehive.ObjectID(f.clusterID), 1,
		ClusterStatus{Server: ClusterServer{Version: &version}}))
}

// createChild creates the discovery object the cache controller would, owned by the cache.
func (f *gvrDiscoveryFixture) createChild(t *testing.T, enabled bool) *beehive.Object[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus] {
	t.Helper()
	obj, err := f.client.Create(context.Background(),
		ClusterCacheGVRDiscoveryName(f.cacheID),
		ClusterCacheGVRDiscoverySpec{Enabled: enabled},
		beehive.WithOwner(f.cacheID))
	require.NoError(t, err)
	return obj
}

// syncChildren returns the discovery object's per-GVR children, ordered by "apiVersion/resource"
// so assertions read deterministically.
func (f *gvrDiscoveryFixture) syncChildren(t *testing.T, id beehive.ObjectID) []ClusterCacheGVRSyncSpec {
	t.Helper()
	children, err := f.syncs.ListOwnedObjects(context.Background(), id)
	require.NoError(t, err)
	out := make([]ClusterCacheGVRSyncSpec, 0, len(children))
	for _, child := range children {
		// ListOwnedObjects includes deletion-pending rows; a pruned child is gone as far
		// as these assertions are concerned.
		if child.DeletionRequestedAt != nil {
			continue
		}
		out = append(out, child.Spec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].APIVersion != out[j].APIVersion {
			return out[i].APIVersion < out[j].APIVersion
		}
		return out[i].Resource < out[j].Resource
	})
	return out
}

// awaitSyncGVRs blocks until the child set is exactly want (as "apiVersion/resource"
// strings), so a test sees the settled result of a pass rather than an intermediate one.
func (f *gvrDiscoveryFixture) awaitSyncGVRs(t *testing.T, id beehive.ObjectID, want []string) []ClusterCacheGVRSyncSpec {
	t.Helper()
	var last []ClusterCacheGVRSyncSpec
	ok := assert.Eventually(t, func() bool {
		last = f.syncChildren(t, id)
		got := make([]string, 0, len(last))
		for _, spec := range last {
			got = append(got, spec.APIVersion+"/"+spec.Resource)
		}
		return assert.ObjectsAreEqual(want, got)
	}, 2*time.Second, 10*time.Millisecond)
	if !ok {
		t.Fatalf("timed out waiting for GVR children %v (last=%+v)", want, last)
	}
	return last
}

// A child whose deletion is pending still comes back from ListOwnedObjects, and counting
// it as live meant a kind the cluster still serves got no replacement: its name was struck
// off the wanted set, the dying row was updated instead, and the pass reported
// Discovered=True for a kind with no worker and none coming.
//
// The child here is deletion-pending for good: its drain finalizer is only cleared by the
// sync controller, which this fixture doesn't run — which is exactly the window the bug
// lived in.
func TestGVRDiscoveryReplacesADeletingChild(t *testing.T) {
	ctx := context.Background()
	f := newGVRDiscoveryFixture(t)
	f.connect()
	obj := f.createChild(t, true)

	f.awaitSyncGVRs(t, obj.ID, []string{"apps/v1/deployments", "v1/events", "v1/pods"})

	// Delete one child the cluster still serves, the way a cascade would.
	children, err := f.syncs.ListOwnedObjects(ctx, obj.ID)
	require.NoError(t, err)
	var pods beehive.ObjectID
	for _, child := range children {
		if child.Spec.Resource == "pods" {
			pods = child.ID
		}
	}
	require.NotZero(t, pods)
	require.NoError(t, f.syncs.Delete(ctx, pods))

	dying, err := f.syncs.Get(ctx, pods)
	require.NoError(t, err)
	require.NotNil(t, dying.DeletionRequestedAt, "the child must still be draining")

	// The next pass cannot re-create pods — the dying row still holds the name — so it must
	// NOT report a converged kind set. Claiming Discovered here is the bug: the dead child
	// satisfied the wanted entry, so nothing was waiting on and nothing came back.
	f.repoke(t)
	cond := f.awaitDiscoveredReason(t, obj.ID, ReasonDiscoveryDraining)
	assert.Equal(t, ConditionFalse, cond.Status,
		"a kind whose child is still draining is not discovered-and-served")
	assert.Equal(t, []ClusterCacheGVRSyncSpec{
		{Enabled: true, APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Namespaced: true},
		{Enabled: true, APIVersion: "v1", Kind: "Event", Resource: "events", Namespaced: true},
	}, f.syncChildren(t, obj.ID), "pods has no live child yet")

	// Once the drain finishes and the row is collected, the kind comes back.
	require.NoError(t, f.syncCC.DeleteFinalizer(ctx, pods, gvrSyncDrainFinalizer))
	f.repoke(t)
	f.awaitSyncGVRs(t, obj.ID, []string{"apps/v1/deployments", "v1/events", "v1/pods"})
	f.awaitDiscoveredReason(t, obj.ID, ReasonDiscovered)
}

// Waiting on a drain must not become a poll. Every discovery pass costs a full
// ServerPreferredResources walk plus a ListOwnedObjects over the cache's children, so a
// fixed one-second requeue turned a drain that keeps timing out — a reinstalled CRD on a
// cache cold-syncing its kinds through a single writer connection — into one discovery
// request per second for as long as it lasted, holding one of the discovery worker slots
// the whole time. The retry belongs to beehive's backoff instead.
func TestGVRDiscoveryHandsADrainingWaitToBackoff(t *testing.T) {
	ctx := context.Background()
	f := newGVRDiscoveryFixture(t)
	f.connect()
	obj := f.createChild(t, true)
	f.awaitSyncGVRs(t, obj.ID, []string{"apps/v1/deployments", "v1/events", "v1/pods"})

	// Put one still-served kind into the draining state: its row holds the name, so the
	// next pass can't re-create it. The fixture runs no sync controller, so the drain
	// finalizer is never cleared and the wait is permanent.
	children, err := f.syncs.ListOwnedObjects(ctx, obj.ID)
	require.NoError(t, err)
	var pods beehive.ObjectID
	for _, child := range children {
		if child.Spec.Resource == "pods" {
			pods = child.ID
		}
	}
	require.NotZero(t, pods)
	require.NoError(t, f.syncs.Delete(ctx, pods))
	f.awaitDiscoveredReason(t, obj.ID, ReasonDiscoveryDraining)

	// Drive one pass directly and read what it asked for. A fixed RequeueAfter is the
	// poll; an error hands the cadence to beehive, which ramps 1s → 2s → … → 30s.
	current, err := f.client.Get(ctx, obj.ID)
	require.NoError(t, err)
	res, err := f.ctrl.Reconcile(ctx, f.discoveryCC, current)
	require.Error(t, err, "a pass that could not converge must report so, not schedule its own retry")
	assert.Zero(t, res.RequeueAfter, "the retry cadence is beehive's, not a fixed poll")
}

// The per-kind Forget on a child's deletion only reaps what it can reach: it looks the
// cache up rather than opening it, so a CRD uninstalled while the app was DOWN leaves its
// rows, edges, catalog entry and resume cookie behind forever — with the dashboard nav
// listing a kind the cluster no longer serves. A complete discovery pass sweeps them.
func TestGVRDiscoverySweepsKindsWithNoChild(t *testing.T) {
	ctx := context.Background()
	f := newGVRDiscoveryFixture(t)
	f.connect()
	obj := f.createChild(t, true)
	f.awaitSyncGVRs(t, obj.ID, []string{"apps/v1/deployments", "v1/events", "v1/pods"})

	// The cache as a previous run left it: a served kind, plus one whose CRD was
	// uninstalled while the app was down, so no child was ever created for it here.
	cdb, err := f.ctrl.cacheManager.Open(ctx, store.CacheRef{ClusterID: int64(f.clusterID), CacheID: int64(f.cacheID)})
	require.NoError(t, err)
	live := objectsync.Kind{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Namespaced: true}
	gone := objectsync.Kind{APIVersion: "widgets.example.com/v1", Kind: "Widget", Resource: "widgets", Namespaced: true}
	for _, k := range []objectsync.Kind{live, gone} {
		st, err := objectsync.NewStore(cdb, k)
		require.NoError(t, err)
		require.NoError(t, st.EnsureCatalog(ctx))
	}
	require.ElementsMatch(t, []string{"deployments", "widgets"}, cachedResources(t, cdb))

	// A complete pass must drop the orphan and leave the served kind alone.
	f.repoke(t)
	require.Eventually(t, func() bool {
		return assert.ObjectsAreEqual([]string{"deployments"}, cachedResources(t, cdb))
	}, 2*time.Second, 10*time.Millisecond, "a kind with no sync child must not stay in the cache")
}

// The sweep's whole safety argument is "no child means no worker, so nothing can be
// mid-write into the rows being dropped". That held only if the child set it is given is
// current — and it was the set read at the START of the pass, before the pass created
// anything. So on the pass that creates a kind's child, the sweep saw no child for it and
// forgot everything the cache held for that kind: rows, edges, catalog entry and resume
// cookie — while its worker, which starts the moment the child is created, was syncing into
// them. Every startup that recreated children wiped a warm cache.
func TestGVRDiscoveryDoesNotSweepAKindItJustCreatedAChildFor(t *testing.T) {
	ctx := context.Background()
	f := newGVRDiscoveryFixture(t)
	f.connect()

	// A warm cache from a previous run: the kind is catalogued and holds rows, and no
	// child exists yet in THIS process.
	cdb, err := f.ctrl.cacheManager.Open(ctx, store.CacheRef{ClusterID: int64(f.clusterID), CacheID: int64(f.cacheID)})
	require.NoError(t, err)
	kind := objectsync.Kind{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Namespaced: true}
	st, err := objectsync.NewStore(cdb, kind)
	require.NoError(t, err)
	require.NoError(t, st.EnsureCatalog(ctx))
	require.NoError(t, st.PersistRV(ctx, "42"))

	// The first pass: it creates the child for that very kind.
	obj := f.createChild(t, true)
	f.awaitSyncGVRs(t, obj.ID, []string{"apps/v1/deployments", "v1/events", "v1/pods"})

	// Nothing about that kind may have been swept.
	require.Never(t, func() bool {
		return !assert.ObjectsAreEqual([]string{"deployments"}, cachedResources(t, cdb))
	}, 300*time.Millisecond, 20*time.Millisecond,
		"a kind whose child this pass created is not an orphan")
	rv, err := st.ResumeRV(ctx)
	require.NoError(t, err)
	assert.Equal(t, "42", rv, "and its warm-resume position must survive, or the kind cold-LISTs again")
}

// cachedResources lists the plural of every kind in the cache's catalog, sorted. Events are
// excluded: their child is seeded on every pass, so they are never an orphan.
func cachedResources(t *testing.T, cdb *store.ClusterDB) []string {
	t.Helper()
	rows, err := cdb.Kinds(context.Background())
	require.NoError(t, err)
	out := []string{}
	for _, r := range rows {
		if r.Resource == "events" {
			continue
		}
		out = append(out, r.Resource)
	}
	sort.Strings(out)
	return out
}

// awaitDiscoveredReason blocks until the object's Discovered condition reaches want.
func (f *gvrDiscoveryFixture) awaitDiscoveredReason(t *testing.T, id beehive.ObjectID, want string) Condition {
	t.Helper()
	return awaitConditionReason(t, f.client, id, ConditionDiscovered, want)
}

// TestGVRDiscoveryCreatesOneChildPerSyncableGVR is the happy path, and with it the filter:
// every mirrorable kind gets a child — Events included — while the subresource, the
// create-only endpoint, and the duplicate events.k8s.io spelling do not. That last one
// matters most: two children for one underlying collection would give it two workers
// fighting over the same uid-keyed rows.
func TestGVRDiscoveryCreatesOneChildPerSyncableGVR(t *testing.T) {
	f := newGVRDiscoveryFixture(t)
	f.connect()
	obj := f.createChild(t, true)

	specs := f.awaitSyncGVRs(t, obj.ID, []string{"apps/v1/deployments", "v1/events", "v1/pods"})
	assert.Equal(t, ClusterCacheGVRSyncSpec{
		Enabled: true, APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Namespaced: true,
	}, specs[0], "the child carries the full GVR identity its worker needs")
	assert.Equal(t, ClusterCacheGVRSyncSpec{
		Enabled: true, APIVersion: "v1", Kind: "Event", Resource: "events", Namespaced: true,
	}, specs[1], "Events are an ordinary child; only their store differs")

	cond := f.awaitDiscoveredReason(t, obj.ID, ReasonDiscovered)
	assert.Equal(t, ConditionTrue, cond.Status)

	// The pass's gauges are held in the controller and read on request — never written
	// to the object, so nothing about them wakes this record's dependents.
	stats, ok := f.ctrl.Stats(obj.ID)
	require.True(t, ok, "a completed pass must record its gauges")
	assert.Equal(t, 3, stats.ResourceCount)
	assert.WithinDuration(t, time.Now(), stats.LastDiscoveryAt, time.Minute)

	stored, err := f.client.Get(context.Background(), obj.ID)
	require.NoError(t, err)
	assert.Nil(t, stored.Status,
		"status is never written at all — the kind's status is empty by design and the "+
			"gauges live out of band")
}

// TestGVRDiscoverySteadyPassWritesNothing pins the reason the gauges are out of band: with
// nothing left in status, a pass that changes no kind and no condition writes nothing at
// all — so a settled cache stops bumping its resource_version (and waking its dependents)
// every interval.
func TestGVRDiscoverySteadyPassWritesNothing(t *testing.T) {
	ctx := context.Background()
	f := newGVRDiscoveryFixture(t)
	f.connect()
	obj := f.createChild(t, true)
	f.awaitSyncGVRs(t, obj.ID, []string{"apps/v1/deployments", "v1/events", "v1/pods"})
	f.awaitDiscoveredReason(t, obj.ID, ReasonDiscovered)

	settled, err := f.client.Get(ctx, obj.ID)
	require.NoError(t, err)

	// Drive a second pass over an unchanged cluster.
	f.repoke(t)
	require.Eventually(t, func() bool {
		stats, ok := f.ctrl.Stats(obj.ID)
		return ok && stats.LastDiscoveryAt.After(settled.UpdatedAt)
	}, 2*time.Second, 10*time.Millisecond, "the second pass must run")

	after, err := f.client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, settled.ResourceVersion, after.ResourceVersion,
		"a steady pass must not bump resource_version — that is what propagates to dependents")
}

// TestGVRDiscoveryConvergesOnTheServedSet verifies the set reconcile in both directions: an
// installed CRD gains a child, an uninstalled kind loses one.
func TestGVRDiscoveryConvergesOnTheServedSet(t *testing.T) {
	f := newGVRDiscoveryFixture(t)
	f.connect()
	obj := f.createChild(t, true)
	f.awaitSyncGVRs(t, obj.ID, []string{"apps/v1/deployments", "v1/events", "v1/pods"})

	// A CRD is installed and Deployments are (implausibly, but the mechanism is the point)
	// no longer served.
	f.api.set([]*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{{Name: "pods", Kind: "Pod", Namespaced: true, Verbs: listableVerbs}},
		},
		{
			GroupVersion: "example.io/v1",
			APIResources: []metav1.APIResource{{Name: "widgets", Kind: "Widget", Namespaced: true, Verbs: listableVerbs}},
		},
	}, nil)
	f.repoke(t)

	f.awaitSyncGVRs(t, obj.ID, []string{"example.io/v1/widgets", "v1/pods"})
}

// TestGVRDiscoveryPartialAnswerDoesNotPrune is the prune-safety rule: when some api groups
// fail (a down aggregated APIService), the kinds that did answer are still converged, but
// nothing is deleted — a group that couldn't answer has not been shown to be gone.
func TestGVRDiscoveryPartialAnswerDoesNotPrune(t *testing.T) {
	f := newGVRDiscoveryFixture(t)
	f.connect()
	obj := f.createChild(t, true)
	f.awaitSyncGVRs(t, obj.ID, []string{"apps/v1/deployments", "v1/events", "v1/pods"})

	// apps/v1 drops out of the answer, but as a failed group rather than a removed one.
	f.api.set([]*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{{Name: "pods", Kind: "Pod", Namespaced: true, Verbs: listableVerbs}},
		},
		{
			GroupVersion: "example.io/v1",
			APIResources: []metav1.APIResource{{Name: "widgets", Kind: "Widget", Namespaced: true, Verbs: listableVerbs}},
		},
	}, &discovery.ErrGroupDiscoveryFailed{Groups: map[schema.GroupVersion]error{
		{Group: "apps", Version: "v1"}: errors.New("apiservice unavailable"),
	}})
	f.repoke(t)

	cond := f.awaitDiscoveredReason(t, obj.ID, ReasonDiscoveryPartial)
	assert.Equal(t, ConditionFalse, cond.Status)
	// The new kind was added; Deployments survived the outage.
	f.awaitSyncGVRs(t, obj.ID, []string{"apps/v1/deployments", "example.io/v1/widgets", "v1/events", "v1/pods"})
}

// TestGVRDiscoveryFailureKeepsChildren verifies a total discovery failure is inert: nothing
// is known about the served kinds, so the existing children stand and the condition says why.
func TestGVRDiscoveryFailureKeepsChildren(t *testing.T) {
	f := newGVRDiscoveryFixture(t)
	f.connect()
	obj := f.createChild(t, true)
	f.awaitSyncGVRs(t, obj.ID, []string{"apps/v1/deployments", "v1/events", "v1/pods"})

	f.api.set(nil, errors.New("connection refused"))
	f.repoke(t)

	cond := f.awaitDiscoveredReason(t, obj.ID, ReasonDiscoveryFailed)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Contains(t, cond.Message, "connection refused")
	assert.Len(t, f.syncChildren(t, obj.ID), 3, "a failed pass must not delete children")
}

// TestGVRDiscoveryPauseRelaysToChildren verifies the pause semantics: the children stay (so
// an unpause doesn't wait out a discovery pass) but each is switched off, and no discovery
// request is made.
func TestGVRDiscoveryPauseRelaysToChildren(t *testing.T) {
	ctx := context.Background()
	f := newGVRDiscoveryFixture(t)
	f.connect()
	obj := f.createChild(t, true)
	f.awaitSyncGVRs(t, obj.ID, []string{"apps/v1/deployments", "v1/events", "v1/pods"})

	_, err := f.client.Update(ctx, obj.ID, ClusterCacheGVRDiscoverySpec{Enabled: false})
	require.NoError(t, err)

	cond := f.awaitDiscoveredReason(t, obj.ID, ReasonPaused)
	assert.Equal(t, ConditionFalse, cond.Status)
	require.Eventually(t, func() bool {
		for _, spec := range f.syncChildren(t, obj.ID) {
			if spec.Enabled {
				return false
			}
		}
		return true
	}, 2*time.Second, 10*time.Millisecond, "a pause must reach every per-GVR child")
	assert.Len(t, f.syncChildren(t, obj.ID), 3, "a pause must not delete children")
}

// TestGVRDiscoveryWaitsForConnection verifies the normal startup ordering: the object can be
// enabled before its cluster has ever connected, and that is a wait, not a failure.
//
// The Event child is the one exception, and deliberately so: it is seeded without reaching
// the cluster at all, because events are the highest-value diagnostic data in the cache and
// must not wait on a discovery pass that may be slow, throttled, or blocked on credentials.
func TestGVRDiscoveryWaitsForConnection(t *testing.T) {
	f := newGVRDiscoveryFixture(t)
	obj := f.createChild(t, true) // no credentials yet

	cond := f.awaitDiscoveredReason(t, obj.ID, ReasonNoConnection)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Equal(t, []ClusterCacheGVRSyncSpec{eventsSyncSpec(true)}, f.syncChildren(t, obj.ID),
		"only Events exists before a connection — every other kind needs the pass")

	// Once the cluster connects, the core controller's status write wakes us through the
	// DependsOn edge and the first pass runs.
	f.connect()
	f.repoke(t)
	f.awaitSyncGVRs(t, obj.ID, []string{"apps/v1/deployments", "v1/events", "v1/pods"})
}

// TestGVRDiscoveryDependsOnParentCluster pins the edge the controller declares on its
// grandparent Cluster. It is load-bearing and invisible in behaviour: credentials live in the
// in-memory ConnectionManager, so a cluster connecting changes nothing this object would
// otherwise be woken by — without the edge, a pass waiting on credentials would only run on
// the next backstop requeue.
func TestGVRDiscoveryDependsOnParentCluster(t *testing.T) {
	ctx := context.Background()
	f := newGVRDiscoveryFixture(t)
	f.connect()
	obj := f.createChild(t, true)
	f.awaitSyncGVRs(t, obj.ID, []string{"apps/v1/deployments", "v1/events", "v1/pods"})

	deps, err := f.client.ListDependencies(ctx, obj.ID)
	require.NoError(t, err)
	ids := make([]beehive.ObjectID, 0, len(deps))
	for _, d := range deps {
		ids = append(ids, d.ID)
	}
	assert.Contains(t, ids, beehive.ObjectID(f.clusterID),
		"discovery must depend on its Cluster so a successful probe wakes it")
}
