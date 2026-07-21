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

// White-box (package cluster): the service test seeds beehive objects directly and
// exercises the data/mutation/watch surface in isolation from the (network-touching)
// real controllers, using the shared helpers in testutil_test.go.
package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// noopController satisfies beehive.Controller without reconciling. A test gets a
// ControllerClient to write status directly (the plain Client cannot) from the
// value beehive.Register returns for the kind.
type noopController[Spec, Status any] struct{}

func (c *noopController[Spec, Status]) Reconcile(context.Context, beehive.ControllerClient[Status], *beehive.Object[Spec, Status]) (beehive.Result, error) {
	return beehive.Result{}, nil
}

// fakeCoreController satisfies the coreController seam so the service test can assert
// the out-of-band dispatch (RetryConnection → Reprobe) without a network-touching
// ClusterCoreController. StartBackground/StopBackground are no-ops; Reprobe records the
// ids it was handed.
type fakeCoreController struct{ reprobed []ClusterID }

func (f *fakeCoreController) StartBackground()     {}
func (f *fakeCoreController) StopBackground()      {}
func (f *fakeCoreController) Reprobe(id ClusterID) { f.reprobed = append(f.reprobed, id) }

// WatchProbe returns a closed channel: this fake never probes, so the interface
// is satisfied without a live in-flight signal.
func (f *fakeCoreController) WatchProbe(context.Context, ClusterID) <-chan bool {
	ch := make(chan bool)
	close(ch)
	return ch
}

// newServiceTest builds a started beehive with no-op controllers and returns a
// service wired to its clients plus a temp cache manager. The returned
// ControllerClients write Cluster status (core) and ClusterCache status — the
// controller-owned surfaces a white-box test stamps directly.
func newServiceTest(t *testing.T) (*Service, beehive.ControllerClient[ClusterStatus], beehive.ControllerClient[ClusterCacheStatus]) {
	t.Helper()
	st, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	bh, err := beehive.New(st, beehive.WithResyncInterval(0))
	require.NoError(t, err)

	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)

	coreCC, err := beehive.Register(bh, ClusterGroupKind, &noopController[ClusterSpec, ClusterStatus]{})
	require.NoError(t, err)
	cacheCC, err := beehive.Register(bh, ClusterCacheGroupKind, &noopController[ClusterCacheSpec, ClusterCacheStatus]{})
	require.NoError(t, err)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	// Shut the cache manager down before the test ends so its SQLite pools close and
	// TempDir cleanup can remove the .db files — on Windows an open file can't be
	// unlinked, so a leaked pool fails the TempDir RemoveAll.
	cacheManager := store.NewManager(t.TempDir())
	t.Cleanup(func() { _ = cacheManager.Shutdown(context.Background()) })

	return &Service{
		coreClient:   coreClient,
		cacheClient:  cacheClient,
		cacheManager: cacheManager,
		connMgr:      NewConnectionManager(),
		coreCtrl:     &fakeCoreController{},
		// A short non-zero debounce keeps the watch tests fast while still exercising
		// the coalescing path (the Coalesces test overrides it to a wider window).
		dataKindsDebounce: 5 * time.Millisecond,
	}, coreCC, cacheCC
}

// seedCluster creates a Cluster (as the importer would, with the kubeconfig
// slug) and returns its ClusterID — the beehive ObjectID beehive assigned.
func seedCluster(t *testing.T, s *Service, ctxName string) ClusterID {
	t.Helper()
	ctx := context.Background()
	name := ctxName
	obj, err := s.coreClient.Create(ctx, ClusterSpec{
		Name:        &name,
		SyncEnabled: true,
		Enabled:     true,
		Source:      ClusterSpecSource{Kubeconfig: &ClusterSpecSourceKubeconfig{Context: ctxName}},
	}, beehive.WithSlug(kubeconfigSlug(ctxName)))
	require.NoError(t, err)
	return ClusterID(obj.ID)
}

// stampActiveUID records uid as a cluster's last-probed kube-system identity by
// writing it to Status.Server.UID (as the ClusterCoreController would after a
// probe). A ClusterCache for the same uid then resolves as the cluster's active
// cache.
func stampActiveUID(t *testing.T, s *Service, coreCC beehive.ControllerClient[ClusterStatus], id ClusterID, uid string) {
	t.Helper()
	ctx := context.Background()
	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	require.NoError(t, coreCC.UpdateStatus(ctx, obj.ID, obj.Generation, ClusterStatus{
		Server: ClusterServer{UID: &uid},
	}))
}

// seedActiveCache creates an active ClusterCache for a cluster: it stamps the
// cluster's connected UID and creates a ClusterCache (owned, UID-keyed slug) for
// that identity. Returns the cache's ObjectID.
func seedActiveCache(t *testing.T, s *Service, coreCC beehive.ControllerClient[ClusterStatus], id ClusterID, uid string) beehive.ObjectID {
	t.Helper()
	ctx := context.Background()
	stampActiveUID(t, s, coreCC, id, uid)
	cacheObj, err := s.cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: uid},
		beehive.WithSlug(ClusterCacheSlug(id, uid)), beehive.WithOwner(beehive.ObjectID(id)))
	require.NoError(t, err)
	return cacheObj.ID
}

func TestServiceListAndGet(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)
	idAlpha := seedCluster(t, s, "alpha")
	seedCluster(t, s, "beta")

	list, err := s.List(ctx)
	require.NoError(t, err)
	assert.Len(t, list, 2)

	c, err := s.Get(ctx, idAlpha)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, idAlpha, c.ID)
	require.NotNil(t, c.Spec.Name)
	assert.Equal(t, "alpha", *c.Spec.Name)

	// Unknown id is (nil, nil), not an error.
	missing, err := s.Get(ctx, ClusterID(999999))
	require.NoError(t, err)
	assert.Nil(t, missing)
}

// WatchCaches streams each ClusterCache standalone: the snapshot carries an Added
// change per cache with its parent ClusterID resolved from the owner edge, its
// ServerUID, and its sync status. Active-ness is a client-side join, not asserted here.
func TestServiceWatchCachesEmitsCaches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, cacheCtl := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	now := time.Now().UTC()
	// Give the ClusterCache a Synced status to carry through.
	cacheObj, err := s.cacheClient.Get(ctx, cacheID)
	require.NoError(t, err)
	require.NoError(t, cacheCtl.UpdateStatus(ctx, cacheObj.ID, cacheObj.Generation, ClusterCacheStatus{
		LastSyncedAt: &now,
	}))

	ch, err := s.WatchCaches(ctx)
	require.NoError(t, err)

	// WatchList replays current state on subscribe (conflated per object), so drain
	// Added changes until the synced status lands.
	deadline := time.After(2 * time.Second)
	for {
		ev := recvBy(t, ch, deadline)
		assert.Equal(t, ChangeAdded, ev.Type)
		require.NotNil(t, ev.Cache)
		assert.Equal(t, ClusterCacheID(cacheID), ev.Cache.ID)
		assert.Equal(t, id, ev.Cache.ClusterID) // resolved from the owner edge
		assert.Equal(t, uid, ev.Cache.ServerUID)
		if ev.Cache.Status.LastSyncedAt != nil {
			assert.WithinDuration(t, now, *ev.Cache.Status.LastSyncedAt, time.Second)
			return
		}
	}
}

func TestServiceGetDeletionPendingIsNil(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	require.NoError(t, s.coreClient.Delete(ctx, obj.ID))

	c, err := s.Get(ctx, id)
	require.NoError(t, err)
	assert.Nil(t, c)
}

func TestServiceSetEnabled(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	c, err := s.SetEnabled(ctx, id, false)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.False(t, c.Spec.Enabled)

	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	assert.False(t, obj.Spec.Enabled)
}

func TestServiceSetSyncEnabled(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	c, err := s.SetSyncEnabled(ctx, id, false)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.False(t, c.Spec.SyncEnabled)

	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	assert.False(t, obj.Spec.SyncEnabled)
}

// RetryConnection dispatches an out-of-band re-probe without mutating the spec. Via the
// fakeCoreController we pin that the dispatch reaches Reprobe and the spec is untouched;
// an unknown id errors before any dispatch.
func TestServiceRetryConnectionDoesNotMutateSpec(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	before, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)

	require.NoError(t, s.RetryConnection(ctx, id))

	after, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	assert.Equal(t, before.Generation, after.Generation, "RetryConnection must not write the spec")
	assert.Equal(t, before.Spec, after.Spec)
	assert.Equal(t, []ClusterID{id}, s.coreCtrl.(*fakeCoreController).reprobed, "retry must dispatch a reprobe")

	// An unknown id is ErrNotFound and dispatches nothing further.
	assert.ErrorIs(t, s.RetryConnection(ctx, ClusterID(999999)), ErrNotFound)
	assert.Equal(t, []ClusterID{id}, s.coreCtrl.(*fakeCoreController).reprobed, "unknown id must not reprobe")
}

func TestServiceClearCacheDeletesCacheAndReturnsCluster(t *testing.T) {
	ctx := context.Background()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	// cacheCtrl is nil in this harness, so ClearCache deletes the on-disk cache (a no-op
	// here) and returns the record without restarting an engine. The engine-restart path
	// is covered in cache_controller_test.go.
	c, err := s.ClearCache(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, id, c.ID)

	stats, err := s.CacheStats(ctx, id, ClusterCacheID(cacheID))
	require.NoError(t, err)
	assert.False(t, stats.Exists)
}

// CacheStats rolls the kind catalog's per-kind counts up into ObjectCount/KindCount
// (the whole-cache totals the sync-status summary shows): only kinds with at least
// one cached object count, and events are excluded (not a catalog kind).
func TestServiceCacheStatsRollup(t *testing.T) {
	ctx := context.Background()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	// Populate the active cache's on-disk DB: an advertised kind catalog (Pod,
	// Deployment, plus an advertised-but-empty Node), the objects the triggers
	// count (2 Pods, 1 Deployment), and one event that must not be counted.
	cdb, err := s.cacheManager.Open(ctx, newCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)
	catalog := func(apiVersion, kind, resource, scope string) {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
			 VALUES(?, ?, ?, ?, 0, NULL)`,
			apiVersion, kind, resource, scope)
		require.NoError(t, err)
	}
	catalog("v1", "Pod", "pods", "Namespaced")
	catalog("apps/v1", "Deployment", "deployments", "Namespaced")
	catalog("v1", "Node", "nodes", "Cluster")

	at := time.Now().UnixMilli()
	insert := func(objUID, apiVersion, kind string) {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
			   created_at, updated_at, raw_json)
			 VALUES (?, ?, ?, 'default', ?, '1', ?, ?, x'7b7d')`,
			objUID, apiVersion, kind, objUID, at, at)
		require.NoError(t, err)
	}
	insert("p1", "v1", "Pod")
	insert("p2", "v1", "Pod")
	insert("d1", "apps/v1", "Deployment")
	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO events(uid, type, reason, message, first_seen, last_seen, count, raw_json, updated_at)
		 VALUES('e1', 'Normal', 'Test', 'hello', ?, ?, 1, x'7b7d', ?)`, at, at, at)
	require.NoError(t, err)

	stats, err := s.CacheStats(ctx, id, ClusterCacheID(cacheID))
	require.NoError(t, err)
	assert.True(t, stats.Exists)
	assert.Equal(t, 2, stats.KindCount, "Pod and Deployment; the empty Node kind and events excluded")
	assert.Equal(t, 3, stats.ObjectCount, "2 Pods + 1 Deployment; the event is excluded")
}

// The Events kind now carries a real count in kind_counts (maintained by triggers
// on the events table), and /apis discovery advertises it in kind_catalog — so it
// appears in Kinds with a non-zero count. CacheStats must still exclude it
// from the whole-cache object totals: events aren't objects, and ObjectCount is
// documented to leave them out. (TestServiceCacheStatsRollup above is insulated
// only because it inserts no Event catalog row; this one adds it to exercise the
// exclusion.)
func TestServiceCacheStatsExcludesEventKind(t *testing.T) {
	ctx := context.Background()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	cdb, err := s.cacheManager.Open(ctx, newCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)
	catalog := func(apiVersion, kind, resource, scope string) {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
			 VALUES(?, ?, ?, ?, 0, NULL)`,
			apiVersion, kind, resource, scope)
		require.NoError(t, err)
	}
	catalog("apps/v1", "Deployment", "deployments", "Namespaced")
	catalog("v1", "Event", "events", "Namespaced")

	at := time.Now().UnixMilli()
	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
		   created_at, updated_at, raw_json)
		 VALUES ('d1', 'apps/v1', 'Deployment', 'default', 'd1', '1', ?, ?, x'7b7d')`, at, at)
	require.NoError(t, err)
	for _, uid := range []string{"e1", "e2"} {
		_, err = cdb.Writer().ExecContext(ctx,
			`INSERT INTO events(uid, type, reason, message, first_seen, last_seen, count, raw_json, updated_at)
			 VALUES(?, 'Normal', 'Test', 'hello', ?, ?, 1, x'7b7d', ?)`, uid, at, at, at)
		require.NoError(t, err)
	}

	stats, err := s.CacheStats(ctx, id, ClusterCacheID(cacheID))
	require.NoError(t, err)
	assert.True(t, stats.Exists)
	assert.Equal(t, 1, stats.KindCount, "only Deployment; the Event kind is excluded")
	assert.Equal(t, 1, stats.ObjectCount, "only the Deployment; the 2 events are excluded")
}

// recvKindChange reads one delta off the ClusterDataKindsWatch stream, failing on
// timeout or an unexpected close.
func recvKindChange(t *testing.T, ch <-chan ClusterDataKindChange) ClusterDataKindChange {
	t.Helper()
	select {
	case ev, ok := <-ch:
		require.True(t, ok, "stream closed early")
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a ClusterDataKindChange")
		return ClusterDataKindChange{}
	}
}

// ClusterDataKindsWatch streams the active cache's kind catalog as a delta watch: the
// current catalog as an Added burst on subscribe, then Added/Modified/Deleted per kind
// as the sync engine writes objects and pings the store — what makes the dashboard
// resource nav (kinds + live counts) update in real time.
func TestServiceClusterDataKindsWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	cdb, err := s.cacheManager.Open(ctx, newCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	insertKind := func(apiVersion, kind, resource, scope string, isCRD int) {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
			 VALUES(?, ?, ?, ?, ?, NULL)`, apiVersion, kind, resource, scope, isCRD)
		require.NoError(t, err)
	}
	insertObj := func(objUID, apiVersion, kind string) {
		at := time.Now().UnixMilli()
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
			   created_at, updated_at, raw_json)
			 VALUES (?, ?, ?, 'default', ?, '1', ?, ?, x'7b7d')`,
			objUID, apiVersion, kind, objUID, at, at)
		require.NoError(t, err)
	}

	// Seed a two-kind catalog with one Deployment cached before subscribing.
	insertKind("apps/v1", "Deployment", "deployments", "Namespaced", 0)
	insertKind("v1", "Node", "nodes", "Cluster", 0)
	insertObj("d1", "apps/v1", "Deployment")

	ch, err := s.ClusterDataKindsWatch(ctx, id, ClusterCacheID(cacheID))
	require.NoError(t, err)

	// Snapshot: an Added per kind, ordered by (api_version, kind) like Kinds.
	snap1 := recvKindChange(t, ch)
	assert.Equal(t, ChangeAdded, snap1.Type)
	assert.Equal(t, "Deployment", snap1.Kind.Kind)
	assert.Equal(t, 1, snap1.Kind.Count)
	// Every frame carries its cache's id as provenance, so a client can reject a late
	// frame from a superseded cache after a cache/context switch.
	assert.Equal(t, ClusterCacheID(cacheID), snap1.CacheID)
	snap2 := recvKindChange(t, ch)
	assert.Equal(t, ChangeAdded, snap2.Type)
	assert.Equal(t, "Node", snap2.Kind.Kind)
	assert.Equal(t, 0, snap2.Kind.Count)

	// A new object of an existing kind bumps its count → Modified.
	insertObj("d2", "apps/v1", "Deployment")
	cdb.ObjectsNotify()
	mod := recvKindChange(t, ch)
	assert.Equal(t, ChangeModified, mod.Type)
	assert.Equal(t, "Deployment", mod.Kind.Kind)
	assert.Equal(t, 2, mod.Kind.Count)

	// A newly-discovered kind → Added.
	insertKind("batch/v1", "Job", "jobs", "Namespaced", 0)
	cdb.ObjectsNotify()
	add := recvKindChange(t, ch)
	assert.Equal(t, ChangeAdded, add.Type)
	assert.Equal(t, "Job", add.Kind.Kind)
	assert.Equal(t, "jobs", add.Kind.Resource)

	// A kind leaving the catalog → Deleted (carries the last-known row).
	_, err = cdb.Writer().ExecContext(ctx,
		`DELETE FROM kind_catalog WHERE kind = 'Node'`)
	require.NoError(t, err)
	cdb.ObjectsNotify()
	del := recvKindChange(t, ch)
	assert.Equal(t, ChangeDeleted, del.Type)
	assert.Equal(t, "Node", del.Kind.Kind)

	// ctx cancel ends the stream.
	cancel()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "stream must close on ctx cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not close on ctx cancel")
	}
}

// A burst of write pings within one debounce window collapses into a single catalog
// re-read, so a high-churn cluster doesn't re-run the count-join per object event.
// Un-coalesced, the first post-burst frame would carry an intermediate count; coalesced,
// it jumps straight to the final one.
func TestServiceClusterDataKindsWatchCoalesces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	// A window wide enough to pack several synchronous pings into before it fires.
	s.dataKindsDebounce = 100 * time.Millisecond
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)
	cdb, err := s.cacheManager.Open(ctx, newCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	insertObj := func(objUID string) {
		at := time.Now().UnixMilli()
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
			   created_at, updated_at, raw_json)
			 VALUES (?, 'apps/v1', 'Deployment', 'default', ?, '1', ?, ?, x'7b7d')`,
			objUID, objUID, at, at)
		require.NoError(t, err)
	}

	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
		 VALUES('apps/v1', 'Deployment', 'deployments', 'Namespaced', 0, NULL)`)
	require.NoError(t, err)
	insertObj("d1")

	ch, err := s.ClusterDataKindsWatch(ctx, id, ClusterCacheID(cacheID))
	require.NoError(t, err)

	// Snapshot: Deployment at count 1.
	snap := recvKindChange(t, ch)
	require.Equal(t, ChangeAdded, snap.Type)
	require.Equal(t, 1, snap.Kind.Count)

	// Three writes + pings back-to-back inside the 100ms window; the store's cap-1
	// Subscribe channel plus the debounce collapse them into one re-read.
	insertObj("d2")
	cdb.ObjectsNotify()
	insertObj("d3")
	cdb.ObjectsNotify()
	insertObj("d4")
	cdb.ObjectsNotify()

	// The single coalesced re-read reports the final count (4), never an intermediate.
	mod := recvKindChange(t, ch)
	assert.Equal(t, ChangeModified, mod.Type)
	assert.Equal(t, 4, mod.Kind.Count)
}

// A cache whose on-disk db isn't open (never synced / sync paused) yields a stream
// that emits no frames and closes when ctx ends — mirroring ClusterDataKinds' empty
// posture without leaking a goroutine.
func TestServiceClusterDataKindsWatchNoOpenCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	ch, err := s.ClusterDataKindsWatch(ctx, id, ClusterCacheID(999999))
	require.NoError(t, err)

	select {
	case ev, ok := <-ch:
		if ok {
			t.Fatalf("expected no frames for an unopened cache, got %+v", ev)
		}
	case <-time.After(200 * time.Millisecond):
		// No frame yet, as expected; cancel and require close.
	}
	cancel()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "stream must close on ctx cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not close on ctx cancel")
	}
}

// insertCatalogKind inserts one kind_catalog row into a cache db (test helper for
// the ClusterDataKindsWatch lifecycle tests).
func insertCatalogKind(t *testing.T, ctx context.Context, cdb *store.ClusterDB, apiVersion, kind, resource, scope string) {
	t.Helper()
	_, err := cdb.Writer().ExecContext(ctx,
		`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
		 VALUES(?, ?, ?, ?, 0, NULL)`, apiVersion, kind, resource, scope)
	require.NoError(t, err)
}

// A subscriber that opens the stream before the cache db is opened (the common
// unsynced-cluster case) must bind to the cache when it opens and start streaming its
// catalog — not miss it forever by binding once to the initial (nil) Lookup.
func TestServiceClusterDataKindsWatchBindsCacheOpenedAfterSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	// Subscribe first — the cache db is not open yet, so no frames arrive.
	ch, err := s.ClusterDataKindsWatch(ctx, id, ClusterCacheID(cacheID))
	require.NoError(t, err)
	select {
	case ev, ok := <-ch:
		if ok {
			t.Fatalf("expected no frames before the cache opens, got %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
	}

	// Open the cache and write a kind: the stream must now bind and emit it.
	cdb, err := s.cacheManager.Open(ctx, newCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)
	insertCatalogKind(t, ctx, cdb, "apps/v1", "Deployment", "deployments", "Namespaced")
	cdb.ObjectsNotify()

	ev := recvKindChange(t, ch)
	assert.Equal(t, ChangeAdded, ev.Type)
	assert.Equal(t, "Deployment", ev.Kind.Kind)
}

// Clearing a cache closes the on-disk db and reopens a fresh one under the same CacheID.
// The stream must rebind to the new handle and keep diffing — the emptied catalog
// surfacing as Deletes, the rebuild as Adds — instead of ending silently (which, with an
// unchanged cache id, the client would never resubscribe from).
func TestServiceClusterDataKindsWatchRebindsAfterCacheReplaced(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)
	ref := newCacheRef(beehive.ObjectID(id), cacheID)

	cdb, err := s.cacheManager.Open(ctx, ref)
	require.NoError(t, err)
	insertCatalogKind(t, ctx, cdb, "apps/v1", "Deployment", "deployments", "Namespaced")

	ch, err := s.ClusterDataKindsWatch(ctx, id, ClusterCacheID(cacheID))
	require.NoError(t, err)
	snap := recvKindChange(t, ch)
	assert.Equal(t, ChangeAdded, snap.Type)
	assert.Equal(t, "Deployment", snap.Kind.Kind)

	// Clear: delete the files (closes the db) then reopen a fresh, empty cache.
	require.NoError(t, s.cacheManager.DeleteCacheFiles(ctx, ref))
	// The emptied catalog surfaces as a Delete of the prior kind once the new db binds.
	cdb2, err := s.cacheManager.Open(ctx, ref)
	require.NoError(t, err)
	del := recvKindChange(t, ch)
	assert.Equal(t, ChangeDeleted, del.Type)
	assert.Equal(t, "Deployment", del.Kind.Kind)

	// A write into the rebuilt cache streams through the rebound handle.
	insertCatalogKind(t, ctx, cdb2, "v1", "Node", "nodes", "Cluster")
	cdb2.ObjectsNotify()
	add := recvKindChange(t, ch)
	assert.Equal(t, ChangeAdded, add.Type)
	assert.Equal(t, "Node", add.Kind.Kind)
}

// A cache that closes and never reopens (a paused/cleared cache whose engine doesn't
// restart) must reconcile the stream against an empty catalog — one Delete per held kind
// — so the dashboard doesn't retain the closed cache's stale kinds waiting for a reopen
// that never comes.
func TestServiceClusterDataKindsWatchEmitsDeletesOnCloseWithoutReopen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)
	ref := newCacheRef(beehive.ObjectID(id), cacheID)

	cdb, err := s.cacheManager.Open(ctx, ref)
	require.NoError(t, err)
	insertCatalogKind(t, ctx, cdb, "apps/v1", "Deployment", "deployments", "Namespaced")

	ch, err := s.ClusterDataKindsWatch(ctx, id, ClusterCacheID(cacheID))
	require.NoError(t, err)
	snap := recvKindChange(t, ch)
	assert.Equal(t, ChangeAdded, snap.Type)
	assert.Equal(t, "Deployment", snap.Kind.Kind)

	// Close the cache without reopening: the held kind must surface as a Delete.
	require.NoError(t, s.cacheManager.Close(ctx, int64(cacheID)))
	del := recvKindChange(t, ch)
	assert.Equal(t, ChangeDeleted, del.Type)
	assert.Equal(t, "Deployment", del.Kind.Kind)
}

func recvEventChange(t *testing.T, ch <-chan ClusterDataEventChange) ClusterDataEventChange {
	t.Helper()
	select {
	case ev, ok := <-ch:
		require.True(t, ok, "stream closed early")
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a ClusterDataEventChange")
		return ClusterDataEventChange{}
	}
}

// insertEvent writes one row directly into the events table (uid + the display columns),
// standing in for what the sync engine's event driver would persist.
func insertEvent(t *testing.T, ctx context.Context, cdb *store.ClusterDB, uid, evType, reason, message string, count, lastSeen int64) {
	t.Helper()
	_, err := cdb.Writer().ExecContext(ctx,
		`INSERT INTO events (uid, involved_kind, involved_ns, involved_name,
		   type, reason, message, first_seen, last_seen, count, raw_json, updated_at)
		 VALUES (?, 'Pod', 'default', 'my-pod', ?, ?, ?, ?, ?, ?, x'7b7d', ?)
		 ON CONFLICT(uid) DO UPDATE SET
		   type=excluded.type, reason=excluded.reason, message=excluded.message,
		   last_seen=excluded.last_seen, count=excluded.count, updated_at=excluded.updated_at`,
		uid, evType, reason, message, lastSeen, lastSeen, count, lastSeen)
	require.NoError(t, err)
}

// recvObjectChange reads one delta off a ClusterDataObjectsWatch stream, failing on
// close or timeout.
func recvObjectChange(t *testing.T, ch <-chan ClusterDataObjectChange) ClusterDataObjectChange {
	t.Helper()
	select {
	case ev, ok := <-ch:
		require.True(t, ok, "stream closed early")
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a ClusterDataObjectChange")
		return ClusterDataObjectChange{}
	}
}

// insertObject writes one row directly into the objects table (the universal-identity
// columns the objects watch projects), standing in for what the sync engine's object
// driver would persist. createdAt is the object's creationTimestamp as unix-millis.
func insertObject(t *testing.T, ctx context.Context, cdb *store.ClusterDB, uid, apiVersion, kind, namespace, name, rv string, createdAt int64) {
	t.Helper()
	_, err := cdb.Writer().ExecContext(ctx,
		`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
		   created_at, updated_at, raw_json)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, x'7b7d')
		 ON CONFLICT(uid) DO UPDATE SET
		   namespace=excluded.namespace, name=excluded.name,
		   resource_version=excluded.resource_version, updated_at=excluded.updated_at`,
		uid, apiVersion, kind, namespace, name, rv, createdAt, createdAt)
	require.NoError(t, err)
}

// insertObjectCatalog writes one kind_catalog row so the objects reader can translate
// the watch's plural resource to its kind.
func insertObjectCatalog(t *testing.T, ctx context.Context, cdb *store.ClusterDB, apiVersion, kind, resource, scope string) {
	t.Helper()
	_, err := cdb.Writer().ExecContext(ctx,
		`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
		 VALUES(?, ?, ?, ?, 0, NULL)`,
		apiVersion, kind, resource, scope)
	require.NoError(t, err)
}

// ClusterDataEventsWatch streams the active cache's cached Kubernetes Events as a delta
// watch: the newest window as an Added burst, then Added/Modified/Deleted as the sync
// engine writes events and pings the events-only store broker — what backs the dashboard
// events table.
func TestServiceClusterDataEventsWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)
	cdb, err := s.cacheManager.Open(ctx, newCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	// One event cached before subscribing forms the snapshot.
	insertEvent(t, ctx, cdb, "e1", "Warning", "BackOff", "Back-off restarting", 1, 100)

	ch, err := s.ClusterDataEventsWatch(ctx, id, ClusterCacheID(cacheID))
	require.NoError(t, err)

	// Snapshot: an Added carrying the flattened involved-object identity + provenance.
	snap := recvEventChange(t, ch)
	assert.Equal(t, ChangeAdded, snap.Type)
	assert.Equal(t, "e1", snap.Event.UID)
	assert.EqualValues(t, "Warning", snap.Event.Type)
	assert.Equal(t, "BackOff", snap.Event.Reason)
	assert.Equal(t, "Pod", snap.Event.InvolvedKind)
	assert.Equal(t, "default", snap.Event.InvolvedNamespace)
	assert.Equal(t, "my-pod", snap.Event.InvolvedName)
	assert.Equal(t, ClusterCacheID(cacheID), snap.CacheID)

	// A brand-new event → Added.
	insertEvent(t, ctx, cdb, "e2", "Normal", "Scheduled", "Successfully assigned", 1, 200)
	cdb.EventsNotify()
	add := recvEventChange(t, ch)
	assert.Equal(t, ChangeAdded, add.Type)
	assert.Equal(t, "e2", add.Event.UID)

	// The same event re-firing (count/last_seen bump) → Modified under the same uid.
	insertEvent(t, ctx, cdb, "e1", "Warning", "BackOff", "Back-off restarting", 5, 300)
	cdb.EventsNotify()
	mod := recvEventChange(t, ch)
	assert.Equal(t, ChangeModified, mod.Type)
	assert.Equal(t, "e1", mod.Event.UID)
	assert.Equal(t, 5, mod.Event.Count)

	// An event removed from the table → Deleted (carries the last-known row).
	_, err = cdb.Writer().ExecContext(ctx, `DELETE FROM events WHERE uid = 'e2'`)
	require.NoError(t, err)
	cdb.EventsNotify()
	del := recvEventChange(t, ch)
	assert.Equal(t, ChangeDeleted, del.Type)
	assert.Equal(t, "e2", del.Event.UID)

	cancel()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "stream must close on ctx cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not close on ctx cancel")
	}
}

// An object write pings the object-write broker, not the events broker, so it must NOT
// wake the events watch — the whole reason events use a dedicated broker. Conversely an
// event write wakes it. This pins that separation.
func TestServiceClusterDataEventsWatchIgnoresObjectWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)
	cdb, err := s.cacheManager.Open(ctx, newCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	insertEvent(t, ctx, cdb, "e1", "Normal", "Started", "Started container", 1, 100)
	ch, err := s.ClusterDataEventsWatch(ctx, id, ClusterCacheID(cacheID))
	require.NoError(t, err)
	require.Equal(t, "e1", recvEventChange(t, ch).Event.UID) // snapshot

	// An object write + Notify (object broker) must not produce an events frame.
	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
		   created_at, updated_at, raw_json)
		 VALUES ('o1', 'v1', 'Pod', 'default', 'o1', '1', 1, 1, x'7b7d')`)
	require.NoError(t, err)
	cdb.ObjectsNotify()
	select {
	case ev := <-ch:
		t.Fatalf("object write must not wake the events watch, got %+v", ev)
	case <-time.After(150 * time.Millisecond):
		// Correct: no frame from an object write.
	}

	// An actual event write does wake it.
	insertEvent(t, ctx, cdb, "e2", "Warning", "Failed", "Error", 1, 200)
	cdb.EventsNotify()
	add := recvEventChange(t, ch)
	assert.Equal(t, ChangeAdded, add.Type)
	assert.Equal(t, "e2", add.Event.UID)
}

// ClusterDataObjectsWatch streams one kind's cached objects as a delta watch: the
// current set for the kind as an Added burst, then Added/Modified/Deleted as the sync
// engine writes objects and pings the object-write store broker — what backs the
// dashboard's generic per-kind object tables. Keyed by (apiVersion, resource) on top of
// the active cache.
func TestServiceClusterDataObjectsWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)
	cdb, err := s.cacheManager.Open(ctx, newCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	// The catalog row lets the reader translate the plural resource to its kind; one
	// object cached before subscribing forms the snapshot.
	insertObjectCatalog(t, ctx, cdb, "apps/v1", "Deployment", "deployments", "Namespaced")
	insertObject(t, ctx, cdb, "d1", "apps/v1", "Deployment", "default", "web", "1", 100)

	ch, err := s.ClusterDataObjectsWatch(ctx, id, ClusterCacheID(cacheID), "apps/v1", "deployments")
	require.NoError(t, err)

	// Snapshot: an Added carrying the universal identity + provenance.
	snap := recvObjectChange(t, ch)
	assert.Equal(t, ChangeAdded, snap.Type)
	assert.Equal(t, "d1", snap.Object.UID)
	assert.Equal(t, "apps/v1", snap.Object.APIVersion)
	assert.Equal(t, "Deployment", snap.Object.Kind)
	assert.Equal(t, "default", snap.Object.Namespace)
	assert.Equal(t, "web", snap.Object.Name)
	assert.False(t, snap.Object.CreationTimestamp.IsZero())
	assert.Equal(t, ClusterCacheID(cacheID), snap.CacheID)

	// A brand-new object → Added.
	insertObject(t, ctx, cdb, "d2", "apps/v1", "Deployment", "kube-system", "coredns", "2", 200)
	cdb.ObjectsNotify()
	add := recvObjectChange(t, ch)
	assert.Equal(t, ChangeAdded, add.Type)
	assert.Equal(t, "d2", add.Object.UID)

	// ClusterDataObject projects only the object's immutable identity (uid/apiVersion/
	// kind/namespace/name/creationTimestamp) for now — the nested body is deferred (see
	// TODO.md). So a bare resourceVersion bump changes no projected field and must NOT
	// produce a frame; a Modified only becomes observable once the body ships.
	insertObject(t, ctx, cdb, "d1", "apps/v1", "Deployment", "default", "web", "9", 100)
	cdb.ObjectsNotify()
	select {
	case ev := <-ch:
		t.Fatalf("a non-identity change must not surface with the identity-only projection, got %+v", ev)
	case <-time.After(150 * time.Millisecond):
		// Correct: no frame — only identity is projected today.
	}

	// An object removed from the table → Deleted (carries the last-known row).
	_, err = cdb.Writer().ExecContext(ctx, `DELETE FROM objects WHERE uid = 'd2'`)
	require.NoError(t, err)
	cdb.ObjectsNotify()
	del := recvObjectChange(t, ch)
	assert.Equal(t, ChangeDeleted, del.Type)
	assert.Equal(t, "d2", del.Object.UID)

	cancel()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "stream must close on ctx cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not close on ctx cancel")
	}
}

// The objects watch is keyed by (apiVersion, resource): a write of a different kind
// pings the shared object-write broker but must not surface on a watch scoped to another
// kind — the reader filters by the resource's kind.
func TestServiceClusterDataObjectsWatchFiltersByKind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)
	cdb, err := s.cacheManager.Open(ctx, newCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	insertObjectCatalog(t, ctx, cdb, "apps/v1", "Deployment", "deployments", "Namespaced")
	insertObjectCatalog(t, ctx, cdb, "v1", "Pod", "pods", "Namespaced")
	insertObject(t, ctx, cdb, "d1", "apps/v1", "Deployment", "default", "web", "1", 100)

	ch, err := s.ClusterDataObjectsWatch(ctx, id, ClusterCacheID(cacheID), "apps/v1", "deployments")
	require.NoError(t, err)
	require.Equal(t, "d1", recvObjectChange(t, ch).Object.UID) // snapshot

	// A Pod write fires the object broker keyed to (v1, pods) — the real write path — which
	// with resource-routing never even wakes the deployments watch, so no frame appears.
	insertObject(t, ctx, cdb, "p1", "v1", "Pod", "default", "web-abc", "1", 200)
	cdb.ObjectsNotifyResource("v1", "pods")
	select {
	case ev := <-ch:
		t.Fatalf("a different resource's write must not surface on this watch, got %+v", ev)
	case <-time.After(150 * time.Millisecond):
		// Correct: no frame for an unrelated resource.
	}

	// A same-resource write does surface.
	insertObject(t, ctx, cdb, "d2", "apps/v1", "Deployment", "default", "api", "1", 300)
	cdb.ObjectsNotifyResource("apps/v1", "deployments")
	add := recvObjectChange(t, ch)
	assert.Equal(t, ChangeAdded, add.Type)
	assert.Equal(t, "d2", add.Object.UID)
}

// With no open cache for the id, the objects watch mirrors the kinds/events empty
// posture: it produces no frames and simply ends when ctx is cancelled (no error).
func TestServiceClusterDataObjectsWatchNoOpenCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	ch, err := s.ClusterDataObjectsWatch(ctx, id, ClusterCacheID(999999), "apps/v1", "deployments")
	require.NoError(t, err)

	select {
	case ev, ok := <-ch:
		require.False(t, ok, "no frames without an open cache; channel only closes")
		_ = ev
	case <-time.After(150 * time.Millisecond):
		// Correct: quiet until ctx ends.
	}

	cancel()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "stream must close on ctx cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not close on ctx cancel")
	}
}

// Resource routing eliminates the wasted re-read, not just the wasted frame: with the
// objects watch subscribed keyed to its (apiVersion, resource), an unrelated resource's
// keyed write never wakes it, so its snapshot read never runs — proven with a counting
// snapshot fn driving cacheDeltaWatch exactly as ClusterDataObjectsWatch does. A
// matching-resource write does wake it and re-reads.
func TestClusterDataObjectsWatchNoReReadForOtherKind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := store.NewManager(t.TempDir())
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	const cacheID = int64(7)
	cdb, err := mgr.Open(ctx, store.CacheRef{ClusterID: 1, CacheID: cacheID})
	require.NoError(t, err)

	var reads int32
	ch := cacheDeltaWatch(ctx, mgr, cacheID, 20*time.Millisecond,
		// The keyed subscribe ClusterDataObjectsWatch uses, verbatim.
		func(db *store.ClusterDB) (<-chan struct{}, func()) {
			return db.ObjectsSubscribeResource("apps/v1", "deployments")
		},
		func(context.Context, *store.ClusterDB) ([]string, error) {
			atomic.AddInt32(&reads, 1)
			return nil, nil
		},
		func(s string) string { return s },
		func(_ ChangeType, s string) string { return s },
	)
	go func() { // drain so sends never block the watch goroutine
		for range ch { //nolint:revive
		}
	}()

	// Wait for the baseline read (on bind) to land.
	require.Eventually(t, func() bool { return atomic.LoadInt32(&reads) == 1 },
		2*time.Second, 5*time.Millisecond, "baseline snapshot must read once on bind")

	// An unrelated resource's keyed write must not wake the watch — no re-read.
	cdb.ObjectsNotifyResource("v1", "pods")
	time.Sleep(100 * time.Millisecond) // longer than the debounce window
	require.Equal(t, int32(1), atomic.LoadInt32(&reads), "an unrelated resource's write must not re-read")

	// A matching-resource write wakes it — one more read after the debounce fires.
	cdb.ObjectsNotifyResource("apps/v1", "deployments")
	require.Eventually(t, func() bool { return atomic.LoadInt32(&reads) == 2 },
		2*time.Second, 5*time.Millisecond, "a matching-resource write must re-read")
}

// A keyed object write still wakes the keyless kind-catalog watch: a kind's first-ever
// object is a catalog Add, so ClusterDataKindsWatch (keyless) must wake on any write,
// keyed or not. Pins that the routing doesn't starve the catalog watch.
func TestClusterDataKindsWatchWakesOnKeyedWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)
	cdb, err := s.cacheManager.Open(ctx, newCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	ch, err := s.ClusterDataKindsWatch(ctx, id, ClusterCacheID(cacheID))
	require.NoError(t, err)

	// A brand-new kind's first object, announced via the real keyed (by-resource) write path.
	insertObjectCatalog(t, ctx, cdb, "apps/v1", "Deployment", "deployments", "Namespaced")
	insertObject(t, ctx, cdb, "d1", "apps/v1", "Deployment", "default", "web", "1", 100)
	cdb.ObjectsNotifyResource("apps/v1", "deployments")

	add := recvKindChange(t, ch)
	assert.Equal(t, ChangeAdded, add.Type)
	assert.Equal(t, "Deployment", add.Kind.Kind)
	assert.Equal(t, 1, add.Kind.Count)
}

// Regression: a CRD deleted and recreated with the same (apiVersion, resource) but a
// different Kind must keep the objects watch live. The watch subscribes on the resource
// identity (stable across the remap), and the replacement (new-Kind) driver notifies by
// that same resource — so the stream tracks the remap. Routing by Kind instead left the
// subscription bound to the dead Kind, missing every write until the next keyless
// discovery broadcast.
func TestServiceClusterDataObjectsWatchSurvivesKindRemap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)
	cdb, err := s.cacheManager.Open(ctx, newCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	// Original CRD: resource "widgets" backed by Kind "Widget".
	insertObjectCatalog(t, ctx, cdb, "example.com/v1", "Widget", "widgets", "Namespaced")
	insertObject(t, ctx, cdb, "w1", "example.com/v1", "Widget", "default", "one", "1", 100)

	ch, err := s.ClusterDataObjectsWatch(ctx, id, ClusterCacheID(cacheID), "example.com/v1", "widgets")
	require.NoError(t, err)
	require.Equal(t, "w1", recvObjectChange(t, ch).Object.UID) // snapshot: the Widget object

	// Remap: the CRD is recreated as Kind "Gadget" under the same (example.com/v1, widgets).
	// The catalog row's kind flips, the old Widget rows are pruned, and the replacement
	// driver writes a Gadget object — notifying by its resource identity (widgets).
	_, err = cdb.Writer().ExecContext(ctx,
		`UPDATE kind_catalog SET kind='Gadget' WHERE api_version='example.com/v1' AND resource='widgets'`)
	require.NoError(t, err)
	_, err = cdb.Writer().ExecContext(ctx, `DELETE FROM objects WHERE uid='w1'`)
	require.NoError(t, err)
	insertObject(t, ctx, cdb, "g1", "example.com/v1", "Gadget", "default", "one", "2", 200)
	cdb.ObjectsNotifyResource("example.com/v1", "widgets")

	// The watch (keyed on the resource) wakes and reconciles the remap: w1 gone, g1 present.
	got := map[string]ChangeType{}
	for len(got) < 2 {
		ev := recvObjectChange(t, ch)
		got[ev.Object.UID] = ev.Type
	}
	assert.Equal(t, ChangeDeleted, got["w1"], "the old Kind's object must be removed")
	assert.Equal(t, ChangeAdded, got["g1"], "the replacement Kind's object must appear")
}

// cacheRef resolves the active cache's on-disk locator: the directory id is the
// ClusterID, the file id is the ClusterCache for the cluster's currently-connected
// identity (UID matches Status.Server.UID). A cluster with no active cache resolves to
// found=false.
func TestServiceCacheRefResolvesActiveCache(t *testing.T) {
	ctx := context.Background()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	ref, found, err := s.cacheRef(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, store.CacheRef{ClusterID: int64(id), CacheID: int64(cacheID)}, ref,
		"ref must be the parent Cluster + active ClusterCache ObjectIDs")

	// A cluster that has never probed (no Server.UID) has no active cache: no error.
	id2 := seedCluster(t, s, "beta")
	_, found2, err := s.cacheRef(ctx, id2)
	require.NoError(t, err)
	assert.False(t, found2)

	// A cluster whose only cache is for a migrated-away identity (UID != active) also
	// has no active cache.
	id3 := seedCluster(t, s, "gamma")
	stampActiveUID(t, s, coreCC, id3, "new-uid")
	_, err = s.cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: "old-uid"},
		beehive.WithSlug(ClusterCacheSlug(id3, "old-uid")), beehive.WithOwner(beehive.ObjectID(id3)))
	require.NoError(t, err)
	_, found3, err := s.cacheRef(ctx, id3)
	require.NoError(t, err)
	assert.False(t, found3)
}

func TestServiceDeleteTombstonesCluster(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)

	// Seed with a finalizer so the soft-delete tombstone is observable without a race:
	// beehive GC is a no-op while an object holds a finalizer, so the deletion-pending
	// row lingers deterministically. Without it, the noop controller collects the
	// finalizer-less row on the reconcile Delete enqueues, and that physical delete
	// races the Get below.
	name := "alpha"
	obj, err := s.coreClient.Create(ctx, ClusterSpec{
		Name:        &name,
		SyncEnabled: true,
		Enabled:     true,
		Source:      ClusterSpecSource{Kubeconfig: &ClusterSpecSourceKubeconfig{Context: "alpha"}},
	}, beehive.WithSlug(kubeconfigSlug("alpha")), beehive.WithFinalizers("test/hold"))
	require.NoError(t, err)
	id := ClusterID(obj.ID)

	require.NoError(t, s.Delete(ctx, id))

	// Delete tombstones the Cluster (soft delete); beehive GC then cascades to its
	// ClusterCache once the finalizers clear.
	obj, err = s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	assert.NotNil(t, obj.DeletionRequestedAt)
}

func TestServiceGetConnection(t *testing.T) {
	s, _, _ := newServiceTest(t)
	id := ClusterID(1)
	cfg := &rest.Config{Host: "https://127.0.0.1:6443"}

	// Nothing stored yet.
	assert.Nil(t, s.GetConnection(id))

	// After the connection manager is populated it is readable via the service.
	s.connMgr.Set(id, cfg)
	assert.Equal(t, cfg, s.GetConnection(id))
}

func TestServiceWatchEmitsSnapshotThenDeltas(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	ch, err := s.Watch(ctx)
	require.NoError(t, err)

	// Snapshot: one Added change for the seeded cluster.
	seed := recv(t, ch)
	assert.Equal(t, ChangeAdded, seed.Type)
	require.NotNil(t, seed.Cluster)
	assert.Equal(t, id, seed.Cluster.ID)

	// A spec change emits a Modified change carrying the new state. WatchList replays
	// current state on subscribe, so drain until the change lands.
	_, err = s.SetSyncEnabled(ctx, id, false)
	require.NoError(t, err)

	deadline := time.After(2 * time.Second)
	for {
		ev := recvBy(t, ch, deadline)
		require.NotNil(t, ev.Cluster)
		if !ev.Cluster.Spec.SyncEnabled {
			assert.Equal(t, ChangeModified, ev.Type)
			return
		}
	}
}

// fakeScheduleClient drives the schedule-watch wrapper with a test-controlled channel,
// so the mapping (beehive.Schedule → Schedule, zero → nil) and the ctx lifecycle are
// exercised deterministically — beehive's own tests cover the queue/gauge semantics.
type fakeScheduleClient struct{ ch chan beehive.Schedule }

func (f *fakeScheduleClient) WatchSchedule(context.Context, beehive.ObjectID) (<-chan beehive.Schedule, error) {
	return f.ch, nil
}

// scheduleWatch maps the beehive Schedule gauge to the domain Schedule: a
// non-zero NextRequeueAt becomes a NextRequeueAt pointer, the zero time becomes
// nil (nothing scheduled). The out channel closes when the source closes.
func TestServiceScheduleWatchMapsGauge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &Service{}
	fake := &fakeScheduleClient{ch: make(chan beehive.Schedule, 1)}

	out, err := s.scheduleWatch(ctx, fake, 1)
	require.NoError(t, err)

	// a scheduled time → NextRequeueAt pointer
	at := time.Now().Add(time.Hour).UTC()
	fake.ch <- beehive.Schedule{NextRequeueAt: at}
	got := recv(t, out)
	require.NotNil(t, got.NextRequeueAt)
	assert.Equal(t, at, *got.NextRequeueAt)

	// the zero time → nil (nothing scheduled)
	fake.ch <- beehive.Schedule{}
	got = recv(t, out)
	assert.Nil(t, got.NextRequeueAt)

	// source close → out closes
	close(fake.ch)
	_, ok := <-out
	assert.False(t, ok, "out must close when the schedule source closes")
}

// mergeSchedule folds the schedule gauge (NextRequeueAt) and the in-flight probe
// signal (Probing) into one Schedule stream, re-emitting the combined latest as
// either side moves; a closed sub-source is dropped without ending the stream,
// and the out channel closes only when both close.
func TestMergeScheduleCombinesGaugeAndProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	schedCh := make(chan Schedule, 1)
	probeCh := make(chan bool, 1)
	out := mergeSchedule(ctx, schedCh, probeCh)

	// A probe starts → Probing true, no scheduled time yet.
	probeCh <- true
	got := recv(t, out)
	assert.True(t, got.Probing)
	assert.Nil(t, got.NextRequeueAt)

	// A scheduled time arrives → NextRequeueAt set, Probing still asserted (the
	// combined latest carries both).
	at := time.Now().Add(time.Hour).UTC()
	schedCh <- Schedule{NextRequeueAt: &at}
	got = recv(t, out)
	assert.True(t, got.Probing)
	require.NotNil(t, got.NextRequeueAt)
	assert.Equal(t, at, *got.NextRequeueAt)

	// The probe finishes → Probing clears, the scheduled time is retained.
	probeCh <- false
	got = recv(t, out)
	assert.False(t, got.Probing)
	require.NotNil(t, got.NextRequeueAt)

	// Closing one source keeps the other flowing.
	close(probeCh)
	schedCh <- Schedule{}
	got = recv(t, out)
	assert.Nil(t, got.NextRequeueAt)
	assert.False(t, got.Probing)

	// Closing both → out closes.
	close(schedCh)
	_, ok := <-out
	assert.False(t, ok, "out must close when both sub-sources close")
}

// ClusterScheduleWatch, through the ClusterService interface, streams the real
// beehive schedule gauge for a cluster (snapshot on subscribe, then live).
func TestClusterScheduleWatchPublicSurface(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _, _ := newServiceTest(t)
	var svc ClusterService = s
	id := seedCluster(t, s, "alpha")

	ch, err := svc.ClusterScheduleWatch(ctx, id)
	require.NoError(t, err)
	// The snapshot arrives (value depends on queue state; the contract is that the
	// stream is live and closes on ctx).
	recv(t, ch)

	cancel()
	assert.Eventually(t, func() bool {
		select {
		case _, ok := <-ch:
			return !ok
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "stream should close on ctx cancel")
}

// The generic event reader maps beehive's coalesced runs to the domain Event wire
// shape, newest-run-first, honoring the category filter and limit. beehive coalesces
// same-(category,type,reason) occurrences into one run, so a repeated reason bumps
// Count and a changed reason starts a new run.
func TestServiceEventsReadsTimeline(t *testing.T) {
	ctx := context.Background()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")
	oid := beehive.ObjectID(id)

	const cat = "test-timeline"
	// run A (failure), repeated → one run, Count 2
	for range 2 {
		require.NoError(t, coreCC.RecordEvent(ctx, oid, beehive.EventSpec{
			Category: cat, Type: beehive.EventWarning, Reason: "ReasonA", Message: "boom",
		}))
	}
	// run B (success) → new run, Count 1
	require.NoError(t, coreCC.RecordEvent(ctx, oid, beehive.EventSpec{
		Category: cat, Type: beehive.EventNormal, Reason: "ReasonB",
	}))

	category := cat
	evs, err := s.events(ctx, s.coreClient, oid, &category, nil)
	require.NoError(t, err)
	require.Len(t, evs, 2, "two runs: A coalesced, B new")

	// newest run first
	assert.Equal(t, "ReasonB", evs[0].Reason)
	assert.Equal(t, beehive.EventNormal, evs[0].Type)
	assert.Equal(t, 1, evs[0].Count)

	assert.Equal(t, "ReasonA", evs[1].Reason)
	assert.Equal(t, beehive.EventWarning, evs[1].Type)
	assert.Equal(t, "boom", evs[1].Message)
	assert.Equal(t, 2, evs[1].Count)

	assert.NotEqual(t, evs[0].ID, evs[1].ID, "distinct run ids")
	assert.NotZero(t, evs[0].ID)
	assert.False(t, evs[0].FirstAt.IsZero())
	assert.False(t, evs[0].LastAt.IsZero())

	// category filter: a non-matching category yields no runs
	other := "no-such-category"
	none, err := s.events(ctx, s.coreClient, oid, &other, nil)
	require.NoError(t, err)
	assert.Empty(t, none)

	// limit bounds the read to the newest run
	limit := 1
	limited, err := s.events(ctx, s.coreClient, oid, &category, &limit)
	require.NoError(t, err)
	require.Len(t, limited, 1)
	assert.Equal(t, "ReasonB", limited[0].Reason)
}

// watchEvents streams bare runs (mirroring beehive's WatchEvents): the snapshot replays
// existing runs, a repeated same-outcome occurrence re-delivers the run with a bumped
// count under the same id (the consumer upserts), and a changed reason delivers a fresh
// run with a distinct id.
func TestServiceWatchEventsStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")
	oid := beehive.ObjectID(id)

	const cat = "test-watch"
	// one existing run before subscribe → replayed in the snapshot
	require.NoError(t, coreCC.RecordEvent(ctx, oid, beehive.EventSpec{
		Category: cat, Type: beehive.EventWarning, Reason: "ReasonA", Message: "boom",
	}))

	category := cat
	ch, err := s.watchEvents(ctx, s.coreClient, oid, &category)
	require.NoError(t, err)

	// snapshot: run A
	e := recv(t, ch)
	assert.Equal(t, "ReasonA", e.Reason)
	assert.Equal(t, beehive.EventWarning, e.Type)
	assert.Equal(t, 1, e.Count)
	runA := e.ID
	assert.NotZero(t, runA)

	// extend run A → re-delivered with the same id, count 2
	require.NoError(t, coreCC.RecordEvent(ctx, oid, beehive.EventSpec{
		Category: cat, Type: beehive.EventWarning, Reason: "ReasonA", Message: "boom",
	}))
	e = recv(t, ch)
	assert.Equal(t, runA, e.ID)
	assert.Equal(t, 2, e.Count)

	// changed reason → new run, distinct id
	require.NoError(t, coreCC.RecordEvent(ctx, oid, beehive.EventSpec{
		Category: cat, Type: beehive.EventNormal, Reason: "ReasonB",
	}))
	e = recv(t, ch)
	assert.Equal(t, "ReasonB", e.Reason)
	assert.NotEqual(t, runA, e.ID)

	// ctx cancel closes the stream
	cancel()
	assert.Eventually(t, func() bool {
		select {
		case _, ok := <-ch:
			return !ok
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "stream should close on ctx cancel")
}

// The kind-scoped public surface (ClusterEvents / ClusterEventsWatch), reached through
// the ClusterService interface, delegates to the generic reader/watch against the
// Cluster kind client. Asserting via the interface value locks the public API shape.
func TestClusterEventsPublicSurface(t *testing.T) {
	ctx := t.Context()
	s, coreCC, _ := newServiceTest(t)
	var svc ClusterService = s
	id := seedCluster(t, s, "alpha")
	oid := beehive.ObjectID(id)

	const cat = "connection"
	require.NoError(t, coreCC.RecordEvent(ctx, oid, beehive.EventSpec{
		Category: cat, Type: beehive.EventWarning, Reason: "ReasonA", Message: "boom",
	}))

	// ClusterEvents: point read, filtered to the category
	category := cat
	evs, err := svc.ClusterEvents(ctx, id, &category, nil)
	require.NoError(t, err)
	require.Len(t, evs, 1)
	assert.Equal(t, "ReasonA", evs[0].Reason)

	// ClusterEventsWatch: snapshot replays the existing run, then a live run arrives
	ch, err := svc.ClusterEventsWatch(ctx, id, &category)
	require.NoError(t, err)

	e := recv(t, ch)
	assert.Equal(t, "ReasonA", e.Reason)

	require.NoError(t, coreCC.RecordEvent(ctx, oid, beehive.EventSpec{
		Category: cat, Type: beehive.EventNormal, Reason: "ReasonB",
	}))
	e = recv(t, ch)
	assert.Equal(t, "ReasonB", e.Reason)
}

// ClusterCacheEvents / ClusterCacheEventsWatch are the ClusterCache-kind counterparts:
// same generic reader/watch, but against the cache client and keyed by the
// ClusterCache's own ObjectID (the sync-event timeline under category "sync"). Asserting
// via the interface value locks the public API shape.
func TestClusterCacheEventsPublicSurface(t *testing.T) {
	ctx := t.Context()
	s, coreCC, cacheCC := newServiceTest(t)
	var svc ClusterService = s
	id := seedCluster(t, s, "alpha")
	cacheOID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")

	const cat = "sync"
	require.NoError(t, cacheCC.RecordEvent(ctx, cacheOID, beehive.EventSpec{
		Category: cat, Type: beehive.EventNormal, Reason: "Watching",
	}))

	// ClusterCacheEvents: point read, filtered to the category, keyed by cache id.
	category := cat
	evs, err := svc.ClusterCacheEvents(ctx, ClusterCacheID(cacheOID), &category, nil)
	require.NoError(t, err)
	require.Len(t, evs, 1)
	assert.Equal(t, "Watching", evs[0].Reason)
	assert.Equal(t, beehive.EventNormal, evs[0].Type)

	// ClusterCacheEventsWatch: snapshot replays the existing run, then a live run.
	ch, err := svc.ClusterCacheEventsWatch(ctx, ClusterCacheID(cacheOID), &category)
	require.NoError(t, err)

	e := recv(t, ch)
	assert.Equal(t, "Watching", e.Reason)

	require.NoError(t, cacheCC.RecordEvent(ctx, cacheOID, beehive.EventSpec{
		Category: cat, Type: beehive.EventWarning, Reason: "SyncFailed", Message: "boom",
	}))
	e = recv(t, ch)
	assert.Equal(t, "SyncFailed", e.Reason)
	assert.Equal(t, "boom", e.Message)
}
