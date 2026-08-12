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
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// recvKindChange reads one delta off the ClusterDataKindsWatch stream, failing on
// timeout or an unexpected close.
func recvKindChange(t *testing.T, ch <-chan domain.ClusterDataKindChange) domain.ClusterDataKindChange {
	t.Helper()
	return testutil.Recv(t, ch, "a ClusterDataKindChange")
}

// ClusterDataKindsWatch streams the active cache's kind catalog as a delta watch: the
// current catalog as an Added burst on subscribe, then Added/Modified/Deleted per kind
// as the sync engine writes objects and pings the store — what makes the dashboard
// resource nav (kinds + live counts) update in real time.

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

	cdb, err := s.cacheManager.Open(ctx, domain.NewCacheRef(beehive.ObjectID(id), cacheID))
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
			 VALUES (?, ?, ?, 'default', ?, '1', ?, ?, ?)`,
			objUID, apiVersion, kind, objUID, at, at, emptyRawJSON(t))
		require.NoError(t, err)
	}

	// Seed a two-kind catalog with one Deployment cached before subscribing.
	insertKind("apps/v1", "Deployment", "deployments", "Namespaced", 0)
	insertKind("v1", "Node", "nodes", "Cluster", 0)
	insertObj("d1", "apps/v1", "Deployment")

	ch, err := s.Data().WatchKinds(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)

	// Snapshot: an Added per kind, ordered by (api_version, kind) like Kinds.
	snap1 := recvKindChange(t, ch)
	assert.Equal(t, domain.ChangeAdded, snap1.Type)
	assert.Equal(t, "Deployment", snap1.Kind.Kind)
	assert.Equal(t, 1, snap1.Kind.Count)
	// Every frame carries its cache's id as provenance, so a client can reject a late
	// frame from a superseded cache after a cache/context switch.
	assert.Equal(t, domain.ClusterCacheID(cacheID), snap1.CacheID)
	snap2 := recvKindChange(t, ch)
	assert.Equal(t, domain.ChangeAdded, snap2.Type)
	assert.Equal(t, "Node", snap2.Kind.Kind)
	assert.Equal(t, 0, snap2.Kind.Count)
	// …then the Bookmark closing it: everything after this is a live change.
	bm := recvKindChange(t, ch)
	requireBookmark(t, bm.Type, bm.Kind)
	assert.Equal(t, domain.ClusterCacheID(cacheID), bm.CacheID, "the Bookmark carries provenance too")

	// A new object of an existing kind bumps its count → Modified.
	insertObj("d2", "apps/v1", "Deployment")
	cdb.ObjectsNotifyResource("apps/v1", "deployments")
	mod := recvKindChange(t, ch)
	assert.Equal(t, domain.ChangeModified, mod.Type)
	assert.Equal(t, "Deployment", mod.Kind.Kind)
	assert.Equal(t, 2, mod.Kind.Count)

	// A newly-discovered kind → Added.
	insertKind("batch/v1", "Job", "jobs", "Namespaced", 0)
	cdb.ObjectsNotifyResource("batch/v1", "jobs")
	add := recvKindChange(t, ch)
	assert.Equal(t, domain.ChangeAdded, add.Type)
	assert.Equal(t, "Job", add.Kind.Kind)
	assert.Equal(t, "jobs", add.Kind.Resource)

	// A kind leaving the catalog → Deleted (carries the last-known row).
	_, err = cdb.Writer().ExecContext(ctx,
		`DELETE FROM kind_catalog WHERE kind = 'Node'`)
	require.NoError(t, err)
	cdb.ObjectsNotifyResource("v1", "nodes")
	del := recvKindChange(t, ch)
	assert.Equal(t, domain.ChangeDeleted, del.Type)
	assert.Equal(t, "Node", del.Kind.Kind)

	// ctx cancel ends the stream.
	cancel()
	testutil.RecvClosed(t, ch, "the stream on ctx cancel")
}

// A burst of write pings within one debounce window collapses into a single catalog
// re-read, so a high-churn cluster doesn't re-run the count-join per object event.
// Un-coalesced, the first post-burst frame would carry an intermediate count; coalesced,
// it jumps straight to the final one.

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
	cdb, err := s.cacheManager.Open(ctx, domain.NewCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	insertObj := func(objUID string) {
		at := time.Now().UnixMilli()
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
			   created_at, updated_at, raw_json)
			 VALUES (?, 'apps/v1', 'Deployment', 'default', ?, '1', ?, ?, ?)`,
			objUID, objUID, at, at, emptyRawJSON(t))
		require.NoError(t, err)
	}

	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
		 VALUES('apps/v1', 'Deployment', 'deployments', 'Namespaced', 0, NULL)`)
	require.NoError(t, err)
	insertObj("d1")

	ch, err := s.Data().WatchKinds(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)

	// Snapshot: Deployment at count 1.
	snap := recvKindChange(t, ch)
	require.Equal(t, domain.ChangeAdded, snap.Type)
	require.Equal(t, 1, snap.Kind.Count)
	bm := recvKindChange(t, ch)
	requireBookmark(t, bm.Type, bm.Kind)

	// Three writes + pings back-to-back inside the 100ms window; the store's cap-1
	// Subscribe channel plus the debounce collapse them into one re-read.
	insertObj("d2")
	cdb.ObjectsNotifyResource("apps/v1", "deployments")
	insertObj("d3")
	cdb.ObjectsNotifyResource("apps/v1", "deployments")
	insertObj("d4")
	cdb.ObjectsNotifyResource("apps/v1", "deployments")

	// The single coalesced re-read reports the final count (4), never an intermediate.
	mod := recvKindChange(t, ch)
	assert.Equal(t, domain.ChangeModified, mod.Type)
	assert.Equal(t, 4, mod.Kind.Count)
}

// A cache whose on-disk db isn't open (never synced / sync paused) yields a stream
// that emits no frames and closes when ctx ends — mirroring ClusterDataKinds' empty
// posture without leaking a goroutine.

// A cache whose on-disk db isn't open (never synced / sync paused) yields a stream
// that emits no frames and closes when ctx ends — mirroring ClusterDataKinds' empty
// posture without leaking a goroutine.
func TestServiceClusterDataKindsWatchNoOpenCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	ch, err := s.Data().WatchKinds(ctx, id, domain.ClusterCacheID(999999))
	require.NoError(t, err)

	// One Bookmark and nothing else: an unopened cache's catalog is empty, and saying so
	// is what lets the dashboard render "no kinds" instead of spinning forever.
	bm := recvKindChange(t, ch)
	requireBookmark(t, bm.Type, bm.Kind)
	select {
	case ev, ok := <-ch:
		if ok {
			t.Fatalf("expected no frames past the Bookmark for an unopened cache, got %+v", ev)
		}
	case <-time.After(200 * time.Millisecond):
		// No frame yet, as expected; cancel and require close.
	}
	cancel()
	testutil.RecvClosed(t, ch, "the stream on ctx cancel")
}

// insertCatalogKind inserts one kind_catalog row into a cache db (test helper for
// the ClusterDataKindsWatch lifecycle tests).

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

	// Subscribe first — the cache db is not open yet, so the stream reports an empty
	// initial state (the Bookmark) and then nothing.
	ch, err := s.Data().WatchKinds(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)
	bm := recvKindChange(t, ch)
	requireBookmark(t, bm.Type, bm.Kind)
	select {
	case ev, ok := <-ch:
		if ok {
			t.Fatalf("expected no frames before the cache opens, got %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
	}

	// Open the cache and write a kind: the stream must now bind and emit it.
	cdb, err := s.cacheManager.Open(ctx, domain.NewCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)
	insertCatalogKind(t, ctx, cdb, "apps/v1", "Deployment", "deployments", "Namespaced")
	cdb.ObjectsNotifyResource("apps/v1", "deployments")

	ev := recvKindChange(t, ch)
	assert.Equal(t, domain.ChangeAdded, ev.Type)
	assert.Equal(t, "Deployment", ev.Kind.Kind)
}

// Clearing a cache closes the on-disk db and reopens a fresh one under the same CacheID.
// The stream must rebind to the new handle and keep diffing — the emptied catalog
// surfacing as Deletes, the rebuild as Adds — instead of ending silently (which, with an
// unchanged cache id, the client would never resubscribe from).

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
	ref := domain.NewCacheRef(beehive.ObjectID(id), cacheID)

	cdb, err := s.cacheManager.Open(ctx, ref)
	require.NoError(t, err)
	insertCatalogKind(t, ctx, cdb, "apps/v1", "Deployment", "deployments", "Namespaced")

	ch, err := s.Data().WatchKinds(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)
	snap := recvKindChange(t, ch)
	assert.Equal(t, domain.ChangeAdded, snap.Type)
	assert.Equal(t, "Deployment", snap.Kind.Kind)
	bm := recvKindChange(t, ch)
	requireBookmark(t, bm.Type, bm.Kind)

	// Clear: delete the files (closes the db) then reopen a fresh, empty cache.
	require.NoError(t, s.cacheManager.DeleteCacheFiles(ctx, ref))
	// The emptied catalog surfaces as a Delete of the prior kind once the new db binds.
	cdb2, err := s.cacheManager.Open(ctx, ref)
	require.NoError(t, err)
	del := recvKindChange(t, ch)
	assert.Equal(t, domain.ChangeDeleted, del.Type)
	assert.Equal(t, "Deployment", del.Kind.Kind)

	// A write into the rebuilt cache streams through the rebound handle.
	insertCatalogKind(t, ctx, cdb2, "v1", "Node", "nodes", "Cluster")
	cdb2.ObjectsNotifyResource("v1", "nodes")
	add := recvKindChange(t, ch)
	assert.Equal(t, domain.ChangeAdded, add.Type)
	assert.Equal(t, "Node", add.Kind.Kind)
}

// A cache that closes and never reopens (a paused/cleared cache whose engine doesn't
// restart) must reconcile the stream against an empty catalog — one Delete per held kind
// — so the dashboard doesn't retain the closed cache's stale kinds waiting for a reopen
// that never comes.

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
	ref := domain.NewCacheRef(beehive.ObjectID(id), cacheID)

	cdb, err := s.cacheManager.Open(ctx, ref)
	require.NoError(t, err)
	insertCatalogKind(t, ctx, cdb, "apps/v1", "Deployment", "deployments", "Namespaced")

	ch, err := s.Data().WatchKinds(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)
	snap := recvKindChange(t, ch)
	assert.Equal(t, domain.ChangeAdded, snap.Type)
	assert.Equal(t, "Deployment", snap.Kind.Kind)
	// The Bookmark is sent once per stream, so the later close emits only the Delete.
	bm := recvKindChange(t, ch)
	requireBookmark(t, bm.Type, bm.Kind)

	// Close the cache without reopening: the held kind must surface as a Delete.
	require.NoError(t, s.cacheManager.Close(ctx, int64(cacheID)))
	del := recvKindChange(t, ch)
	assert.Equal(t, domain.ChangeDeleted, del.Type)
	assert.Equal(t, "Deployment", del.Kind.Kind)
}

func recvEventChange(t *testing.T, ch <-chan domain.ClusterDataEventChange) domain.ClusterDataEventChange {
	t.Helper()
	return testutil.Recv(t, ch, "a ClusterDataEventChange")
}

// insertEvent writes one row directly into the events table (uid + the display columns),
// standing in for what the sync engine's event driver would persist.

// recvObjectChange reads one delta off a ClusterDataObjectsWatch stream, failing on
// close or timeout.
func recvObjectChange(t *testing.T, ch <-chan domain.ClusterDataObjectChange) domain.ClusterDataObjectChange {
	t.Helper()
	return testutil.Recv(t, ch, "a ClusterDataObjectChange")
}

// insertObject writes one row directly into the objects table (the universal-identity
// columns the objects watch projects), standing in for what the sync engine's object
// driver would persist. createdAt is the object's creationTimestamp as unix-millis.

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
	cdb, err := s.cacheManager.Open(ctx, domain.NewCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	// One event cached before subscribing forms the snapshot.
	insertEvent(t, ctx, cdb, "e1", "Warning", "BackOff", "Back-off restarting", 1, 100)

	ch, err := s.Data().WatchEvents(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)

	// Snapshot: an Added carrying the flattened involved-object identity + provenance.
	snap := recvEventChange(t, ch)
	assert.Equal(t, domain.ChangeAdded, snap.Type)
	assert.Equal(t, "e1", snap.Event.UID)
	assert.EqualValues(t, "Warning", snap.Event.Type)
	assert.Equal(t, "BackOff", snap.Event.Reason)
	assert.Equal(t, "Pod", snap.Event.InvolvedKind)
	assert.Equal(t, "default", snap.Event.InvolvedNamespace)
	assert.Equal(t, "my-pod", snap.Event.InvolvedName)
	assert.Equal(t, domain.ClusterCacheID(cacheID), snap.CacheID)
	bm := recvEventChange(t, ch)
	requireBookmark(t, bm.Type, bm.Event)

	// A brand-new event → Added.
	insertEvent(t, ctx, cdb, "e2", "Normal", "Scheduled", "Successfully assigned", 1, 200)
	cdb.EventsNotify()
	add := recvEventChange(t, ch)
	assert.Equal(t, domain.ChangeAdded, add.Type)
	assert.Equal(t, "e2", add.Event.UID)

	// The same event re-firing (count/last_seen bump) → Modified under the same uid.
	insertEvent(t, ctx, cdb, "e1", "Warning", "BackOff", "Back-off restarting", 5, 300)
	cdb.EventsNotify()
	mod := recvEventChange(t, ch)
	assert.Equal(t, domain.ChangeModified, mod.Type)
	assert.Equal(t, "e1", mod.Event.UID)
	assert.Equal(t, 5, mod.Event.Count)

	// An event removed from the table → Deleted (carries the last-known row).
	_, err = cdb.Writer().ExecContext(ctx, `DELETE FROM events WHERE uid = 'e2'`)
	require.NoError(t, err)
	cdb.EventsNotify()
	del := recvEventChange(t, ch)
	assert.Equal(t, domain.ChangeDeleted, del.Type)
	assert.Equal(t, "e2", del.Event.UID)

	cancel()
	testutil.RecvClosed(t, ch, "the stream on ctx cancel")
}

// An object write pings the object-write broker, not the events broker, so it must NOT
// wake the events watch — the whole reason events use a dedicated broker. Conversely an
// event write wakes it. This pins that separation.

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
	cdb, err := s.cacheManager.Open(ctx, domain.NewCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	insertEvent(t, ctx, cdb, "e1", "Normal", "Started", "Started container", 1, 100)
	ch, err := s.Data().WatchEvents(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)
	require.Equal(t, "e1", recvEventChange(t, ch).Event.UID) // snapshot
	bmEv := recvEventChange(t, ch)
	requireBookmark(t, bmEv.Type, bmEv.Event)

	// An object write + Notify (object broker) must not produce an events frame.
	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
		   created_at, updated_at, raw_json)
		 VALUES ('o1', 'v1', 'Pod', 'default', 'o1', '1', 1, 1, x'7b7d')`)
	require.NoError(t, err)
	cdb.ObjectsNotifyResource("v1", "pods")
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
	assert.Equal(t, domain.ChangeAdded, add.Type)
	assert.Equal(t, "e2", add.Event.UID)
}

// ClusterDataObjectsWatch streams one kind's cached objects as a delta watch: the
// current set for the kind as an Added burst, then Added/Modified/Deleted as the sync
// engine writes objects and pings the object-write store broker — what backs the
// dashboard's generic per-kind object tables. Keyed by (apiVersion, resource) on top of
// the active cache.

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
	cdb, err := s.cacheManager.Open(ctx, domain.NewCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	// The catalog row lets the reader translate the plural resource to its kind; one
	// object cached before subscribing forms the snapshot.
	insertObjectCatalog(t, ctx, cdb, "apps/v1", "Deployment", "deployments", "Namespaced")
	insertObject(t, ctx, cdb, "d1", "apps/v1", "Deployment", "default", "web", "1", 100)

	ch, err := s.Data().WatchObjects(ctx, id, domain.ClusterCacheID(cacheID), "apps/v1", "deployments")
	require.NoError(t, err)

	// Snapshot: an Added carrying the universal identity + provenance.
	snap := recvObjectChange(t, ch)
	assert.Equal(t, domain.ChangeAdded, snap.Type)
	assert.Equal(t, "d1", snap.Object.UID)
	assert.Equal(t, "apps/v1", snap.Object.APIVersion)
	assert.Equal(t, "Deployment", snap.Object.Kind)
	assert.Equal(t, "default", snap.Object.Namespace)
	assert.Equal(t, "web", snap.Object.Name)
	assert.False(t, snap.Object.CreationTimestamp.IsZero())
	assert.Equal(t, domain.ClusterCacheID(cacheID), snap.CacheID)
	// The frame carries its (apiVersion, resource) kind provenance, so a client switching
	// resources within one cache can reject a straggler from the previous kind.
	assert.Equal(t, "apps/v1", snap.APIVersion)
	assert.Equal(t, "deployments", snap.Resource)
	// The native body rides along, forwarded verbatim from the cache.
	assert.JSONEq(t, `{"metadata":{"namespace":"default","name":"web","resourceVersion":"1"}}`,
		string(snap.Object.RawJSON))
	bm := recvObjectChange(t, ch)
	requireBookmark(t, bm.Type, bm.Object)
	assert.Equal(t, "deployments", bm.Resource, "the Bookmark carries kind provenance too")

	// A brand-new object → Added.
	insertObject(t, ctx, cdb, "d2", "apps/v1", "Deployment", "kube-system", "coredns", "2", 200)
	cdb.ObjectsNotifyResource("apps/v1", "deployments")
	add := recvObjectChange(t, ch)
	assert.Equal(t, domain.ChangeAdded, add.Type)
	assert.Equal(t, "d2", add.Object.UID)

	// The projection carries the native body, so an in-place edit — a bare resourceVersion
	// bump that rewrites raw_json under a stable uid/identity — now differs across reads and
	// surfaces as Modified (the closed gap the identity-only projection couldn't observe).
	insertObject(t, ctx, cdb, "d1", "apps/v1", "Deployment", "default", "web", "9", 100)
	cdb.ObjectsNotifyResource("apps/v1", "deployments")
	mod := recvObjectChange(t, ch)
	assert.Equal(t, domain.ChangeModified, mod.Type)
	assert.Equal(t, "d1", mod.Object.UID)
	assert.JSONEq(t, `{"metadata":{"namespace":"default","name":"web","resourceVersion":"9"}}`,
		string(mod.Object.RawJSON))

	// An object removed from the table → Deleted (carries the last-known row).
	_, err = cdb.Writer().ExecContext(ctx, `DELETE FROM objects WHERE uid = 'd2'`)
	require.NoError(t, err)
	cdb.ObjectsNotifyResource("apps/v1", "deployments")
	del := recvObjectChange(t, ch)
	assert.Equal(t, domain.ChangeDeleted, del.Type)
	assert.Equal(t, "d2", del.Object.UID)

	cancel()
	testutil.RecvClosed(t, ch, "the stream on ctx cancel")
}

// The objects watch is keyed by (apiVersion, resource): a write of a different kind
// pings the shared object-write broker but must not surface on a watch scoped to another
// kind — the reader filters by the resource's kind.

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
	cdb, err := s.cacheManager.Open(ctx, domain.NewCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	insertObjectCatalog(t, ctx, cdb, "apps/v1", "Deployment", "deployments", "Namespaced")
	insertObjectCatalog(t, ctx, cdb, "v1", "Pod", "pods", "Namespaced")
	insertObject(t, ctx, cdb, "d1", "apps/v1", "Deployment", "default", "web", "1", 100)

	ch, err := s.Data().WatchObjects(ctx, id, domain.ClusterCacheID(cacheID), "apps/v1", "deployments")
	require.NoError(t, err)
	require.Equal(t, "d1", recvObjectChange(t, ch).Object.UID) // snapshot
	bm := recvObjectChange(t, ch)
	requireBookmark(t, bm.Type, bm.Object)

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
	assert.Equal(t, domain.ChangeAdded, add.Type)
	assert.Equal(t, "d2", add.Object.UID)
}

// With no open cache for the id, the objects watch mirrors the kinds/events empty
// posture: it produces no frames and simply ends when ctx is cancelled (no error).

// With no open cache for the id, the objects watch mirrors the kinds/events empty
// posture: it produces no frames and simply ends when ctx is cancelled (no error).
func TestServiceClusterDataObjectsWatchNoOpenCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	ch, err := s.Data().WatchObjects(ctx, id, domain.ClusterCacheID(999999), "apps/v1", "deployments")
	require.NoError(t, err)

	// One Bookmark reporting the empty initial state, then quiet.
	bm := recvObjectChange(t, ch)
	requireBookmark(t, bm.Type, bm.Object)
	select {
	case ev, ok := <-ch:
		require.False(t, ok, "no frames past the Bookmark without an open cache")
		_ = ev
	case <-time.After(150 * time.Millisecond):
		// Correct: quiet until ctx ends.
	}

	cancel()
	testutil.RecvClosed(t, ch, "the stream on ctx cancel")
}

// Resource routing eliminates the wasted re-read, not just the wasted frame: with the
// objects watch subscribed keyed to its (apiVersion, resource), an unrelated resource's
// keyed write never wakes it, so its snapshot read never runs — proven with a counting
// snapshot fn driving cacheDeltaWatch exactly as ClusterDataObjectsWatch does. A
// matching-resource write does wake it and re-reads.

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

	const debounce = 20 * time.Millisecond
	reads := testutil.NewProbe[struct{}](8)
	ch := cacheDeltaWatch(ctx, mgr, cacheID, debounce,
		// The keyed subscribe ClusterDataObjectsWatch uses, verbatim.
		func(db *store.ClusterDB) (<-chan store.WriteWake, func()) {
			return db.ObjectsSubscribeResource("apps/v1", "deployments")
		},
		func(context.Context, *store.ClusterDB) ([]string, error) {
			reads.Fire(struct{}{})
			return nil, nil
		},
		func(s string) string { return s },
		func(_ domain.ChangeType, s *string) string {
			if s == nil {
				return "" // the Bookmark; this test only counts reads
			}
			return *s
		},
	)
	go func() { // drain so sends never block the watch goroutine
		for range ch { //nolint:revive
		}
	}()

	// Off the probe, not a sampled count that could already include a re-read.
	reads.Await(t, "the baseline snapshot read on bind")

	// An unrelated resource's keyed write must not wake the watch. A negative, so it needs a
	// window: several debounces, tripping the instant a read lands.
	cdb.ObjectsNotifyResource("v1", "pods")
	select {
	case <-reads.Chan():
		t.Fatal("an unrelated resource's write must not re-read")
	case <-time.After(5 * debounce):
	}

	// A matching-resource write wakes it — one more read after the debounce fires.
	cdb.ObjectsNotifyResource("apps/v1", "deployments")
	reads.Await(t, "a matching-resource write to re-read")
}

// The kind catalog spans BOTH tables: every synced kind's count comes from the objects
// triggers, and the Event kind's from the events triggers. Following only the object-write
// broker froze the Events badge on a cluster that is event-busy but object-quiet — which is
// exactly the cluster whose event count someone is watching.

// The kind catalog spans BOTH tables: every synced kind's count comes from the objects
// triggers, and the Event kind's from the events triggers. Following only the object-write
// broker froze the Events badge on a cluster that is event-busy but object-quiet — which is
// exactly the cluster whose event count someone is watching.
func TestClusterDataKindsWatchWakesOnEventWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	cacheID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")
	cdb, err := s.cacheManager.Open(ctx, domain.NewCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	// The Event kind's catalog row, as its sync worker registers it on start.
	insertObjectCatalog(t, ctx, cdb, "v1", "Event", "events", "Namespaced")
	cdb.ObjectsNotifyResource("v1", "events")

	ch, err := s.Data().WatchKinds(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)

	add := recvKindChange(t, ch)
	require.Equal(t, "Event", add.Kind.Kind)
	require.Zero(t, add.Kind.Count)
	bm := recvKindChange(t, ch)
	requireBookmark(t, bm.Type, bm.Kind)

	// An event write pings ONLY the events broker — the two are deliberately separate, so
	// an event burst can't drive the expensive per-kind objects re-reads.
	insertEvent(t, ctx, cdb, "ev-1", "Warning", "BackOff", "boom", 1, 100)
	cdb.EventsNotify()

	for {
		ch2 := recvKindChange(t, ch)
		if ch2.Kind.Kind != "Event" {
			continue
		}
		assert.Equal(t, domain.ChangeModified, ch2.Type)
		assert.Equal(t, 1, ch2.Kind.Count, "the Events badge must track event writes")
		return
	}
}

// A keyed object write still wakes the keyless kind-catalog watch: a kind's first-ever
// object is a catalog Add, so ClusterDataKindsWatch (keyless) must wake on any write,
// keyed or not. Pins that the routing doesn't starve the catalog watch.

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
	cdb, err := s.cacheManager.Open(ctx, domain.NewCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	ch, err := s.Data().WatchKinds(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)

	// A brand-new kind's first object, announced via the real keyed (by-resource) write path.
	//
	// The object goes in BEFORE its catalog row on purpose. store.Kinds is a kind_catalog
	// LEFT JOIN, so a kind is invisible until its catalog row exists — writing that row
	// last means any read the watch happens to run mid-setup (its first one fires when
	// WatchDB delivers the cache handle, which races these writes) either sees no kind at
	// all or sees it with its count already correct. The reverse order lets an interim read
	// emit an Added with count 0.
	insertObject(t, ctx, cdb, "d1", "apps/v1", "Deployment", "default", "web", "1", 100)
	insertObjectCatalog(t, ctx, cdb, "apps/v1", "Deployment", "deployments", "Namespaced")
	cdb.ObjectsNotifyResource("apps/v1", "deployments")

	// The Bookmark may land either side of the writes above (the bind-time read races
	// them, as the comment explains), so skip it wherever it falls.
	add := recvKindChange(t, ch)
	if add.Type == domain.ChangeBookmark {
		add = recvKindChange(t, ch)
	}
	assert.Equal(t, domain.ChangeAdded, add.Type)
	assert.Equal(t, "Deployment", add.Kind.Kind)
	assert.Equal(t, 1, add.Kind.Count)
}

// Regression: a CRD deleted and recreated with the same (apiVersion, resource) but a
// different Kind must keep the objects watch live. The watch subscribes on the resource
// identity (stable across the remap), and the replacement (new-Kind) driver notifies by
// that same resource — so the stream tracks the remap. Routing by Kind instead left the
// subscription bound to the dead Kind, missing every write until the next keyless
// discovery broadcast.

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
	cdb, err := s.cacheManager.Open(ctx, domain.NewCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	// Original CRD: resource "widgets" backed by Kind "Widget".
	insertObjectCatalog(t, ctx, cdb, "example.com/v1", "Widget", "widgets", "Namespaced")
	insertObject(t, ctx, cdb, "w1", "example.com/v1", "Widget", "default", "one", "1", 100)

	ch, err := s.Data().WatchObjects(ctx, id, domain.ClusterCacheID(cacheID), "example.com/v1", "widgets")
	require.NoError(t, err)
	require.Equal(t, "w1", recvObjectChange(t, ch).Object.UID) // snapshot: the Widget object
	bm := recvObjectChange(t, ch)
	requireBookmark(t, bm.Type, bm.Object)

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
	got := map[string]domain.ChangeType{}
	for len(got) < 2 {
		ev := recvObjectChange(t, ch)
		got[ev.Object.UID] = ev.Type
	}
	assert.Equal(t, domain.ChangeDeleted, got["w1"], "the old Kind's object must be removed")
	assert.Equal(t, domain.ChangeAdded, got["g1"], "the replacement Kind's object must appear")
}

// cacheRef resolves the active cache's on-disk locator: the directory id is the
// ClusterID, the file id is the ClusterCache for the cluster's currently-connected
// identity (UID matches Status.Server.UID). A cluster with no active cache resolves to
// found=false.

// seedGVRDiscovery creates the discovery anchor the cache controller would.
// catalogSubscribe fans two brokers into one channel, so it owns a goroutine and two
// registrations. Closing its output when both brokers close is how a caller learns through
// the ping path that the db went away — the same signal a bare broker subscription gives —
// and it is what lets the caller release the composite rather than dropping it on the floor.
func TestCatalogSubscribeClosesWhenBothBrokersDo(t *testing.T) {
	dir := t.TempDir()
	mgr := store.NewManager(dir)
	ctx := context.Background()
	db, err := mgr.Open(ctx, store.CacheRef{ClusterID: 1, CacheID: 1})
	require.NoError(t, err)

	pings, cancel := catalogSubscribe(db)
	defer cancel()

	// A write on either broker still reaches the caller.
	db.EventsNotify()
	testutil.Recv(t, pings, "an event write to wake the catalog watch")

	// Shutting the db down closes both brokers, which must close the composite.
	require.NoError(t, mgr.Shutdown(ctx))
	testutil.WaitClosed(t, pings, "the composite")
}

// The per-kind watch is cache-scoped but rides a FLEET-wide stream, so its filter runs on
// every sync record of every cache. While our own anchor is unresolved there is nothing
// cached to compare against, so each of those frames cost its own point query — ~1500
// lookups to drain a ten-cluster snapshot.
//
// One lookup per DISTINCT anchor is enough: a verdict never flips, and an anchor created
// after we looked cannot be one we already rejected (ids are AUTOINCREMENT).
