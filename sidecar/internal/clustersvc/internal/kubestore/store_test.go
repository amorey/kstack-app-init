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

package kubestore

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

var (
	podKind    = Kind{APIVersion: "v1", Kind: "Pod", Resource: "pods"}
	eventsKind = Kind{APIVersion: "v1", Kind: "Event", Resource: "events"}
)

// newTestStore opens one cache's store for the test's life.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	m := NewManager(t.TempDir(), Retention{})
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	t.Cleanup(store.Release)
	return store
}

// pod builds a Pod body with the given uid, name and resourceVersion.
func pod(uid, name, rv string) *unstructured.Unstructured {
	return obj(map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"uid": uid, "name": name, "namespace": "prod", "resourceVersion": rv,
			"labels": map[string]any{"app": "api"},
		},
		"status": map[string]any{"phase": "Running"},
	})
}

// event builds an Event body with the given uid and last-seen time.
func event(uid, lastSeen string) *unstructured.Unstructured {
	return obj(map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata":       map[string]any{"uid": uid},
		"involvedObject": map[string]any{"uid": "pod-1", "kind": "Pod", "name": "api-0"},
		"reason":         "BackOff", "message": "restarting",
		"lastTimestamp": lastSeen,
	})
}

// beginReplace opens a relist session for the test.
func beginReplace(t *testing.T, s *Store, k Kind) *ReplaceSession {
	t.Helper()
	session, err := s.BeginReplace(k)
	require.NoError(t, err)
	return session
}

// subscribe opens the store's change feed for the test.
func subscribe(t *testing.T, s *Store) Subscription {
	t.Helper()
	sub, err := s.Subscribe()
	require.NoError(t, err)
	return sub
}

// recvKey takes the next ping's key, bounded by the failsafe so a bus that never
// fires fails the test instead of hanging it.
func recvKey(t *testing.T, sub Subscription) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testutil.Timeout)
	defer cancel()
	ev, err := sub.RecvContext(ctx)
	require.NoError(t, err)
	return ev.Key
}

// db is the open database behind a claim — the white-box reach these tests assert
// through.
func db(t *testing.T, s *Store) *sql.DB {
	t.Helper()
	f, err := s.file()
	require.NoError(t, err)
	return f.db
}

// countRows is one scalar SQL read.
func countRows(t *testing.T, s *Store, q string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, db(t, s).QueryRowContext(context.Background(), q, args...).Scan(&n))
	return n
}

// A delta lands the row, its edges, and the position that would replay it — in one
// transaction, so no restart resumes from a position the rows do not back.
func TestApplyChangeWritesTheObjectAndAdvancesTheCookie(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "42")))

	assert.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM objects WHERE uid='uid-1'`))
	assert.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM labels WHERE uid='uid-1'`))
	assert.Equal(t, 1, countRows(t, s, `SELECT count FROM kind_counts WHERE api_version='v1' AND kind='Pod'`))

	rv, ok, err := s.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "42", rv)
}

// A body the projection cannot key is skipped rather than failing the delta, and the cookie
// advances over it all the same: the server replays from that position, so a run that failed
// here would be handed the same body every time it resumed.
func TestApplyChangeSkipsAnUnprojectableBodyAndMovesPast(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("", "api-0", "42")))

	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM objects`), "nothing keyable to write")

	rv, ok, err := s.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "42", rv, "the position moves, or the next resume replays the same body")
}

// The stored body is the sanitized one, and it round-trips through the codec — which is
// what lets a read serve raw_json verbatim.
func TestApplyChangeStoresTheBodyCompressed(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "42")))

	var blob []byte
	require.NoError(t, db(t, s).QueryRowContext(ctx, `SELECT raw_json FROM objects WHERE uid='uid-1'`).Scan(&blob))
	assert.Equal(t, byte(0x78), blob[0])
	body, err := decompressRaw(blob)
	require.NoError(t, err)
	assert.Contains(t, string(body), `"api-0"`)
}

func TestApplyChangeDeletedRemovesTheRowAndItsEdges(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "42")))

	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Deleted, pod("uid-1", "api-0", "43")))

	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM objects WHERE uid='uid-1'`))
	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM labels WHERE uid='uid-1'`))
	// The kind's tally rests at 0 rather than vanishing: an advertised but empty kind
	// must read 0, and only a clear retires the row.
	assert.Zero(t, countRows(t, s, `SELECT count FROM kind_counts WHERE api_version='v1' AND kind='Pod'`))
}

// The Event kind is written to its own table: routing it to objects would leave the
// events watch, its FTS index, and the event count with nothing.
func TestApplyChangeRoutesCoreEventsToTheEventsTable(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	require.NoError(t, s.ApplyChange(ctx, eventsKind, watch.Added, event("ev-1", "2026-08-01T00:00:00Z")))

	assert.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM events WHERE uid='ev-1'`))
	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM objects`))
	assert.Equal(t, 1, countRows(t, s, `SELECT count FROM kind_counts WHERE api_version='v1' AND kind='Event'`))
}

// A relist reconciles by mark and sweep: what the pass rewrote survives, what it did not
// is gone — which is where an object deleted while we were disconnected finally leaves.
func TestReplacePrunesWhatTheListDidNotCarry(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("gone", "old", "1")))
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("kept", "api-0", "1")))

	session := beginReplace(t, s, podKind)
	require.NoError(t, session.WritePage(ctx, []*unstructured.Unstructured{pod("kept", "api-0", "9")}))
	pruned, err := session.Commit(ctx, "100")
	require.NoError(t, err)

	assert.Equal(t, 1, pruned)
	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM objects WHERE uid='gone'`))
	assert.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM objects WHERE uid='kept'`))
	rv, _, err := s.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	assert.Equal(t, "100", rv)
}

// A relist prunes only its own kind: every other kind's rows belong to another worker.
func TestReplacePrunesOnlyItsOwnKind(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	deployments := Kind{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments"}
	dep := obj(map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"uid": "dep-1", "name": "api", "resourceVersion": "1"},
	})
	require.NoError(t, s.ApplyChange(ctx, deployments, watch.Added, dep))

	session := beginReplace(t, s, podKind)
	require.NoError(t, session.WritePage(ctx, []*unstructured.Unstructured{pod("kept", "api-0", "9")}))
	_, err := session.Commit(ctx, "100")
	require.NoError(t, err)

	assert.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM objects WHERE uid='dep-1'`))
}

// A pass that wrote rows and then failed must leave no cookie: the rows on disk are
// half a collection, and resuming a watch from that position would never reconcile
// them. Only Commit writes one back.
func TestReplaceClearsTheCookieOnItsFirstPage(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.SetCookie(ctx, "v1", "pods", "old"))

	session := beginReplace(t, s, podKind)
	require.NoError(t, session.WritePage(ctx, []*unstructured.Unstructured{pod("uid-1", "api-0", "9")}))

	_, ok, err := s.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	assert.False(t, ok)
}

// A body that will not project is skipped rather than failing the page: one malformed
// object must not stop a collection from syncing.
func TestReplaceSkipsAnUnprojectableBody(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	session := beginReplace(t, s, podKind)
	err := session.WritePage(ctx, []*unstructured.Unstructured{
		obj(map[string]any{"apiVersion": "v1", "kind": "Pod"}), // no uid
		pod("uid-1", "api-0", "9"),
	})

	require.NoError(t, err)
	assert.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM objects`))
}

func TestReplaceLandsEventsInTheEventsTable(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.ApplyChange(ctx, eventsKind, watch.Added, event("gone", "2026-08-01T00:00:00Z")))

	session := beginReplace(t, s, eventsKind)
	require.NoError(t, session.WritePage(ctx, []*unstructured.Unstructured{event("ev-1", "2026-08-01T00:01:00Z")}))
	pruned, err := session.Commit(ctx, "100")

	require.NoError(t, err)
	assert.Equal(t, 1, pruned)
	assert.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM events WHERE uid='ev-1'`))
	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM events WHERE uid='gone'`))
}

// The ping is what a read re-reads on. It is keyed per kind so an unrelated kind's
// write costs a kind watch nothing.
func TestAnObjectWriteNotifiesItsOwnKey(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	sub := subscribe(t, s)
	defer sub.Close()

	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "42")))

	assert.Equal(t, ObjectsKey("v1", "pods"), recvKey(t, sub))
}

// Events have their own bus key, so an event-storm cluster does not wake every object
// watch in the cache.
func TestAnEventWriteNotifiesTheEventsKey(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	sub := subscribe(t, s)
	defer sub.Close()

	require.NoError(t, s.ApplyChange(ctx, eventsKind, watch.Added, event("ev-1", "2026-08-01T00:00:00Z")))

	assert.Equal(t, EventsKey, recvKey(t, sub))
}

// Aging out is not a write, so nothing would emit the Deleted a client needs for free.
// The pruner is what keeps the window a window — and it pings, so the read sees it.
func TestRollupCountsObjectsAndKindsExcludingEvents(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "1")))
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-2", "api-1", "1")))
	require.NoError(t, s.ApplyChange(ctx, eventsKind, watch.Added, event("ev-1", "2026-08-01T00:00:00Z")))

	rollup, err := s.Counts(ctx)

	require.NoError(t, err)
	assert.Equal(t, 2, rollup.ObjectCount)
	assert.Equal(t, 1, rollup.KindCount)
}

// A kind emptied by deletes reads 0 rather than counting as a kind the cache holds.
func TestRollupIgnoresAKindWithNoRowsLeft(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "1")))
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Deleted, pod("uid-1", "api-0", "2")))

	rollup, err := s.Counts(ctx)

	require.NoError(t, err)
	assert.Zero(t, rollup.ObjectCount)
	assert.Zero(t, rollup.KindCount)
}

// CountKind is the worker's own tally — the number its observation carries.
func TestCountKindReadsOneKindsTally(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "1")))
	require.NoError(t, s.ApplyChange(ctx, eventsKind, watch.Added, event("ev-1", "2026-08-01T00:00:00Z")))

	pods, err := s.CountKind(ctx, podKind)
	require.NoError(t, err)
	events, err := s.CountKind(ctx, eventsKind)
	require.NoError(t, err)

	assert.Equal(t, 1, pods)
	assert.Equal(t, 1, events)
}

func TestCountKindOfAnUnwrittenKindIsZero(t *testing.T) {
	n, err := newTestStore(t).CountKind(context.Background(), podKind)

	require.NoError(t, err)
	assert.Zero(t, n)
}

// Closing the store ends every subscriber — which is how a Clear or a shutdown reaches
// a live watch instead of leaving it waiting on a store that is gone.
func TestClosingTheStoreEndsSubscribers(t *testing.T) {
	m := NewManager(t.TempDir(), Retention{})
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	sub, err := store.Subscribe()
	require.NoError(t, err)
	defer sub.Close()

	store.Release()

	ctx, cancel := context.WithTimeout(context.Background(), testutil.Timeout)
	defer cancel()
	_, err = sub.RecvContext(ctx)
	assert.Error(t, err)
}

func TestCookieRoundtripIsPerKind(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir, Retention{})
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	ctx := context.Background()
	require.NoError(t, store.SetCookie(ctx, "apps/v1", "deployments", "100"))
	require.NoError(t, store.SetCookie(ctx, "v1", "pods", "200"))

	v, ok, err := store.Cookie(ctx, "apps/v1", "deployments")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "100", v)

	v, ok, err = store.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "200", v)
}

func TestStoreClearKindRemovesOnlyThatKindsCookie(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir, Retention{})
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	ctx := context.Background()
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	require.NoError(t, store.SetCookie(ctx, "v1", "pods", "1"))
	require.NoError(t, store.SetCookie(ctx, "apps/v1", "deployments", "2"))

	require.NoError(t, store.ClearKind(ctx, podKind))

	_, ok, err := store.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.False(t, ok)

	v, ok, err := store.Cookie(ctx, "apps/v1", "deployments")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "2", v)
}

func TestStoreClearKindOnEmptyStoreSucceeds(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir, Retention{})
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	require.NoError(t, store.ClearKind(context.Background(), podKind))
}

// ClearKind deletes the kind's objects and every row hanging off them — the schema
// has no cascading foreign keys — and touches no other kind.
func TestStoreClearKindDeletesObjectsAndTheirDependentRows(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir, Retention{})
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	ctx := context.Background()
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	db := db(t, store)
	for _, kind := range []struct{ apiVersion, kind, resource, uid string }{
		{"v1", "Pod", "pods", "uid-pod"},
		{"apps/v1", "Deployment", "deployments", "uid-dep"},
	} {
		_, err := db.ExecContext(ctx,
			`INSERT INTO kind_catalog (api_version, kind, resource, scope, is_crd) VALUES (?, ?, ?, 'Namespaced', 0)`,
			kind.apiVersion, kind.kind, kind.resource)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx,
			`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
			 VALUES (?, ?, ?, 'x', '1', 0, 0, x'7b7d')`,
			kind.uid, kind.apiVersion, kind.kind)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO owner_refs (child_uid, owner_uid) VALUES (?, 'owner')`, kind.uid)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO labels (uid, key, value) VALUES (?, 'app', 'x')`, kind.uid)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO status_history (uid, at, summary) VALUES (?, 0, 'Running')`, kind.uid)
		require.NoError(t, err)
	}

	require.NoError(t, store.ClearKind(ctx, podKind))

	for _, q := range []string{
		`SELECT COUNT(*) FROM objects WHERE uid = 'uid-pod'`,
		`SELECT COUNT(*) FROM owner_refs WHERE child_uid = 'uid-pod'`,
		`SELECT COUNT(*) FROM labels WHERE uid = 'uid-pod'`,
		`SELECT COUNT(*) FROM status_history WHERE uid = 'uid-pod'`,
	} {
		var n int
		require.NoError(t, db.QueryRowContext(ctx, q).Scan(&n))
		assert.Zerof(t, n, "rows left behind by: %s", q)
	}

	// The other kind is untouched, dependent rows included.
	for _, q := range []string{
		`SELECT COUNT(*) FROM objects WHERE uid = 'uid-dep'`,
		`SELECT COUNT(*) FROM owner_refs WHERE child_uid = 'uid-dep'`,
		`SELECT COUNT(*) FROM labels WHERE uid = 'uid-dep'`,
		`SELECT COUNT(*) FROM status_history WHERE uid = 'uid-dep'`,
	} {
		var n int
		require.NoError(t, db.QueryRowContext(ctx, q).Scan(&n))
		assert.Equalf(t, 1, n, "row removed by another kind's clear: %s", q)
	}
}

// Event rows live in their own table, so clearing the Events kind has to empty it —
// deleting from objects would leave every cached event behind.
func TestStoreClearKindClearsTheEventsTable(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir, Retention{})
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	ctx := context.Background()
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	db := db(t, store)
	_, err = db.ExecContext(ctx,
		`INSERT INTO kind_catalog (api_version, kind, resource, scope, is_crd) VALUES ('v1', 'Event', 'events', 'Namespaced', 0)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`INSERT INTO events (uid, reason, message, raw_json, updated_at) VALUES ('uid-ev', 'Pulled', 'ok', x'7b7d', 0)`)
	require.NoError(t, err)

	require.NoError(t, store.ClearKind(ctx, eventsKind))

	var n int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&n))
	assert.Zero(t, n, "cached events survived their kind's clear")
}

// A retained child keeps its edge into a cleared owner: the edge is the child's own
// ownerReference, and nothing would put it back — a re-synced owner kind writes owner
// rows, not its children's edges.
func TestStoreClearKindKeepsARetainedChildsEdgeIntoTheClearedKind(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir, Retention{})
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	ctx := context.Background()
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	db := db(t, store)
	_, err = db.ExecContext(ctx,
		`INSERT INTO kind_catalog (api_version, kind, resource, scope, is_crd) VALUES ('apps/v1', 'Deployment', 'deployments', 'Namespaced', 0)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
		 VALUES ('uid-dep', 'apps/v1', 'Deployment', 'web', '1', 0, 0, x'7b7d')`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
		 VALUES ('uid-pod', 'v1', 'Pod', 'web-1', '1', 0, 0, x'7b7d')`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO owner_refs (child_uid, owner_uid) VALUES ('uid-pod', 'uid-dep')`)
	require.NoError(t, err)

	require.NoError(t, store.ClearKind(ctx, Kind{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments"}))

	var n int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM owner_refs WHERE child_uid = 'uid-pod'`).Scan(&n))
	assert.Equal(t, 1, n, "the retained Pod's own ownerReference was deleted with its owner")

	// The edge resolves to no owner, which is what a traversal's join already answers
	// for an owner kind that is not mirrored.
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM owner_refs r JOIN objects o ON o.uid = r.owner_uid WHERE r.child_uid = 'uid-pod'`).Scan(&n))
	assert.Zero(t, n)
}

// Another group may serve a Kind called "Event"; its rows are ordinary objects, so
// clearing it must leave the cached core events alone.
func TestStoreClearKindLeavesEventsAloneForANonCoreEventKind(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir, Retention{})
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	ctx := context.Background()
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	db := db(t, store)
	_, err = db.ExecContext(ctx,
		`INSERT INTO kind_catalog (api_version, kind, resource, scope, is_crd) VALUES ('example.com/v1', 'Event', 'events', 'Namespaced', 1)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
		 VALUES ('uid-crd', 'example.com/v1', 'Event', 'x', '1', 0, 0, x'7b7d')`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`INSERT INTO events (uid, reason, message, raw_json, updated_at) VALUES ('uid-ev', 'Pulled', 'ok', x'7b7d', 0)`)
	require.NoError(t, err)

	require.NoError(t, store.ClearKind(ctx, Kind{APIVersion: "example.com/v1", Kind: "Event", Resource: "events"}))

	var n int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&n))
	assert.Equal(t, 1, n, "the CRD's clear wiped the cached core events")

	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM objects WHERE uid = 'uid-crd'`).Scan(&n))
	assert.Zero(t, n, "the CRD's own rows survived its clear")
}

// The events table is not keyed by kind, so its clear does not hang off the catalog
// row: a retry after a partial failure has already dropped that row.
func TestStoreClearKindClearsEventsWithNoCatalogRow(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir, Retention{})
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	ctx := context.Background()
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	db := db(t, store)
	_, err = db.ExecContext(ctx,
		`INSERT INTO events (uid, reason, message, raw_json, updated_at) VALUES ('uid-ev', 'Pulled', 'ok', x'7b7d', 0)`)
	require.NoError(t, err)

	require.NoError(t, store.ClearKind(ctx, eventsKind))

	var n int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&n))
	assert.Zero(t, n, "cached events survived a clear that found no catalog row")
}

// A claim outlives the file under it: the cache's teardown closes and deletes it while
// a worker is still holding one. Every method answers ErrClosed rather than reaching a
// closed database, which is what turns a torn-down cache into one failed attempt.
func TestAStoreWhoseFileIsGoneAnswersErrClosed(t *testing.T) {
	ctx := context.Background()
	m := NewManager(t.TempDir(), Retention{})
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	require.NoError(t, m.Remove(1))

	assert.ErrorIs(t, store.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "1")), ErrClosed)
	assert.ErrorIs(t, store.SetCookie(ctx, "v1", "pods", "1"), ErrClosed)
	assert.ErrorIs(t, store.ClearKind(ctx, podKind), ErrClosed)
	_, err = store.CountKind(ctx, podKind)
	assert.ErrorIs(t, err, ErrClosed)
	_, err = store.Counts(ctx)
	assert.ErrorIs(t, err, ErrClosed)
	_, err = store.BeginReplace(podKind)
	assert.ErrorIs(t, err, ErrClosed)
	_, _, err = store.Cookie(ctx, "v1", "pods")
	assert.ErrorIs(t, err, ErrClosed)
	_, err = store.Subscribe()
	assert.ErrorIs(t, err, ErrClosed)
}

// The first page invalidates the position even when it carries nothing. A cookie means
// a completed LIST landed on disk, so a relist that has begun must not leave one
// standing: a pass that then fails would let the next start resume from it, skipping
// the reconcile the rows still need.
func TestReplaceClearsTheCookieOnAnEmptyFirstPage(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.SetCookie(ctx, "v1", "pods", "old"))

	session := beginReplace(t, s, podKind)
	require.NoError(t, session.WritePage(ctx, nil))

	_, ok, err := s.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	assert.False(t, ok, "an in-flight relist left a position claiming the cache is caught up")
}

// A page carrying nothing is not a write: it must not ping the bus, or an empty relist
// would wake every reader of that kind for no change.
func TestAnEmptyPageDoesNotNotify(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	sub := subscribe(t, s)
	defer sub.Close()

	session := beginReplace(t, s, podKind)
	require.NoError(t, session.WritePage(ctx, nil))

	// A negative assertion needs a bounded window; a ping would already be pending.
	_, err := sub.TryRecv()
	assert.Error(t, err, "an empty page pinged the bus")
}

// A kind's rows are keyed by Kind, and the caller knows which one — a teardown must not
// depend on the catalog to find them. Nothing writes catalog rows on the sync path (the
// sweep owns that table), so a clear that resolved through it would leave every row, edge
// and count behind for a kind that stopped being synced.
func TestClearKindDeletesRowsWithNoCatalogRow(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "42")))
	require.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM kind_catalog`), "nothing on the sync path writes catalog rows")

	require.NoError(t, s.ClearKind(ctx, podKind))

	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM objects`))
	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM labels`))
	// The tally goes with the kind rather than resting at 0: nothing will name it again.
	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM kind_counts WHERE api_version='v1' AND kind='Pod'`))
	_, ok, err := s.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	assert.False(t, ok)
}

// The catalog is what the CLUSTER serves, and clearing a kind's cache does not stop it
// being served — a user emptying one kind must not have it vanish from the nav until the
// next sweep. The row leaves through the sweep's prune alone.
func TestClearKindKeepsTheCatalogRow(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow}, true, 7))
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "42")))

	require.NoError(t, s.ClearKind(ctx, podKind))

	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM objects`))
	assert.Equal(t, []KindRow{podRow}, catalogRows(t, s))
}

// A subscription named for one kind must not wake on another's writes, or every object
// write in the cache re-reads every open watch — which is what the per-kind bus keys exist
// to prevent. conflate filters at enqueue, so the key belongs on the subscription.
func TestSubscribeWithAKeyIgnoresOtherKeys(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	sub, err := s.Subscribe(ObjectsKey("v1", "pods"))
	require.NoError(t, err)
	defer sub.Close()

	require.NoError(t, s.ApplyChange(ctx, eventsKind, watch.Added, event("uid-ev", "2026-08-26T10:00:00Z")))
	_, err = sub.TryRecv()
	assert.Error(t, err, "an events write woke a pods subscription")

	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "42")))
	assert.Equal(t, ObjectsKey("v1", "pods"), recvKey(t, sub))
}

// No keys is the whole feed, which is what the kinds watch needs: object writes move counts
// and event writes move the hardcoded ('v1','Event') count, and there is one hub.
func TestSubscribeWithNoKeysTakesEveryKey(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	sub, err := s.Subscribe()
	require.NoError(t, err)
	defer sub.Close()

	require.NoError(t, s.ApplyChange(ctx, eventsKind, watch.Added, event("uid-ev", "2026-08-26T10:00:00Z")))
	assert.Equal(t, EventsKey, recvKey(t, sub))
}

// podOwnedBy is a Pod carrying one ownerReference, so the sweep has an edge to cascade.
func podOwnedBy(uid, name, rv, ownerUID string) *unstructured.Unstructured {
	return obj(map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"uid": uid, "name": name, "namespace": "prod", "resourceVersion": rv,
			"labels":          map[string]any{"app": "api"},
			"ownerReferences": []any{map[string]any{"uid": ownerUID, "kind": "ReplicaSet", "name": "rs"}},
		},
		"status": map[string]any{"phase": "Running"},
	})
}

// The prune is the only path that removes an object without knowing its uid, so it is the
// only one that can leave a side table behind. owner_refs is checked in both directions: a
// swept object is a child of something and an owner of something, and its children outlive
// it, so an inbound edge left behind points at a uid that is gone.
func TestReplacePruneTakesEverySideTableRowWithIt(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	for _, uid := range []string{"gone-1", "gone-2", "gone-3"} {
		require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, podOwnedBy(uid, uid, "1", "rs-1")))
	}
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, podOwnedBy("kept", "kept", "1", "rs-1")))
	// Another kind, so the Pod relist keeps it — and it owns gone-1, which is the inbound
	// edge nothing else would clear.
	deployments := Kind{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments"}
	require.NoError(t, s.ApplyChange(ctx, deployments, watch.Added, obj(map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"uid": "dep-1", "name": "api", "resourceVersion": "1",
			"ownerReferences": []any{map[string]any{"uid": "gone-1", "kind": "Pod", "name": "gone-1"}},
		},
	})))
	require.Equal(t, 3, countRows(t, s,
		`SELECT COUNT(*) FROM status_history WHERE uid IN ('gone-1', 'gone-2', 'gone-3')`),
		"the fixture writes one per pod")

	session := beginReplace(t, s, podKind)
	require.NoError(t, session.WritePage(ctx, []*unstructured.Unstructured{podOwnedBy("kept", "kept", "9", "rs-1")}))
	pruned, err := session.Commit(ctx, "100")
	require.NoError(t, err)

	require.Equal(t, 3, pruned)
	const gone = `IN ('gone-1', 'gone-2', 'gone-3')`
	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM labels WHERE uid `+gone))
	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM owner_refs WHERE child_uid `+gone))
	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM owner_refs WHERE owner_uid `+gone),
		"the Deployment's edge into gone-1")
	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM status_history WHERE uid `+gone))
	assert.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM labels WHERE uid='kept'`))
	assert.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM owner_refs WHERE child_uid='kept'`))
}
