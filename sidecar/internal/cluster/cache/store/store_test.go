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

package store

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ref builds a CacheRef from a parent-cluster and cache ObjectID. Most tests use
// (1, 1); the values only have to be positive and distinct where a test opens
// more than one incarnation.
func ref(clusterID, cacheID int64) CacheRef {
	return CacheRef{ClusterID: clusterID, CacheID: cacheID}
}

func TestOpenMigrateClose(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()

	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	require.FileExists(t, clusterDBPath(dir, ref(1, 1)))

	var n int
	require.NoError(t, cdb.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM schema_migrations`).Scan(&n))
	require.Greater(t, n, 0, "expected at least one migration recorded")

	require.NoError(t, r.Shutdown(ctx))
}

// auto_vacuum must end up INCREMENTAL (2) so the janitor's incremental_vacuum
// can return trimmed pages to the OS. It has to be set before any table exists,
// including the migration runner's schema_migrations table — a regression here
// is silent (the DB just grows), so guard the resulting mode explicitly.
func TestAutoVacuumIsIncremental(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()

	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	var mode int
	require.NoError(t, cdb.Reader().QueryRowContext(ctx, `PRAGMA auto_vacuum`).Scan(&mode))
	require.Equal(t, 2, mode, "auto_vacuum should be INCREMENTAL (2)")
}

// status_history is intentionally a plain rowid table with no (uid, at) primary
// key, so two distinct status transitions for the same object that land in the
// same millisecond both survive. Re-adding a unique constraint would silently
// drop the second transition — guard the schema directly.
func TestStatusHistoryKeepsSameMillisecondTransitions(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()

	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	// A current timestamp: the janitor (running since Open) must not sweep
	// the rows out from under the schema assertion.
	uid, at := "pod-1", time.Now().UnixMilli()
	for _, summary := range []string{"Pending", "Running"} {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO status_history(uid, at, summary) VALUES(?, ?, ?)`, uid, at, summary)
		require.NoError(t, err, "same (uid, at) must not collide")
	}

	var n int
	require.NoError(t, cdb.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM status_history WHERE uid=? AND at=?`, uid, at).Scan(&n))
	require.Equal(t, 2, n, "both same-millisecond transitions should persist")
}

// A CacheRef's segments are beehive AUTOINCREMENT ObjectIDs (int64 > 0), so the
// on-disk path is digits only and can never escape the clusters dir — path
// traversal is structurally impossible. The only invalid ref is a non-positive
// id, which every disk-touching method must reject.
func TestRejectsNonPositiveRef(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()

	// A sentinel sibling of the clusters dir that must stay untouched.
	sentinel := filepath.Join(dir, "foo.db")
	require.NoError(t, os.WriteFile(sentinel, []byte("keep"), 0o600))

	for _, bad := range []CacheRef{ref(0, 1), ref(1, 0), ref(-1, 1), {}} {
		_, err := r.Open(ctx, bad)
		require.Error(t, err, "Open rejects %+v", bad)
		require.Error(t, r.DeleteCacheFiles(ctx, bad), "DeleteCacheFiles rejects %+v", bad)
		_, exists := r.CacheBytes(bad)
		require.False(t, exists, "CacheBytes rejects %+v", bad)
	}
	require.FileExists(t, sentinel, "no filesystem writes for an invalid ref")
}

func TestDeleteCacheFilesRemovesClosedCluster(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()

	_, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	path := clusterDBPath(dir, ref(1, 1))
	require.FileExists(t, path)

	require.NoError(t, r.DeleteCacheFiles(ctx, ref(1, 1)))
	require.NoFileExists(t, path)
	require.NoDirExists(t, clusterDir(dir, ref(1, 1)), "empty per-cluster dir is reaped")
	require.Nil(t, r.Lookup(1), "delete closes the open handle")

	// Bytes report gone; a repeat delete is a no-op.
	_, exists := r.CacheBytes(ref(1, 1))
	require.False(t, exists)
	require.NoError(t, r.DeleteCacheFiles(ctx, ref(1, 1)))
	require.NoError(t, r.Shutdown(ctx))
}

func TestCacheBytesWorksClosed(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	r1 := NewManager(dir)
	_, err := r1.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	require.NoError(t, r1.Shutdown(ctx))

	r2 := NewManager(dir)
	n, exists := r2.CacheBytes(ref(1, 1))
	require.True(t, exists, "stat-only size works without opening")
	require.Greater(t, n, int64(0))

	_, exists = r2.CacheBytes(ref(1, 2))
	require.False(t, exists, "never-opened incarnation reports no cache")
}

func TestOpenIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()

	a, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	b, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	require.Same(t, a, b, "second Open should return the same handle")
	require.Same(t, a, r.Lookup(1))
	require.Nil(t, r.Lookup(2))

	require.NoError(t, r.Shutdown(ctx))
}

func TestReopenRunsNoMigrations(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	r1 := NewManager(dir)
	cdb1, err := r1.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	var firstCount int
	require.NoError(t, cdb1.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM schema_migrations`).Scan(&firstCount))
	require.NoError(t, r1.Shutdown(ctx))

	r2 := NewManager(dir)
	cdb2, err := r2.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	var secondCount int
	require.NoError(t, cdb2.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM schema_migrations`).Scan(&secondCount))
	require.Equal(t, firstCount, secondCount, "reopen should not re-apply migrations")
	require.NoError(t, r2.Shutdown(ctx))
}

func TestShutdownRefusesNewOpens(t *testing.T) {
	r := NewManager(t.TempDir())
	ctx := context.Background()
	require.NoError(t, r.Shutdown(ctx))
	_, err := r.Open(ctx, ref(1, 1))
	require.Error(t, err)
}

func TestConcurrentReadersDuringWriter(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	// Writer: 1000 pod upserts in a single transaction.
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- func() error {
			tx, err := cdb.Writer().BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			for i := range 1000 {
				if _, err := tx.ExecContext(ctx,
					`INSERT OR REPLACE INTO objects
					 (uid, api_version, kind, namespace, name, resource_version, generation,
					  created_at, updated_at, status_summary, raw_json)
					 VALUES (?, 'v1', 'Pod', 'default', ?, '1', 1, ?, ?, 'Running', x'7b7d')`,
					"uid-"+itoa(i), "pod-"+itoa(i), time.Now().UnixMilli(), time.Now().UnixMilli(),
				); err != nil {
					return err
				}
			}
			return tx.Commit()
		}()
	}()

	// Readers run concurrently. They should see either pre- or post-commit
	// state — never an error.
	var wg sync.WaitGroup
	readerErr := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				var n int
				if err := cdb.Reader().QueryRowContext(ctx,
					`SELECT COUNT(*) FROM objects WHERE kind='Pod'`).Scan(&n); err != nil {
					readerErr <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(readerErr)
	for err := range readerErr {
		require.NoError(t, err, "reader saw SQLITE_BUSY or similar during writer txn")
	}
	require.NoError(t, <-writerDone)

	var final int
	require.NoError(t, cdb.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM objects WHERE kind='Pod'`).Scan(&final))
	require.Equal(t, 1000, final)
}

func TestCorruptFileQuarantined(t *testing.T) {
	dir := t.TempDir()
	broken := ref(1, 1)
	dbPath := clusterDBPath(dir, broken)
	require.NoError(t, os.MkdirAll(clusterDir(dir, broken), 0o700))
	// SQLite files start with a magic string; arbitrary bytes are an
	// invalid header and will fail integrity_check on first query.
	require.NoError(t, os.WriteFile(dbPath, []byte("not a sqlite file"), 0o600))

	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, broken)
	require.NoError(t, err, "open should recover from a corrupt file by quarantining it")
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	matches, err := filepath.Glob(dbPath + ".corrupt-*")
	require.NoError(t, err)
	require.NotEmpty(t, matches, "expected a quarantined .corrupt-* file")

	// New DB is usable.
	require.NoError(t, cdb.Reader().PingContext(ctx))
}

// The janitor does not manage event retention: events are server-mirrored by the
// sync engine (a relist prunes rows the cluster no longer has), so a stale-looking
// event is not the janitor's to sweep. Even a long-dead event survives a sweep.
func TestJanitorLeavesEventsAlone(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	now := time.Now().UnixMilli()
	twoDaysAgo := now - (48 * 60 * 60 * 1000)
	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO events(uid, type, reason, message, first_seen, last_seen, count, raw_json, updated_at)
		 VALUES('ev-old', 'Normal', 'Test', 'hello', ?, ?, 1, x'7b7d', ?)`,
		twoDaysAgo, twoDaysAgo, now)
	require.NoError(t, err)

	sweep(ctx, "c", cdb.Writer(), Retention{
		StatusHistoryTTL: 7 * 24 * time.Hour,
		Interval:         time.Minute,
	})

	var count int
	require.NoError(t, cdb.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&count))
	require.Equal(t, 1, count, "the janitor must not sweep events; retention is server-mirrored")
}

func TestSubscribeNotifyAndCoalesce(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	ch, cancel := cdb.Subscribe()
	defer cancel()

	// Two notifies with no consumer in between coalesce into one ping.
	cdb.Notify()
	cdb.Notify()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("expected a ping after Notify")
	}
	select {
	case <-ch:
		t.Fatal("coalesced pings must not deliver twice")
	default:
	}

	// A notify after draining delivers again.
	cdb.Notify()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("expected a ping after re-Notify")
	}
}

func TestShutdownClosesSubscribers(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)

	ch, cancel := cdb.Subscribe()
	defer cancel()
	require.NoError(t, r.Shutdown(ctx))

	select {
	case _, ok := <-ch:
		require.False(t, ok, "shutdown must close subscriber channels")
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber channel not closed on shutdown")
	}
}

// recvDB reads one handle off a WatchDB stream, failing on timeout or an
// unexpected close.
func recvDB(t *testing.T, ch <-chan *ClusterDB) *ClusterDB {
	t.Helper()
	select {
	case db, ok := <-ch:
		require.True(t, ok, "WatchDB stream closed early")
		return db
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a WatchDB handle")
		return nil
	}
}

// WatchDB is a latest-value stream of a CacheID's open handle across its
// lifecycle: current-on-subscribe (nil when not open), then a fresh value on open,
// close, and replace. This is what lets a long-lived reader bind to a cache that
// opens after it subscribes, or rebind when the db is swapped under it.
func TestWatchDBFollowsHandleLifecycle(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	// Subscribe before the cache is open: current-on-subscribe yields nil.
	ch, cancel := r.WatchDB(1)
	defer cancel()
	require.Nil(t, recvDB(t, ch), "unopened cache should watch as nil")

	// Open → the handle appears.
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	require.Equal(t, cdb, recvDB(t, ch), "open should deliver the new handle")

	// Delete (close) then reopen under the same CacheID → nil, then a fresh handle.
	require.NoError(t, r.DeleteCacheFiles(ctx, ref(1, 1)))
	require.Nil(t, recvDB(t, ch), "close should deliver nil")
	cdb2, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	require.Equal(t, cdb2, recvDB(t, ch), "reopen should deliver the replacement handle")
	require.NotSame(t, cdb, cdb2, "reopen must be a fresh handle")
}

// A WatchDB subscriber that arrives after the cache is already open sees that live
// handle immediately (current-on-subscribe), with no open event to wait for.
func TestWatchDBCurrentOnSubscribe(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)

	ch, cancel := r.WatchDB(1)
	defer cancel()
	require.Equal(t, cdb, recvDB(t, ch), "an already-open cache should watch as its handle")
}

// Shutdown closes every WatchDB channel so its subscriber's stream ends.
func TestWatchDBClosedOnShutdown(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()

	ch, cancel := r.WatchDB(1)
	defer cancel()
	require.Nil(t, recvDB(t, ch))

	require.NoError(t, r.Shutdown(ctx))
	select {
	case _, ok := <-ch:
		require.False(t, ok, "shutdown must close WatchDB channels")
	case <-time.After(2 * time.Second):
		t.Fatal("WatchDB channel not closed on shutdown")
	}
}

func TestKindCatalog(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	// Empty until discovery populates it.
	rows, err := cdb.KindCatalog(ctx)
	require.NoError(t, err)
	require.Empty(t, rows)

	insert := func(apiVersion, kind, resource, scope string, isCRD int) {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
			 VALUES(?, ?, ?, ?, ?, NULL)`,
			apiVersion, kind, resource, scope, isCRD)
		require.NoError(t, err)
	}
	insert("apps/v1", "Deployment", "deployments", "Namespaced", 0)
	insert("v1", "Node", "nodes", "Cluster", 0)
	insert("example.com/v1", "Widget", "widgets", "Namespaced", 1)

	// Cache two Deployments so the LEFT JOIN counts them; Node/Widget have no
	// cached objects and must count 0.
	at := time.Now().UnixMilli()
	insertObj := func(uid, apiVersion, kind string) {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
			   created_at, updated_at, raw_json)
			 VALUES (?, ?, ?, 'default', ?, '1', ?, ?, x'7b7d')`,
			uid, apiVersion, kind, uid, at, at)
		require.NoError(t, err)
	}
	insertObj("d1", "apps/v1", "Deployment")
	insertObj("d2", "apps/v1", "Deployment")

	rows, err = cdb.KindCatalog(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	// Ordered by (api_version, kind).
	require.Equal(t, "apps/v1", rows[0].APIVersion)
	require.Equal(t, "Deployment", rows[0].Kind)
	require.Equal(t, "deployments", rows[0].Resource)
	require.Equal(t, "Namespaced", rows[0].Scope)
	require.False(t, rows[0].IsCRD)
	require.Equal(t, 2, rows[0].Count, "counts cached objects of the kind")

	require.Equal(t, "example.com/v1", rows[1].APIVersion)
	require.True(t, rows[1].IsCRD, "is_crd decodes to bool")
	require.Equal(t, 0, rows[1].Count, "advertised-but-empty kind counts 0")

	require.Equal(t, "v1", rows[2].APIVersion)
	require.Equal(t, "Cluster", rows[2].Scope)
	require.Equal(t, 0, rows[2].Count)
}

// The per-kind counts are maintained by triggers on the objects table (so
// KindCatalog reads them without scanning objects). This pins the two properties
// the trigger design relies on: a delete decrements the count, and an object
// written before its catalog row still counts — kind_counts is keyed only by
// (api_version, kind), independent of kind_catalog's discovery rewrite.
func TestKindCatalogCountsMaintainedByTriggers(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	at := time.Now().UnixMilli()
	insertObj := func(uid, apiVersion, kind string) {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
			   created_at, updated_at, raw_json)
			 VALUES (?, ?, ?, 'default', ?, '1', ?, ?, x'7b7d')`,
			uid, apiVersion, kind, uid, at, at)
		require.NoError(t, err)
	}
	countFor := func(kind string) int {
		rows, err := cdb.KindCatalog(ctx)
		require.NoError(t, err)
		for _, row := range rows {
			if row.Kind == kind {
				return row.Count
			}
		}
		t.Fatalf("kind %q not in catalog", kind)
		return 0
	}

	// Objects written BEFORE the catalog row exists (discovery can land after the
	// first object writes). The count is tracked regardless.
	insertObj("d1", "apps/v1", "Deployment")
	insertObj("d2", "apps/v1", "Deployment")
	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
		 VALUES('apps/v1', 'Deployment', 'deployments', 'Namespaced', 0, NULL)`)
	require.NoError(t, err)
	require.Equal(t, 2, countFor("Deployment"), "objects written before the catalog row still count")

	// A delete decrements; the last delete leaves the kind at 0, not missing.
	_, err = cdb.Writer().ExecContext(ctx, `DELETE FROM objects WHERE uid='d1'`)
	require.NoError(t, err)
	require.Equal(t, 1, countFor("Deployment"), "delete decrements the maintained count")

	_, err = cdb.Writer().ExecContext(ctx, `DELETE FROM objects WHERE uid='d2'`)
	require.NoError(t, err)
	require.Equal(t, 0, countFor("Deployment"), "an emptied but still-advertised kind counts 0")
}

// Events live in their own table, not objects, so the objects-side kind_counts
// triggers never see them — a dedicated pair of triggers on the events table
// keeps the ('v1','Event') count so the dashboard nav's Events badge isn't stuck
// at 0. This pins that the count tracks event inserts/deletes and that a
// re-observed event (the warm-resume upsert path) does not double-count.
func TestEventKindCountMaintainedByTriggers(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	// Discovery advertises Event in the catalog; the count rides the LEFT JOIN
	// against kind_counts just like every other kind.
	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
		 VALUES('v1', 'Event', 'events', 'Namespaced', 0, NULL)`)
	require.NoError(t, err)

	countFor := func(kind string) int {
		rows, err := cdb.KindCatalog(ctx)
		require.NoError(t, err)
		for _, row := range rows {
			if row.Kind == kind {
				return row.Count
			}
		}
		t.Fatalf("kind %q not in catalog", kind)
		return 0
	}
	at := time.Now().UnixMilli()
	insertEvent := func(uid string) {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO events(uid, type, reason, message, first_seen, last_seen, count, raw_json, updated_at)
			 VALUES(?, 'Normal', 'Test', 'hello', ?, ?, 1, x'7b7d', ?)
			 ON CONFLICT(uid) DO UPDATE SET last_seen = excluded.last_seen`,
			uid, at, at, at)
		require.NoError(t, err)
	}

	require.Equal(t, 0, countFor("Event"), "no events cached yet")

	insertEvent("e1")
	insertEvent("e2")
	require.Equal(t, 2, countFor("Event"), "each new event bumps the count")

	// Re-observing an existing event upserts through ON CONFLICT DO UPDATE, which
	// fires the UPDATE trigger (undefined here), not INSERT — so the count holds.
	insertEvent("e1")
	require.Equal(t, 2, countFor("Event"), "a re-observed event does not double-count")

	_, err = cdb.Writer().ExecContext(ctx, `DELETE FROM events WHERE uid='e1'`)
	require.NoError(t, err)
	require.Equal(t, 1, countFor("Event"), "a delete decrements the count")

	_, err = cdb.Writer().ExecContext(ctx, `DELETE FROM events WHERE uid='e2'`)
	require.NoError(t, err)
	require.Equal(t, 0, countFor("Event"), "an emptied but still-advertised kind counts 0")
}

func itoa(i int) string {
	// fmt.Sprintf would pull fmt into the hot loop; this is tight and
	// deterministic.
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
