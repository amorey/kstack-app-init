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
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// The janitor sweeps one small table, but the writers that actually free pages are
// elsewhere and none of them vacuum: an events relist prune, a kind's Forget, an object
// delete sweep. Gating the vacuum on the janitor's OWN deletions meant uninstalling a CRD
// holding tens of thousands of objects freed every one of its pages and the file stayed at
// its high-water mark until an unrelated status_history row happened to age out.
func TestJanitorReclaimsPagesFreedByOtherWriters(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	// A kind's worth of objects, then the delete a Forget would do — no status_history row
	// involved anywhere, so the janitor's own sweep finds nothing to trim.
	body, err := CompressRaw([]byte(`{"spec":{"padding":"` + strings.Repeat("x", 4096) + `"}}`))
	require.NoError(t, err)
	for i := range 200 {
		_, err = cdb.Writer().ExecContext(ctx,
			`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
			   created_at, updated_at, raw_json)
			 VALUES (?, 'widgets.example.com/v1', 'Widget', 'default', ?, '1', 1, 1, ?)`,
			fmt.Sprintf("uid-%d", i), fmt.Sprintf("w-%d", i), body)
		require.NoError(t, err)
	}
	_, err = cdb.Writer().ExecContext(ctx, `DELETE FROM objects`)
	require.NoError(t, err)

	freelist := func() int64 {
		var n int64
		require.NoError(t, cdb.Writer().QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&n))
		return n
	}
	require.NotZero(t, freelist(), "the delete must have freed pages for the sweep to reclaim")

	sweep(ctx, "c", cdb.Writer(), Retention{
		StatusHistoryTTL: 7 * 24 * time.Hour,
		Interval:         time.Minute,
	})

	require.Zero(t, freelist(), "the janitor must hand back pages whoever freed them")
}

// The cache has a single writer, so a vacuum holds every kind's sync behind it while it
// runs — and the freelist is biggest exactly when that hurts most, right after an events
// prune or a CRD's Forget. One pass therefore reclaims a bounded number of pages; a bigger
// backlog drains over the next few.
func TestJanitorReclaimsPagesInBoundedChunks(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	// A small budget, so the backlog below reliably exceeds it whatever the page size.
	defer func(orig int64) { vacuumPagesPerSweep = orig }(vacuumPagesPerSweep)
	vacuumPagesPerSweep = 64

	// Free more pages than one sweep may reclaim. Incompressible bodies, so each row costs
	// real pages rather than compressing to nothing.
	body := make([]byte, 8192)
	for i := range body {
		body[i] = byte(i * 7)
	}
	blob, err := CompressRaw(body)
	require.NoError(t, err)
	for i := range 2000 {
		_, err = cdb.Writer().ExecContext(ctx,
			`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
			   created_at, updated_at, raw_json)
			 VALUES (?, 'widgets.example.com/v1', 'Widget', 'default', ?, '1', 1, 1, ?)`,
			fmt.Sprintf("uid-%d", i), fmt.Sprintf("w-%d", i), blob)
		require.NoError(t, err)
	}
	_, err = cdb.Writer().ExecContext(ctx, `DELETE FROM objects`)
	require.NoError(t, err)

	freelist := func() int64 {
		var n int64
		require.NoError(t, cdb.Writer().QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&n))
		return n
	}
	start := freelist()
	require.Greater(t, start, int64(vacuumPagesPerSweep),
		"the backlog must exceed one sweep's budget for this to mean anything")

	ret := Retention{StatusHistoryTTL: 7 * 24 * time.Hour, Interval: time.Minute}
	sweep(ctx, "c", cdb.Writer(), ret)

	after := freelist()
	require.Equal(t, start-vacuumPagesPerSweep, after, "one sweep reclaims exactly its budget")

	// And the backlog does drain, a sweep at a time.
	for range 200 {
		if freelist() == 0 {
			break
		}
		sweep(ctx, "c", cdb.Writer(), ret)
	}
	require.Zero(t, freelist(), "repeated sweeps must finish the job")
}

func TestSubscribeNotifyAndCoalesce(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	ch, cancel := cdb.ObjectsSubscribe()
	defer cancel()

	// Two notifies with no consumer in between coalesce into one ping.
	cdb.ObjectsNotify()
	cdb.ObjectsNotify()
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
	cdb.ObjectsNotify()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("expected a ping after re-Notify")
	}
}

// A keyed object-write notify routes by resource: ObjectsNotifyResource wakes the
// subscriber registered for that (apiVersion, resource) AND every keyless subscriber (the
// kind-catalog watch, which must wake on any write), but NOT a subscriber keyed to a
// different resource — so an unrelated resource's writes cost a keyed objects watch no
// re-read. A keyless ObjectsNotify() broadcast still wakes everyone (the discovery/prune
// fallback).
func TestObjectsBrokerKeyedNotify(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	pods, cancelPods := cdb.ObjectsSubscribeResource("v1", "pods")
	defer cancelPods()
	deploys, cancelDeploys := cdb.ObjectsSubscribeResource("apps/v1", "deployments")
	defer cancelDeploys()
	keyless, cancelKeyless := cdb.ObjectsSubscribe()
	defer cancelKeyless()

	pinged := func(ch <-chan struct{}) bool {
		select {
		case <-ch:
			return true
		case <-time.After(200 * time.Millisecond):
			return false
		}
	}
	// Drain any coalesced ping without waiting on the (slow) timeout.
	drain := func(ch <-chan struct{}) {
		select {
		case <-ch:
		default:
		}
	}

	// A pods-keyed write wakes the pods subscriber + the keyless subscriber, never the
	// deployments subscriber.
	cdb.ObjectsNotifyResource("v1", "pods")
	require.True(t, pinged(pods), "keyed notify must wake its matching subscriber")
	require.True(t, pinged(keyless), "keyed notify must wake keyless subscribers")
	require.False(t, pinged(deploys), "keyed notify must not wake an unrelated-resource subscriber")
	drain(pods)
	drain(keyless)

	// A deployments-keyed write wakes the deployments subscriber + keyless, never pods.
	cdb.ObjectsNotifyResource("apps/v1", "deployments")
	require.True(t, pinged(deploys), "keyed notify must wake its matching subscriber")
	require.True(t, pinged(keyless), "keyed notify must wake keyless subscribers")
	require.False(t, pinged(pods), "keyed notify must not wake an unrelated-resource subscriber")
	drain(deploys)
	drain(keyless)

	// A keyless broadcast wakes every subscriber (discovery/prune fallback).
	cdb.ObjectsNotify()
	require.True(t, pinged(pods), "keyless broadcast must wake keyed subscribers")
	require.True(t, pinged(deploys), "keyless broadcast must wake keyed subscribers")
	require.True(t, pinged(keyless), "keyless broadcast must wake keyless subscribers")
}

// Events reads the newest cached events (ordered by last_seen DESC), flattens the
// involved-object identity, and honors the limit — the read that backs the dashboard
// events table.
func TestEventsReadNewestFirstWithLimit(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	insert := func(uid string, lastSeen int64) {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO events(uid, involved_kind, involved_ns, involved_name,
			   type, reason, message, first_seen, last_seen, count, raw_json, updated_at)
			 VALUES(?, 'Pod', 'default', 'my-pod', 'Warning', 'BackOff', 'msg', ?, ?, 3, x'7b7d', ?)`,
			uid, lastSeen, lastSeen, lastSeen)
		require.NoError(t, err)
	}
	insert("a", 100)
	insert("b", 300)
	insert("c", 200)

	all, err := cdb.Events(ctx, 0) // 0 → default window
	require.NoError(t, err)
	require.Len(t, all, 3)
	// Newest last_seen first.
	require.Equal(t, []string{"b", "c", "a"}, []string{all[0].UID, all[1].UID, all[2].UID})
	// Fields flattened as expected.
	require.Equal(t, "Warning", all[0].Type)
	require.Equal(t, "BackOff", all[0].Reason)
	require.Equal(t, 3, all[0].Count)
	require.EqualValues(t, 300, all[0].LastSeen)
	require.Equal(t, "Pod", all[0].InvolvedKind)
	require.Equal(t, "default", all[0].InvolvedNS)
	require.Equal(t, "my-pod", all[0].InvolvedName)

	// A positive limit bounds the read to the newest N.
	top2, err := cdb.Events(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, []string{"b", "c"}, []string{top2[0].UID, top2[1].UID})
}

// The events broker is independent of the object-write broker: EventsNotify wakes only
// EventsSubscribe, and Notify wakes only Subscribe. This separation is what keeps an
// event burst from triggering the kind-catalog re-read.
func TestEventsBrokerIsSeparateFromWrites(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	writes, cancelW := cdb.ObjectsSubscribe()
	defer cancelW()
	events, cancelE := cdb.EventsSubscribe()
	defer cancelE()

	// EventsNotify pings only the events subscriber.
	cdb.EventsNotify()
	select {
	case <-events:
	case <-time.After(2 * time.Second):
		t.Fatal("EventsNotify must ping an events subscriber")
	}
	select {
	case <-writes:
		t.Fatal("EventsNotify must not ping the object-write subscriber")
	default:
	}

	// Notify pings only the object-write subscriber.
	cdb.ObjectsNotify()
	select {
	case <-writes:
	case <-time.After(2 * time.Second):
		t.Fatal("Notify must ping a write subscriber")
	}
	select {
	case <-events:
		t.Fatal("Notify must not ping the events subscriber")
	default:
	}
}

func TestShutdownClosesSubscribers(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)

	ch, cancel := cdb.ObjectsSubscribe()
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

func TestKinds(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	// Empty until discovery populates it.
	rows, err := cdb.Kinds(ctx)
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

	rows, err = cdb.Kinds(ctx)
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

// A shutdown that misses its deadline deliberately leaves the pools open — a janitor
// mid-write must not have its connection closed underneath it — so the handle is still
// live, and the close must both report that and stay retryable.
//
// DeleteCacheFiles is why: it closes before removing files, and its error keeps the cache's
// finalizer so the controller retries. If Close forgot the handle, the retry would find
// nothing to close, return nil, and os.Remove the .db out from under the live janitor and
// pools — dropping the close-before-delete invariant on exactly the retry meant to preserve
// it.
func TestCloseStaysRetryableWhenShutdownFails(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	ctx := context.Background()
	cdb, err := m.Open(ctx, ref(1, 1))
	require.NoError(t, err)

	// Withhold the janitor's stop signal, so the shutdown below can only end by deadline —
	// the path that deliberately leaves the pools open. Restored via Cleanup rather than
	// inline: a failed assertion below would otherwise leave the manager's own shutdown
	// waiting on a janitor that is never asked to stop.
	realCancel := cdb.janitorCancel
	cdb.janitorCancel = func() {}
	t.Cleanup(func() {
		cdb.janitorCancel = realCancel
		_ = m.Shutdown(ctx)
	})

	expired, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, m.Close(expired, 1), "a shutdown that could not close the pools must say so")

	// Not open — Open must not hand back a handle whose pools are on their way out — but
	// not gone either: the retry has to re-wait on the same one.
	require.Nil(t, m.Lookup(1))
	require.NoError(t, cdb.Writer().PingContext(ctx),
		"the pools are still live, which is why the files must not be removed yet")

	cdb.janitorCancel = realCancel
	require.NoError(t, m.Close(ctx, 1), "the retry closes it for real")

	// The retry must have closed THIS handle's pools, not silently no-opped on a forgotten
	// entry and left a live janitor behind for DeleteCacheFiles to delete files under.
	require.Error(t, cdb.Writer().PingContext(ctx), "the retry must close the same handle")

	reopened, err := m.Open(ctx, ref(1, 1))
	require.NoError(t, err, "a fully closed cache reopens")
	require.NotSame(t, cdb, reopened)
}

// Shutdown owns the process teardown, so it must not report itself done while a cache's
// pools may still be live. A per-cache Close mid-attempt is exactly that case: the handle
// has left the open set, and once Shutdown has taken the closing set nothing can reach it
// through the Manager again — so this is the last chance to account for it.
func TestShutdownWaitsForAnInFlightClose(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	ctx := context.Background()
	cdb, err := m.Open(ctx, ref(1, 1))
	require.NoError(t, err)

	// Hold the close inside its own shutdown: janitorCancel is called first, so blocking it
	// parks the attempt at a known point, with the entry already out of the open set and in
	// the closing one. The FIRST call (the racing Close) withholds the janitor's stop
	// signal so that attempt can only end by deadline — leaving the pools open, which is
	// what Shutdown then has to finish. The second call is Shutdown's own takeover.
	realCancel := cdb.janitorCancel
	entered := make(chan struct{})
	release := make(chan struct{})
	racing := true
	cdb.janitorCancel = func() {
		if racing {
			racing = false
			close(entered)
			<-release
			return
		}
		realCancel()
	}

	closeCtx, cancelClose := context.WithCancel(ctx)
	closeErr := make(chan error, 1)
	go func() { closeErr <- m.Close(closeCtx, 1) }()
	<-entered

	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- m.Shutdown(ctx) }()

	cancelClose() // the in-flight attempt fails, its pools still live
	close(release)

	require.Error(t, <-closeErr)
	require.NoError(t, <-shutdownErr)
	require.Error(t, cdb.Writer().PingContext(ctx),
		"shutdown must not return while a cache's pools are still open")
}

// The other half: a close that already FAILED left its handle in the closing set, live
// pools and all. Shutdown is the only remaining caller that can finish it — after Shutdown
// takes that set, nothing reaches the handle through the Manager again.
func TestShutdownFinishesAFailedClose(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	ctx := context.Background()
	cdb, err := m.Open(ctx, ref(1, 1))
	require.NoError(t, err)

	realCancel := cdb.janitorCancel
	cdb.janitorCancel = func() {}
	expired, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, m.Close(expired, 1), "the close must fail with its pools still open")
	require.NoError(t, cdb.Writer().PingContext(ctx))

	cdb.janitorCancel = realCancel
	require.NoError(t, m.Shutdown(ctx))
	require.Error(t, cdb.Writer().PingContext(ctx),
		"a handle no per-cache Close can reach any more must be closed by Shutdown")
}

// Deleting a cache is close-then-unlink, and Open CREATES a missing file — so a reconcile
// landing between the two would recreate the .db, register a handle, and have that file
// unlinked out from under it a moment later. Every worker rebuilt afterwards would then
// write into an inode with no name, silently, for the rest of the process.
func TestOpenRefusesACacheBeingDeleted(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	ctx := context.Background()
	_, err := m.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Shutdown(ctx) })

	// Observed from inside the sequence, after the handle is closed and before the unlink —
	// exactly where the reconcile used to slip in.
	var midDelete error
	require.NoError(t, m.deleteCacheFilesWithHook(ctx, ref(1, 1), func() {
		_, midDelete = m.Open(ctx, ref(1, 1))
	}))
	require.Error(t, midDelete, "a cache mid-deletion must not be openable")

	// The file really is gone, and the cache opens again once the deletion is over.
	_, exists := m.CacheBytes(ref(1, 1))
	require.False(t, exists)
	reopened, err := m.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	require.NotNil(t, reopened)
}

// Open must not hand out a handle that is being shut down. A reconcile opening the cache
// while a clear or a deletion closes it would otherwise take the doomed handle and have the
// pools close mid-query — and opening a SECOND pool over the same file is no better, since
// the deletion is about to remove the .db under it. The caller retries; by then the close
// has resolved one way or the other.
func TestOpenRefusesACacheThatIsClosing(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	ctx := context.Background()
	cdb, err := m.Open(ctx, ref(1, 1))
	require.NoError(t, err)

	realCancel := cdb.janitorCancel
	cdb.janitorCancel = func() {}
	t.Cleanup(func() {
		cdb.janitorCancel = realCancel
		_ = m.Shutdown(ctx)
	})

	// Leave the cache mid-close: the shutdown could not finish, so its pools are still live.
	expired, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, m.Close(expired, 1))

	got, err := m.Open(ctx, ref(1, 1))
	require.Error(t, err, "a closing cache is not open")
	require.Nil(t, got)

	// Once it is really closed, opening works again.
	cdb.janitorCancel = realCancel
	require.NoError(t, m.Close(ctx, 1))
	reopened, err := m.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	require.NotNil(t, reopened)
}

// Objects reads one kind's cached objects, filtered by (api_version, resource)
// and ordered by (namespace, name). The watch args are the plural resource, but
// the objects table is keyed by kind, so the reader must translate resource→kind
// through kind_catalog and never leak another kind's rows.
func TestObjects(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	// Empty until the sync engine has written objects.
	rows, err := cdb.Objects(ctx, "apps/v1", "deployments")
	require.NoError(t, err)
	require.Empty(t, rows)

	insertCatalog := func(apiVersion, kind, resource, scope string, isCRD int) {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
			 VALUES(?, ?, ?, ?, ?, NULL)`,
			apiVersion, kind, resource, scope, isCRD)
		require.NoError(t, err)
	}
	insertCatalog("apps/v1", "Deployment", "deployments", "Namespaced", 0)
	insertCatalog("v1", "Pod", "pods", "Namespaced", 0)

	// raw_json is stored zlib-compressed (the write path always compresses), so
	// Objects can decompress it back — seed compressed bodies, not raw bytes.
	body, err := CompressRaw([]byte(`{}`))
	require.NoError(t, err)
	insertObj := func(uid, apiVersion, kind, namespace, name string, createdAt int64) {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
			   created_at, updated_at, raw_json)
			 VALUES (?, ?, ?, ?, ?, '1', ?, ?, ?)`,
			uid, apiVersion, kind, namespace, name, createdAt, createdAt, body)
		require.NoError(t, err)
	}
	// Two Deployments (out of (namespace, name) order on purpose) + an unrelated Pod.
	insertObj("d2", "apps/v1", "Deployment", "kube-system", "coredns", 200)
	insertObj("d1", "apps/v1", "Deployment", "default", "web", 100)
	insertObj("p1", "v1", "Pod", "default", "web-abc", 300)

	rows, err = cdb.Objects(ctx, "apps/v1", "deployments")
	require.NoError(t, err)
	require.Len(t, rows, 2, "only the deployments, not the pod")

	// Ordered by (namespace, name): default/web before kube-system/coredns.
	require.Equal(t, "d1", rows[0].UID)
	require.Equal(t, "apps/v1", rows[0].APIVersion)
	require.Equal(t, "Deployment", rows[0].Kind)
	require.Equal(t, "default", rows[0].Namespace)
	require.Equal(t, "web", rows[0].Name)
	require.EqualValues(t, 100, rows[0].CreatedAt)

	require.Equal(t, "d2", rows[1].UID)
	require.Equal(t, "kube-system", rows[1].Namespace)
	require.Equal(t, "coredns", rows[1].Name)
}

// A plural resource names exactly one Kind within an api group-version, and the reader's
// resource→kind translation is an unconstrained scalar subquery — two matching rows and
// SQLite silently answers with an arbitrary one, so a fully-synced kind's table renders
// empty forever.
//
// The collision is reachable: a CRD whose Kind is renamed while the sidecar is down leaves
// the old catalog row behind, because the in-process cleanup that drops it needs the
// previous worker still running to know what it was. So the registering worker clears any
// row holding its plural under another Kind, and a unique index makes that the only
// possible state.
func TestEnsureKindCatalogReplacesARenamedKind(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	// The pre-rename registration, left behind by a previous process.
	require.NoError(t, EnsureKindCatalog(ctx, cdb, KindRow{
		APIVersion: "widgets.example.com/v1", Kind: "Widget",
		Resource: "widgets", Scope: "Namespaced", IsCRD: true,
	}))
	// This process's worker registers the same plural under the new Kind. "Gadget" sorts
	// before "Widget", so an index-first subquery would answer with it either way — the
	// assertion below only means something because the stale row is gone.
	require.NoError(t, EnsureKindCatalog(ctx, cdb, KindRow{
		APIVersion: "widgets.example.com/v1", Kind: "Gadget",
		Resource: "widgets", Scope: "Namespaced", IsCRD: true,
	}))

	var kinds int
	require.NoError(t, cdb.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM kind_catalog WHERE api_version = ? AND resource = ?`,
		"widgets.example.com/v1", "widgets").Scan(&kinds))
	require.Equal(t, 1, kinds, "one plural, one Kind")

	body, err := CompressRaw([]byte(`{}`))
	require.NoError(t, err)
	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
		   created_at, updated_at, raw_json)
		 VALUES ('g1', 'widgets.example.com/v1', 'Gadget', 'default', 'one', '1', 1, 1, ?)`, body)
	require.NoError(t, err)

	rows, err := cdb.Objects(ctx, "widgets.example.com/v1", "widgets")
	require.NoError(t, err)
	require.Len(t, rows, 1, "the plural must resolve to the Kind the live worker registered")
	require.Equal(t, "Gadget", rows[0].Kind)
}

// Objects returns each row's full native body, decompressed from the zlib-
// compressed raw_json column (the write path always compresses), so the caller
// gets the object JSON verbatim without re-reading the store.
func TestObjectsReadsBody(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()
	cdb, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
		 VALUES('apps/v1', 'Deployment', 'deployments', 'Namespaced', 0, NULL)`)
	require.NoError(t, err)

	body := []byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"web"},"spec":{"replicas":3}}`)
	compressed, err := CompressRaw(body)
	require.NoError(t, err)

	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
		   created_at, updated_at, raw_json)
		 VALUES ('d1', 'apps/v1', 'Deployment', 'default', 'web', '1', 100, 100, ?)`,
		compressed)
	require.NoError(t, err)

	rows, err := cdb.Objects(ctx, "apps/v1", "deployments")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.JSONEq(t, string(body), string(rows[0].RawJSON), "body decompressed and returned verbatim")
}

// The per-kind counts are maintained by triggers on the objects table (so
// Kinds reads them without scanning objects). This pins the two properties
// the trigger design relies on: a delete decrements the count, and an object
// written before its catalog row still counts — kind_counts is keyed only by
// (api_version, kind), independent of kind_catalog's discovery rewrite.
func TestKindCountsMaintainedByTriggers(t *testing.T) {
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
		rows, err := cdb.Kinds(ctx)
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
		rows, err := cdb.Kinds(ctx)
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

// Shutdown takes every handle out of dbs and closing BEFORE closing it, so afterwards a
// per-cache Close finds nothing registered — which is NOT the same as "nothing is open".
// Answering nil there told DeleteCacheFiles the cache was closed while Shutdown's own
// attempt was still tearing the pools down, and it went on to unlink the .db/-wal/-shm out
// from under a live janitor.
func TestCloseAfterShutdownReportsTheShutdown(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir)
	ctx := context.Background()

	_, err := r.Open(ctx, ref(1, 1))
	require.NoError(t, err)
	require.NoError(t, r.Shutdown(ctx))

	require.ErrorIs(t, r.Close(ctx, 1), ErrManagerShutDown,
		"a shut-down manager owns the handles; it must not answer 'nothing to close'")
	// An id that was never open answers the same way, for the same reason: the manager
	// can no longer tell.
	require.ErrorIs(t, r.Close(ctx, 99), ErrManagerShutDown)

	// Which is what keeps the file deletion honest — it closes first and gives up here.
	require.ErrorIs(t, r.DeleteCacheFiles(ctx, ref(1, 1)), ErrManagerShutDown)
	require.FileExists(t, clusterDBPath(dir, ref(1, 1)))
}

// A shutdown whose janitor has ALREADY stopped is a clean one, no matter how long the
// caller's deadline has been gone. Both select arms were ready in that case, so Go picked
// between them at random: half the time an expired context turned a completed teardown
// into a timeout error — which leaves the pools open and the handle stranded in the
// Manager's closing set, refusing every later Open for that cache.
func TestShutdownWithAStoppedJanitorIgnoresAnExpiredDeadline(t *testing.T) {
	dir := t.TempDir()
	expired, cancel := context.WithCancel(context.Background())
	cancel()

	// One run decides nothing when the outcome is a coin flip; a run of them does.
	for i := range 25 {
		m := NewManager(dir)
		cdb, err := m.Open(context.Background(), ref(1, int64(i+1)))
		require.NoError(t, err)

		// Stop the janitor and wait for it, so the only thing left for shutdown to do is
		// close the pools — the state where the deadline is irrelevant.
		cdb.janitorCancel()
		<-cdb.janitorDone

		require.NoError(t, cdb.shutdown(expired),
			"a teardown with nothing left to wait for must not report a timeout")
	}
}

// A close that misses its deadline leaves the handle registered as closing on purpose, and
// nothing else ever retried it: Close is reachable only through DeleteCacheFiles, and its
// caller (a cache clear) just surfaces the error. Every later Open was then refused for the
// life of the process, while the pools stayed live and every watcher had already been told
// the cache closed. Open now drives the retry — off its own goroutine, since the thing that
// stranded the handle is a janitor that would not stop.
func TestOpenDrivesTheRetryOfAFailedClose(t *testing.T) {
	ctx := context.Background()
	m := NewManager(t.TempDir())
	t.Cleanup(func() { _ = m.Shutdown(context.Background()) })

	cdb, err := m.Open(ctx, ref(1, 1))
	require.NoError(t, err)

	// Exactly what a failed close leaves behind: out of dbs, into closing, with no attempt
	// in flight (done nil) because the attempt ended — unsuccessfully.
	m.mu.Lock()
	delete(m.dbs, 1)
	m.closing[1] = &closingDB{cdb: cdb}
	m.mu.Unlock()

	// This Open still refuses — the handle's pools may be live, so it must — but it is what
	// sets the retry going.
	_, err = m.Open(ctx, ref(1, 1))
	require.Error(t, err, "a closing cache is not open")

	// The retry succeeds here (the janitor is long stopped), so the wedge clears and the
	// caller's next attempt — the reconcile requeue the refusal assumes — gets a handle.
	require.Eventually(t, func() bool {
		reopened, err := m.Open(ctx, ref(1, 1))
		return err == nil && reopened != nil
	}, 2*time.Second, 10*time.Millisecond, "nothing ever finished the failed close")
}
