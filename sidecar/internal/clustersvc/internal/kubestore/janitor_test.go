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
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// addStatusRow appends one status transition at a given age, standing in for the write an
// object's status change makes.
func addStatusRow(t *testing.T, s *Store, uid string, age time.Duration) {
	t.Helper()
	_, err := db(t, s).ExecContext(context.Background(),
		`INSERT INTO status_history(uid, at, summary) VALUES (?, ?, 'Running')`,
		uid, time.Now().Add(-age).UnixMilli())
	require.NoError(t, err)
}

// status_history is the one table the store owns outright rather than mirroring from the
// server, so nothing upstream ages it out and the janitor is the only bound it has.
func TestSweepTrimsStatusHistoryPastItsTTL(t *testing.T) {
	store := newTestStore(t)
	addStatusRow(t, store, "uid-old", 48*time.Hour)
	addStatusRow(t, store, "uid-fresh", time.Minute)

	sweep(context.Background(), "1", openFileOf(t, store), Retention{StatusHistoryTTL: time.Hour})

	assert.Equal(t, 0, countRows(t, store, `SELECT COUNT(*) FROM status_history WHERE uid = 'uid-old'`))
	assert.Equal(t, 1, countRows(t, store, `SELECT COUNT(*) FROM status_history WHERE uid = 'uid-fresh'`))
}

// freelist is how many pages the file holds free — the number the vacuum hands back, and
// the only honest gate on whether there is anything to do.
func freelist(t *testing.T, s *Store) int64 {
	t.Helper()
	var pages int64
	require.NoError(t, db(t, s).QueryRowContext(context.Background(), `PRAGMA freelist_count`).Scan(&pages))
	return pages
}

// fillStatusHistory writes enough expired rows to span many pages, so freeing them leaves a
// freelist worth measuring.
func fillStatusHistory(t *testing.T, s *Store, rows int) {
	t.Helper()
	tx, err := db(t, s).BeginTx(context.Background(), nil)
	require.NoError(t, err)
	at := time.Now().Add(-48 * time.Hour).UnixMilli()
	for i := range rows {
		_, err := tx.ExecContext(context.Background(),
			`INSERT INTO status_history(uid, at, summary) VALUES (?, ?, ?)`,
			"uid", at, strings.Repeat("x", 200)+strconv.Itoa(i))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}

// auto_vacuum=INCREMENTAL on its own reclaims nothing: freed pages join the freelist and the
// file sits at its high-water mark until something runs the vacuum. Nothing else does, so the
// size gauge would report the worst the cache has ever been rather than what it holds.
func TestSweepHandsFreePagesBack(t *testing.T) {
	store := newTestStore(t)
	fillStatusHistory(t, store, 5000)
	require.Zero(t, freelist(t, store), "a file that has freed nothing yet")

	sweep(context.Background(), "1", openFileOf(t, store), Retention{StatusHistoryTTL: time.Hour})

	assert.Zero(t, freelist(t, store), "the pages the delete freed went back to the OS")
}

// A cache has one writer, so an unbounded vacuum blocks every kind's sync — and the freelist
// is biggest exactly when that hurts most, right after a relist. A backlog drains over the
// following sweeps instead.
func TestSweepBoundsWhatOneVacuumHandsBack(t *testing.T) {
	store := newTestStore(t)
	fillStatusHistory(t, store, 5000)
	shrinkVacuumBound(t, 8)

	sweep(context.Background(), "1", openFileOf(t, store), Retention{StatusHistoryTTL: time.Hour})
	before := freelist(t, store)
	require.NotZero(t, before, "a bounded sweep leaves a backlog")

	sweep(context.Background(), "1", openFileOf(t, store), Retention{StatusHistoryTTL: time.Hour})

	assert.Equal(t, before-8, freelist(t, store), "the backlog drains at the same bound")
}

// shrinkVacuumBound paces the vacuum by parameter for the test's life, so no assertion here
// encodes the production page count.
func shrinkVacuumBound(t *testing.T, pages int64) {
	t.Helper()
	prev := vacuumPagesPerSweep
	vacuumPagesPerSweep = pages
	t.Cleanup(func() { vacuumPagesPerSweep = prev })
}

// Nothing else runs a sweep, so a file that opens with a retention gets its own janitor —
// and it sweeps first thing inside its goroutine rather than inline, since a cache should
// not wait a full interval for its first pass and a vacuum must never run under m.mu.
func TestAJanitorSweepsTheFileItWasOpenedFor(t *testing.T) {
	m := NewManager(t.TempDir(), Retention{StatusHistoryTTL: time.Hour, Interval: time.Millisecond})
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	t.Cleanup(store.Release)

	fillStatusHistory(t, store, 5000)

	require.Eventually(t, func() bool { return sweptClean(t, store) },
		5*time.Second, time.Millisecond, "the janitor swept without anybody asking it")
}

// openFile has two call sites and the second is the one that gets missed: Clear reopens a
// fresh file mid-clear, for the claims still held. Starting the janitor at the open rather
// than in openFile silently leaves a cleared cache without one — the cache most likely to
// need it, since a clear is what frees the pages.
func TestAClearedCacheKeepsItsJanitor(t *testing.T) {
	m := NewManager(t.TempDir(), Retention{StatusHistoryTTL: time.Hour, Interval: time.Millisecond})
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	t.Cleanup(store.Release)

	require.NoError(t, m.Clear(1))
	fillStatusHistory(t, store, 5000)

	require.Eventually(t, func() bool { return sweptClean(t, store) },
		5*time.Second, time.Millisecond, "the file a clear swapped in sweeps like any other")
}

// sweptClean reports that a sweep has run over this file: the expired rows are the evidence,
// since the freelist of a file that has freed nothing is legitimately zero.
func sweptClean(t *testing.T, s *Store) bool {
	t.Helper()
	return countRows(t, s, `SELECT COUNT(*) FROM status_history`) == 0
}

// The janitor keeps sweeping: the first pass runs at open, and the interval is what makes
// the second one happen. Waiting for the first to finish before dirtying the file is what
// separates them — a fill racing the opening sweep proves only that one ran.
func TestAJanitorKeepsSweepingOnItsInterval(t *testing.T) {
	m := NewManager(t.TempDir(), Retention{StatusHistoryTTL: time.Hour, Interval: time.Millisecond})
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	t.Cleanup(store.Release)
	fillStatusHistory(t, store, 100)
	require.Eventually(t, func() bool { return sweptClean(t, store) },
		5*time.Second, time.Millisecond, "the opening sweep")

	fillStatusHistory(t, store, 100)

	require.Eventually(t, func() bool { return sweptClean(t, store) },
		5*time.Second, time.Millisecond, "a second sweep, which only the ticker starts")
}

// A vacuum that will not run is logged and the sweep returns — the janitor's next
// interval tries again. Failing louder would take down the only thing that hands pages
// back, over a condition that is usually momentary.
func TestASweepSurvivesAVacuumItCannotRun(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m := NewManager(dir, Retention{})
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	t.Cleanup(store.Release)
	// Rows written and deleted, so the freelist the vacuum walks is not empty.
	fillStatusHistory(t, store, 2000)
	_, err = db(t, store).ExecContext(ctx, `DELETE FROM status_history`)
	require.NoError(t, err)

	// A read-only pool over the same file: the freelist still reads, and the vacuum that
	// would hand its pages back cannot write.
	f := openFileOf(t, store)
	readOnly, err := openReadOnly(filepath.Join(dir, "1.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, readOnly.Close()) })
	f.db = readOnly

	assert.NotPanics(t, func() { sweep(ctx, "1", f, Retention{StatusHistoryTTL: time.Hour}) })
}

// addDeleteEntry logs one delete at a given age, standing in for the entry a delete left.
func addDeleteEntry(t *testing.T, s *Store, kind, uid string, seq int64, age time.Duration) {
	t.Helper()
	_, err := db(t, s).ExecContext(context.Background(),
		`INSERT INTO deletes(seq, api_version, kind, uid, at) VALUES (?, 'v1', ?, ?, ?)`,
		seq, kind, uid, time.Now().Add(-age).UnixMilli())
	require.NoError(t, err)
}

// trimmedMark is how far a kind's log has been trimmed, as a reader reads it.
func trimmedMark(t *testing.T, s *Store, kind string) int64 {
	t.Helper()
	mark, err := trimmed(context.Background(), openFileOf(t, s).stmts(), "v1", kind)
	require.NoError(t, err)
	return mark
}

// A reader's cursor goes stale by time, not by count, so the log is bounded by age. What
// it trims it must also announce: a cursor at or below the mark can no longer be trusted
// to have seen every delete above it.
func TestSweepTrimsTheDeletesLogPastItsTTL(t *testing.T) {
	store := newTestStore(t)
	addDeleteEntry(t, store, "Pod", "uid-old", 7, 48*time.Hour)
	addDeleteEntry(t, store, "Pod", "uid-older", 4, 49*time.Hour)
	addDeleteEntry(t, store, "Pod", "uid-fresh", 9, time.Minute)

	sweep(context.Background(), "1", openFileOf(t, store), Retention{DeletesTTL: time.Hour})

	assert.Equal(t, 1, countRows(t, store, `SELECT COUNT(*) FROM deletes`), "only the fresh entry stays")
	assert.Equal(t, int64(7), trimmedMark(t, store, "Pod"), "the highest position removed")
}

// Cursors are per kind, so the marks are too: a global one would have a busy kind's
// deletes push every quiet kind's cursor into the diff within minutes.
func TestSweepMarksEachKindSeparately(t *testing.T) {
	store := newTestStore(t)
	addDeleteEntry(t, store, "Pod", "uid-1", 3, 48*time.Hour)
	addDeleteEntry(t, store, "ConfigMap", "uid-2", 8, 48*time.Hour)
	addDeleteEntry(t, store, "Secret", "uid-3", 9, time.Minute)

	sweep(context.Background(), "1", openFileOf(t, store), Retention{DeletesTTL: time.Hour})

	assert.Equal(t, int64(3), trimmedMark(t, store, "Pod"))
	assert.Equal(t, int64(8), trimmedMark(t, store, "ConfigMap"))
	assert.Zero(t, trimmedMark(t, store, "Secret"), "a kind with nothing removed has no mark")
}

// Retention's fields are independent, so a manager given one TTL and not the other must
// leave the unset table alone. A cutoff of now trims the whole log and raises every kind's
// mark to the head of it, invalidating every reader's cursor in one sweep.
func TestAZeroTTLTrimsNothing(t *testing.T) {
	store := newTestStore(t)
	addDeleteEntry(t, store, "Pod", "uid-1", 5, 48*time.Hour)
	addStatusRow(t, store, "uid-1", 48*time.Hour)

	sweep(context.Background(), "1", openFileOf(t, store), Retention{})

	assert.Equal(t, 1, countRows(t, store, `SELECT COUNT(*) FROM deletes`))
	assert.Zero(t, trimmedMark(t, store, "Pod"), "nothing went, so nothing is marked")
	assert.Equal(t, 1, countRows(t, store, `SELECT COUNT(*) FROM status_history`))
}

// A mark only ever goes up: a later sweep that takes nothing from a kind must not lower
// what an earlier one recorded, or a cursor below it would read as valid again.
func TestASweepThatRemovesNothingLeavesTheMarkAlone(t *testing.T) {
	store := newTestStore(t)
	addDeleteEntry(t, store, "Pod", "uid-1", 5, 48*time.Hour)
	sweep(context.Background(), "1", openFileOf(t, store), Retention{DeletesTTL: time.Hour})

	sweep(context.Background(), "1", openFileOf(t, store), Retention{DeletesTTL: time.Hour})

	assert.Equal(t, int64(5), trimmedMark(t, store, "Pod"))
}

// `at` and `seq` rise together only within one file lifetime: the clock stamp is forced
// upward in memory and starts again from the wall clock on reopen, while the counter is on
// disk and does not. So a burst that ran the stamps ahead of the clock leaves entries whose
// age and position disagree, and a later sweep can take a LOWER position than an earlier
// one. The mark must not follow it down — a cursor between the two marks would read as
// valid over deletes that are already gone.
func TestASweepNeverLowersAKindsMark(t *testing.T) {
	store := newTestStore(t)
	addDeleteEntry(t, store, "Pod", "uid-high", 200, 90*time.Minute)
	addDeleteEntry(t, store, "Pod", "uid-low", 100, 30*time.Minute)

	sweep(context.Background(), "1", openFileOf(t, store), Retention{DeletesTTL: time.Hour})
	require.Equal(t, int64(200), trimmedMark(t, store, "Pod"))
	sweep(context.Background(), "1", openFileOf(t, store), Retention{DeletesTTL: 10 * time.Minute})

	assert.Equal(t, int64(200), trimmedMark(t, store, "Pod"), "the older entry's lower position")
	assert.Zero(t, countRows(t, store, `SELECT COUNT(*) FROM deletes`), "both entries went")
}

// A failed trim is logged and the sweep carries on to the vacuum: the next sweep retries,
// and a cache whose log will not trim must still hand its free pages back.
func TestSweepSurvivesATrimItCannotMake(t *testing.T) {
	store := newTestStore(t)
	swapTable(t, store, "deletes",
		`SELECT 1 AS seq, 'v1' AS api_version, 'Pod' AS kind, 'uid-1' AS uid, 0 AS at`)

	assert.NotPanics(t, func() {
		sweep(context.Background(), "1", openFileOf(t, store), Retention{DeletesTTL: time.Hour})
	})
}
