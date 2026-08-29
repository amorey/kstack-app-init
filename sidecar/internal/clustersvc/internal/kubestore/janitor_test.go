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
