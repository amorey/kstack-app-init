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
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

func TestOpenCreatesFreshStoreWithNoCookie(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	require.FileExists(t, filepath.Join(dir, "1.db"))

	_, ok, err := store.Cookie(context.Background(), "v1", "pods")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestTwoOpensShareOneStore(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	ha, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer ha.Release()

	hb, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer hb.Release()

	ctx := context.Background()
	require.NoError(t, ha.SetCookie(ctx, "v1", "pods", "42"))

	v, ok, err := hb.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "42", v)
}

func TestCookiePersistsAcrossReleaseAndReacquire(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	ctx := context.Background()

	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	require.NoError(t, store.SetCookie(ctx, "v1", "pods", "7"))
	store.Release()

	h2, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer h2.Release()

	v, ok, err := h2.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "7", v)
}

func TestClearWithNoHandlesDeletesFiles(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	require.NoError(t, store.SetCookie(context.Background(), "v1", "pods", "1"))
	store.Release()

	require.NoError(t, m.Clear(1))

	stats, err := m.Stats(context.Background(), 1)
	require.NoError(t, err)
	require.False(t, stats.Exists)
}

func TestClearOnNeverExistingCacheIsNotAnError(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	require.NoError(t, m.Clear(999))
}

func TestClearUnderLiveHandleReopensAndKeepsHandleWorking(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	ctx := context.Background()

	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	require.NoError(t, store.SetCookie(ctx, "v1", "pods", "1"))

	require.NoError(t, m.Clear(1))

	_, ok, err := store.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.False(t, ok, "cookie must be gone after Clear")

	// The same handle keeps working against the fresh store.
	require.NoError(t, store.SetCookie(ctx, "v1", "pods", "99"))
	v, ok, err := store.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "99", v)
}

func TestOpenExistingClaimsACacheThatHasAFile(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	ctx := context.Background()
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	require.NoError(t, store.SetCookie(ctx, "v1", "pods", "1"))
	store.Release()

	reopened, ok, err := m.OpenExisting(1)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, reopened.ClearKind(ctx, podKind))
	reopened.Release()

	h2, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer h2.Release()

	_, ok, err = h2.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestStatsReportsExistsAndBytesIncludingWalAndShm(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	require.NoError(t, store.SetCookie(context.Background(), "v1", "pods", "1"))

	stats, err := m.Stats(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, stats.Exists)
	require.Greater(t, stats.Bytes, int64(0))

	// Confirm Bytes accounts for the -wal/-shm sidecars when present, not just the main file.
	var mainOnly int64
	if fi, statErr := os.Stat(filepath.Join(dir, "1.db")); statErr == nil {
		mainOnly = fi.Size()
	}
	store.Release()
	require.GreaterOrEqual(t, stats.Bytes, mainOnly)
}

func TestManagerCloseClosesEveryOpenStoreWithoutPanicking(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)

	h1, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	h2, err := m.OpenOrCreate(2)
	require.NoError(t, err)
	_ = h1
	_ = h2

	require.NoError(t, m.Close())
}

// Remove deletes the files and, unlike Clear, leaves nothing open behind it: no
// empty store is reopened for a cache that is going away.
func TestRemoveDeletesTheFilesAndDropsTheOpenStore(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	require.NoError(t, store.SetCookie(context.Background(), "v1", "pods", "1"))

	require.NoError(t, m.Remove(1))

	stats, err := m.Stats(context.Background(), 1)
	require.NoError(t, err)
	assert.False(t, stats.Exists, "the cache's files survived Remove")
	assert.ErrorIs(t, store.SetCookie(context.Background(), "v1", "pods", "2"), ErrClosed,
		"a claim still out reached a file that is gone")

	// Releasing a claim whose file is gone is not an error.
	store.Release()
}

func TestRemoveOnNeverExistingCacheIsNotAnError(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	require.NoError(t, m.Remove(999))
}

// A deleted cache stays deleted: a straggler holding a view of it from before the
// teardown must not open a fresh file nothing will ever name again.
func TestOpenAfterRemoveIsRefused(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	store.Release()

	require.NoError(t, m.Remove(1))

	_, err = m.OpenOrCreate(1)
	assert.ErrorIs(t, err, ErrRemoved)
	// The file went with the cache, so there is nothing to claim — and finding that out
	// must not put one back.
	_, ok, err := m.OpenExisting(1)
	require.NoError(t, err)
	assert.False(t, ok)

	stats, err := m.Stats(context.Background(), 1)
	require.NoError(t, err)
	assert.False(t, stats.Exists, "the refused open recreated the file")

	// Another cache is unaffected.
	h2, err := m.OpenOrCreate(2)
	require.NoError(t, err)
	h2.Release()
}

// A Delete whose cleanup failed still retires the id: the caller retries, and an
// Acquire in the meantime would recreate the file the retry is there to remove.
func TestRemoveRetiresTheCacheEvenWhenCleanupFails(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	store.Release()

	// An unremovable sidecar: os.Remove refuses a non-empty directory.
	wal := filepath.Join(dir, "1.db-wal")
	require.NoError(t, os.MkdirAll(filepath.Join(wal, "blocker"), 0o700))

	require.Error(t, m.Remove(1))

	_, err = m.OpenOrCreate(1)
	assert.ErrorIs(t, err, ErrRemoved)

	// The retry, once the obstruction is gone, still finishes the job.
	require.NoError(t, os.RemoveAll(wal))
	require.NoError(t, m.Remove(1))
	stats, err := m.Stats(context.Background(), 1)
	require.NoError(t, err)
	assert.False(t, stats.Exists)
}

// A Clear whose files will not go still leaves the cache usable: the caller retries,
// and until then a handle must not resolve to the store Clear closed.
func TestClearKeepsTheCacheUsableWhenTheFilesWillNotGo(t *testing.T) {
	dir := t.TempDir()
	failDelete := errors.New("boom")
	blocked := true
	m := newManagerWithOptions(dir, withDeleteFiles(func(path string) error {
		if blocked {
			return failDelete
		}
		return deleteStoreFiles(path)
	}))
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	ctx := context.Background()
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()
	require.NoError(t, store.SetCookie(ctx, "v1", "pods", "1"))

	require.ErrorIs(t, m.Clear(1), failDelete)

	// The rows are still there — nothing was deleted — and the handle still writes.
	v, ok, err := store.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "1", v)
	require.NoError(t, store.SetCookie(ctx, "v1", "pods", "2"))

	// The retry, once the files can go, wipes the store as usual.
	blocked = false
	require.NoError(t, m.Clear(1))
	_, ok, err = store.Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	assert.False(t, ok)
}

// A retired cache's kind clear is an error, not a panic. Acquire refuses the id here;
// the nil-store guard behind it covers the interleaving Acquire cannot see, a Delete
// landing between the claim and the resolve.
func TestManagerClearKindOnARetiredCacheIsAnError(t *testing.T) {
	dir := t.TempDir()
	// Delete leaves the file, so ClearKind gets past the existence check and reaches
	// the store the delete retired.
	m := newManagerWithOptions(dir, withDeleteFiles(func(string) error { return nil }))
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	ctx := context.Background()
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	require.NoError(t, m.Remove(1))

	assert.ErrorIs(t, store.ClearKind(ctx, podKind), ErrClosed, "a claim on a retired entry")
	// The file is still on disk (this clear's unlink is stubbed out), so the refusal has
	// to come from the tombstone rather than from the file being absent.
	_, _, err = m.OpenExisting(1)
	assert.ErrorIs(t, err, ErrRemoved)
}

// A Clear that cannot reopen retires its entry, and the handles left on it are that
// entry's alone: they read no store, and releasing them must not count down — or
// close — the store a fresh claim opened at the same id.
func TestReleaseAfterAFailedClearLeavesAFreshClaimAlone(t *testing.T) {
	dir := t.TempDir()
	// The unlink also blocks the reopen: a store cannot be created in a directory
	// nothing may write.
	m := newManagerWithOptions(dir, withDeleteFiles(func(path string) error {
		if err := deleteStoreFiles(path); err != nil {
			return err
		}
		return os.Chmod(dir, 0o500)
	}))
	t.Cleanup(func() {
		require.NoError(t, os.Chmod(dir, 0o700))
		require.NoError(t, m.Close())
	})

	ctx := context.Background()
	first, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	second, err := m.OpenOrCreate(1)
	require.NoError(t, err)

	require.Error(t, m.Clear(1), "the reopen must have failed for this test to mean anything")
	assert.ErrorIs(t, first.SetCookie(ctx, "v1", "pods", "0"), ErrClosed, "a claim on the retired entry")

	require.NoError(t, os.Chmod(dir, 0o700))
	fresh, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer fresh.Release()

	first.Release()
	second.Release()

	require.NotNil(t, fresh, "the fresh claim's store was dropped by a stale release")
	require.NoError(t, fresh.SetCookie(ctx, "v1", "pods", "1"))

	// The fresh entry is still the one the id resolves to, so a later claim joins that
	// store rather than opening a second one over the same file.
	other, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer other.Release()
	assert.Same(t, fresh.e, other.e, "a second file was opened beside the live one")
}

// A cache that was never opened has no file to claim, and asking must not create one —
// schema, sidecars and all.
func TestOpenExistingCreatesNothingForACacheWithNoFile(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	store, ok, err := m.OpenExisting(1)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, store)

	stats, err := m.Stats(context.Background(), 1)
	require.NoError(t, err)
	assert.False(t, stats.Exists, "the open created the cache it found nothing of")

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "the clear left files behind")
}

// The gauge borrows the feed of whatever is open, and borrowing must never create a file:
// that is what would resurrect a cache whose teardown deleted it.
func TestManagerSubscribeAnswersNothingForAnUnopenedCache(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	_, ok := m.Subscribe(1)
	assert.False(t, ok)
	assert.NoFileExists(t, filepath.Join(dir, "1.db"))
}

// The feed comes with no claim, so a borrower cannot keep the cache alive: the writer's
// release still closes the file.
func TestManagerSubscribeTakesNoClaim(t *testing.T) {
	m := NewManager(t.TempDir())
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)

	sub, ok := m.Subscribe(1)
	require.True(t, ok)
	defer sub.Close()

	store.Release()

	assert.False(t, cacheIsOpen(m, 1), "the borrowed feed held the file open")
}

// cacheIsOpen asks whether anything holds cacheID's file open, which is what Subscribe's
// ok reports. The receiver is closed straight away: this is a question, not a watch.
func cacheIsOpen(m *Manager, cacheID int64) bool {
	sub, ok := m.Subscribe(cacheID)
	if ok {
		sub.Close()
	}
	return ok
}

// The counts answer for a cache nobody is syncing too: the workers are stopped while a
// cluster is paused, and a gauge reporting no objects beside a megabyte-sized file
// would read as data loss. Reading it opens the file read-only, so a cache that does
// not exist stays not existing.
func TestCountsReadsAClosedCacheWithoutOpeningItForWrites(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	require.NoError(t, store.ApplyChange(ctx, Kind{APIVersion: "v1", Kind: "Pod", Resource: "pods"},
		watch.Added, obj(map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"uid": "uid-1", "name": "api-0", "resourceVersion": "1"},
		})))
	store.Release()
	require.False(t, cacheIsOpen(m, 1), "the store stayed open")

	got, err := m.Stats(ctx, 1)

	require.NoError(t, err)
	assert.Equal(t, Counts{ObjectCount: 1, KindCount: 1}, got.Counts)
}

func TestCountsOfANeverExistingCacheIsEmpty(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	got, err := m.Stats(context.Background(), 1)

	require.NoError(t, err)
	assert.Equal(t, Stats{}, got)
	assert.NoFileExists(t, filepath.Join(dir, "1.db"))
}

// A clear closes the file whatever is holding it, so a read that resolved the file a
// moment earlier finds it closed mid-query. The measurement must not fail for that —
// the gauge behind it is meant to survive a clear — and the answer is on disk either
// way.
func TestStatsSurvivesTheFileClosingUnderIt(t *testing.T) {
	ctx := context.Background()
	m := NewManager(t.TempDir())
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()
	require.NoError(t, store.ApplyChange(ctx, Kind{APIVersion: "v1", Kind: "Pod", Resource: "pods"},
		watch.Added, obj(map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"uid": "uid-1", "name": "api-0", "resourceVersion": "1"},
		})))

	// The state a clear passes through: the entry still holds a file, and that file is
	// closed. What a racing reader sees between resolving one and querying it.
	require.True(t, cacheIsOpen(m, 1))
	require.NoError(t, m.entries[1].file.close())

	got, err := m.Stats(ctx, 1)

	require.NoError(t, err)
	assert.True(t, got.Exists)
	assert.Equal(t, 1, got.ObjectCount, "the read gave up instead of falling back to the file")
}

// A close that fails leaves the file unusable whatever it reports, so the entry has to
// be retired: a claim on it must answer ErrClosed rather than reach a dead database, and
// the next open starts the cache fresh.
func TestClearRetiresTheEntryWhenTheFileWillNotClose(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("close failed")
	m := newManagerWithOptions(t.TempDir(), withCloseFile(func(*file) error { return boom }))
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	require.ErrorIs(t, m.Clear(1), boom)

	assert.ErrorIs(t, store.SetCookie(ctx, "v1", "pods", "1"), ErrClosed,
		"a claim reached the file the clear could not close")
	_, err = store.Subscribe()
	assert.ErrorIs(t, err, ErrClosed, "a subscription was handed back on a dead bus")
	assert.False(t, cacheIsOpen(m, 1), "the retired entry is still bound")
}

// A clear closes, unlinks and re-creates the file, and the fresh one has no schema until
// its migrations run — all of it under the manager's lock. A measurement must wait that
// out rather than read what is there mid-way: the gauge above it is subscribed by the
// very view holding the Clear button, and a failed read ends that subscription.
func TestStatsWaitsOutAClear(t *testing.T) {
	ctx := context.Background()
	clearing, release := make(chan struct{}), make(chan struct{})
	m := newManagerWithOptions(t.TempDir(), withDeleteFiles(func(path string) error {
		close(clearing)
		<-release
		return deleteStoreFiles(path)
	}))
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	// A file on disk with nothing open over it, which is the path that reads from disk.
	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	store.Release()

	cleared := make(chan struct{})
	go func() { defer close(cleared); assert.NoError(t, m.Clear(1)) }()
	testutil.Wait(t, clearing, "the clear to take the lock")
	// Released whatever the assertions do, or a failure would leave the clear holding
	// the lock and deadlock the manager's own close.
	letClearFinish := sync.OnceFunc(func() { close(release) })
	defer func() { letClearFinish(); <-cleared }()

	measured := make(chan error, 1)
	go func() {
		_, err := m.Stats(ctx, 1)
		measured <- err
	}()

	// A negative assertion needs a bounded window: the measurement must be waiting on the
	// clear, not reading through it.
	testutil.NoRecv(t, measured, 50*time.Millisecond, "a measurement taken mid-clear")
	letClearFinish()

	assert.NoError(t, testutil.Recv(t, measured, "the measurement once the clear landed"))
}

// A Clear closes the file and installs a fresh empty one on the same entry. A claim taken
// before that must not follow the swap: a re-read of the new file would answer "no rows" for
// a cache that was full, and the watch would emit a Deleted for every row it held.
func TestAClaimTakenBeforeAClearDoesNotFollowTheSwap(t *testing.T) {
	ctx := context.Background()
	m := NewManager(t.TempDir())
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	writer, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer writer.Release()
	require.NoError(t, writer.SyncKinds(ctx, []KindRow{podRow}, true))

	bound, ok, err := m.OpenExisting(1)
	require.NoError(t, err)
	require.True(t, ok)
	defer bound.Release()
	kinds, err := bound.Kinds(ctx)
	require.NoError(t, err)
	require.Len(t, kinds, 1)

	require.NoError(t, m.Clear(1))

	_, err = bound.Kinds(ctx)
	assert.ErrorIs(t, err, ErrClosed, "the bound store read the file the clear swapped in")
}

// A fresh claim after the clear is how a reconnecting watch reaches the new file.
func TestAFreshClaimAfterAClearReadsTheNewFile(t *testing.T) {
	ctx := context.Background()
	m := NewManager(t.TempDir())
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	writer, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer writer.Release()
	require.NoError(t, writer.SyncKinds(ctx, []KindRow{podRow}, true))
	require.NoError(t, m.Clear(1))

	bound, ok, err := m.OpenExisting(1)
	require.NoError(t, err)
	require.True(t, ok)
	defer bound.Release()
	kinds, err := bound.Kinds(ctx)
	require.NoError(t, err)
	assert.Empty(t, kinds, "the clear emptied the file")
}

// An idle cache — paused, or freshly restarted — has no claim on it and so no open file,
// but its rows are still on disk. A read that only bound to an already-open file would
// report it empty.
func TestOpenExistingReadsACacheNobodyHoldsOpen(t *testing.T) {
	ctx := context.Background()
	m := NewManager(t.TempDir())
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	writer, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	require.NoError(t, writer.SyncKinds(ctx, []KindRow{podRow}, true))
	writer.Release()
	require.False(t, cacheIsOpen(m, 1), "the last release left the file open")

	store, ok, err := m.OpenExisting(1)
	require.NoError(t, err)
	require.True(t, ok)
	defer store.Release()

	kinds, err := store.Kinds(ctx)
	require.NoError(t, err)
	assert.Len(t, kinds, 1)
}

// A watch can open before anything has created the cache's file, and must go live the moment
// one does rather than waiting out a poll.
func TestWatchOpenFiresWhenTheStoreOpens(t *testing.T) {
	m := NewManager(t.TempDir())
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	opened := m.WatchOpen(1)
	defer opened.Close()

	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	testutil.Recv(t, opened.Chan(), "the open signal")
}

// The signal is for a store that was not there yet; one already open needs no wait, and
// OpenExisting is what the caller tries first.
func TestWatchOpenDoesNotFireForAnAlreadyOpenStore(t *testing.T) {
	m := NewManager(t.TempDir())
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	store, err := m.OpenOrCreate(1)
	require.NoError(t, err)
	defer store.Release()

	opened := m.WatchOpen(1)
	defer opened.Close()

	// A negative assertion needs a bounded window; the signal would already be pending.
	if _, err := opened.TryRecv(); err == nil {
		t.Fatal("the open signal fired for a store that was already open")
	}
}
