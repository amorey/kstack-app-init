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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// row is the stand-in the loop is tested over, the way deltaWatch is tested once over its
// own: what the loop does is the same for all three watches, and testing it per watch would
// pin the projection three times and the protocol none.
type row struct {
	UID  string
	Data string
}

// frame is row's delta frame.
type frame struct {
	Type DeltaFrameType
	Row  *row
}

// watchFixture drives one loop: a real store manager, a hand-driven read, and the frames.
type watchFixture struct {
	t     *testing.T
	m     *kubestore.Manager
	store *kubestore.Store

	mu   sync.Mutex
	rows []row
	err  error
	// reads counts the read calls, so a test can pin a debounce collapsing a burst.
	reads int
}

func newWatchFixture(t *testing.T) *watchFixture {
	t.Helper()
	m := kubestore.NewManager(t.TempDir(), kubestore.Retention{})
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	return &watchFixture{t: t, m: m}
}

// open creates the cache's file, the way a worker or the sweep does.
func (f *watchFixture) open() {
	f.t.Helper()
	store, err := f.m.OpenOrCreate(1)
	require.NoError(f.t, err)
	f.store = store
}

// set replaces what the next read answers, and clears any failure standing over it.
func (f *watchFixture) set(rows ...row) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows, f.err = rows, nil
}

// fail makes the next reads fail until set is called again.
func (f *watchFixture) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *watchFixture) read(context.Context, *kubestore.Store) ([]row, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	if f.err != nil {
		return nil, f.err
	}
	return append([]row(nil), f.rows...), nil
}

func (f *watchFixture) readCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads
}

// ping is a write landing on the bus.
func (f *watchFixture) ping() {
	f.t.Helper()
	require.NoError(f.t, f.store.SyncKinds(context.Background(), nil, true, 7))
}

// start runs the loop with the test's own cadences.
func (f *watchFixture) start(ctx context.Context, debounce, retry time.Duration) *Stream[frame] {
	return runCachedDataWatch(ctx, cachedDataWatchSpec[row, frame]{
		stores:   f.m,
		cacheID:  1,
		debounce: debounce,
		retry:    retry,
		key:      func(r row) string { return r.UID },
		read:     f.read,
		frame: func(_ context.Context, _ *kubestore.Store, t DeltaFrameType, r row) frame {
			return frame{Type: t, Row: &r}
		},
		bookmark: frame{Type: DeltaFrameBookmark},
	})
}

// recvFrame takes the next frame, bounded by the failsafe.
func recvFrame(t *testing.T, s *Stream[frame]) frame {
	t.Helper()
	return testutil.Recv(t, s.Frames, "a watch frame")
}

// The snapshot is every row as Added, then the Bookmark that closes it — never an empty
// state before the Bookmark, which a client renders as "nothing here".
func TestCacheWatchEmitsTheSnapshotThenTheBookmark(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newWatchFixture(t)
	f.open()
	f.set(row{UID: "a", Data: "1"}, row{UID: "b", Data: "1"})

	s := f.start(ctx, time.Millisecond, time.Millisecond)

	assert.Equal(t, frame{Type: DeltaFrameAdded, Row: &row{UID: "a", Data: "1"}}, recvFrame(t, s))
	assert.Equal(t, frame{Type: DeltaFrameAdded, Row: &row{UID: "b", Data: "1"}}, recvFrame(t, s))
	assert.Equal(t, DeltaFrameBookmark, recvFrame(t, s).Type)
}

// The three ways a re-read differs from the snapshot before it. A departure carries its
// last-known row, since the client keys the removal by UID and the row is gone from disk.
func TestCacheWatchDiffsAgainstThePreviousSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newWatchFixture(t)
	f.open()
	f.set(row{UID: "a", Data: "1"}, row{UID: "gone", Data: "1"})

	s := f.start(ctx, time.Millisecond, time.Millisecond)
	recvFrame(t, s)
	recvFrame(t, s)
	require.Equal(t, DeltaFrameBookmark, recvFrame(t, s).Type)

	f.set(row{UID: "a", Data: "2"}, row{UID: "new", Data: "1"})
	f.ping()

	got := map[DeltaFrameType]row{}
	for range 3 {
		fr := recvFrame(t, s)
		got[fr.Type] = *fr.Row
	}
	assert.Equal(t, row{UID: "a", Data: "2"}, got[DeltaFrameModified])
	assert.Equal(t, row{UID: "new", Data: "1"}, got[DeltaFrameAdded])
	assert.Equal(t, row{UID: "gone", Data: "1"}, got[DeltaFrameDeleted], "the departure lost its body")
}

// A burst is one re-read. The bus coalesces only what is undelivered, so a loop that drains
// promptly would otherwise re-read the whole collection once per write.
func TestCacheWatchCollapsesABurstIntoOneReRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newWatchFixture(t)
	f.open()
	f.set(row{UID: "a", Data: "1"})

	s := f.start(ctx, 40*time.Millisecond, time.Millisecond)
	recvFrame(t, s)
	require.Equal(t, DeltaFrameBookmark, recvFrame(t, s).Type)
	require.Equal(t, 1, f.readCount())

	for range 5 {
		f.ping()
	}
	f.set(row{UID: "a", Data: "2"})

	assert.Equal(t, DeltaFrameModified, recvFrame(t, s).Type)
	assert.Equal(t, 2, f.readCount(), "the burst was not collapsed")
}

// A failed read is retried in place rather than ending the stream: the bus is keyed, so a
// kind nobody writes to may not ping for hours, and one transient error would otherwise
// leave the client's table empty until it did.
func TestCacheWatchRetriesAFailedReRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newWatchFixture(t)
	f.open()
	f.set(row{UID: "a", Data: "1"})

	s := f.start(ctx, time.Millisecond, 5*time.Millisecond)
	recvFrame(t, s)
	require.Equal(t, DeltaFrameBookmark, recvFrame(t, s).Type)

	f.fail(errors.New("disk I/O error"))
	f.ping()
	// The retry timer is what drives the recovery; nothing else pings.
	f.set(row{UID: "a", Data: "2"})

	assert.Equal(t, DeltaFrameModified, recvFrame(t, s).Type)
	assert.NoError(t, s.Err(), "a recovered read ended the stream")
}

// brokenStores is a registry whose cache has a file that will not open.
type brokenStores struct {
	m   *kubestore.Manager
	err error
}

func (b brokenStores) OpenExisting(int64) (*kubestore.Store, bool, error) { return nil, false, b.err }

func (b brokenStores) WatchOpen(cacheID int64) kubestore.OpenSubscription {
	return b.m.WatchOpen(cacheID)
}

// A store that will not open is a broken source, and the watch has to say so. Reading it as
// "no file yet" would leave the stream parked on WatchOpen — which only fires when a file is
// CREATED, so a cache whose file is already there would wait for a signal that never comes.
func TestCacheWatchReportsAStoreThatWillNotOpen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newWatchFixture(t)
	boom := errors.New("database disk image is malformed")

	s := runCachedDataWatch(ctx, cachedDataWatchSpec[row, frame]{
		stores:   brokenStores{m: f.m, err: boom},
		cacheID:  1,
		debounce: time.Millisecond,
		retry:    time.Millisecond,
		key:      func(r row) string { return r.UID },
		read:     f.read,
		frame: func(_ context.Context, _ *kubestore.Store, t DeltaFrameType, r row) frame {
			return frame{Type: t, Row: &r}
		},
		bookmark: frame{Type: DeltaFrameBookmark},
	})

	testutil.WaitClosed(t, s.Frames, "the stream to end")
	assert.ErrorIs(t, s.Err(), boom, "a broken store read as an empty one")
}

// The loop's claim is its own to give back: it holds the cache's file open while it reads,
// so a watch that ended without releasing would keep every cache anyone ever looked at open
// for the process's life.
func TestCacheWatchReleasesItsClaimWhenItEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := newWatchFixture(t)
	f.open()
	f.store.Release() // the workers stop; the loop becomes the only holder
	f.set(row{UID: "a", Data: "1"})

	s := f.start(ctx, time.Millisecond, time.Millisecond)
	recvFrame(t, s)
	require.Equal(t, DeltaFrameBookmark, recvFrame(t, s).Type)
	require.True(t, cacheIsOpen(f.m, 1), "the loop is not holding the file open")

	cancel()
	for range s.Frames { //nolint:revive // drain until the loop has exited
	}

	assert.False(t, cacheIsOpen(f.m, 1), "the loop kept its claim after ending")
}

// A cleared cache ends the watch CLEANLY. A non-nil Err is filed as a watch failure and
// reaches the client as an error toast — for a user pressing "clear cache", once per open
// watch. The reconnect re-snapshots against the fresh file, so nothing is lost by silence.
func TestCacheWatchEndsCleanlyWhenTheStoreIsCleared(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newWatchFixture(t)
	f.open()
	f.set(row{UID: "a", Data: "1"})

	s := f.start(ctx, time.Millisecond, time.Millisecond)
	recvFrame(t, s)
	require.Equal(t, DeltaFrameBookmark, recvFrame(t, s).Type)

	require.NoError(t, f.m.Clear(1))

	for fr := range s.Frames {
		assert.NotEqual(t, DeltaFrameDeleted, fr.Type, "the clear blanked the client's table")
	}
	assert.NoError(t, s.Err(), "a clear reported itself as a watch failure")
}

// A watch can open before anything has created the file. It must not stall: the Bookmark
// goes out on the empty snapshot, and the rows arrive as ordinary Added frames once a
// writer opens the cache.
func TestCacheWatchBookmarksBeforeTheStoreExists(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newWatchFixture(t)
	f.set(row{UID: "a", Data: "1"})

	s := f.start(ctx, time.Millisecond, time.Millisecond)
	assert.Equal(t, DeltaFrameBookmark, recvFrame(t, s).Type, "the watch stalled on a cache with no file")

	f.open()

	fr := recvFrame(t, s)
	assert.Equal(t, DeltaFrameAdded, fr.Type)
	assert.Equal(t, row{UID: "a", Data: "1"}, *fr.Row)
}
