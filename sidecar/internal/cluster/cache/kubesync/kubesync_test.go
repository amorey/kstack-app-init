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

package kubesync

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// These tests drive the worker end to end against a fake upstream and a fake store, so
// the list/watch state machine is exercised on its own terms — what it lists, when it
// re-lists, what the resume cookie holds, and which liveness verdict it reaches. The SQL
// each concrete store writes is that store's own business, tested beside it (see
// eventsync/store_test.go and objectsync/store_test.go).

const testTimeout = 5 * time.Second

// fakeStore is an in-memory kubesync.Store: a uid-keyed set plus the resume cookie, with
// the same prune-on-replace semantics a real store has.
type fakeStore struct {
	mu   sync.Mutex
	rows map[string]struct{}
	rv   string
	// block, when non-nil, holds ApplyChange until it is closed — for tests about what the
	// driver may do while a delta has NOT landed.
	block chan struct{}
	// failApply, when non-nil, decides whether a given delta fails to land — for tests
	// about a write the store rejects.
	failApply func(*unstructured.Unstructured) error
}

// blockApply makes every later ApplyChange park until the store is unblocked.
func (f *fakeStore) blockApply() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.block = make(chan struct{})
}

func newFakeStore() *fakeStore {
	return &fakeStore{rows: make(map[string]struct{})}
}

func (f *fakeStore) EnsureCatalog(context.Context) error { return nil }

func (f *fakeStore) ResumeRV(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rv, nil
}

func (f *fakeStore) Count(context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows), nil
}

// ApplyChange lands the delta AND advances the cookie, as the Store contract requires —
// a real store does both in one transaction.
func (f *fakeStore) ApplyChange(ctx context.Context, t watch.EventType, u *unstructured.Unstructured) error {
	f.mu.Lock()
	block, fail := f.block, f.failApply
	f.mu.Unlock()
	if fail != nil {
		if err := fail(u); err != nil {
			return err
		}
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	switch t {
	case watch.Added, watch.Modified:
		f.rows[string(u.GetUID())] = struct{}{}
	case watch.Deleted:
		delete(f.rows, string(u.GetUID()))
	}
	if rv := u.GetResourceVersion(); rv != "" {
		f.rv = rv
	}
	return nil
}

func (f *fakeStore) PersistRV(_ context.Context, rv string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rv = rv
	return nil
}

func (f *fakeStore) BeginReplace() ReplaceSession {
	return &fakeReplace{f: f, keep: make(map[string]struct{})}
}

// uids returns the store's current contents.
func (f *fakeStore) uids() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.rows))
	for uid := range f.rows {
		out = append(out, uid)
	}
	return out
}

// resumeRV reads the cookie without a context, for assertions.
func (f *fakeStore) resumeRV() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rv
}

// fakeReplace mirrors a real session: pages accumulate, and Commit reconciles the store
// to their union — so a test can assert the prune actually drops what the server dropped.
type fakeReplace struct {
	f    *fakeStore
	keep map[string]struct{}
}

func (r *fakeReplace) WritePage(_ context.Context, items []*unstructured.Unstructured) error {
	r.f.mu.Lock()
	defer r.f.mu.Unlock()
	// The first page durably clears the cookie, exactly as a real session does.
	r.f.rv = ""
	for _, u := range items {
		uid := string(u.GetUID())
		r.keep[uid] = struct{}{}
		r.f.rows[uid] = struct{}{}
	}
	return nil
}

func (r *fakeReplace) Commit(_ context.Context, rv string) (int, error) {
	r.f.mu.Lock()
	defer r.f.mu.Unlock()
	pruned := 0
	for uid := range r.f.rows {
		if _, ok := r.keep[uid]; !ok {
			delete(r.f.rows, uid)
			pruned++
		}
	}
	r.f.rv = rv
	return pruned, nil
}

// newTestItem builds a minimal object body. resourceVersion matters: RetryWatcher tracks
// its position from each delta's RV, so an item without one stops the watch.
func newTestItem(uid, name, rv string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Event",
		"metadata": map[string]any{
			"uid":             uid,
			"name":            name,
			"namespace":       "default",
			"resourceVersion": rv,
		},
	}}
}

// fakeSource is a scriptable Source. listFn/watchFn are swapped per test; both are
// counted so a test can assert a LIST was (or was not) issued.
type fakeSource struct {
	mu      sync.Mutex
	listFn  func(opts metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error)
	watchFn func(opts metav1.ListOptions) (watch.Interface, error)
	// metaFn/getFn back the diff resync. Both nil by default, which makes the metadata
	// list fail — so a source that doesn't opt in behaves like a server with no metadata
	// endpoint and the driver falls back to a full LIST, exactly as in production.
	metaFn    func(opts metav1.ListOptions) ([]ObjectMeta, string, string, error)
	getFn     func(namespace, name string) (*unstructured.Unstructured, error)
	listCalls int
	metaCalls int
	getCalls  int
}

func (f *fakeSource) List(_ context.Context, opts metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error) {
	f.mu.Lock()
	f.listCalls++
	fn := f.listFn
	f.mu.Unlock()
	return fn(opts)
}

func (f *fakeSource) Watch(_ context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	f.mu.Lock()
	fn := f.watchFn
	f.mu.Unlock()
	return fn(opts)
}

func (f *fakeSource) ListMetadata(_ context.Context, opts metav1.ListOptions) ([]ObjectMeta, string, string, error) {
	f.mu.Lock()
	f.metaCalls++
	fn := f.metaFn
	f.mu.Unlock()
	if fn == nil {
		return nil, "", "", errors.New("fakeSource: no metadata endpoint")
	}
	return fn(opts)
}

func (f *fakeSource) Get(_ context.Context, namespace, name string) (*unstructured.Unstructured, error) {
	f.mu.Lock()
	f.getCalls++
	fn := f.getFn
	f.mu.Unlock()
	if fn == nil {
		return nil, errors.New("fakeSource: no get")
	}
	return fn(namespace, name)
}

func (f *fakeSource) metas() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.metaCalls
}

func (f *fakeSource) gets() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getCalls
}

func (f *fakeSource) lists() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCalls
}

// staticList returns the same single page on every LIST.
func staticList(rv string, items ...*unstructured.Unstructured) func(metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error) {
	return func(metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error) {
		return items, "", rv, nil
	}
}

// recordingSink captures every report so a test can wait for the one it cares about.
type recordingSink struct {
	mu   sync.Mutex
	all  []Status
	sigC chan struct{}
	// beforeReport, when set, runs at the top of Report — a seam for a test that needs a
	// report to still be in flight while it does something else.
	beforeReport func(Status)
}

func newRecordingSink() *recordingSink {
	return &recordingSink{sigC: make(chan struct{}, 64)}
}

func (s *recordingSink) Report(st Status) {
	if s.beforeReport != nil {
		s.beforeReport(st)
	}
	s.mu.Lock()
	s.all = append(s.all, st)
	s.mu.Unlock()
	select {
	case s.sigC <- struct{}{}:
	default:
	}
}

// states returns the states of every recorded report, in arrival order.
func (s *recordingSink) states() []State {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]State, 0, len(s.all))
	for _, st := range s.all {
		out = append(out, st.State)
	}
	return out
}

// await blocks until a recorded report satisfies pred, returning it. It re-scans the whole
// history on each signal, so a report that landed before await was called still counts.
func (s *recordingSink) await(t *testing.T, what string, pred func(Status) bool) Status {
	t.Helper()
	deadline := time.After(testTimeout)
	for {
		s.mu.Lock()
		for _, st := range s.all {
			if pred(st) {
				s.mu.Unlock()
				return st
			}
		}
		s.mu.Unlock()
		select {
		case <-s.sigC:
		case <-deadline:
			s.mu.Lock()
			defer s.mu.Unlock()
			t.Fatalf("timed out waiting for %s; saw %+v", what, s.all)
		}
	}
}

// awaitN blocks until at least n reports have been recorded, returning a copy of them.
// Used to let the monitor tick a few times before asserting about what did NOT happen —
// a predicate can't wait for the absence of a state.
func (s *recordingSink) awaitN(t *testing.T, n int) []Status {
	t.Helper()
	deadline := time.After(testTimeout)
	for {
		s.mu.Lock()
		if len(s.all) >= n {
			got := append([]Status(nil), s.all...)
			s.mu.Unlock()
			return got
		}
		s.mu.Unlock()
		select {
		case <-s.sigC:
		case <-deadline:
			s.mu.Lock()
			defer s.mu.Unlock()
			t.Fatalf("timed out waiting for %d reports; saw %+v", n, s.all)
		}
	}
}

func isState(want State) func(Status) bool {
	return func(st Status) bool { return st.State == want }
}

// startWorker builds and starts a worker over src + st, stopping it at cleanup.
func startWorker(t *testing.T, src Source, st Store, opts ...Option) *recordingSink {
	t.Helper()
	sink := newRecordingSink()
	w, err := New(src, st, sink, "seed", opts...)
	require.NoError(t, err)
	w.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		_ = w.Stop(ctx)
	})
	return sink
}

// TestWorkerColdStartListsThenWatches covers the cold path end to end: no resume cookie,
// so the worker full-LISTs into the cache, persists the cookie, reports the catch-up, and
// then applies watch deltas on top.
func TestWorkerColdStartListsThenWatches(t *testing.T) {
	fs := newFakeStore()
	fw := watch.NewRaceFreeFake()
	src := &fakeSource{
		listFn:  staticList("100", newTestItem("uid-1", "e1", "10"), newTestItem("uid-2", "e2", "11")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return fw, nil },
	}

	sink := startWorker(t, src, fs)

	start := sink.await(t, "opening Syncing report", isState(StateSyncing))
	assert.True(t, start.ColdStart, "no resume cookie means a cold build")

	caught := sink.await(t, "catch-up", isState(StateWatching))
	assert.True(t, caught.ColdStart)
	assert.Equal(t, 2, caught.SyncedItems)
	assert.NotNil(t, caught.LastLiveAt, "a live watch must stamp liveness")

	assert.ElementsMatch(t, []string{"uid-1", "uid-2"}, fs.uids())

	assert.Equal(t, "100", fs.resumeRV(), "a completed LIST persists the list's rv as the resume cookie")

	// A delta on top of the snapshot lands, and advances the cookie.
	fw.Add(newTestItem("uid-3", "e3", "101"))
	require.Eventually(t, func() bool { return len(fs.uids()) == 3 }, testTimeout, 10*time.Millisecond,
		"a watch Added must reach the store")
	require.Eventually(t, func() bool { return fs.resumeRV() == "101" }, testTimeout, 10*time.Millisecond,
		"an applied delta must advance the resume cookie")

	// And a Deleted removes its row.
	fw.Delete(newTestItem("uid-1", "e1", "102"))
	require.Eventually(t, func() bool { return len(fs.uids()) == 2 }, testTimeout, 10*time.Millisecond,
		"a watch Deleted must remove the row")
}

// TestWorkerExpiredWatchRelistsAndPrunes covers the correctness backstop: a 410 ends the
// watch, the worker re-LISTs, and the re-list drops rows the server no longer has — so the
// cache mirrors the cluster rather than accumulating events the api server has already GC'd.
func TestWorkerExpiredWatchRelistsAndPrunes(t *testing.T) {
	fs := newFakeStore()

	var phase int
	fws := make(chan *watch.RaceFreeFakeWatcher, 4)
	src := &fakeSource{
		listFn: func(metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error) {
			phase++
			if phase == 1 {
				return []*unstructured.Unstructured{
					newTestItem("uid-1", "e1", "10"),
					newTestItem("uid-2", "e2", "11"),
				}, "", "100", nil
			}
			// The server has since aged uid-2 out.
			return []*unstructured.Unstructured{newTestItem("uid-1", "e1", "10")}, "", "200", nil
		},
		watchFn: func(metav1.ListOptions) (watch.Interface, error) {
			fw := watch.NewRaceFreeFake()
			fws <- fw
			return fw, nil
		},
	}

	startWorker(t, src, fs, withDriverOptions(withSleep(func(context.Context, time.Duration) error { return nil })))

	require.Eventually(t, func() bool { return len(fs.uids()) == 2 }, testTimeout, 10*time.Millisecond,
		"the cold LIST must land first")

	// Expire the watch: RetryWatcher forwards the Status and closes, which the driver reads
	// as errExpired and answers with a fresh LIST.
	fw := <-fws
	fw.Error(&metav1.Status{Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired})

	require.Eventually(t, func() bool {
		return assert.ObjectsAreEqual([]string{"uid-1"}, fs.uids())
	}, testTimeout, 10*time.Millisecond, "the re-list must prune the item the server no longer serves")
	assert.GreaterOrEqual(t, src.lists(), 2)
}

// TestWorkerWarmResumeSkipsList verifies the whole point of the resume cookie: a worker
// that starts with one resumes its watch directly instead of re-listing every item.
func TestWorkerWarmResumeSkipsList(t *testing.T) {
	ctx := context.Background()
	fs := newFakeStore()

	// Seed a warm cache: one row plus the cookie a completed LIST would have left.
	require.NoError(t, fs.ApplyChange(ctx, watch.Added, newTestItem("uid-1", "e1", "10")))
	require.NoError(t, fs.PersistRV(ctx, "42"))

	fw := watch.NewRaceFreeFake()
	src := &fakeSource{
		listFn: func(metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error) {
			return nil, "", "", errors.New("warm resume must not LIST")
		},
		watchFn: func(opts metav1.ListOptions) (watch.Interface, error) {
			assert.Equal(t, "42", opts.ResourceVersion, "the watch must resume from the stored cookie")
			return fw, nil
		},
	}

	// The resume grace is what turns "connected, no 410" into a reported clean resume;
	// fire it immediately so the test doesn't wait out the real 2s.
	sink := startWorker(t, src, fs, withDriverOptions(withGraceTimer(firedTimer)))

	start := sink.await(t, "opening Syncing report", isState(StateSyncing))
	assert.False(t, start.ColdStart, "a stored cookie means a warm resume")
	assert.Equal(t, 1, start.CachedItems, "the warm report describes the cache it resumes from")

	caught := sink.await(t, "catch-up", isState(StateWatching))
	assert.False(t, caught.ColdStart)
	assert.False(t, caught.Resynced, "a clean resume re-lists nothing")
	assert.Zero(t, src.lists(), "a warm resume must not LIST")
}

// TestWorkerColdListIsPaginated verifies the LIST streams page by page (bounding memory to
// one page) rather than pulling every item at once.
func TestWorkerColdListIsPaginated(t *testing.T) {
	fs := newFakeStore()
	fw := watch.NewRaceFreeFake()
	src := &fakeSource{
		listFn: func(opts metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error) {
			assert.Equal(t, int64(listPageSize), opts.Limit, "every page must be bounded")
			switch opts.Continue {
			case "":
				return []*unstructured.Unstructured{newTestItem("uid-1", "e1", "10")}, "tok", "100", nil
			case "tok":
				return []*unstructured.Unstructured{newTestItem("uid-2", "e2", "11")}, "", "100", nil
			default:
				return nil, "", "", fmt.Errorf("unexpected continue %q", opts.Continue)
			}
		},
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return fw, nil },
	}

	sink := startWorker(t, src, fs)
	caught := sink.await(t, "catch-up", isState(StateWatching))
	assert.Equal(t, 2, caught.SyncedItems, "both pages count toward the catch-up")
	assert.ElementsMatch(t, []string{"uid-1", "uid-2"}, fs.uids())
}

// TestWorkerPeriodicResyncRelists covers the pull-based backstop, the only thing that
// reconciles drift a healthy watch silently missed (a dropped delta, a stale cookie):
// a watch alive past the resync period must end itself and re-list, with no error
// anywhere to prompt it.
func TestWorkerPeriodicResyncRelists(t *testing.T) {
	fs := newFakeStore()
	src := &fakeSource{
		listFn:  staticList("100", newTestItem("uid-1", "e1", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return watch.NewRaceFreeFake(), nil },
	}

	startWorker(t, src, fs, withDriverOptions(
		withResyncPeriod(20*time.Millisecond),
		withSleep(func(context.Context, time.Duration) error { return nil }),
	))

	require.Eventually(t, func() bool { return src.lists() >= 3 }, testTimeout, 10*time.Millisecond,
		"a watch that stays alive past the resync period must re-list anyway")
}

// firedTimer is an already-fired timer seam, so a test needn't wait out a real duration.
func firedTimer(time.Duration) (<-chan time.Time, func()) {
	ch := make(chan time.Time, 1)
	ch <- time.Now()
	return ch, func() {}
}

// TestWorkerStuckWatchReportsStale covers the list-but-not-watch case (RBAC granting list
// without watch): the LIST keeps working, so nothing errors outright, but the cache can
// never be current — the error budget must surface that rather than leaving the worker
// looking healthy or wedged in Syncing forever.
func TestWorkerStuckWatchReportsStale(t *testing.T) {
	fs := newFakeStore()
	src := &fakeSource{
		listFn:  staticList("100", newTestItem("uid-1", "e1", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return nil, errors.New("watch is forbidden") },
	}

	sink := startWorker(t, src, fs, withDriverOptions(
		// The watch never connects, so the establishment timeout is the only thing that
		// ends each phase; fire it at once and spend the budget in two rounds.
		withEstablishTimer(firedTimer),
		withStuckThreshold(2),
		withSleep(func(context.Context, time.Duration) error { return nil }),
	))

	stale := sink.await(t, "stale report", isState(StateStale))
	assert.Equal(t, CauseWatchFailed, stale.Cause,
		"a cluster that can LIST but not WATCH must be reported as a watch failure, not a list failure")
}

// A worker that goes stuck BEFORE its first catch-up must keep being reported. The
// error-budget callback fires once, on the exact threshold crossing, so if the monitor also
// stays silent (a not-yet-caught-up worker is normally just working) that single report is
// the only word ever said about a permanently broken kind — and the controller only logs a
// failure to apply it, so one transient error would strand the child on Syncing forever.
func TestWorkerStuckBeforeCatchUpKeepsReporting(t *testing.T) {
	fs := newFakeStore()
	src := &fakeSource{
		listFn: func(metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error) {
			return nil, "", "", errors.New("list is forbidden")
		},
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return watch.NewRaceFreeFake(), nil },
	}

	sink := startWorker(t, src, fs,
		withMonitorInterval(10*time.Millisecond),
		withDriverOptions(
			withStuckThreshold(2),
			withSleep(func(context.Context, time.Duration) error { return nil }),
		),
	)

	// The threshold crossing's own report, then the monitor's re-reports of it.
	sink.await(t, "the error-budget report", isState(StateStale))
	var stale int
	for _, st := range sink.awaitN(t, 6) {
		if st.State == StateStale {
			stale++
		}
	}
	assert.Greater(t, stale, 1,
		"a stuck worker must be re-reported by the monitor, not left on its one budget report")
}

// TestWorkerStuckListReportsStale is the same for a cluster that can't LIST at all — the
// cause must name the phase that actually failed so the message stays actionable.
func TestWorkerStuckListReportsStale(t *testing.T) {
	fs := newFakeStore()
	src := &fakeSource{
		listFn: func(metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error) {
			return nil, "", "", errors.New("list is forbidden")
		},
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return watch.NewRaceFreeFake(), nil },
	}

	sink := startWorker(t, src, fs, withDriverOptions(
		withStuckThreshold(2),
		withSleep(func(context.Context, time.Duration) error { return nil }),
	))

	stale := sink.await(t, "stale report", isState(StateStale))
	assert.Equal(t, CauseListFailed, stale.Cause)
}

// TestWorkerStalledWatchReportsStale covers the silently-wedged watch: it connected,
// streamed (a bookmark — proof the stream was genuinely live), and then went quiet.
// Nothing errors — the only evidence is the absence of any further delta or bookmark — so
// the liveness monitor is what surfaces it.
//
// The bookmark is what makes this a STALL rather than the never-streamed case: it clears
// the no-proof episode, so the cause is the plain "was live, went quiet" reading instead
// of WatchNotStreaming.
func TestWorkerStalledWatchReportsStale(t *testing.T) {
	fs := newFakeStore()
	fw := watch.NewRaceFreeFake()
	src := &fakeSource{
		listFn:  staticList("100", newTestItem("uid-1", "e1", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return fw, nil },
	}

	// A clock frozen far past the staleness threshold: the watch's last liveness stamp is
	// immediately ancient, so the first monitor tick after it must call it stale.
	frozen := time.Now()
	sink := startWorker(t, src, fs,
		withMonitorInterval(10*time.Millisecond),
		withStaleThreshold(time.Millisecond),
		withClock(func() time.Time { return frozen.Add(time.Hour) }),
		withDriverOptions(withNow(func() time.Time { return frozen })),
	)

	sink.await(t, "catch-up", isState(StateWatching))
	fw.Action(watch.Bookmark, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"resourceVersion": "150"},
	}})

	stale := sink.await(t, "stale report", func(st Status) bool {
		return st.State == StateStale && st.Cause == CauseWatchStalled
	})
	assert.Zero(t, stale.CaughtUpIn, "a monitor report caught nothing up")
}

// TestWorkerReconnectLoopReportsNotStreaming pins the blind spot the graded liveness
// stamps exist to close: a watch that ACCEPTS the connection and then immediately errors.
//
// RetryWatcher retries a 500 internally, so the phase never ends and the error budget is
// never even reached — the stamps are the only thing that can catch this. Each retry
// (re)connects, so if a bare connect could refresh freshness the worker would report
// Watching forever while the cache received nothing after the initial LIST.
// Only the strong proofs (write / bookmark / completed LIST) count for freshness, and the
// stale clock runs from the FIRST connect of the no-proof episode — which a reconnect
// can't push forward — so the churn ages into Stale and names itself.
func TestWorkerReconnectLoopReportsNotStreaming(t *testing.T) {
	fs := newFakeStore()

	// Each watch opens (proving "established") and then errors with a non-410, which
	// RetryWatcher treats as terminal — so the phase ends without progress and Run
	// re-lists and re-watches, forever.
	watches := 0
	src := &fakeSource{
		listFn: staticList("100", newTestItem("uid-1", "e1", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) {
			watches++
			fw := watch.NewRaceFreeFake()
			fw.Error(&metav1.Status{Status: metav1.StatusFailure, Code: 500, Reason: metav1.StatusReasonInternalError})
			return fw, nil
		},
	}

	// Freeze the driver's clock and run the monitor's ahead of it, so every stamp is
	// instantly older than the threshold. The LIST's own proof is subject to the same
	// frozen clock, so it can't mask the churn either.
	frozen := time.Now()
	sink := startWorker(t, src, fs,
		withMonitorInterval(10*time.Millisecond),
		withStaleThreshold(time.Millisecond),
		withClock(func() time.Time { return frozen.Add(time.Hour) }),
		withDriverOptions(
			withNow(func() time.Time { return frozen }),
			withSleep(func(context.Context, time.Duration) error { return nil }),
		),
	)

	sink.await(t, "catch-up", isState(StateWatching))
	stale := sink.await(t, "stale report", isState(StateStale))
	assert.Equal(t, CauseWatchNotStreaming, stale.Cause,
		"a watch that keeps re-establishing without streaming must be named as such, not left looking healthy")
}

// TestWorkerOpenThenErrorWatchSpendsTheErrorBudget pins the OTHER half of the reconnect
// loop above: not just what it reports, but that it stops hammering the API server.
//
// Every cycle establishes, so crediting the bare open would refill the error budget on
// every pass — the sync could never become stuck, so it would never drop to the slow
// stuck cadence, and would instead re-LIST the whole collection every backoff step (a
// paginated LIST plus a ListLimiter slot, every 30s, for as long as the process ran). The
// budget therefore tracks strong proofs, not opens: this watch carries nothing, so the
// budget runs out and the retry cadence falls to stuckRetryInterval.
func TestWorkerOpenThenErrorWatchSpendsTheErrorBudget(t *testing.T) {
	fs := newFakeStore()
	// The watch is ACCEPTED — so the phase counts as established — and then hands back an
	// object carrying no resourceVersion. RetryWatcher can't resume past that, so it gives
	// up and closes its channel: the phase ends having connected, applied nothing, and
	// proven nothing. Run then re-lists and tries again, forever.
	src := &fakeSource{
		listFn: staticList("100", newTestItem("uid-1", "e1", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) {
			fw := watch.NewRaceFreeFake()
			fw.Add(newTestItem("uid-2", "e2", ""))
			return fw, nil
		},
	}

	// The retry cadence is what the budget buys, so observe it directly: every backoff
	// sleep the driver asks for, with the real wait skipped so the test doesn't pay it.
	var mu sync.Mutex
	var slept []time.Duration
	sleptC := make(chan struct{}, 64)

	sink := startWorker(t, src, fs, withDriverOptions(
		withStuckThreshold(3),
		withSleep(func(_ context.Context, d time.Duration) error {
			mu.Lock()
			slept = append(slept, d)
			mu.Unlock()
			select {
			case sleptC <- struct{}{}:
			default:
			}
			return nil
		}),
	))

	// The monitor's default interval is far longer than this test runs, so a Stale report
	// can only have come from the budget being spent.
	stale := sink.await(t, "stale report", isState(StateStale))
	assert.Equal(t, CauseWatchNotStreaming, stale.Cause,
		"the budget was spent by a watch that opened and streamed nothing, which is what the user should be told")

	deadline := time.After(testTimeout)
	for {
		mu.Lock()
		dropped := slices.Contains(slept, defaultStuckRetryInterval)
		seen := slices.Clone(slept)
		mu.Unlock()
		if dropped {
			return
		}
		select {
		case <-sleptC:
		case <-deadline:
			t.Fatalf("watch never dropped to the stuck retry cadence; it kept re-listing on backoff sleeps %v", seen)
		}
	}
}

// TestWorkerBookmarklessQuietWatchStaysHealthy covers the collection the api server serves
// without its watch cache — Events. It never sends bookmarks, so on a cluster producing
// less than one event per staleness window the watch delivers NO proof at all, and the
// no-proof reading below (WatchNotStreaming, since the connection is up and has streamed
// nothing) would fire on every idle cluster. WithoutBookmarks is what keeps an idle
// cluster's badge from going amber with "Possibly stale — Events".
//
// The setup is TestWorkerReconnectLoopReportsNotStreaming's minus the reconnect churn: one
// healthy watch that simply never streams, with the monitor's clock an hour past the
// driver's so every tick is deep past the threshold.
func TestWorkerBookmarklessQuietWatchStaysHealthy(t *testing.T) {
	fs := newFakeStore()
	fw := watch.NewRaceFreeFake()
	src := &fakeSource{
		listFn:  staticList("100", newTestItem("uid-1", "e1", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return fw, nil },
	}

	frozen := time.Now()
	sink := startWorker(t, src, fs,
		WithoutBookmarks(),
		withMonitorInterval(10*time.Millisecond),
		withStaleThreshold(time.Millisecond),
		withClock(func() time.Time { return frozen.Add(time.Hour) }),
		withDriverOptions(withNow(func() time.Time { return frozen })),
	)

	sink.await(t, "catch-up", isState(StateWatching))
	// Opening Syncing + catch-up + several monitor heartbeats. Waiting on a count rather
	// than on a heartbeat predicate because the frozen driver clock makes the catch-up
	// report's own CaughtUpIn zero, so a heartbeat is not distinguishable by its fields.
	for _, st := range sink.awaitN(t, 5) {
		assert.NotEqual(t, StateStale, st.State,
			"a bookmarkless collection proves nothing by being quiet; silence must not read as stale")
	}
}

// The exemption covers the no-proof reading ONLY. A bookmarkless kind that is genuinely
// broken has real evidence — a spent error budget — and must still surface.
func TestWorkerBookmarklessStuckWatchStillReportsStale(t *testing.T) {
	fs := newFakeStore()
	src := &fakeSource{
		listFn:  staticList("100", newTestItem("uid-1", "e1", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return nil, errors.New("watch is forbidden") },
	}

	sink := startWorker(t, src, fs, WithoutBookmarks(), withDriverOptions(
		withEstablishTimer(firedTimer),
		withStuckThreshold(2),
		withSleep(func(context.Context, time.Duration) error { return nil }),
	))

	stale := sink.await(t, "stale report", isState(StateStale))
	assert.Equal(t, CauseWatchFailed, stale.Cause)
}

// A bookmark advances the resume cookie only once every delta the tap forwarded before it
// has been applied — that ordering is what stops a restart skipping an unapplied change.
// The accounting has to be symmetrical, though: the apply loop discards any event that is
// not an unstructured object, so counting one as forwarded left the applied count
// permanently behind and every later bookmark declined to advance. The cookie then froze
// for the rest of the watch phase (up to the 30m re-list), and the next resume was a
// stale-RV 410 and a cold re-LIST of the whole collection.
func TestWorkerBookmarkAdvancesPastAnUnappliableDelta(t *testing.T) {
	fs := newFakeStore()
	fw := watch.NewRaceFreeFake()
	src := &fakeSource{
		listFn:  staticList("100", newTestItem("uid-1", "e1", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return fw, nil },
	}

	sink := startWorker(t, src, fs)
	sink.await(t, "catch-up", isState(StateWatching))

	// A delta the store can never land: not an *unstructured.Unstructured.
	fw.Action(watch.Added, &metav1.Status{Status: metav1.StatusSuccess})

	fw.Action(watch.Bookmark, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"resourceVersion": "150"},
	}})

	require.Eventually(t, func() bool { return fs.resumeRV() == "150" }, 2*time.Second, 10*time.Millisecond,
		"a delta nothing could apply must not freeze the resume cookie")
}

// The ordering itself still holds: a bookmark must NOT advance past a delta that was
// forwarded and has not been applied, or a restart would resume beyond a change it never
// wrote.
func TestWorkerBookmarkWaitsForAnUnappliedDelta(t *testing.T) {
	fs := newFakeStore()
	fs.blockApply()
	fw := watch.NewRaceFreeFake()
	src := &fakeSource{
		listFn:  staticList("100", newTestItem("uid-1", "e1", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return fw, nil },
	}

	sink := startWorker(t, src, fs)
	sink.await(t, "catch-up", isState(StateWatching))
	before := fs.resumeRV()

	fw.Action(watch.Added, newTestItem("uid-2", "e2", "120"))
	fw.Action(watch.Bookmark, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"resourceVersion": "150"},
	}})

	require.Never(t, func() bool { return fs.resumeRV() == "150" }, 200*time.Millisecond, 20*time.Millisecond,
		"the cookie must not pass a delta that has not landed")
	require.Equal(t, before, fs.resumeRV())
}

// TestWorkerQuietClusterStaysHealthy is the counterpart, and the reason freshness can't
// simply be "when did we last write a row": a cluster where nothing happens delivers no
// events at all, yet the api server's periodic bookmarks keep proving the stream is live.
// The worker must stay Watching, and must report the two stamps apart — a live LastLiveAt
// beside a LastUpdateAt that stopped at the initial LIST.
func TestWorkerQuietClusterStaysHealthy(t *testing.T) {
	fs := newFakeStore()
	fw := watch.NewRaceFreeFake()
	src := &fakeSource{
		listFn:  staticList("100", newTestItem("uid-1", "e1", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return fw, nil },
	}

	// The driver's clock advances only when the test says so, while the monitor's clock
	// sits well past the threshold — so a tick calls the worker stale unless a bookmark
	// has moved the stamp forward to "now".
	start := time.Now()
	driverNow := start
	var nowMu sync.Mutex
	setDriverNow := func(t time.Time) { nowMu.Lock(); driverNow = t; nowMu.Unlock() }
	getDriverNow := func() time.Time { nowMu.Lock(); defer nowMu.Unlock(); return driverNow }

	monitorNow := start.Add(time.Hour)
	sink := startWorker(t, src, fs,
		withMonitorInterval(10*time.Millisecond),
		withStaleThreshold(time.Minute),
		withClock(func() time.Time { return monitorNow }),
		withDriverOptions(withNow(getDriverNow)),
	)
	sink.await(t, "catch-up", isState(StateWatching))

	// A bookmark at the monitor's "now": the only proof this cluster ever produces.
	setDriverNow(monitorNow)
	fw.Action(watch.Bookmark, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"resourceVersion": "150"},
	}})

	// A monitor tick after the bookmark must still read Watching — the report we want is
	// one carrying the bookmark's stamp, which only a post-bookmark tick has.
	live := sink.await(t, "a heartbeat carrying the bookmark's stamp", func(st Status) bool {
		return st.LastLiveAt != nil && !st.LastLiveAt.Before(monitorNow)
	})
	assert.Equal(t, StateWatching, live.State,
		"bookmarks are proof of life: a quiet cluster must not be called stale")
	require.NotNil(t, live.LastUpdateAt)
	assert.True(t, live.LastUpdateAt.Before(monitorNow),
		"the bookmark proves liveness but delivered no data, so the update stamp must not move")
}

// TestWorkerStopDrainsBeforeReturning pins the barrier the cache-file teardown depends on:
// once Stop returns, the worker's goroutines are done, so nothing can still be writing when
// the .db file is deleted.
func TestWorkerStopDrainsBeforeReturning(t *testing.T) {
	fs := newFakeStore()
	fw := watch.NewRaceFreeFake()
	src := &fakeSource{
		listFn:  staticList("100", newTestItem("uid-1", "e1", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return fw, nil },
	}

	sink := newRecordingSink()
	w, err := New(src, fs, sink, "seed")
	require.NoError(t, err)
	w.Start()
	sink.await(t, "catch-up", isState(StateWatching))

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	require.NoError(t, w.Stop(ctx))

	select {
	case <-w.done:
	default:
		t.Fatal("Stop returned while the run goroutine was still live")
	}
	// Stopping twice is a no-op, not a panic — the deletion path may retry.
	require.NoError(t, w.Stop(ctx))
}

// A worker is handed to its owner BEFORE it is started — that ordering is what decides
// which of two racing builders wins the registration — so a Stop can genuinely arrive
// first. It must LATCH: a Start after it does nothing.
//
// Without the latch, stopping an unstarted worker returned nil immediately (cancel was
// nil), its owner dropped the entry, and the Start that followed launched a goroutine
// nobody held a handle to — untracked at shutdown, all its reports discarded, still writing
// into a cache file whose teardown believed every writer had drained.
func TestWorkerStopBeforeStartPreventsTheRun(t *testing.T) {
	st := newFakeStore()
	src := &fakeSource{
		listFn:  staticList("42", newTestItem("uid-a", "a", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return watch.NewFake(), nil },
	}
	sink := newRecordingSink()
	w, err := New(src, st, sink, "seed")
	require.NoError(t, err)

	require.NoError(t, w.Stop(context.Background()))
	w.Start()

	// The run goroutine performs a LIST first, so no list means it never ran.
	time.Sleep(50 * time.Millisecond)
	assert.Zero(t, src.lists(), "a stopped worker must not start")
	sink.mu.Lock()
	got := len(sink.all)
	sink.mu.Unlock()
	assert.Zero(t, got, "a stopped worker must report nothing")

	// And a second Stop stays clean rather than blocking on a done channel that was
	// never created.
	require.NoError(t, w.Stop(context.Background()))
}

// The ordinary path still works, and Stop is idempotent.
func TestWorkerStopIsIdempotent(t *testing.T) {
	st := newFakeStore()
	src := &fakeSource{
		listFn:  staticList("42", newTestItem("uid-a", "a", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return watch.NewFake(), nil },
	}
	w, err := New(src, st, newRecordingSink(), "seed")
	require.NoError(t, err)

	w.Start()
	require.NoError(t, w.Stop(context.Background()))
	require.NoError(t, w.Stop(context.Background()))
}

// Run normally returns only on cancellation, but it can also return a seam failure while
// ctx is alive. The monitor watches ctx, so waiting on it after such a return would hang
// run forever: done would never close, Stop would block to its deadline, and the drain
// finalizer would stay set — wedging the cache file's deletion barrier behind a worker that
// had already given up.
func TestWorkerUnwindsWhenRunFailsWithoutCancellation(t *testing.T) {
	st := newFakeStore()
	src := &fakeSource{
		listFn: func(metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error) {
			return nil, "", "", errors.New("list refused")
		},
	}
	sink := newRecordingSink()

	// A failed LIST backs off through sleep; failing the sleep is what makes Run return
	// with ctx still live.
	w, err := New(src, st, sink, "seed", withDriverOptions(withSleep(func(context.Context, time.Duration) error {
		return errors.New("sleep seam failed")
	})))
	require.NoError(t, err)

	w.Start()

	// Run gives up and the failure is reported — which only happens if run got past
	// wg.Wait() on the still-ticking monitor.
	sink.await(t, "the failure is reported", func(s Status) bool { return s.State == StateErrored })

	// And Stop must not need its deadline: the run goroutine has already unwound.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, w.Stop(ctx), "a worker whose Run returned must drain immediately")
}

// Reports are built by two goroutines — the driver's Run (catch-up, stuck) and the
// monitor (heartbeat, stale, recovery) — and the controller folds them last-writer-wins.
// So the order they are DELIVERED in has to match the order they were BUILT in. It did
// not: each built its Status under mu and released it before calling Report, leaving a
// window where a monitor tick that computed Stale could be descheduled, a recovery
// reported Watching behind it, and the stale verdict then land on top — sticking the kind
// on Stale, with a bogus SyncStale event in the log, until the next monitor tick.
func TestWorkerReportsArriveInTheOrderTheyWereBuilt(t *testing.T) {
	sink := newRecordingSink()
	w, err := New(&fakeSource{}, newFakeStore(), sink, "seed")
	require.NoError(t, err)

	var (
		mu    sync.Mutex
		trace []string
	)
	note := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		trace = append(trace, s)
	}

	// The first report is in flight — built, and inside the sink's write — for as long as
	// this holds. A real Report writes the object through the controller, so blocking here
	// is only a slow one.
	inReport := make(chan struct{})
	releaseReport := make(chan struct{})
	// Stand in for the sink's write by holding the first delivery open.
	sink.beforeReport = func(st Status) {
		if st.State == StateStale {
			close(inReport)
			<-releaseReport
		}
	}

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		w.emit(func() (Status, bool) {
			note("build:stale")
			return Status{State: StateStale}, true
		})
	}()
	go func() {
		<-inReport
		note("report:stale")
		close(releaseReport)
	}()

	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		<-inReport // the recovery is computed while the stale report is still in flight
		w.emit(func() (Status, bool) {
			note("build:watching")
			return Status{State: StateWatching}, true
		})
	}()

	<-firstDone
	<-secondDone

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"build:stale", "report:stale", "build:watching"}, trace,
		"a report in flight must block the next one from being built behind it")
	assert.Equal(t, []State{StateStale, StateWatching}, sink.states(),
		"and the sink sees them in that order, so the fold lands on the newer verdict")
}

// The package's guarantee is watch-for-latency, POLL-for-correctness: whatever the watch
// silently misses, the periodic re-list reconciles. That backstop lived entirely inside
// watchPhase, which a watch that never connects returns from long before the timer can
// fire — and Run deliberately retains the RV in that case, so the worker retried the watch
// and never listed again. A kind whose LIST is allowed and whose WATCH hangs or is denied
// then served rows that drifted from the cluster indefinitely, with nothing scheduled to
// correct them.
func TestWorkerRelistsEvenWhenTheWatchNeverEstablishes(t *testing.T) {
	fs := newFakeStore()
	// A watch request that is accepted and then never answers — the case the establish
	// timeout exists for. Not an error: RetryWatcher treats a non-retriable error as fatal
	// and closes its channel, which ends the phase the ordinary way and does re-list.
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })
	src := &fakeSource{
		listFn: staticList("100", newTestItem("uid-1", "e1", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) {
			<-hang
			return nil, errors.New("watch closed")
		},
	}

	startWorker(t, src, fs, withDriverOptions(
		withEstablishTimer(firedTimer),
		withResyncPeriod(10*time.Millisecond),
		withSleep(func(context.Context, time.Duration) error { return nil }),
	))

	require.Eventually(t, func() bool { return src.lists() >= 2 }, 2*time.Second, 5*time.Millisecond,
		"the re-list must still come due for a watch that never streams")
}

// The catch-up fast path — "a fresh post-LIST RV can't 410, so connecting IS the proof" —
// keyed off didResync, which is set by the first pass and never cleared. But Run RETAINS
// the RV when a watch fails to establish and retries from it, for as long as the watch
// stays broken. A connect on that path is not a fresh position at all, and taking the fast
// path there reports a recovery the sync never made: the panel flips to green with a
// "re-sync complete" event, then back to Stale as soon as the aged RV 410s.
func TestWorkerRetainedRVDoesNotShortCircuitTheCatchUpProof(t *testing.T) {
	fs := newFakeStore()

	// phase2 opens when the SECOND watch phase starts (the establish timer is requested
	// once per phase, from the Run goroutine). Until then no watch request can return, so
	// the first phase provably ends the way this test needs it to: never connected, RV
	// retained.
	phase2 := make(chan struct{})
	watchers := make(chan *watch.RaceFreeFakeWatcher, 8)
	src := &fakeSource{
		listFn: staticList("100", newTestItem("uid-1", "e1", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) {
			<-phase2
			fw := watch.NewRaceFreeFake()
			watchers <- fw
			return fw, nil
		},
	}

	// The establish bound fires for the first phase only; the resume grace never fires. So
	// a Watching report can only come from the fast path or from a real delta.
	establishes := 0
	sink := startWorker(t, src, fs, withDriverOptions(
		withEstablishTimer(func(time.Duration) (<-chan time.Time, func()) {
			if establishes++; establishes == 1 {
				return firedTimer(0)
			}
			close(phase2)
			return nil, func() {}
		}),
		withGraceTimer(func(time.Duration) (<-chan time.Time, func()) { return nil, func() {} }),
		withSleep(func(context.Context, time.Duration) error { return nil }),
	))

	var handed []*watch.RaceFreeFakeWatcher
	select {
	case fw := <-watchers: // a watch request has been answered, so the retry connected
		handed = append(handed, fw)
	case <-time.After(2 * time.Second):
		t.Fatal("the retained RV was never retried")
	}

	require.Never(t, func() bool { return slices.Contains(sink.states(), StateWatching) },
		200*time.Millisecond, 10*time.Millisecond,
		"connecting on an aged, retained RV is not proof the sync recovered")

	// A delta IS proof, and still reports the catch-up. Fed to every watcher handed out:
	// the first phase's request is answered too (its RetryWatcher is long stopped), so
	// which one the live phase holds is not something this test should guess at.
	for draining := true; draining; {
		select {
		case fw := <-watchers:
			handed = append(handed, fw)
		default:
			draining = false
		}
	}
	for _, fw := range handed {
		fw.Action(watch.Added, newTestItem("uid-2", "e2", "101"))
	}
	sink.await(t, "catch-up on a real delta", isState(StateWatching))
}

// A delta the store REJECTS cannot simply be skipped. The cookie is advanced by
// ApplyChange itself, in the row's own transaction, so the next delta that does land moves
// the resume position past the one that didn't — and a restart resumes after it. The
// dropped object keeps its stale body until the next periodic re-list, or forever on a
// quiet collection whose store can't diff-resync. Ending the phase re-lists, which
// reconciles it.
func TestWorkerRelistsAfterADeltaTheStoreRejects(t *testing.T) {
	fs := newFakeStore()
	fs.failApply = func(u *unstructured.Unstructured) error {
		if u.GetUID() == "uid-bad" {
			return errors.New("database is locked")
		}
		return nil
	}

	fw := watch.NewRaceFreeFake()
	src := &fakeSource{
		listFn:  staticList("100", newTestItem("uid-1", "e1", "10")),
		watchFn: func(metav1.ListOptions) (watch.Interface, error) { return fw, nil },
	}
	sink := startWorker(t, src, fs, withDriverOptions(
		withSleep(func(context.Context, time.Duration) error { return nil }),
	))
	sink.await(t, "catch-up", isState(StateWatching))
	require.Equal(t, 1, src.lists())

	// The rejected delta, then a later one that would land: without the re-list the second
	// one's RV becomes the resume position and the first object is lost.
	fw.Modify(newTestItem("uid-bad", "e-bad", "101"))
	fw.Modify(newTestItem("uid-1", "e1", "102"))

	require.Eventually(t, func() bool { return src.lists() >= 2 }, testTimeout, 10*time.Millisecond,
		"a delta that did not land must end the phase and re-list, not be resumed past")
}
