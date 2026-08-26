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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"

	watchbus "github.com/amorey/gobus/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// listCall is one List the loop made, so a test can pin paging and the cold/warm split.
type listCall struct{ opts metav1.ListOptions }

// watchCall is one Watch the loop opened, with the fake stream it was given.
type watchCall struct {
	opts    metav1.ListOptions
	watcher *watch.RaceFreeFakeWatcher
}

// fakeCollection stands in for one collection's REST surface: scripted list pages, and
// a fake watch per open that the test drives by hand.
type fakeCollection struct {
	mu    sync.Mutex
	pages []*unstructured.UnstructuredList
	// listErr, when set, fails every List.
	listErr error

	lists   *testutil.Probe[listCall]
	watches *testutil.Probe[*watchCall]
	// closeWatches ends every stream as soon as it is opened — a proxy that accepts a
	// watch and hangs up. bookmarkThenClose does the same after one bookmark, which is a
	// server closing right after proving the stream live.
	closeWatches      bool
	bookmarkThenClose bool
}

func newFakeCollection(pages ...*unstructured.UnstructuredList) *fakeCollection {
	return &fakeCollection{
		pages:   pages,
		lists:   testutil.NewProbe[listCall](8),
		watches: testutil.NewProbe[*watchCall](8),
	}
}

func (f *fakeCollection) List(_ context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists.Fire(listCall{opts: opts})
	if f.listErr != nil {
		return nil, f.listErr
	}
	if len(f.pages) == 0 {
		return &unstructured.UnstructuredList{}, nil
	}
	page := f.pages[0]
	if len(f.pages) > 1 {
		f.pages = f.pages[1:]
	}
	return page, nil
}

func (f *fakeCollection) Watch(_ context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	w := watch.NewRaceFreeFake()
	f.watches.Fire(&watchCall{opts: opts, watcher: w})

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.bookmarkThenClose {
		w.Action(watch.Bookmark, podBody("", "", "56"))
	}
	if f.closeWatches || f.bookmarkThenClose {
		w.Stop()
	}
	return w, nil
}

// list builds one LIST answer: its items, the collection's resourceVersion, and the
// continue token for the page after it.
func list(rv, cont string, items ...*unstructured.Unstructured) *unstructured.UnstructuredList {
	out := &unstructured.UnstructuredList{Object: map[string]any{
		"apiVersion": "v1", "kind": "PodList",
		"metadata": map[string]any{"resourceVersion": rv, "continue": cont},
	}}
	for _, item := range items {
		out.Items = append(out.Items, *item)
	}
	return out
}

func podBody(uid, name, rv string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"uid": uid, "name": name, "namespace": "prod", "resourceVersion": rv,
		},
	}}
}

// syncFixture is one loop under test: the collection it syncs, the store it writes,
// and the observations it publishes.
type syncFixture struct {
	coll  *fakeCollection
	store *kubestore.Store
	obs   *testutil.Probe[Observation]
}

// openStore opens one cache's store for the test's life.
func openStore(t *testing.T, cacheID int64) *kubestore.Store {
	t.Helper()
	mgr := kubestore.NewManager(t.TempDir())
	t.Cleanup(func() { assert.NoError(t, mgr.Close()) })
	store, err := mgr.OpenOrCreate(cacheID)
	require.NoError(t, err)
	t.Cleanup(store.Release)
	return store
}

// startSync runs one worker over coll for the test's life, with every cadence shrunk.
func startSync(t *testing.T, p Params, coll *fakeCollection, opts ...syncOption) *syncFixture {
	t.Helper()
	return startSyncOn(t, openStore(t, p.CacheID), p, coll, opts...)
}

// startSyncOn is startSync over a store the test has already seeded.
func startSyncOn(t *testing.T, store *kubestore.Store, p Params, coll *fakeCollection, opts ...syncOption) *syncFixture {
	t.Helper()
	obs := testutil.NewProbe[Observation](32)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	base := []syncOption{
		withCollection(func(*kubeconn.Connection, Params) (collection, error) { return coll, nil }),
		// Shrunk cadences: the production values are the constants these replace.
		withStaleAfter(20 * time.Millisecond),
		withBackoff(time.Millisecond, 2*time.Millisecond),
	}
	s := newSyncer(p, &fakeLease{conns: &fakeConns{}}, store, obs.Fire, append(base, opts...)...)
	go func() {
		defer close(done)
		s.run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		testutil.Wait(t, done, "the sync loop's exit")
	})
	return &syncFixture{coll: coll, store: store, obs: obs}
}

// awaitReason waits for the observation carrying reason, failing on the failsafe.
func (f *syncFixture) awaitReason(t *testing.T, reason string) Observation {
	t.Helper()
	deadline := time.After(testutil.Timeout)
	for {
		select {
		case obs := <-f.obs.Chan():
			if obs.Reason == reason {
				return obs
			}
		case <-deadline:
			t.Fatalf("no observation with reason %q", reason)
		}
	}
}

// count is the kind's cached rows, read the way the boundary reads them.
func (f *syncFixture) count(t *testing.T, k kubestore.Kind) int {
	t.Helper()
	n, err := f.store.CountKind(context.Background(), k)
	require.NoError(t, err)
	return n
}

// cookie is the position a restart would resume this kind from.
func (f *syncFixture) cookie(t *testing.T, p Params) string {
	t.Helper()
	rv, _, err := f.store.Cookie(context.Background(), p.APIVersion, p.Resource)
	require.NoError(t, err)
	return rv
}

var (
	podRows   = kubestore.Kind{APIVersion: "v1", Kind: "Pod", Resource: "pods"}
	eventRows = kubestore.Kind{APIVersion: "v1", Kind: "Event", Resource: "events"}
)

var podParams = Params{
	CacheID: 1, ContextName: "prod", ServerUID: "uid-1",
	APIVersion: "v1", Kind: "Pod", Resource: "pods",
}

// A cold cache lists the collection in, records the position it may resume from, and
// reports Watching once the watch is live.
func TestColdSyncListsThenWatches(t *testing.T) {
	coll := newFakeCollection(list("100", "", podBody("uid-1", "api-0", "9")))
	f := startSync(t, podParams, coll)

	syncing := f.awaitReason(t, ReasonSyncing)
	assert.False(t, syncing.Resumed, "a cold build reported itself as a resume")

	w := f.coll.watches.Await(t, "the watch's open")
	assert.Equal(t, "100", w.opts.ResourceVersion, "the watch did not resume from the list")
	assert.True(t, w.opts.AllowWatchBookmarks)

	f.awaitReason(t, ReasonWatching)
	assert.Equal(t, 1, f.count(t, podRows))
	assert.Equal(t, "100", f.cookie(t, podParams))
}

// A cold list pages through the continue token: one transaction per page, so a large
// collection never has to fit in memory.
func TestColdSyncFollowsTheContinueToken(t *testing.T) {
	coll := newFakeCollection(
		list("100", "next", podBody("uid-1", "api-0", "9")),
		list("100", "", podBody("uid-2", "api-1", "9")),
	)
	f := startSync(t, podParams, coll)

	first := f.coll.lists.Await(t, "the first page's list")
	assert.Empty(t, first.opts.Continue)
	second := f.coll.lists.Await(t, "the second page's list")
	assert.Equal(t, "next", second.opts.Continue)

	f.awaitReason(t, ReasonWatching)
	assert.Equal(t, 2, f.count(t, podRows))
}

// A warm cache resumes from its cookie and does not list — the whole point of keeping
// one. The resume is what the fold turns into a ResyncStart rather than a SyncStart.
func TestWarmStartResumesFromTheCookieWithoutListing(t *testing.T) {
	coll := newFakeCollection()
	store := openStore(t, podParams.CacheID)
	require.NoError(t, store.SetCookie(context.Background(), "v1", "pods", "55"))
	f := startSyncOn(t, store, podParams, coll)

	w := f.coll.watches.Await(t, "the watch's open")
	assert.Equal(t, "55", w.opts.ResourceVersion)
	w.watcher.Action(watch.Bookmark, podBody("", "", "56"))
	assert.True(t, f.awaitReason(t, ReasonWatching).Resumed)
	testutil.NoRecv(t, f.coll.lists.Chan(), testutil.Timeout/50, "a list on a warm start")
}

// Deltas write through to the store, and the count the fold reports moves with them.
func TestWatchDeltasWriteThrough(t *testing.T) {
	coll := newFakeCollection(list("100", ""))
	f := startSync(t, podParams, coll)
	w := f.coll.watches.Await(t, "the watch's open")
	f.awaitReason(t, ReasonWatching)

	w.watcher.Add(podBody("uid-1", "api-0", "101"))

	require.Eventually(t, func() bool {
		return f.count(t, podRows) == 1
	}, testutil.Timeout, time.Millisecond)

	w.watcher.Delete(podBody("uid-1", "api-0", "102"))

	require.Eventually(t, func() bool {
		return f.count(t, podRows) == 0
	}, testutil.Timeout, time.Millisecond)
	// The delta's own position is what a restart resumes from.
	assert.Equal(t, "102", f.cookie(t, podParams))
}

// A watch that stops proving itself alive flips to Stale without tearing anything
// down — the rows stand, and the next proof of life flips it back.
func TestAQuietWatchGoesStaleAndRecovers(t *testing.T) {
	coll := newFakeCollection(list("100", ""))
	f := startSync(t, podParams, coll)
	w := f.coll.watches.Await(t, "the watch's open")
	f.awaitReason(t, ReasonWatching)

	f.awaitReason(t, ReasonStale)
	// Staleness is a verdict, not a teardown: the stream stands, so nothing reopens.
	testutil.NoRecv(t, f.coll.watches.Chan(), testutil.Timeout/50, "a re-opened watch")

	w.watcher.Action(watch.Bookmark, podBody("", "", "110"))

	assert.Equal(t, ReasonWatching, f.awaitReason(t, ReasonWatching).Reason)
}

// A bookmark is a proof of life and a position, never an object: it advances the
// cookie and writes no row.
func TestABookmarkAdvancesTheCookieOnly(t *testing.T) {
	coll := newFakeCollection(list("100", ""))
	f := startSync(t, podParams, coll)
	w := f.coll.watches.Await(t, "the watch's open")
	f.awaitReason(t, ReasonWatching)

	w.watcher.Action(watch.Bookmark, podBody("bookmark-uid", "", "150"))

	require.Eventually(t, func() bool {
		return f.cookie(t, podParams) == "150"
	}, testutil.Timeout, time.Millisecond)
	assert.Zero(t, f.count(t, podRows))
}

// A position the server refuses is a gap of unknown size, and a re-list is the only
// answer — which is where an object deleted while we were away finally leaves.
//
// Over both shapes an error frame arrives in: a decoder that knows metav1 hands back a
// Status, and one that does not hands back the same document as an unstructured object.
// Reading only the typed one would leave a worker retrying a cookie the server will
// refuse forever.
func TestAnExpiredWatchRelistsAndPrunes(t *testing.T) {
	expired := map[string]runtime.Object{
		"a typed status": &apierrors.NewResourceExpired("too old").ErrStatus,
		"an unstructured status": &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Status",
			"status": "Failure", "reason": "Expired", "code": int64(410),
			"message": "too old resource version",
		}},
	}

	for name, status := range expired {
		t.Run(name, func(t *testing.T) {
			coll := newFakeCollection(
				list("100", "", podBody("gone", "old", "9"), podBody("kept", "api-0", "9")),
				list("200", "", podBody("kept", "api-0", "20")),
			)
			f := startSync(t, podParams, coll)
			w := f.coll.watches.Await(t, "the watch's open")
			f.awaitReason(t, ReasonWatching)
			require.Equal(t, 2, f.count(t, podRows))

			w.watcher.Error(status)

			next := f.coll.watches.Await(t, "the watch reopened after the re-list")
			assert.Equal(t, "200", next.opts.ResourceVersion)
			// The row the second list did not carry is what the prune took.
			assert.Equal(t, 1, f.count(t, podRows))
		})
	}
}

// A failure that is not a gap is retried up the ladder, and reported meanwhile.
func TestAFailingListBacksOffAndReports(t *testing.T) {
	coll := newFakeCollection()
	coll.listErr = errors.New("forbidden")
	f := startSync(t, podParams, coll)

	obs := f.awaitReason(t, ReasonSyncFailed)

	assert.Contains(t, obs.Message, "forbidden")
	// The ladder retries rather than giving up.
	f.coll.lists.Await(t, "the first list")
	f.coll.lists.Await(t, "the retry")
}

// The events kind writes to its own table, and the pruner is what keeps the window a
// window — nothing else would ever remove an event.
func TestTheEventsKindWritesEventsAndPrunes(t *testing.T) {
	eventBody := func(uid, at string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Event",
			"metadata":       map[string]any{"uid": uid, "resourceVersion": "9"},
			"involvedObject": map[string]any{"kind": "Pod", "name": "api-0"},
			"lastTimestamp":  at,
		}}
	}
	p := podParams
	p.APIVersion, p.Kind, p.Resource = "v1", "Event", "events"
	coll := newFakeCollection(list("100", "",
		eventBody("old", "2026-08-01T00:00:00Z"),
		eventBody("new", "2026-08-01T00:01:00Z"),
	))

	f := startSync(t, p, coll, withEventsWindow(1))

	f.awaitReason(t, ReasonWatching)
	require.Eventually(t, func() bool {
		return f.count(t, eventRows) == 1
	}, testutil.Timeout, time.Millisecond)
	counts, err := f.store.Counts(context.Background())
	require.NoError(t, err)
	assert.Zero(t, counts.ObjectCount, "an event landed in the objects table")
}

// A connection the pool will not vouch for is reported by its own reason and waited
// on — never synced around, since a re-pointed context would mirror another cluster's
// objects into this cache.
func TestNoConnectionIsReportedAndWaitedOn(t *testing.T) {
	coll := newFakeCollection()
	obs := testutil.NewProbe[Observation](8)
	mgr := kubestore.NewManager(t.TempDir())
	t.Cleanup(func() { assert.NoError(t, mgr.Close()) })
	store, err := mgr.OpenOrCreate(1)
	require.NoError(t, err)
	t.Cleanup(store.Release)

	lease := newRefusingLease(kubeconn.ErrIdentityMismatch)
	t.Cleanup(lease.Release)
	s := newSyncer(podParams, lease, store, obs.Fire,
		withCollection(func(*kubeconn.Connection, Params) (collection, error) { return coll, nil }),
		withStaleAfter(time.Minute),
		withBackoff(time.Millisecond, 2*time.Millisecond),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.run(ctx)
	}()
	t.Cleanup(func() { cancel(); testutil.Wait(t, done, "the sync loop's exit") })

	got := testutil.Recv(t, obs.Chan(), "the refusal's observation")

	assert.Equal(t, ReasonIdentityMismatch, got.Reason)
	testutil.NoRecv(t, coll.lists.Chan(), testutil.Timeout/50, "a list without a connection")
}

// refusingLease is a claim whose connection is always refused, so the loop's only path
// is to report and wait. Its WatchState is a real keyed hub: the wait attaches to one
// before it re-checks, and nothing ever publishes, which is the outage being modelled.
type refusingLease struct {
	fakeLease
	err error
	hub *watchbus.Hub[string, kubeconn.State]
}

func newRefusingLease(err error) *refusingLease {
	return &refusingLease{err: err, hub: watchbus.New[string, kubeconn.State]()}
}

func (l *refusingLease) ConnFor(context.Context, string) (*kubeconn.Connection, error) {
	return nil, l.err
}
func (l *refusingLease) WatchState() kubeconn.StateSubscription { return l.hub.Watch("prod") }
func (l *refusingLease) Release()                               { l.hub.Close() }

// A collection that emptied while nothing was watching must leave nothing behind: the
// worker restarts warm, its position is refused, and the replacement LIST carries no
// body to learn the Kind from. The rows are keyed by Kind, so a worker that waited for a
// body would sweep on an empty one — matching nothing, leaving every stale row, and
// recording a fresh cookie over them.
func TestAnExpiredWatchPrunesWhenTheReplacementListIsEmpty(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, podParams.CacheID)
	require.NoError(t, store.ApplyChange(ctx, podRows, watch.Added, podBody("stale", "api-0", "9")))
	require.Equal(t, 1, mustCount(t, store, podRows))

	// A fresh worker over that store, as a restart leaves things: rows on disk, a
	// position to resume from, and nothing learned from the stream yet.
	f := startSyncOn(t, store, podParams, newFakeCollection(list("100", "")))
	w := f.coll.watches.Await(t, "the watch's open")
	w.watcher.Action(watch.Bookmark, podBody("", "", "10"))
	f.awaitReason(t, ReasonWatching)

	w.watcher.Error(&apierrors.NewResourceExpired("too old").ErrStatus)

	f.coll.watches.Await(t, "the watch reopened after the re-list")
	assert.Zero(t, f.count(t, podRows), "rows the relist did not carry survived it")
}

// A warm resume reports what the cache already holds. The count is per Kind, so a
// worker that has not seen a body yet still has to read the right one.
func TestAWarmStartReportsTheCachedCountBeforeAnyDelta(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, podParams.CacheID)
	require.NoError(t, store.ApplyChange(ctx, podRows, watch.Added, podBody("uid-1", "api-0", "9")))
	require.NoError(t, store.SetCookie(ctx, "v1", "pods", "55"))

	f := startSyncOn(t, store, podParams, newFakeCollection())
	w := f.coll.watches.Await(t, "the watch's open")
	w.watcher.Action(watch.Bookmark, podBody("", "", "56"))

	assert.Equal(t, 1, f.awaitReason(t, ReasonWatching).ObjectCount)
}

// mustCount is the kind's cached rows, read straight off the store.
func mustCount(t *testing.T, store *kubestore.Store, k kubestore.Kind) int {
	t.Helper()
	n, err := store.CountKind(context.Background(), k)
	require.NoError(t, err)
	return n
}

// A watch the server accepts and closes at once is a clean end, and retrying it costs a
// connect and a LIST-free reopen — per reopen, nothing; per second, a hot loop against
// the API server for every kind in the cache. A stream that carried nothing is paced
// like a failure, without being reported as one.
func TestAWatchThatCarriesNothingIsPaced(t *testing.T) {
	coll := newFakeCollection(list("100", ""))
	coll.closeWatches = true
	// A base wide enough that an unpaced reopen is unmistakable: paced, the third open is
	// two base intervals away; unpaced, it is immediate.
	f := startSync(t, podParams, coll, withBackoff(200*time.Millisecond, time.Second))

	f.coll.watches.Await(t, "the watch's open")
	f.coll.watches.Await(t, "the reopen")

	testutil.NoRecv(t, f.coll.watches.Chan(), 50*time.Millisecond, "an unpaced reopen")
}

// The rows a worker writes and the rows a teardown clears must be keyed by the same
// Kind. The teardown has only the record's, so that is what the writer uses — a body
// claiming another Kind would otherwise leave rows under a name nothing will look up.
func TestRowsAreKeyedByTheRecordsKind(t *testing.T) {
	ctx := context.Background()
	odd := podBody("uid-1", "api-0", "9")
	odd.SetKind("PodDisruptionBudget")
	coll := newFakeCollection(list("100", "", odd))

	f := startSync(t, podParams, coll)
	f.awaitReason(t, ReasonWatching)

	assert.Equal(t, 1, f.count(t, podRows), "the row was written under the body's Kind")
	// And the teardown, which knows only the record's Kind, finds it.
	require.NoError(t, f.store.ClearKind(ctx, podRows))
	assert.Zero(t, f.count(t, podRows))
}

// An API server closes a watch on its own timeout, and the resume that follows lists
// nothing. Reporting Syncing for it would walk a healthy cache Watching → Syncing →
// Watching per reopen — a requeue and a resync event pair per kind, forever.
func TestAWarmReopenDoesNotReportSyncing(t *testing.T) {
	coll := newFakeCollection(list("100", ""))
	f := startSync(t, podParams, coll)
	f.coll.lists.Await(t, "the cold sync's list")
	w := f.coll.watches.Await(t, "the watch's open")
	f.awaitReason(t, ReasonWatching)
	// A delta, so the reopen is the "worked and ended" path rather than the paced one.
	w.watcher.Add(podBody("uid-1", "api-0", "101"))
	f.obs.Drain()

	w.watcher.Stop()

	f.coll.watches.Await(t, "the reopen")
	for {
		obs, ok := f.obs.TryAwait()
		if !ok {
			break
		}
		assert.NotEqual(t, ReasonSyncing, obs.Reason, "a warm reopen reported a build")
	}
	testutil.NoRecv(t, f.coll.lists.Chan(), testutil.Timeout/50, "a list on a warm reopen")
}

// A proxy that accepts a watch and delivers nothing has proven nothing. Reporting
// Watching for the open — and stamping the freshness the health gauge reads — would let
// a cache that receives no updates look healthy for as long as the stream is reopened
// inside the staleness threshold.
func TestAWarmWatchIsNotHealthyUntilItCarriesSomething(t *testing.T) {
	ctx := context.Background()
	coll := newFakeCollection()
	store := openStore(t, podParams.CacheID)
	require.NoError(t, store.SetCookie(ctx, "v1", "pods", "55"))
	f := startSyncOn(t, store, podParams, coll, withStaleAfter(time.Minute))

	w := f.coll.watches.Await(t, "the watch's open")

	// A negative assertion needs a bounded window; the open would publish at once.
	testutil.NoRecv(t, f.obs.Chan(), 50*time.Millisecond, "a verdict from a bare connect")

	w.watcher.Action(watch.Bookmark, podBody("", "", "60"))

	assert.False(t, f.awaitReason(t, ReasonWatching).LastLiveAt.IsZero(),
		"the proof of life the bookmark carried")
}

// A cold sync is the other half: the list put the whole collection on disk, so being
// connected IS being caught up — but the watch has still proven nothing about itself,
// and the freshness stamp says so.
func TestAColdSyncReportsWatchingWithoutClaimingLiveness(t *testing.T) {
	coll := newFakeCollection(list("100", ""))
	f := startSync(t, podParams, coll)

	got := f.awaitReason(t, ReasonWatching)

	assert.True(t, got.LastLiveAt.IsZero(), "a connect was counted as proof of life")
}

// An error frame is the stream reporting a failure, not the stream proving itself: a
// proxy that accepts the watch and answers with one every attempt would otherwise keep
// refreshing the freshness the health gauge reads, and re-arm the staleness timer, for a
// kind that has received nothing.
func TestAnErrorFrameIsNotProofOfLife(t *testing.T) {
	ctx := context.Background()
	coll := newFakeCollection()
	store := openStore(t, podParams.CacheID)
	require.NoError(t, store.SetCookie(ctx, "v1", "pods", "55"))
	f := startSyncOn(t, store, podParams, coll, withStaleAfter(time.Minute),
		withBackoff(200*time.Millisecond, time.Second))
	w := f.coll.watches.Await(t, "the watch's open")

	w.watcher.Error(&apierrors.NewInternalError(errors.New("proxy said no")).ErrStatus)

	assert.True(t, f.awaitReason(t, ReasonSyncFailed).LastLiveAt.IsZero(),
		"an error frame was counted as proof of life")
	// And it is paced like any other failure rather than reopened at once.
	f.coll.watches.Await(t, "the reopen")
	testutil.NoRecv(t, f.coll.watches.Chan(), 50*time.Millisecond, "an unpaced reopen")
}

// A stream that carried something and was then hung up on reopens promptly, but not
// without a floor: a server closing right after each bookmark would spin every kind in
// the cache against the API server.
func TestAWatchThatCarriedSomethingStillPacesItsReopen(t *testing.T) {
	ctx := context.Background()
	coll := newFakeCollection()
	coll.bookmarkThenClose = true
	store := openStore(t, podParams.CacheID)
	require.NoError(t, store.SetCookie(ctx, "v1", "pods", "55"))
	f := startSyncOn(t, store, podParams, coll, withStaleAfter(time.Minute),
		withBackoff(200*time.Millisecond, time.Second))

	f.coll.watches.Await(t, "the watch's open")
	f.awaitReason(t, ReasonWatching)

	f.coll.watches.Await(t, "the reopen")
	testutil.NoRecv(t, f.coll.watches.Chan(), 50*time.Millisecond, "an unpaced reopen")
}

// Staleness is a property of the cache, not of one connection: a proxy that accepts and
// closes an empty watch just inside the threshold would otherwise restart the clock every
// reconnect, and a kind that last saw traffic hours ago would read healthy forever.
func TestStalenessSurvivesReconnects(t *testing.T) {
	ctx := context.Background()
	coll := newFakeCollection()
	coll.closeWatches = true
	store := openStore(t, podParams.CacheID)
	require.NoError(t, store.SetCookie(ctx, "v1", "pods", "55"))
	f := startSyncOn(t, store, podParams, coll,
		withStaleAfter(60*time.Millisecond), withBackoff(time.Millisecond, 2*time.Millisecond))

	// Reconnects land well inside the threshold, so no single connection outlives it.
	f.coll.watches.Await(t, "the watch's open")
	f.coll.watches.Await(t, "a reconnect")

	f.awaitReason(t, ReasonStale)
}
