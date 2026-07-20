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

package engine

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// --- fakeSource: a scriptable kubeSource for driver state-machine tests -------

type fakeSource struct {
	mu sync.Mutex

	listObjs     []*unstructured.Unstructured
	listRV       string
	listOutcomes []bool // true => return an error on that List call (by index)

	// Pagination scripting (optional; when listPages is nil, List returns the
	// single listObjs page). Each entry is one page's objects; the driver walks
	// them by continue token ("page-1", "page-2", …), the last page returning "".
	// expireOnContinue, when set, makes the List call that carries that continue
	// token fail with a 410 Expired (a Continue-token expiry mid-pagination) —
	// once, then it's cleared so the restart-from-top can succeed.
	listPages        [][]*unstructured.Unstructured
	expireOnContinue string
	// expireWithGone makes expireOnContinue return a bare 410 Gone (reason "Gone", no
	// "Expired") instead of ResourceExpired — a nonconforming intermediary's answer to
	// an expired continue token.
	expireWithGone bool
	// expireEveryContinue makes EVERY List with a non-empty Continue 410 (never
	// cleared) — the persistent-expiry case where paginated restarts can't complete, so
	// fullList exhausts its restart budget and returns errListRestartBudget.
	expireEveryContinue bool
	listContinues       []string // continue token passed on each List call, in order
	listLimits          []int64  // Limit passed on each List call, in order

	metas   []objMeta
	metaRV  string
	metaErr error

	getByName map[string]*unstructured.Unstructured

	// listEntered, if non-nil, receives a signal (non-blocking) at the top of every
	// List call — a test seam for observing WHEN a driver reaches its LIST, used to
	// assert the concurrency limiter gates it.
	listEntered chan struct{}
	// listBlock, if non-nil, makes List block (after signaling listEntered) until the
	// channel is closed/sent or ctx is cancelled — lets a test hold the driver inside its
	// LIST, e.g. to observe it holds a listLimiter slot across the list-heavy work.
	listBlock chan struct{}

	// watchers, if non-nil, receives a fresh FakeWatcher on each Watch call so a
	// test can push events into it. autoExpireFirst makes the first Watch deliver
	// a 410 on its own (no test coordination needed). watchErr makes every Watch
	// fail to establish (e.g. list-but-not-watch RBAC). watchBlocks makes every Watch
	// hang until its ctx is cancelled (a wedged watch request whose headers never arrive,
	// so onConnect never fires and RetryWatcher never closes its ResultChan).
	watchers        chan *watch.FakeWatcher
	autoExpireFirst bool
	watchErr        error
	watchBlocks     bool

	listCalls, metaCalls, getCalls, watchCalls int
}

var _ kubeSource = (*fakeSource)(nil)

func (f *fakeSource) List(ctx context.Context, opts metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error) {
	f.mu.Lock()
	i := f.listCalls
	f.listCalls++
	f.listContinues = append(f.listContinues, opts.Continue)
	f.listLimits = append(f.listLimits, opts.Limit)
	if f.listEntered != nil {
		select {
		case f.listEntered <- struct{}{}:
		default:
		}
	}
	block := f.listBlock
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, "", "", ctx.Err()
		}
	}
	f.mu.Lock()
	// A persistent Continue-token expiry: every paginated continue fails, forever, so
	// the restarts can't make progress and fullList exhausts its budget (errListRestartBudget).
	if f.expireEveryContinue && opts.Continue != "" {
		f.mu.Unlock()
		return nil, "", "", apierrors.NewResourceExpired("continue token expired")
	}
	// A Continue-token expiry fires once, then clears so the restart-from-top wins.
	if f.expireOnContinue != "" && opts.Continue == f.expireOnContinue {
		f.expireOnContinue = ""
		gone := f.expireWithGone
		f.mu.Unlock()
		if gone {
			return nil, "", "", apierrors.NewGone("continue token gone")
		}
		return nil, "", "", apierrors.NewResourceExpired("continue token expired")
	}
	f.mu.Unlock()
	if i < len(f.listOutcomes) && f.listOutcomes[i] {
		return nil, "", "", fmt.Errorf("list failed (call %d)", i)
	}
	// Paginated mode: resolve the requested page by continue token.
	if f.listPages != nil {
		idx := 0
		if opts.Continue != "" {
			// tokens are "page-<n>" pointing at listPages[n]
			fmt.Sscanf(opts.Continue, "page-%d", &idx)
		}
		page := f.listPages[idx]
		cont := ""
		if idx+1 < len(f.listPages) {
			cont = fmt.Sprintf("page-%d", idx+1)
		}
		return page, cont, f.listRV, nil
	}
	return f.listObjs, "", f.listRV, nil
}

// continuesSeen returns the continue tokens List was called with, in order.
func (f *fakeSource) continuesSeen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.listContinues...)
}

// limitsSeen returns the Limit each List call was made with, in order.
func (f *fakeSource) limitsSeen() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.listLimits...)
}

func (f *fakeSource) ListMetadata(context.Context, metav1.ListOptions) ([]objMeta, string, error) {
	f.mu.Lock()
	f.metaCalls++
	f.mu.Unlock()
	if f.metaErr != nil {
		return nil, "", f.metaErr
	}
	return f.metas, f.metaRV, nil
}

func (f *fakeSource) Get(_ context.Context, ns, name string) (*unstructured.Unstructured, error) {
	f.mu.Lock()
	f.getCalls++
	f.mu.Unlock()
	if u, ok := f.getByName[ns+"/"+name]; ok {
		return u, nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "objects"}, name)
}

func (f *fakeSource) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	f.mu.Lock()
	n := f.watchCalls
	f.watchCalls++
	blocks := f.watchBlocks
	f.mu.Unlock()
	if blocks {
		<-ctx.Done() // a wedged watch request: hangs until RetryWatcher (rw.Stop) cancels it
		return nil, ctx.Err()
	}
	if f.watchErr != nil {
		return nil, f.watchErr
	}
	w := watch.NewFake()
	if f.autoExpireFirst && n == 0 {
		go w.Error(expiredStatus())
	}
	if f.watchers != nil {
		select {
		case f.watchers <- w:
		default:
		}
	}
	return w, nil
}

func (f *fakeSource) counts() (list, meta, get int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCalls, f.metaCalls, f.getCalls
}

func (f *fakeSource) watchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.watchCalls
}

// setWatchErr toggles whether Watch fails to establish (stand-in for WATCH RBAC being
// revoked or restored), safe to call while a watcher goroutine is running.
func (f *fakeSource) setWatchErr(err error) {
	f.mu.Lock()
	f.watchErr = err
	f.mu.Unlock()
}

// --- helpers ------------------------------------------------------------------

func expiredStatus() *metav1.Status {
	return &metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   metav1.StatusFailure,
		Reason:   metav1.StatusReasonExpired,
		Message:  "too old resource version",
		Code:     http.StatusGone,
	}
}

func uObj(uid, apiVersion, kind, ns, name, rv string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata": map[string]any{
			"uid":             uid,
			"name":            name,
			"namespace":       ns,
			"resourceVersion": rv,
		},
	}}
}

// metaValue reads a cluster_meta value (empty string when absent).
func metaValue(t *testing.T, cdb *store.ClusterDB, key string) string {
	t.Helper()
	var v string
	err := cdb.Writer().QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, key).Scan(&v)
	if err != nil {
		return ""
	}
	return v
}

func podGVK() schema.GroupVersionKind { return schema.GroupVersionKind{Version: "v1", Kind: "Pod"} }

// mkEvt builds a minimal Warning Event for the events-store tests.
func mkEvt(uid string) *unstructured.Unstructured {
	e := uObj(uid, "v1", "Event", "default", uid, "5")
	e.Object["type"] = "Warning"
	e.Object["reason"] = "BackOff"
	e.Object["message"] = "boom"
	return e
}

// --- tests --------------------------------------------------------------------

// Resume from a stored RV applies watch deltas and never LISTs — the core
// efficiency win on a wake when the server still has our resourceVersion.
func TestResumeSuccessAppliesDeltasNoLIST(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)
	fs := &fakeSource{watchers: make(chan *watch.FakeWatcher, 4)}
	d := newKindDriver(fs, store, podGVK(), "100")

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	fw := <-fs.watchers
	fw.Add(uObj("p1", "v1", "Pod", "default", "nginx", "101"))

	waitFor(t, func() bool { return countObjectsByKind(t, cdb.Writer(), "Pod", "v1") == 1 }, "object applied")
	waitFor(t, func() bool { return metaValue(t, cdb, "v1/Pod.last_list_rv") == "101" }, "rv persisted")

	list, meta, _ := fs.counts()
	require.Equal(t, 0, list, "resume must not LIST")
	require.Equal(t, 0, meta, "resume must not list metadata")

	cancel()
	<-done
}

// A 410 Gone on the resume watch drops us into the metadata-first re-sync, which
// fetches only the changed object's body.
func TestResume410FallsToMetadataResync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	// Pre-seed so the cache is non-empty (metadata-diff path, not the cold-cache
	// full LIST) and uid "a" is stale.
	insertObject(t, w, "a", "v1", "Pod")
	store := newObjectsStore(ctx, "c1", podGVK(), w, cdb)

	fs := &fakeSource{
		autoExpireFirst: true,
		metas:           []objMeta{{UID: "a", Namespace: "default", Name: "a", ResourceVersion: "2"}},
		metaRV:          "200",
		getByName: map[string]*unstructured.Unstructured{
			"default/a": uObj("a", "v1", "Pod", "default", "a", "2"),
		},
	}
	// No-op sleep: the 410 is a no-progress watch end, so Run backs off before
	// re-syncing — skip the real wait to keep the test fast.
	noSleep := func(context.Context, time.Duration) error { return nil }
	d := newKindDriverWithOptions(fs, store, podGVK(), "5", withSleep(noSleep))

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	// After the 410, the driver re-syncs via metadata + a targeted GET.
	waitFor(t, func() bool { _, m, g := fs.counts(); return m >= 1 && g >= 1 }, "metadata re-sync ran")
	waitFor(t, func() bool {
		var rv string
		_ = w.QueryRow(`SELECT resource_version FROM objects WHERE uid='a'`).Scan(&rv)
		return rv == "2"
	}, "changed object refreshed")

	cancel()
	<-done
}

// Unchanged objects (stored RV == live RV) are skipped — no GET, no rewrite.
func TestMetadataDiffSkipsUnchanged(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	_, err := w.Exec(`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
		VALUES ('a','v1','Pod','a','9',0,0,x'7b7d')`)
	require.NoError(t, err)
	store := newObjectsStore(ctx, "c1", podGVK(), w, cdb)

	fs := &fakeSource{
		metas:  []objMeta{{UID: "a", Namespace: "default", Name: "a", ResourceVersion: "9"}},
		metaRV: "50",
	}
	d := newKindDriver(fs, store, podGVK(), "")

	rv, err := d.fullResync(ctx)
	require.NoError(t, err)
	require.Equal(t, "50", rv)
	_, _, get := fs.counts()
	require.Equal(t, 0, get, "unchanged object must not be fetched")
}

// A changed object (live RV differs) gets exactly one GET and its row updated.
func TestMetadataDiffFetchesChanged(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	_, err := w.Exec(`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
		VALUES ('a','v1','Pod','a','1',0,0,x'7b7d')`)
	require.NoError(t, err)
	store := newObjectsStore(ctx, "c1", podGVK(), w, cdb)

	fs := &fakeSource{
		metas:  []objMeta{{UID: "a", Namespace: "default", Name: "a", ResourceVersion: "2"}},
		metaRV: "50",
		getByName: map[string]*unstructured.Unstructured{
			"default/a": uObj("a", "v1", "Pod", "default", "a", "2"),
		},
	}
	d := newKindDriver(fs, store, podGVK(), "")

	_, err = d.fullResync(ctx)
	require.NoError(t, err)
	_, _, get := fs.counts()
	require.Equal(t, 1, get, "exactly the changed object is fetched")
	var rv string
	require.NoError(t, w.QueryRow(`SELECT resource_version FROM objects WHERE uid='a'`).Scan(&rv))
	require.Equal(t, "2", rv)
}

// A resume that falls back to a metadata-diff re-sync records the work: didResync
// plus the count of bodies re-pulled — so the engine can report "re-synced N
// objects" rather than a bare "watches resumed".
func TestFullResyncRecordsResyncWorkViaMetadataDiff(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	_, err := w.Exec(`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
		VALUES ('a','v1','Pod','a','1',0,0,x'7b7d')`)
	require.NoError(t, err)
	store := newObjectsStore(ctx, "c1", podGVK(), w, cdb)

	// 'a' changed (1→2) and 'c' is new — two bodies to re-pull via the metadata diff.
	fs := &fakeSource{
		metas: []objMeta{
			{UID: "a", Namespace: "default", Name: "a", ResourceVersion: "2"},
			{UID: "c", Namespace: "default", Name: "c", ResourceVersion: "2"},
		},
		metaRV: "50",
		getByName: map[string]*unstructured.Unstructured{
			"default/a": uObj("a", "v1", "Pod", "default", "a", "2"),
			"default/c": uObj("c", "v1", "Pod", "default", "c", "2"),
		},
	}
	d := newKindDriver(fs, store, podGVK(), "")

	_, err = d.fullResync(ctx)
	require.NoError(t, err)
	require.True(t, d.didResync, "a full re-sync records didResync")
	require.Equal(t, 2, d.resyncObjects, "counts the two bodies re-pulled")
}

// The full-LIST fallback (empty cache, or a changeset over the diff threshold)
// counts every listed body as re-synced.
func TestFullResyncRecordsResyncWorkViaFullList(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	_, err := w.Exec(`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
		VALUES ('a','v1','Pod','a','1',0,0,x'7b7d')`)
	require.NoError(t, err)
	store := newObjectsStore(ctx, "c1", podGVK(), w, cdb)

	fs := &fakeSource{
		metas: []objMeta{
			{UID: "a", Namespace: "default", Name: "a", ResourceVersion: "2"},
			{UID: "c", Namespace: "default", Name: "c", ResourceVersion: "2"},
		},
		metaRV:   "50",
		listObjs: []*unstructured.Unstructured{uObj("a", "v1", "Pod", "default", "a", "2"), uObj("c", "v1", "Pod", "default", "c", "2")},
		listRV:   "60",
	}
	d := newKindDriverWithOptions(fs, store, podGVK(), "", withDiffThreshold(1)) // two changed > 1 => full LIST

	_, err = d.fullResync(ctx)
	require.NoError(t, err)
	require.True(t, d.didResync)
	require.Equal(t, 2, d.resyncObjects, "counts every listed body")
}

// --- cold-start LIST concurrency limiter --------------------------------------

// A size-1 limiter admits exactly one holder; a second acquire blocks until the
// first releases its slot, then proceeds.
func TestListLimiterBoundsConcurrency(t *testing.T) {
	lim := newListLimiter(1)
	rel1, err := lim.acquire(context.Background())
	require.NoError(t, err)

	acquired := make(chan func(), 1)
	go func() {
		r, aerr := lim.acquire(context.Background())
		require.NoError(t, aerr)
		acquired <- r
	}()

	select {
	case <-acquired:
		t.Fatal("second acquire must block while the only slot is held")
	case <-time.After(50 * time.Millisecond):
	}

	rel1() // free the slot
	select {
	case r := <-acquired:
		r()
	case <-time.After(time.Second):
		t.Fatal("second acquire must proceed once the slot frees")
	}
}

// acquire honours ctx cancellation while queued, returning the ctx error rather
// than blocking forever, and its returned release is a safe no-op.
func TestListLimiterAcquireRespectsCtx(t *testing.T) {
	lim := newListLimiter(1)
	rel, err := lim.acquire(context.Background())
	require.NoError(t, err)
	defer rel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	release, err := lim.acquire(ctx)
	require.ErrorIs(t, err, context.Canceled)
	release() // must not panic or over-release the held slot
}

// A nil limiter (drivers built directly in unit tests) imposes no bound and its
// release is a no-op, so callers can defer it unconditionally.
func TestNilListLimiterNoBound(t *testing.T) {
	var lim listLimiter
	rel, err := lim.acquire(context.Background())
	require.NoError(t, err)
	rel()
}

// fullResync acquires a limiter slot BEFORE its LIST and holds it across the list-heavy
// work — the memory bound that keeps cold-start peak O(N pages) regardless of kind count.
// Verified deterministically: the LIST blocks, and once it's entered the sole slot is
// observably taken (a synchronous len(lim), not a timing window); on return it's freed.
// If fullResync listed WITHOUT acquiring, the slot would read free while inside the LIST.
func TestFullResyncHoldsListLimiterSlotDuringLIST(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)
	fs := &fakeSource{
		listObjs:    []*unstructured.Unstructured{uObj("p1", "v1", "Pod", "default", "p1", "10")},
		listRV:      "10",
		listEntered: make(chan struct{}, 1),
		listBlock:   make(chan struct{}), // hold the driver inside its LIST until released
	}

	lim := newListLimiter(1)
	d := newKindDriverWithOptions(fs, store, podGVK(), "", withListLimiter(lim))
	done := make(chan string, 1)
	go func() { rv, _ := d.fullResync(ctx); done <- rv }()

	// The LIST is reached only after a slot is acquired.
	select {
	case <-fs.listEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("fullResync never reached its LIST")
	}
	// While inside the LIST it holds the sole slot — a synchronous, race-free check.
	require.Equal(t, 1, len(lim), "the LIST runs while holding the limiter slot")

	close(fs.listBlock) // let the LIST finish
	select {
	case rv := <-done:
		require.Equal(t, "10", rv)
	case <-time.After(2 * time.Second):
		t.Fatal("fullResync did not finish after listing")
	}
	require.Equal(t, 0, len(lim), "the slot is released when fullResync returns")
}

// A uid present in the cache but absent from the live metadata list is deleted.
func TestMetadataDiffDeletesMissing(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	_, err := w.Exec(`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
		VALUES ('a','v1','Pod','a','1',0,0,x'7b7d'), ('b','v1','Pod','b','1',0,0,x'7b7d')`)
	require.NoError(t, err)
	store := newObjectsStore(ctx, "c1", podGVK(), w, cdb)

	fs := &fakeSource{
		metas:  []objMeta{{UID: "a", Namespace: "default", Name: "a", ResourceVersion: "1"}},
		metaRV: "50",
	}
	d := newKindDriver(fs, store, podGVK(), "")

	_, err = d.fullResync(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, countObjectsByKind(t, w, "Pod", "v1"))
	require.Equal(t, 1, countWhere(t, w, "objects", "uid", "a"))
	require.Equal(t, 0, countWhere(t, w, "objects", "uid", "b"), "vanished uid deleted")
	_, _, get := fs.counts()
	require.Equal(t, 0, get, "unchanged survivor not fetched")
}

// When the changed set exceeds the threshold, a single full LIST replaces N GETs.
func TestLargeChangesetFallsBackToFullLIST(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	_, err := w.Exec(`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
		VALUES ('a','v1','Pod','a','1',0,0,x'7b7d')`)
	require.NoError(t, err)
	store := newObjectsStore(ctx, "c1", podGVK(), w, cdb)

	fs := &fakeSource{
		metas: []objMeta{
			{UID: "a", Namespace: "default", Name: "a", ResourceVersion: "2"},
			{UID: "c", Namespace: "default", Name: "c", ResourceVersion: "2"},
		},
		metaRV:   "50",
		listObjs: []*unstructured.Unstructured{uObj("a", "v1", "Pod", "default", "a", "2"), uObj("c", "v1", "Pod", "default", "c", "2")},
		listRV:   "60",
	}
	d := newKindDriverWithOptions(fs, store, podGVK(), "", withDiffThreshold(1)) // two changed > 1 => full LIST

	rv, err := d.fullResync(ctx)
	require.NoError(t, err)
	require.Equal(t, "60", rv)
	list, _, get := fs.counts()
	require.Equal(t, 1, list, "took the full LIST branch")
	require.Equal(t, 0, get, "no individual GETs")
	require.Equal(t, 2, countObjectsByKind(t, w, "Pod", "v1"))
}

// An empty cache (initial sync) always takes the full LIST — N GETs would be
// strictly worse than one streamed list.
func TestEmptyCacheFallsBackToFullLIST(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)

	fs := &fakeSource{
		metas:    []objMeta{{UID: "a", Namespace: "default", Name: "a", ResourceVersion: "2"}},
		metaRV:   "50",
		listObjs: []*unstructured.Unstructured{uObj("a", "v1", "Pod", "default", "a", "2")},
		listRV:   "60",
	}
	d := newKindDriver(fs, store, podGVK(), "")

	rv, err := d.fullResync(ctx)
	require.NoError(t, err)
	require.Equal(t, "60", rv)
	list, _, get := fs.counts()
	require.Equal(t, 1, list)
	require.Equal(t, 0, get)
	require.Equal(t, 1, countObjectsByKind(t, cdb.Writer(), "Pod", "v1"))
}

// A bookmark proves the watch is alive without any object delta: it stamps liveness
// and advances the persisted resume RV but applies no object. RetryWatcher swallows
// bookmarks, so the driver taps the watch stream ahead of it — what lets the engine
// tell a quiet-but-healthy watch from a wedged one.
func TestDriverBookmarkMarksLiveAndPersistsRV(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)
	fs := &fakeSource{watchers: make(chan *watch.FakeWatcher, 4)}

	at := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)
	d := newKindDriverWithOptions(fs, store, podGVK(), "100", withNow(func() time.Time { return at }))

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	fw := <-fs.watchers
	// A bookmark carrying a fresh RV, no object change.
	fw.Action(watch.Bookmark, uObj("", "v1", "Pod", "", "", "205"))

	waitFor(t, func() bool { return metaValue(t, cdb, "v1/Pod.last_list_rv") == "205" }, "bookmark rv persisted")
	require.Equal(t, at, d.liveAt(), "the bookmark stamped liveness")
	require.Equal(t, 0, countObjectsByKind(t, cdb.Writer(), "Pod", "v1"), "a bookmark applies no object")

	cancel()
	<-done
}

// A bookmark must not advance the resume cookie past deltas not yet applied: the
// tap sees the bookmark ahead of the apply loop, so persisting its RV eagerly would
// let a restart skip those changes. onBookmark holds the cookie until deltaApplied
// catches deltaSeen, while still marking liveness.
func TestBookmarkHoldsRVUntilDeltasApplied(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)
	at := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)
	d := newKindDriverWithOptions(nil, store, podGVK(), "100", withNow(func() time.Time { return at }))

	// The tap forwarded two deltas the apply loop hasn't stored yet — a bookmark
	// past them must hold the cookie, but still prove the watch is alive. Epoch 0 is
	// the driver's initial watchEpoch (no watch phase has retired it). seen/applied are
	// the phase-local counters onBookmark compares (see watchPhase).
	var seen, applied atomic.Int64
	seen.Store(2)
	d.onBookmark(ctx, 0, &seen, &applied, "205")
	require.Equal(t, at, d.liveAt(), "the bookmark stamps liveness even while the cookie is held")
	require.Empty(t, metaValue(t, cdb, "v1/Pod.last_list_rv"), "cookie held while deltas are un-applied")

	// Once both deltas are durable, the next bookmark advances the cookie.
	applied.Store(2)
	d.onBookmark(ctx, 0, &seen, &applied, "206")
	require.Equal(t, "206", metaValue(t, cdb, "v1/Pod.last_list_rv"), "cookie advances once applied catches up")
}

// A successful RetryWatcher reconnect is itself proof the watch is alive: on a
// quiet cluster a watch can drop and reopen with no delta or bookmark in between,
// and without stamping liveness on reconnect the monitor would falsely flag the
// kind stale past the threshold. The replacement watch's open must refresh liveAt.
func TestReconnectMarksLive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)
	fs := &fakeSource{watchers: make(chan *watch.FakeWatcher, 4)}

	t0 := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(10 * time.Minute)
	var clk atomic.Int64
	clk.Store(t0.UnixNano())
	d := newKindDriverWithOptions(fs, store, podGVK(), "100",
		withNow(func() time.Time { return time.Unix(0, clk.Load()) }))

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	w1 := <-fs.watchers
	waitFor(t, func() bool { return d.liveAt().Equal(t0) }, "initial connect stamped liveness at t0")

	// Advance the clock and drop the watch (a clean close). RetryWatcher reopens a
	// replacement watch with no delta or bookmark in between; liveness must refresh.
	clk.Store(t1.UnixNano())
	w1.Stop()
	<-fs.watchers // the reconnect's replacement watch
	waitFor(t, func() bool { return d.liveAt().Equal(t1) }, "reconnect refreshed liveness at t1")

	cancel()
	<-done
}

// The persisted resume RV advances with each object delta.
func TestRVPersistedOnEachEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)
	fs := &fakeSource{watchers: make(chan *watch.FakeWatcher, 4)}
	d := newKindDriver(fs, store, podGVK(), "100")

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	fw := <-fs.watchers
	fw.Add(uObj("p1", "v1", "Pod", "default", "p1", "110"))
	waitFor(t, func() bool { return metaValue(t, cdb, "v1/Pod.last_list_rv") == "110" }, "rv after add")
	fw.Modify(uObj("p1", "v1", "Pod", "default", "p1", "130"))
	waitFor(t, func() bool { return metaValue(t, cdb, "v1/Pod.last_list_rv") == "130" }, "rv after modify")

	cancel()
	<-done
}

// Events take the plain full-LIST path on a re-sync: no metadata diff (the
// events table has no resource_version column / no delete-missing).
func TestEventsFullResyncUsesPlainLIST(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	gvk := schema.GroupVersionKind{Version: "v1", Kind: "Event"}
	store := newEventsStore(ctx, "c1", gvk, cdb.Writer(), cdb)

	fs := &fakeSource{
		listObjs: []*unstructured.Unstructured{mkEvt("e1")},
		listRV:   "70",
	}
	d := newKindDriver(fs, store, gvk, "")

	rv, err := d.fullResync(ctx)
	require.NoError(t, err)
	require.Equal(t, "70", rv)
	list, meta, _ := fs.counts()
	require.Equal(t, 1, list, "events use a full LIST")
	require.Equal(t, 0, meta, "events never list metadata")
	var n int
	require.NoError(t, cdb.Writer().QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n))
	require.Equal(t, 1, n)
}

// A cold full LIST is paginated: each page is streamed into the store so an
// entire kind's bodies never sit in memory at once. Every page's objects land,
// and the driver walks the continue tokens page by page.
func TestFullListPaginatesAllPages(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	store := newObjectsStore(ctx, "c1", podGVK(), w, cdb)

	fs := &fakeSource{
		listRV: "60",
		listPages: [][]*unstructured.Unstructured{
			{uObj("a", "v1", "Pod", "default", "a", "2")},
			{uObj("b", "v1", "Pod", "default", "b", "2")},
			{uObj("c", "v1", "Pod", "default", "c", "2")},
		},
	}
	d := newKindDriver(fs, store, podGVK(), "")

	rv, err := d.fullList(ctx)
	require.NoError(t, err)
	require.Equal(t, "60", rv)
	require.Equal(t, 3, countObjectsByKind(t, w, "Pod", "v1"), "every page's objects landed")
	require.Equal(t, []string{"", "page-1", "page-2"}, fs.continuesSeen(), "walked the continue tokens")
	require.Equal(t, 3, d.resyncObjects, "counts every listed body across pages")
}

// The delete-missing prune must see the UNION of all pages' uids, not just the
// last page's — otherwise a row present only in an earlier page would be wrongly
// pruned. A stale uid absent from every page is deleted; page-1 and page-2
// objects both survive.
func TestFullListPrunesAgainstUnionOfPages(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	_, err := w.Exec(`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
		VALUES ('old','v1','Pod','old','1',0,0,x'7b7d')`)
	require.NoError(t, err)
	store := newObjectsStore(ctx, "c1", podGVK(), w, cdb)

	fs := &fakeSource{
		listRV: "60",
		listPages: [][]*unstructured.Unstructured{
			{uObj("a", "v1", "Pod", "default", "a", "2")},
			{uObj("b", "v1", "Pod", "default", "b", "2")},
		},
	}
	d := newKindDriver(fs, store, podGVK(), "")

	_, err = d.fullList(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, countWhere(t, w, "objects", "uid", "a"), "page-1 object survives the later page's write")
	require.Equal(t, 1, countWhere(t, w, "objects", "uid", "b"), "page-2 object present")
	require.Equal(t, 0, countWhere(t, w, "objects", "uid", "old"), "stale uid absent from all pages pruned")
}

// A Continue token can expire (410) mid-pagination. The driver discards the
// partial pass and restarts a fresh paginated LIST from the top, so the final
// cache is still correct and no error escapes.
func TestFullListRestartsOnContinueExpiry(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	store := newObjectsStore(ctx, "c1", podGVK(), w, cdb)

	fs := &fakeSource{
		listRV:           "60",
		expireOnContinue: "page-1", // the second page's fetch 410s, once
		listPages: [][]*unstructured.Unstructured{
			{uObj("a", "v1", "Pod", "default", "a", "2")},
			{uObj("b", "v1", "Pod", "default", "b", "2")},
		},
	}
	d := newKindDriver(fs, store, podGVK(), "")

	rv, err := d.fullList(ctx)
	require.NoError(t, err, "a mid-pagination 410 is recovered, not surfaced")
	require.Equal(t, "60", rv)
	require.Equal(t, 2, countObjectsByKind(t, w, "Pod", "v1"), "fresh pass lands every object")
	// "" (page0), "page-1" (410), "" (restart page0), "page-1" (page1 succeeds).
	require.Equal(t, []string{"", "page-1", "", "page-1"}, fs.continuesSeen(), "restarted from the top")
	require.Equal(t, 2, d.resyncObjects, "aborted pages are not double-counted")
}

// A bare 410 Gone (reason "Gone", not "Expired") on a continue token — what a
// nonconforming intermediary might answer with — is treated as continue-token expiry
// too, so the driver restarts in place rather than surfacing it as a fatal error.
func TestFullListRestartsOnBareGone(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	store := newObjectsStore(ctx, "c1", podGVK(), w, cdb)

	fs := &fakeSource{
		listRV:           "60",
		expireOnContinue: "page-1",
		expireWithGone:   true, // a bare 410 Gone, not ResourceExpired
		listPages: [][]*unstructured.Unstructured{
			{uObj("a", "v1", "Pod", "default", "a", "2")},
			{uObj("b", "v1", "Pod", "default", "b", "2")},
		},
	}
	d := newKindDriver(fs, store, podGVK(), "")

	rv, err := d.fullList(ctx)
	require.NoError(t, err, "a bare 410 Gone mid-pagination is recovered like ResourceExpired")
	require.Equal(t, "60", rv)
	require.Equal(t, 2, countObjectsByKind(t, w, "Pod", "v1"), "the restart lands every object")
}

// When paginated restarts can't complete — a Continue token that keeps expiring at the
// same point every pass (a kind whose full paginated pass outlives the token lifetime)
// — fullList spends its restart budget then returns errListRestartBudget rather than
// falling back to a whole-kind unpaginated LIST (which would defeat the memory bound).
// The caller's error budget surfaces the kind; no unpaginated LIST is ever issued.
func TestFullListErrorsOnPersistentExpiry(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	store := newObjectsStore(ctx, "c1", podGVK(), w, cdb)

	fs := &fakeSource{
		listRV:              "60",
		expireEveryContinue: true, // every continue-token fetch 410s, forever
		listPages: [][]*unstructured.Unstructured{
			{uObj("a", "v1", "Pod", "default", "a", "2")},
			{uObj("b", "v1", "Pod", "default", "b", "2")},
		},
	}
	d := newKindDriver(fs, store, podGVK(), "")

	_, err := d.fullList(ctx)
	require.ErrorIs(t, err, errListRestartBudget, "persistent continue-token expiry surfaces as an error, not a fallback")

	// The initial pass plus each restart 410s on "page-1"; then it gives up — no LIST is
	// ever issued unpaginated (Limit 0).
	limits, conts := fs.limitsSeen(), fs.continuesSeen()
	for _, l := range limits {
		require.Equal(t, int64(listPageSize), l, "every LIST stays paginated — no unpaginated fallback")
	}
	page1s := 0
	for _, c := range conts {
		if c == "page-1" {
			page1s++
		}
	}
	require.Equal(t, maxListRestarts+1, page1s, "the initial pass plus each restart 410 on the continue token before giving up")
}

// BeginReplace defers the resume-cookie delete to the first WritePage, so a relist
// interrupted before any page is written (the first LIST fails, or the app quits)
// leaves the untouched on-disk snapshot's cookie intact — the next start still resumes
// cheaply instead of paying a needless cold LIST.
func TestFullListZeroPagesKeepsResumeCookie(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	gvk := podGVK()
	s := newObjectsStore(ctx, "c1", gvk, cdb.Writer(), cdb)

	// A resume cookie + objects from a prior complete sync.
	insertObject(t, cdb.Writer(), "p1", "v1", "Pod")
	require.NoError(t, s.PersistRV(ctx, "40"))

	// The very first List (page-0) fails, before any page is written.
	fs := &fakeSource{listOutcomes: []bool{true}}
	d := newKindDriver(fs, s, gvk, "")

	_, err := d.fullList(ctx)
	require.Error(t, err, "the first List fails")
	require.Equal(t, "40", metaValue(t, cdb, lastListRVKey(gvk)),
		"no page was written, so the cookie (and the untouched snapshot) survive")
	require.Equal(t, 1, countObjectsByKind(t, cdb.Writer(), "Pod", "v1"), "the snapshot is untouched")

	rv, err := s.ResumeRV(ctx)
	require.NoError(t, err)
	require.Equal(t, "40", rv, "the next start still resumes from the saved cookie")
}

// A straggler bookmark from a watch phase that already ended must not resurrect the
// resume cookie: watch.Filter's forwarding goroutine is never joined, so a buffered
// bookmark can surface after Run moved on to a fullList that cleared the cookie.
// onBookmark persists only while its captured epoch is still the driver's current one.
func TestBookmarkStragglerDoesNotResurrectCookie(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	gvk := podGVK()
	s := newObjectsStore(ctx, "c1", gvk, cdb.Writer(), cdb)
	d := newKindDriver(nil, s, gvk, "")

	// The driver has moved to epoch 1 (its watch phase retired). A bookmark still
	// carrying epoch 0 is a straggler — it must not write the cookie. seen/applied are
	// zero (no deltas), so the applied-caught-up gate passes and only the epoch fences it.
	var seen, applied atomic.Int64
	d.watchEpoch = 1
	d.onBookmark(ctx, 0, &seen, &applied, "999")
	require.Empty(t, metaValue(t, cdb, lastListRVKey(gvk)),
		"a straggler bookmark from a retired watch phase does not persist the cookie")

	// A bookmark whose epoch matches the current one advances the cookie normally.
	d.onBookmark(ctx, 1, &seen, &applied, "1000")
	require.Equal(t, "1000", metaValue(t, cdb, lastListRVKey(gvk)),
		"a bookmark from the current watch phase persists normally")
}

// fullList clears the resume cookie on the first page written, so a pass that fails
// mid-pagination leaves NO cookie: the next start cold-LISTs (its Commit prune
// reconciling the partial pages) rather than resuming from an RV that no longer matches
// the half-written rows. This is what stops a committed page-1 from defeating ResumeRV's
// objects-exist gate and resuming from an ancient cookie.
func TestFullListClearsResumeCookieOnPartialFailure(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	gvk := podGVK()
	s := newObjectsStore(ctx, "c1", gvk, cdb.Writer(), cdb)

	// A resume cookie from a prior complete sync.
	require.NoError(t, s.PersistRV(ctx, "40"))
	require.Equal(t, "40", metaValue(t, cdb, lastListRVKey(gvk)))

	fs := &fakeSource{
		listRV:       "70",
		listOutcomes: []bool{false, true}, // page-0 ok, page-1 fails non-410
		listPages: [][]*unstructured.Unstructured{
			{uObj("p1", "v1", "Pod", "default", "p1", "5")},
			{uObj("p2", "v1", "Pod", "default", "p2", "6")},
		},
	}
	d := newKindDriver(fs, s, gvk, "")

	_, err := d.fullList(ctx)
	require.Error(t, err, "page-1 fails, so fullList surfaces it")
	require.Equal(t, 1, countObjectsByKind(t, cdb.Writer(), "Pod", "v1"),
		"page-0's committed rows remain — a partial snapshot the next pass's prune reconciles")
	require.Empty(t, metaValue(t, cdb, lastListRVKey(gvk)),
		"the stale cookie is cleared, so the next start cold-LISTs")

	rv, err := s.ResumeRV(ctx)
	require.NoError(t, err)
	require.Empty(t, rv, "ResumeRV returns empty despite page-0 rows existing — no resume from the ancient cookie")
}

// Events clear the cookie on a failed pass too, for behavioral uniformity with
// objects (see eventsStore.BeginReplace) — not for correctness, since an unguarded
// ResumeRV resuming from the pre-pass cookie would be equally safe (events never
// prune, so a partial pass can't leave a false-positive "this kind has data" signal).
func TestFullListEventsClearsResumeCookieOnPartialFailure(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	gvk := schema.GroupVersionKind{Version: "v1", Kind: "Event"}
	store := newEventsStore(ctx, "c1", gvk, cdb.Writer(), cdb)

	require.NoError(t, store.PersistRV(ctx, "40"))

	fs := &fakeSource{
		listRV:       "70",
		listOutcomes: []bool{false, true}, // page-0 ok, page-1 fails non-410
		listPages: [][]*unstructured.Unstructured{
			{mkEvt("n1")},
			{mkEvt("n2")},
		},
	}
	d := newKindDriver(fs, store, gvk, "")

	_, err := d.fullList(ctx)
	require.Error(t, err)
	rv, err := store.ResumeRV(ctx)
	require.NoError(t, err)
	require.Empty(t, rv, "a failed events pass leaves no cookie, so the next start cold-LISTs")
}

// Events also take fullList (they have no metadata-diff), so their cold LIST
// paginates too — every page's events land in the events table.
func TestFullListPaginatesEvents(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	gvk := schema.GroupVersionKind{Version: "v1", Kind: "Event"}
	store := newEventsStore(ctx, "c1", gvk, cdb.Writer(), cdb)

	fs := &fakeSource{
		listRV: "70",
		listPages: [][]*unstructured.Unstructured{
			{mkEvt("e1")},
			{mkEvt("e2")},
		},
	}
	d := newKindDriver(fs, store, gvk, "")

	rv, err := d.fullList(ctx)
	require.NoError(t, err)
	require.Equal(t, "70", rv)
	var n int
	require.NoError(t, cdb.Writer().QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n))
	require.Equal(t, 2, n, "both pages' events landed")
}

// A complete event relist prunes rows absent from the fresh LIST, mirroring the
// server: an event the apiserver has already GC'd (gone from the LIST) is dropped, so
// the cache reflects the cluster's current event set. Retention is server-side, not a
// local TTL.
func TestFullListEventsRelistPrunesMissingRows(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	gvk := schema.GroupVersionKind{Version: "v1", Kind: "Event"}
	store := newEventsStore(ctx, "c1", gvk, cdb.Writer(), cdb)

	// A baseline event the apiserver has already GC'd — absent from every LIST below.
	// The relist mirrors the server, so it must be pruned.
	require.NoError(t, store.upsert(mkEvt("gcd-by-apiserver")))

	fs := &fakeSource{
		listRV: "70",
		listPages: [][]*unstructured.Unstructured{
			{mkEvt("e1")},
			{mkEvt("e2")},
		},
	}
	d := newKindDriver(fs, store, gvk, "")

	_, err := d.fullList(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, countWhere(t, cdb.Writer(), "events", "uid", "e1"), "a listed event is present")
	require.Equal(t, 1, countWhere(t, cdb.Writer(), "events", "uid", "e2"), "a listed event is present")
	require.Equal(t, 0, countWhere(t, cdb.Writer(), "events", "uid", "gcd-by-apiserver"),
		"an event absent from the LIST (GC'd server-side) is pruned — the cache mirrors the server")
}

// A failed partial pass leaves whatever it managed to upsert on disk (nothing rolls
// a committed page back — see eventsReplaceSession), but a later successful relist
// that no longer lists that row prunes it: the delete-missing pass reconciles the
// leftover rows against the server's current set.
func TestFullListEventsPartialPassRowsPrunedByLaterRelist(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	gvk := schema.GroupVersionKind{Version: "v1", Kind: "Event"}
	store := newEventsStore(ctx, "c1", gvk, cdb.Writer(), cdb)

	// First pass: page-0 upserts n1, then page-1 fails non-410 — fullList surfaces
	// the error, and n1 stays on disk.
	failing := &fakeSource{
		listRV:       "70",
		listOutcomes: []bool{false, true}, // page-0 ok, page-1 fails non-410
		listPages: [][]*unstructured.Unstructured{
			{mkEvt("n1")},
			{mkEvt("n2")},
		},
	}
	d := newKindDriver(failing, store, gvk, "")
	_, err := d.fullList(ctx)
	require.Error(t, err)
	require.Equal(t, 1, countWhere(t, cdb.Writer(), "events", "uid", "n1"),
		"a partial pass's upserted row is left on disk")

	// Second pass completes and no longer lists n1 — the prune reconciles it away.
	fresh := &fakeSource{
		listRV: "80",
		listPages: [][]*unstructured.Unstructured{
			{mkEvt("n2")},
		},
	}
	d2 := newKindDriver(fresh, store, gvk, "")
	_, err = d2.fullList(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, countWhere(t, cdb.Writer(), "events", "uid", "n1"),
		"n1 is pruned by a later complete relist that no longer lists it")
	require.Equal(t, 1, countWhere(t, cdb.Writer(), "events", "uid", "n2"), "the surviving event is present")
}

// Commit's two resume-cookie writes (last_list_rv + last_list_at) must be atomic:
// if the timestamp write fails, last_list_rv must NOT stay durably advanced, else a
// restart resumes from a newer RV and skips the Added events before it. A trigger
// forces the timestamp upsert to fail.
func TestFullListEventsCommitCookieAtomic(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	gvk := schema.GroupVersionKind{Version: "v1", Kind: "Event"}
	_, err := cdb.Writer().Exec(
		`CREATE TRIGGER fail_at BEFORE INSERT ON cluster_meta
		   WHEN NEW.key LIKE '%` + lastListAtSuffix + `'
		   BEGIN SELECT RAISE(ABORT, 'boom'); END;`)
	require.NoError(t, err)
	store := newEventsStore(ctx, "c1", gvk, cdb.Writer(), cdb)

	sess, err := store.BeginReplace(ctx)
	require.NoError(t, err)
	err = sess.Commit("999")
	require.Error(t, err, "the last_list_at upsert fails")
	require.Empty(t, metaValue(t, cdb, lastListRVKey(gvk)),
		"a failed Commit must not leave last_list_rv advanced — the two cookie writes are one transaction")
}

// Run returns promptly when its context is cancelled, even mid-watch.
func TestRunStopsOnCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)
	fs := &fakeSource{watchers: make(chan *watch.FakeWatcher, 4)}
	d := newKindDriver(fs, store, podGVK(), "100")

	errCh := make(chan error, 1)
	go func() { errCh <- d.Run(ctx) }()
	<-fs.watchers // ensure the watch is established
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// recordingSleep returns a withSleep seam that captures each backoff duration.
func recordingSleep() (func(context.Context, time.Duration) error, func() []time.Duration) {
	var mu sync.Mutex
	var sleeps []time.Duration
	rec := func(_ context.Context, dur time.Duration) error {
		mu.Lock()
		sleeps = append(sleeps, dur)
		mu.Unlock()
		return nil
	}
	snapshot := func() []time.Duration {
		mu.Lock()
		defer mu.Unlock()
		return append([]time.Duration(nil), sleeps...)
	}
	return rec, snapshot
}

// Consecutive full re-sync (list) failures back off exponentially.
func TestFullResyncBacksOffOnListError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)

	fs := &fakeSource{listOutcomes: []bool{true, true, true, true}} // every LIST fails
	rec, snapshot := recordingSleep()
	d := newKindDriverWithOptions(fs, store, podGVK(), "", withSleep(rec))

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	waitFor(t, func() bool { return len(snapshot()) >= 3 }, "three backoffs recorded")
	cancel()
	<-done

	sleeps := snapshot()
	require.Equal(t, d.backoffInit, sleeps[0], "first backoff is the initial")
	require.Equal(t, 2*d.backoffInit, sleeps[1], "second backoff doubled")
	require.Equal(t, 4*d.backoffInit, sleeps[2], "third backoff doubled again")
}

// After stuckThreshold consecutive resync failures the driver marks itself stuck and
// fires onStuck exactly once — the signal that retires its catch-up token so a kind
// that can never sync stops blocking the milestone and gets surfaced by staleLaggards.
func TestDriverMarksStuckAfterRepeatedFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)

	fs := &fakeSource{} // every resync yields no usable resourceVersion → a failure
	var stuckCalls atomic.Int64
	noSleep := func(context.Context, time.Duration) error { return nil }
	d := newKindDriverWithOptions(fs, store, podGVK(), "", withSleep(noSleep), withStuckThreshold(3))
	d.onStuck = func() { stuckCalls.Add(1) }

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	waitFor(t, func() bool { return d.isStuck() }, "driver becomes stuck after repeated failures")
	waitFor(t, func() bool { return stuckCalls.Load() == 1 }, "onStuck fires when the budget is spent")
	require.Equal(t, CauseListFailed, d.stuckCause(), "a kind that can't LIST reports ListFailed")

	cancel()
	<-done
	require.Equal(t, int64(1), stuckCalls.Load(), "onStuck is one-shot even as failures continue")
}

// A successful resync clears the stuck flag — a kind that recovers is no longer
// surfaced as stale. Driven deterministically by gating each backoff sleep so the test
// steps the driver failure-by-failure.
func TestDriverClearsStuckOnRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)

	// Three LISTs fail (→ stuck at threshold 3); the fourth (outcomes exhausted) succeeds.
	fs := &fakeSource{
		listOutcomes: []bool{true, true, true},
		listObjs:     []*unstructured.Unstructured{uObj("p1", "v1", "Pod", "default", "p1", "10")},
		listRV:       "10",
		watchers:     make(chan *watch.FakeWatcher, 4),
	}
	sleeps := make(chan struct{})
	proceed := make(chan struct{})
	gatedSleep := func(sctx context.Context, _ time.Duration) error {
		select {
		case sleeps <- struct{}{}:
		case <-sctx.Done():
			return sctx.Err()
		}
		select {
		case <-proceed:
			return nil
		case <-sctx.Done():
			return sctx.Err()
		}
	}
	d := newKindDriverWithOptions(fs, store, podGVK(), "", withSleep(gatedSleep), withStuckThreshold(3))

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	for i := 1; i <= 3; i++ {
		select {
		case <-sleeps:
		case <-time.After(2 * time.Second):
			t.Fatalf("expected a backoff sleep after failure %d", i)
		}
		if i < 3 {
			require.False(t, d.isStuck(), "not stuck before the threshold")
		} else {
			require.True(t, d.isStuck(), "stuck once the budget is spent")
		}
		proceed <- struct{}{}
	}
	// The fourth attempt lists successfully → resync completes → stuck clears.
	waitFor(t, func() bool { return !d.isStuck() }, "stuck clears after a successful resync")

	cancel()
	<-done
}

// Regression (P2): a previously-stuck driver whose cookie-resume watch merely OPENS then
// immediately 410s must NOT clear the stuck flag on the bare connection. onConnect proves
// only that the HTTP stream opened, not that it's usable — clearing stuck there would let
// the milestone/liveness monitor report Watching (or refresh liveness in an open-then-error
// loop) for a kind whose stream is unusable. The flag clears only at the usability proof
// that fires onWatch (a delta, a clean-resume grace, or a fresh-RV connect); a watch that
// opens then errors before proving usable leaves the kind stuck.
func TestStuckNotClearedByBareWatchOpen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)

	// autoExpireFirst: the (cookie-resume) watch opens — firing onConnect — then 410s before
	// any delta or the resume grace, so this phase yields no usability proof.
	fs := &fakeSource{autoExpireFirst: true}
	// seedRV non-empty → cookie-resume path (didResync=false), so connecting alone is not the
	// catch-up proof and fireOnWatch is deferred to a delta / clean grace.
	d := newKindDriverWithOptions(fs, store, podGVK(), "5")
	d.stuck.Store(true) // enter the phase already stuck

	progressed, established, err := d.watchPhase(ctx, "5")
	require.False(t, progressed, "no delta was applied")
	require.True(t, established, "the watch opened, so onConnect fired")
	require.ErrorIs(t, err, errExpired, "and then immediately 410'd")
	require.True(t, d.isStuck(), "a bare open-then-410 must NOT clear the stuck flag")
}

// Regression: a kind whose LIST succeeds but WATCH is unusable (e.g.
// list-but-not-watch RBAC, or an aggregated API that rejects watch) must back
// off between attempts rather than hot-looping full LISTs. The watch never makes
// progress, so a resync-success does not reset the backoff.
func TestUnwatchableKindBacksOff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)

	fs := &fakeSource{
		metaErr:  fmt.Errorf("no metadata endpoint"), // keep every re-sync on the full-LIST path
		listObjs: []*unstructured.Unstructured{uObj("a", "v1", "Pod", "default", "a", "2")},
		listRV:   "60",
		watchErr: apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", fmt.Errorf("cannot watch")),
	}
	rec, snapshot := recordingSleep()
	d := newKindDriverWithOptions(fs, store, podGVK(), "", withSleep(rec))

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	waitFor(t, func() bool { return len(snapshot()) >= 3 }, "backs off between failed watch attempts")
	cancel()
	<-done

	sleeps := snapshot()
	require.Equal(t, d.backoffInit, sleeps[0], "first backoff is the initial")
	require.Equal(t, 2*d.backoffInit, sleeps[1], "backoff grows across unwatchable retries")
	require.Equal(t, 4*d.backoffInit, sleeps[2], "and keeps growing — not a hot loop")
}

// A kind that can LIST but never WATCH (list-but-not-watch RBAC) must eventually surface
// as stuck. fullResync keeps succeeding — which alone would reset the budget forever — but
// the watch never establishes, so that counts against the budget instead. And because
// liveness is stamped only on real establishment (not on watch-phase entry), the driver's
// lastLiveAt never advances, so it can't hide as healthy. Regression for the
// "silently stale while the engine reports healthy" gap.
func TestListButNotWatchBecomesStuck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)

	fs := &fakeSource{
		metaErr:  fmt.Errorf("no metadata endpoint"), // keep every resync on the full-LIST path (LIST succeeds)
		listObjs: []*unstructured.Unstructured{uObj("a", "v1", "Pod", "default", "a", "2")},
		listRV:   "60",
		watchErr: apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", fmt.Errorf("cannot watch")),
	}
	noSleep := func(context.Context, time.Duration) error { return nil }
	d := newKindDriverWithOptions(fs, store, podGVK(), "", withSleep(noSleep), withStuckThreshold(3))

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	waitFor(t, func() bool { return d.isStuck() }, "list-but-not-watch surfaces as stuck")
	require.True(t, d.liveAt().IsZero(), "a watch that never established never stamps liveness")
	require.Equal(t, CauseWatchFailed, d.stuckCause(), "list-but-not-watch reports WatchFailed, not ListFailed")

	cancel()
	<-done
}

// Regression (P1): a cold-start driver (didResync path) whose LIST succeeds but WATCH is
// forbidden must NOT fire onWatch just because it entered its watch phase — the fresh
// post-resync RV is real, but firing on entry would retire the catch-up token and report a
// clean sync a kind that has no watch never achieved. onWatch fires only once the watch
// actually ESTABLISHES; a never-establishing watch instead spends the error budget and goes
// stuck, so the milestone surfaces it as stale rather than a false EngineWatching.
func TestOnWatchWaitsForEstablishmentWhenWatchForbidden(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)

	fs := &fakeSource{
		metaErr:  fmt.Errorf("no metadata endpoint"), // keep every re-sync on the full-LIST path (LIST succeeds)
		listObjs: []*unstructured.Unstructured{uObj("a", "v1", "Pod", "default", "a", "2")},
		listRV:   "60",
		watchErr: apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", fmt.Errorf("cannot watch")),
	}
	noSleep := func(context.Context, time.Duration) error { return nil }
	var mu sync.Mutex
	onWatchCalls := 0
	d := newKindDriverWithOptions(fs, store, podGVK(), "", withSleep(noSleep), withStuckThreshold(3))
	d.onWatch = func(bool, int) { mu.Lock(); onWatchCalls++; mu.Unlock() }

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	waitFor(t, func() bool { return d.isStuck() }, "a list-but-not-watch cold start goes stuck")
	cancel()
	<-done

	mu.Lock()
	require.Zero(t, onWatchCalls, "onWatch must not fire before the watch establishes — no false catch-up")
	mu.Unlock()
}

// immediateTimer is a graceTimer/establishTimer seam that fires the instant it's armed, so a
// test drives the resume-grace / establishment-timeout deterministically without a real clock.
func immediateTimer(time.Duration) (<-chan time.Time, func()) {
	ch := make(chan time.Time, 1)
	ch <- time.Time{}
	return ch, func() {}
}

// Regression (P1): a watch that never CONNECTS must not block watchPhase forever. When the
// watch request hangs (headers never arrive), RetryWatcher never closes its ResultChan and
// onConnect never fires — without the establishment timeout the driver would sit in its watch
// phase indefinitely, never spending the error budget and holding its catch-up token, wedging
// the engine in Syncing. The bounded wait charges CauseWatchFailed instead, so the kind
// surfaces as stuck.
func TestWatchThatNeverConnectsBecomesStuck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)

	// LIST succeeds so only the WATCH fails; the watch request hangs (never connects).
	fs := &fakeSource{
		metaErr:     fmt.Errorf("no metadata endpoint"),
		listObjs:    []*unstructured.Unstructured{uObj("a", "v1", "Pod", "default", "a", "2")},
		listRV:      "60",
		watchBlocks: true,
	}
	noSleep := func(context.Context, time.Duration) error { return nil }
	d := newKindDriverWithOptions(fs, store, podGVK(), "",
		withSleep(noSleep), withStuckThreshold(3), withEstablishTimer(immediateTimer))

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	waitFor(t, func() bool { return d.isStuck() }, "a watch that never connects surfaces as stuck")
	require.Equal(t, CauseWatchFailed, d.stuckCause(), "the establishment timeout charges WatchFailed")
	require.True(t, d.liveAt().IsZero(), "a watch that never connected never stamps liveness")

	cancel()
	<-done
}

// Regression: a list-OK / watch-denied kind must not re-run a full re-sync every retry cycle.
// fullResync obtains a valid resourceVersion; the watch then fails only to ESTABLISH
// (errWatchNotEstablished). Because that leaves the RV valid — nothing expired it, the
// connection just never came up — Run keeps it and retries the watch from it, rather than
// clearing it and re-listing the whole kind's metadata on every backoff/stuck cycle
// (perpetually once stuck at the slow cadence). So across many establishment failures the
// source is LISTed exactly once.
func TestWatchDeniedDoesNotRelistEveryCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)

	// Cold start (no seed RV) so the first iteration re-syncs via a full LIST; the watch then
	// hangs forever (never connects), so every watch phase times out establishing.
	fs := &fakeSource{
		metaErr:     fmt.Errorf("no metadata endpoint"),
		listObjs:    []*unstructured.Unstructured{uObj("a", "v1", "Pod", "default", "a", "2")},
		listRV:      "60",
		watchBlocks: true,
	}
	noSleep := func(context.Context, time.Duration) error { return nil }
	d := newKindDriverWithOptions(fs, store, podGVK(), "",
		withSleep(noSleep), withStuckThreshold(3), withEstablishTimer(immediateTimer))

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	// Let many establishment-failure cycles elapse (stuck trips at 3; keep going well past it,
	// noSleep makes even the stuck cadence instant) so a per-cycle re-list would be obvious.
	waitFor(t, func() bool { return d.isStuck() }, "the never-connecting kind surfaces as stuck")
	waitFor(t, func() bool { return fs.watchCount() >= 6 },
		"the driver keeps retrying the watch across cycles")

	list, _, _ := fs.counts()
	require.Equal(t, 1, list, "the valid RV is retained across establishment failures — no re-list per cycle")

	cancel()
	<-done
}

// Regression (P2): the resume grace must be armed only AFTER the watch connects. Here the
// watch never connects and the grace timer fires the instant it's armed — so if watchPhase
// armed the grace at entry (the bug), it would fire onWatch and mis-report a clean resume
// while no watch exists. It must not: the grace is armed from onConnect (which never happens),
// so onWatch never fires and the never-connecting kind surfaces as stuck instead.
func TestResumeGraceDoesNotFireBeforeConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)

	fs := &fakeSource{
		metaErr:     fmt.Errorf("no metadata endpoint"),
		listObjs:    []*unstructured.Unstructured{uObj("a", "v1", "Pod", "default", "a", "2")},
		listRV:      "60",
		watchBlocks: true, // never connects, so the grace must never be armed
	}
	noSleep := func(context.Context, time.Duration) error { return nil }
	var mu sync.Mutex
	onWatchCalls := 0
	// A warm resume (valid seed RV) — the cookie path, which is the one that arms the grace.
	d := newKindDriverWithOptions(fs, store, podGVK(), "100",
		withSleep(noSleep), withStuckThreshold(3),
		withGraceTimer(immediateTimer), withEstablishTimer(immediateTimer))
	d.onWatch = func(bool, int) { mu.Lock(); onWatchCalls++; mu.Unlock() }

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	waitFor(t, func() bool { return d.isStuck() }, "a never-connecting warm resume still surfaces as stuck")
	cancel()
	<-done

	mu.Lock()
	require.Zero(t, onWatchCalls, "the resume grace must not fire a clean-resume onWatch before the watch connects")
	mu.Unlock()
}
