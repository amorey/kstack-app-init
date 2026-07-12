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

	metas   []objMeta
	metaRV  string
	metaErr error

	getByName map[string]*unstructured.Unstructured

	// watchers, if non-nil, receives a fresh FakeWatcher on each Watch call so a
	// test can push events into it. autoExpireFirst makes the first Watch deliver
	// a 410 on its own (no test coordination needed). watchErr makes every Watch
	// fail to establish (e.g. list-but-not-watch RBAC).
	watchers        chan *watch.FakeWatcher
	autoExpireFirst bool
	watchErr        error

	listCalls, metaCalls, getCalls, watchCalls int
}

var _ kubeSource = (*fakeSource)(nil)

func (f *fakeSource) List(context.Context, metav1.ListOptions) ([]*unstructured.Unstructured, string, error) {
	f.mu.Lock()
	i := f.listCalls
	f.listCalls++
	f.mu.Unlock()
	if i < len(f.listOutcomes) && f.listOutcomes[i] {
		return nil, "", fmt.Errorf("list failed (call %d)", i)
	}
	return f.listObjs, f.listRV, nil
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

func (f *fakeSource) Watch(context.Context, metav1.ListOptions) (watch.Interface, error) {
	f.mu.Lock()
	n := f.watchCalls
	f.watchCalls++
	f.mu.Unlock()
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

// A bookmark proves the watch is alive without any object delta: it stamps the
// driver's liveness time and advances the persisted resume RV, but applies no
// object. RetryWatcher swallows bookmarks, so the driver observes them by tapping
// the watch stream ahead of it — this is what lets the engine tell a quiet-but-
// healthy watch (still receiving periodic bookmarks) from a wedged one.
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

// A bookmark must not advance the resume cookie past deltas the driver hasn't
// applied yet: the tap sees a bookmark ahead of the apply loop, so persisting its
// RV eagerly would let a crash/restart resume from it and skip un-applied changes,
// leaving the cache permanently behind. onBookmark holds the cookie until
// deltaApplied catches the deltaSeen high-water mark, while still marking liveness.
func TestBookmarkHoldsRVUntilDeltasApplied(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)
	at := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)
	d := newKindDriverWithOptions(nil, store, podGVK(), "100", withNow(func() time.Time { return at }))

	// The tap forwarded two deltas the apply loop hasn't stored yet — a bookmark
	// past them must hold the cookie, but still prove the watch is alive.
	d.deltaSeen.Store(2)
	d.onBookmark(ctx, "205")
	require.Equal(t, at, d.liveAt(), "the bookmark stamps liveness even while the cookie is held")
	require.Empty(t, metaValue(t, cdb, "v1/Pod.last_list_rv"), "cookie held while deltas are un-applied")

	// Once both deltas are durable, the next bookmark advances the cookie.
	d.deltaApplied.Store(2)
	d.onBookmark(ctx, "206")
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

	evt := uObj("e1", "v1", "Event", "default", "e1", "5")
	evt.Object["type"] = "Warning"
	evt.Object["reason"] = "BackOff"
	evt.Object["message"] = "boom"
	fs := &fakeSource{
		listObjs: []*unstructured.Unstructured{evt},
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
	case <-time.After(2 * time.Second):
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
