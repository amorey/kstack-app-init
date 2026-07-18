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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	restfake "k8s.io/client-go/rest/fake"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// fakePreferredDiscovery is a minimal DiscoveryInterface covering the two things
// discoverGVRs uses: ServerPreferredResources (the resource lists) and the RESTClient
// listCRDs reaches for. The REST client is stubbed to fail every request, so listCRDs
// records no CRDs (its error is ignored) without any network I/O.
type fakePreferredDiscovery struct {
	discovery.DiscoveryInterface
	lists []*metav1.APIResourceList
}

func (f *fakePreferredDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return f.lists, nil
}

func (f *fakePreferredDiscovery) RESTClient() rest.Interface {
	return &restfake.RESTClient{Err: errors.New("no CRDs in test")}
}

// fakeClock is a manually-advanced clock for the liveness-monitor tests, safe for
// the monitor goroutine and the test to share.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// recordingSink captures every Report for assertion; statuses() snapshots them.
type recordingSink struct {
	mu       sync.Mutex
	reports  []EngineStatus
	reported chan struct{}
}

func newRecordingSink() *recordingSink {
	return &recordingSink{reported: make(chan struct{}, 64)}
}

func (s *recordingSink) Report(st EngineStatus) {
	s.mu.Lock()
	s.reports = append(s.reports, st)
	s.mu.Unlock()
	select {
	case s.reported <- struct{}{}:
	default:
	}
}

func (s *recordingSink) statuses() []EngineStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]EngineStatus(nil), s.reports...)
}

// An engine whose cluster is unreachable reports Syncing (the attempt) then
// Errored, and retries with growing backoff until stopped.
func TestEngineReportsErroredAndRetriesWithBackoff(t *testing.T) {
	cdb := migratedCDB(t)
	sink := newRecordingSink()
	rec, snapshot := recordingSleep()

	// A host nothing listens on: client construction succeeds, discovery fails.
	cfg := &rest.Config{Host: "https://127.0.0.1:1", Timeout: 50 * time.Millisecond}
	e := newEngineWithOptions(cfg, cdb, sink, withEngineSleep(rec))
	e.Start()

	waitFor(t, func() bool { return len(snapshot()) >= 3 }, "three retries recorded")
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, e.Stop(stopCtx))

	sleeps := snapshot()
	require.Equal(t, e.backoffInit, sleeps[0], "first backoff is the initial")
	require.Equal(t, 2*e.backoffInit, sleeps[1], "second backoff doubled")

	var sawSyncing, sawErrored bool
	for _, st := range sink.statuses() {
		switch st.State {
		case EngineSyncing:
			sawSyncing = true
			require.Empty(t, st.LastError, "Syncing reports carry no error")
			require.True(t, st.ColdStart,
				"an empty cache's Syncing report is a cold start (drives the SyncStart event)")
		case EngineErrored:
			sawErrored = true
			require.NotEmpty(t, st.LastError, "Errored reports carry the failure")
		}
	}
	require.True(t, sawSyncing, "the attempt is reported before its failure")
	require.True(t, sawErrored)
}

// The freshness tracker stamps cache write pings and flushes them through the
// sink on the coarse cadence, preserving the engine's current state fields.
func TestEngineFreshnessFlush(t *testing.T) {
	cdb := migratedCDB(t)
	sink := newRecordingSink()

	at := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)
	e := newEngineWithOptions(nil, cdb, sink,
		withFlushInterval(time.Millisecond),
		withEngineNow(func() time.Time { return at }))

	// Drive the freshness loop alone — the run loop needs a live cluster. The
	// subscription is taken synchronously (as Start does) so the Notify below
	// cannot race it.
	pings, cancelSub := cdb.Subscribe()
	defer cancelSub()
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.freshnessLoop(e.baseCtx, pings)
	}()

	cdb.Notify()
	waitFor(t, func() bool {
		for _, st := range sink.statuses() {
			if st.LastSyncedAt != nil && st.LastSyncedAt.Equal(at) {
				return true
			}
		}
		return false
	}, "freshness stamp flushed")

	e.baseCtxCancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("freshness loop did not stop on cancel")
	}
}

// A ping landing just before shutdown is flushed on the way out — the final
// stamp must not be lost to the 30s cadence.
func TestEngineFreshnessFinalFlushOnStop(t *testing.T) {
	cdb := migratedCDB(t)
	sink := newRecordingSink()

	at := time.Date(2026, 6, 12, 11, 0, 0, 0, time.UTC)
	e := newEngineWithOptions(nil, cdb, sink,
		withFlushInterval(time.Hour), // far past the test's lifetime
		withEngineNow(func() time.Time { return at }))

	pings, cancelSub := cdb.Subscribe()
	defer cancelSub()
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.freshnessLoop(e.baseCtx, pings)
	}()

	// The ping sits in the subscription's slot (taken before the loop ran), so
	// the shutdown drain must carry it into the final flush even if the loop
	// never consumed it before the cancel.
	cdb.Notify()
	e.baseCtxCancel()
	<-done

	statuses := sink.statuses()
	require.NotEmpty(t, statuses, "the final stamp flushes on shutdown")
	last := statuses[len(statuses)-1]
	require.NotNil(t, last.LastSyncedAt)
	require.True(t, last.LastSyncedAt.Equal(at))
}

// Stop joins both loops within its deadline.
func TestEngineStopJoins(t *testing.T) {
	cdb := migratedCDB(t)
	cfg := &rest.Config{Host: "https://127.0.0.1:1", Timeout: 50 * time.Millisecond}
	e := NewEngine(cfg, cdb, newRecordingSink())
	e.Start()

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, e.Stop(stopCtx))
}

// ConfigFingerprint must change when any auth-related field is edited — including
// exec/auth-provider/impersonation — so a kubeconfig edit while the app runs
// restarts sync instead of leaving drivers on stale credentials.
func TestConfigFingerprintCoversAuthFields(t *testing.T) {
	base := func() *rest.Config {
		return &rest.Config{Host: "https://x:6443", BearerToken: "tok"}
	}
	fp := ConfigFingerprint(base(), "")
	require.Equal(t, fp, ConfigFingerprint(base(), ""), "identical configs hash equal")

	// proxy-url lives in the kubeconfig, not rest.Config's hashable fields, so it
	// is passed alongside: a changed proxy must restart sync.
	require.NotEqual(t, fp, ConfigFingerprint(base(), "http://proxy:8080"), "proxy-url change must change the fingerprint")

	edits := map[string]func(*rest.Config){
		"exec command": func(c *rest.Config) {
			c.ExecProvider = &clientcmdapi.ExecConfig{Command: "get-token"}
		},
		"exec args": func(c *rest.Config) {
			c.ExecProvider = &clientcmdapi.ExecConfig{Command: "get-token", Args: []string{"--v2"}}
		},
		"exec env": func(c *rest.Config) {
			c.ExecProvider = &clientcmdapi.ExecConfig{Command: "get-token", Env: []clientcmdapi.ExecEnvVar{{Name: "K", Value: "V"}}}
		},
		"auth provider": func(c *rest.Config) {
			c.AuthProvider = &clientcmdapi.AuthProviderConfig{Name: "oidc", Config: map[string]string{"client-id": "abc"}}
		},
		"impersonate user": func(c *rest.Config) {
			c.Impersonate = rest.ImpersonationConfig{UserName: "admin"}
		},
	}
	for name, edit := range edits {
		t.Run(name, func(t *testing.T) {
			c := base()
			edit(c)
			require.NotEqual(t, fp, ConfigFingerprint(c, ""), "edit must change the fingerprint")
		})
	}

	// Editing an existing exec field (not just adding the block) must also change it.
	e1, e2 := base(), base()
	e1.ExecProvider = &clientcmdapi.ExecConfig{Command: "t", Args: []string{"--a"}}
	e2.ExecProvider = &clientcmdapi.ExecConfig{Command: "t", Args: []string{"--b"}}
	require.NotEqual(t, ConfigFingerprint(e1, ""), ConfigFingerprint(e2, ""), "editing exec args changes the fingerprint")
}

// ContextProxyURL resolves the proxy-url through the context's cluster entry,
// tolerating absent contexts/clusters.
func TestContextProxyURL(t *testing.T) {
	cfg := &clientcmdapi.Config{
		Contexts: map[string]*clientcmdapi.Context{
			"a": {Cluster: "a-cluster"},
			"b": {Cluster: "missing"},
		},
		Clusters: map[string]*clientcmdapi.Cluster{
			"a-cluster": {Server: "https://a", ProxyURL: "http://proxy:8080"},
		},
	}
	require.Equal(t, "http://proxy:8080", ContextProxyURL(cfg, "a"))
	require.Equal(t, "", ContextProxyURL(cfg, "b"), "dangling cluster reference")
	require.Equal(t, "", ContextProxyURL(cfg, "nope"), "absent context")
}

// cacheHasData tells a cold cache (never synced) from a resume of prior state,
// keyed on any kind having persisted a resume cookie. It's what lets the catch-up
// milestone report ColdStart, so the controller can distinguish
// SyncStart/SyncComplete from a ResyncStart/ResyncComplete resume.
func TestEngineCacheHasData(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)

	has, err := cacheHasData(ctx, cdb)
	require.NoError(t, err)
	require.False(t, has, "a freshly migrated cache is cold")

	// A persisted resume cookie marks the cache as previously synced — even for a
	// cluster that mirrors zero objects, so an empty cluster's resume isn't
	// misread as a cold first-ever sync.
	store := newObjectsStore(ctx, cdb.ID(), podGVK(), cdb.Writer(), cdb)
	require.NoError(t, store.PersistRV(ctx, "123"))

	has, err = cacheHasData(ctx, cdb)
	require.NoError(t, err)
	require.True(t, has, "a persisted resume cookie means a resume, not a cold start")
}

// reportCaughtUp emits the catch-up milestone: State=Watching plus the facts the
// controller composes into its message — whether this was a cold build, how many
// objects/kinds were mirrored, and how long catching up took.
func TestEngineReportsCatchUpFacts(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)

	// Seed two cached objects so the reported count is real.
	store := newObjectsStore(ctx, cdb.ID(), podGVK(), cdb.Writer(), cdb)
	require.NoError(t, store.ApplyChange(watch.Added, uObj("p1", "v1", "Pod", "default", "p1", "1")))
	require.NoError(t, store.ApplyChange(watch.Added, uObj("p2", "v1", "Pod", "default", "p2", "2")))

	sink := newRecordingSink()
	at := time.Date(2026, 6, 12, 10, 0, 5, 0, time.UTC)
	startedAt := at.Add(-3 * time.Second)
	e := newEngineWithOptions(nil, cdb, sink, withEngineNow(func() time.Time { return at }))

	e.reportCaughtUp(ctx, true /*coldStart*/, 4 /*kinds*/, startedAt, 0 /*resyncedKinds*/, 0 /*resyncedObjects*/)

	sts := sink.statuses()
	require.Len(t, sts, 1)
	got := sts[0]
	require.Equal(t, EngineWatching, got.State)
	require.True(t, got.ColdStart)
	require.Equal(t, 4, got.SyncedKinds)
	require.Equal(t, 2, got.SyncedObjects)
	require.Equal(t, 3*time.Second, got.CaughtUpIn)
}

// discoverGVRs rewrites kind_catalog and prunes orphaned kinds outside the per-object
// stores that ping on write, so it must signal catalog subscribers itself — otherwise a
// kind added/removed since the last run (e.g. a CRD uninstalled during an in-place engine
// restart, where the db handle doesn't change and no object write follows) would stay
// stale until an unrelated write happened to ping.
func TestDiscoverGVRsNotifiesCatalogSubscribers(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	dc := &fakePreferredDiscovery{lists: []*metav1.APIResourceList{{
		GroupVersion: "apps/v1",
		APIResources: []metav1.APIResource{
			{Name: "deployments", Kind: "Deployment", Namespaced: true, Verbs: []string{"list", "watch"}},
		},
	}}}

	// Subscribe before discovery so the ping can't race the subscription (as the
	// GraphQL watch does).
	pings, cancelSub := cdb.Subscribe()
	defer cancelSub()

	entries, err := discoverGVRs(ctx, dc, cdb)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	select {
	case <-pings:
	case <-time.After(2 * time.Second):
		t.Fatal("discoverGVRs did not notify catalog subscribers after rewriting kind_catalog")
	}

	rows, err := cdb.KindCatalog(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "Deployment", rows[0].Kind)
}

// A warm resume's catch-up carries the re-sync breakdown the drivers aggregated:
// how many kinds fell back to a full re-sync and how many bodies they re-pulled,
// which the controller renders into the ResyncComplete message.
func TestEngineReportsResyncFacts(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	sink := newRecordingSink()
	at := time.Date(2026, 6, 12, 10, 0, 8, 0, time.UTC)
	startedAt := at.Add(-8 * time.Second)
	e := newEngineWithOptions(nil, cdb, sink, withEngineNow(func() time.Time { return at }))

	e.reportCaughtUp(ctx, false /*coldStart*/, 120 /*kinds*/, startedAt, 4 /*resyncedKinds*/, 340 /*resyncedObjects*/)

	sts := sink.statuses()
	require.Len(t, sts, 1)
	got := sts[0]
	require.Equal(t, 4, got.ResyncedKinds)
	require.Equal(t, 340, got.ResyncedObjects)
}

// The liveness monitor flags EngineStale (naming the wedged kind) once a driver
// stops proving its watch is alive past the threshold, and recovers to Watching
// once it does again — the engine half of SyncStale, keyed on watch liveness
// rather than object churn, so a quiet-but-healthy kind never trips it.
func TestEngineLivenessMonitorFlagsStaleAndRecovers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)

	clk := &fakeClock{t: time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)}
	store := newObjectsStore(ctx, cdb.ID(), podGVK(), cdb.Writer(), cdb)
	svcGVK := schema.GroupVersionKind{Version: "v1", Kind: "Service"}
	dPod := newKindDriverWithOptions(&fakeSource{}, store, podGVK(), "1", withNow(clk.now))
	dSvc := newKindDriverWithOptions(&fakeSource{}, store, svcGVK, "1", withNow(clk.now))
	dPod.markLive()
	dSvc.markLive()

	sink := newRecordingSink()
	e := newEngineWithOptions(nil, cdb, sink,
		withEngineNow(clk.now),
		withStaleThreshold(5*time.Minute),
		withStaleCheckInterval(time.Millisecond))

	caughtUp := make(chan struct{})
	close(caughtUp)
	done := make(chan struct{})
	go func() { defer close(done); e.livenessMonitor(ctx, []*kindDriver{dPod, dSvc}, caughtUp) }()

	// Keep Service alive but let Pod go quiet past the threshold → stale, naming Pod.
	clk.advance(6 * time.Minute)
	dSvc.markLive()
	waitFor(t, func() bool {
		for _, st := range sink.statuses() {
			if st.State == EngineStale && len(st.StaleKinds) == 1 && st.StaleKinds[0] == "Pod" {
				return true
			}
		}
		return false
	}, "EngineStale flagged, naming the wedged kind")

	// Pod proves liveness again → recovery back to Watching.
	dPod.markLive()
	waitFor(t, func() bool {
		sts := sink.statuses()
		return len(sts) > 0 && sts[len(sts)-1].State == EngineWatching
	}, "recovers to Watching once liveness returns")

	cancel()
	<-done
}

// While a multi-kind cache stays stale overall, a shift in which kinds are wedged
// must re-report: otherwise StaleKinds keeps naming a kind that has recovered and
// omits the newly-wedged one until every watch recovers.
func TestEngineLivenessMonitorReReportsChangedLaggards(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)

	clk := &fakeClock{t: time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)}
	store := newObjectsStore(ctx, cdb.ID(), podGVK(), cdb.Writer(), cdb)
	svcGVK := schema.GroupVersionKind{Version: "v1", Kind: "Service"}
	dPod := newKindDriverWithOptions(&fakeSource{}, store, podGVK(), "1", withNow(clk.now))
	dSvc := newKindDriverWithOptions(&fakeSource{}, store, svcGVK, "1", withNow(clk.now))
	dPod.markLive()
	dSvc.markLive()

	sink := newRecordingSink()
	e := newEngineWithOptions(nil, cdb, sink,
		withEngineNow(clk.now),
		withStaleThreshold(5*time.Minute),
		withStaleCheckInterval(time.Millisecond))

	caughtUp := make(chan struct{})
	close(caughtUp)
	done := make(chan struct{})
	go func() { defer close(done); e.livenessMonitor(ctx, []*kindDriver{dPod, dSvc}, caughtUp) }()

	staleNaming := func(kind string) func() bool {
		return func() bool {
			for _, st := range sink.statuses() {
				if st.State == EngineStale && len(st.StaleKinds) == 1 && st.StaleKinds[0] == kind {
					return true
				}
			}
			return false
		}
	}

	// Pod goes quiet past the threshold, Service stays alive → stale, naming Pod.
	clk.advance(6 * time.Minute)
	dSvc.markLive()
	waitFor(t, staleNaming("Pod"), "stale flagged naming Pod")

	// The wedged set flips while still stale: Pod recovers, Service goes quiet. The
	// report must now name Service, not the stale Pod set.
	clk.advance(6 * time.Minute)
	dPod.markLive()
	waitFor(t, staleNaming("Service"), "re-reports the new laggard set once it changes")

	cancel()
	<-done
}

// The driver invokes onWatch exactly once — the first time it enters its watch
// phase — which is what the engine's Syncing→Watching countdown rides on.
func TestDriverOnWatchFiresOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)
	fs := &fakeSource{
		watchers: make(chan *watch.FakeWatcher, 4),
		// The post-watch re-sync needs a successful metadata diff to reach the
		// second watch phase (p1 unchanged at its applied RV).
		metas:  []objMeta{{UID: "p1", Namespace: "default", Name: "p1", ResourceVersion: "101"}},
		metaRV: "150",
	}

	var mu sync.Mutex
	calls := 0
	var gotResynced bool
	var gotObjects int
	noSleep := func(context.Context, time.Duration) error { return nil }
	d := newKindDriverWithOptions(fs, store, podGVK(), "100", withSleep(noSleep))
	d.onWatch = func(resynced bool, objects int) {
		mu.Lock()
		calls++
		gotResynced, gotObjects = resynced, objects
		mu.Unlock()
	}

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	// First watch establishes (onWatch fires), then ends cleanly with progress
	// → re-sync → second watch (onWatch must not fire again).
	fw := <-fs.watchers
	fw.Add(uObj("p1", "v1", "Pod", "default", "p1", "101"))
	waitFor(t, func() bool { return metaValue(t, cdb, "v1/Pod.last_list_rv") == "101" }, "delta applied")
	fw.Stop()
	<-fs.watchers // second watch established

	mu.Lock()
	got := calls
	require.Equal(t, 1, got, "onWatch fires exactly once per driver")
	require.False(t, gotResynced, "a clean watch-resume (valid seed RV) did no full re-sync")
	require.Equal(t, 0, gotObjects, "and re-pulled no bodies")
	mu.Unlock()

	cancel()
	<-done
}

// A warm resume whose saved cookie is server-expired reports the re-sync, not a
// clean reconnect. The first watch is accepted and only then 410s, so onWatch is
// deferred on that attempt; Run falls back to fullResync and re-enters with a fresh
// RV, where onWatch fires with didResync=true and the re-pulled count. The
// never-firing grace isolates the fix: the only path to onWatch here is the
// post-410 re-sync.
func TestDriverOnWatchReportsResyncAfterExpiredCookie(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	insertObject(t, w, "a", "v1", "Pod") // warm cache: 'a' present at RV 1, now stale
	store := newObjectsStore(ctx, "c1", podGVK(), w, cdb)

	fs := &fakeSource{
		watchers:        make(chan *watch.FakeWatcher, 4),
		autoExpireFirst: true, // first watch 410s → forces the re-sync
		metas:           []objMeta{{UID: "a", Namespace: "default", Name: "a", ResourceVersion: "2"}},
		metaRV:          "200",
		getByName:       map[string]*unstructured.Unstructured{"default/a": uObj("a", "v1", "Pod", "default", "a", "2")},
	}

	var mu sync.Mutex
	calls := 0
	var gotResynced bool
	var gotObjects int
	noSleep := func(context.Context, time.Duration) error { return nil }
	neverGrace := func(time.Duration) (<-chan time.Time, func()) { return nil, func() {} }
	d := newKindDriverWithOptions(fs, store, podGVK(), "5", withSleep(noSleep), withGraceTimer(neverGrace))
	d.onWatch = func(resynced bool, objects int) {
		mu.Lock()
		calls++
		gotResynced, gotObjects = resynced, objects
		mu.Unlock()
	}

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 }, "onWatch fired after the re-sync")

	mu.Lock()
	require.Equal(t, 1, calls, "onWatch fires exactly once")
	require.True(t, gotResynced, "the expired cookie forced a full re-sync — reported, not a clean resume")
	require.Equal(t, 1, gotObjects, "the one changed body the re-sync re-pulled")
	mu.Unlock()

	cancel()
	<-done
}

// A warm resume with a still-valid but quiet cookie (no deltas, no 410) reports a
// clean resume: onWatch is deferred on entry and fires once the resumeGrace elapses
// with didResync=false — the grace being what lets a prompt 410 disqualify the
// clean-resume report first (see TestDriverOnWatchReportsResyncAfterExpiredCookie).
func TestDriverOnWatchFiresOnGraceForQuietResume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)
	fs := &fakeSource{watchers: make(chan *watch.FakeWatcher, 4)} // quiet: never pushes a delta

	grace := make(chan time.Time, 1)
	var mu sync.Mutex
	calls := 0
	var gotResynced bool
	d := newKindDriverWithOptions(fs, store, podGVK(), "100",
		withGraceTimer(func(time.Duration) (<-chan time.Time, func()) { return grace, func() {} }))
	d.onWatch = func(resynced bool, _ int) {
		mu.Lock()
		calls++
		gotResynced = resynced
		mu.Unlock()
	}

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	<-fs.watchers // watch established and accepted; it stays quiet
	mu.Lock()
	require.Equal(t, 0, calls, "a quiet valid-cookie resume doesn't report until the grace elapses")
	mu.Unlock()

	grace <- time.Time{} // grace elapses with no 410 → clean resume
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 }, "onWatch fired on grace")

	mu.Lock()
	require.False(t, gotResynced, "a quiet valid cookie is a clean resume, not a re-sync")
	mu.Unlock()

	cancel()
	<-done
}
