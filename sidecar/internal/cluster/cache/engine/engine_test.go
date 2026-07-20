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
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	mu    sync.Mutex
	lists []*metav1.APIResourceList
	err   error // non-nil alongside usable lists → a partial (incomplete) discovery
}

func (f *fakePreferredDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lists, f.err
}

// setLists swaps the advertised resource lists mid-run (error left unchanged) — used
// to simulate a CRD installed/uninstalled while a run is live.
func (f *fakePreferredDiscovery) setLists(lists []*metav1.APIResourceList) {
	f.mu.Lock()
	f.lists = lists
	f.mu.Unlock()
}

// setResult swaps both the lists and the discovery error — used to flip a partial
// discovery to a complete one once the unavailable API group recovers.
func (f *fakePreferredDiscovery) setResult(lists []*metav1.APIResourceList, err error) {
	f.mu.Lock()
	f.lists = lists
	f.err = err
	f.mu.Unlock()
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
	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	case <-time.After(30 * time.Second):
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

	entries, complete, err := discoverGVRs(ctx, dc, cdb)
	require.NoError(t, err)
	require.True(t, complete, "a clean discovery (no group errors) is complete")
	require.Len(t, entries, 1)

	select {
	case <-pings:
	case <-time.After(30 * time.Second):
		t.Fatal("discoverGVRs did not notify catalog subscribers after rewriting kind_catalog")
	}

	rows, err := cdb.KindCatalog(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "Deployment", rows[0].Kind)
}

// A PARTIAL discovery must not truncate kind_catalog: the momentarily-unavailable
// group's kinds are absent from the pass, and wiping them would make the dashboard
// lose them. Only a complete pass is authoritative and drops a removed kind (P2).
func TestDiscoverGVRsPreservesCatalogOnPartialDiscovery(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	deploymentList := &metav1.APIResourceList{
		GroupVersion: "apps/v1",
		APIResources: []metav1.APIResource{
			{Name: "deployments", Kind: "Deployment", Namespaced: true, Verbs: []string{"list", "watch"}},
		},
	}

	// A complete pass records both kinds.
	dc := &fakePreferredDiscovery{lists: []*metav1.APIResourceList{deploymentList, widgetList("widgets", "Widget")}}
	_, complete, err := discoverGVRs(ctx, dc, cdb)
	require.NoError(t, err)
	require.True(t, complete)
	rows, err := cdb.KindCatalog(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 2, "complete pass records both kinds")

	// A partial pass (example.com errored, only deployment returned) must PRESERVE the
	// widget catalog row rather than truncating it.
	dc.setResult([]*metav1.APIResourceList{deploymentList}, errors.New("example.com group is unavailable"))
	entries, complete, err := discoverGVRs(ctx, dc, cdb)
	require.NoError(t, err)
	require.False(t, complete)
	require.Len(t, entries, 1, "the partial pass only discovered deployment")
	rows, err = cdb.KindCatalog(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 2, "a partial discovery preserves the prior catalog rows")

	// A later complete pass without widget IS authoritative → widget dropped.
	dc.setResult([]*metav1.APIResourceList{deploymentList}, nil)
	_, complete, err = discoverGVRs(ctx, dc, cdb)
	require.NoError(t, err)
	require.True(t, complete)
	rows, err = cdb.KindCatalog(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1, "a complete discovery is authoritative and drops the removed kind")
}

// A COMPLETE discovery whose CRD list fails must NOT reset a known CRD's
// is_crd/schema_json: kind existence comes from /apis (so pruning uninstalled kinds
// stays authoritative even without CRD read access), but the CRD metadata is
// backfilled from the existing catalog so a transient apiextensions read failure
// can't erase previously valid CRD schemas (P2).
func TestDiscoverGVRsPreservesCRDMetadataWhenCRDListFails(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)

	// Seed a CRD row as a prior pass with a good CRD list would have recorded it.
	_, err := cdb.Writer().ExecContext(ctx,
		`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
		 VALUES('example.com/v1', 'Widget', 'widgets', 'Namespaced', 1, '{"type":"object"}')`)
	require.NoError(t, err)

	// A complete discovery re-lists Widget, but fakePreferredDiscovery's CRD read
	// always fails ("no CRDs in test").
	dc := &fakePreferredDiscovery{lists: []*metav1.APIResourceList{widgetList("widgets", "Widget")}}
	_, complete, err := discoverGVRs(ctx, dc, cdb)
	require.NoError(t, err)
	require.True(t, complete, "ServerPreferredResources answered cleanly, so the pass is complete despite the CRD-list failure")

	rows, err := cdb.KindCatalog(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.True(t, rows[0].IsCRD, "is_crd preserved across a CRD-list failure")

	var schemaJSON string
	require.NoError(t, cdb.Writer().QueryRowContext(ctx,
		`SELECT schema_json FROM kind_catalog WHERE api_version='example.com/v1' AND kind='Widget'`,
	).Scan(&schemaJSON))
	assert.Equal(t, `{"type":"object"}`, schemaJSON, "schema_json preserved across a CRD-list failure")
}

func widgetList(resource, kind string) *metav1.APIResourceList {
	return &metav1.APIResourceList{
		GroupVersion: "example.com/v1",
		APIResources: []metav1.APIResource{
			{Name: resource, Kind: kind, Namespaced: true, Verbs: []string{"list", "watch"}},
		},
	}
}

// A discovery trigger makes the reconciler re-walk /apis and reconcile the running
// drivers toward it: launch a driver for a GVR that appeared (a freshly-installed
// CRD) and stop one for a GVR that vanished (an uninstalled CRD), while leaving an
// unchanged kind alone. Drivers are seeded directly into the set (no goroutines);
// onNew and each driver's cancel record what the reconcile did.
func TestEngineDiscoveryLoopReconcilesDriversToDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)

	deploymentGVK := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	deploymentList := &metav1.APIResourceList{
		GroupVersion: "apps/v1",
		APIResources: []metav1.APIResource{
			{Name: "deployments", Kind: "Deployment", Namespaced: true, Verbs: []string{"list", "watch"}},
		},
	}
	widgetGVK := schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"}
	gadgetGVK := schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Gadget"}
	deploymentGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	widgetGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}

	// Start with Deployment (a built-in, stays) and Widget (a CRD, will be removed).
	dc := &fakePreferredDiscovery{lists: []*metav1.APIResourceList{deploymentList, widgetList("widgets", "Widget")}}
	// Zero debounce keeps the test deterministic — a trigger reconciles immediately.
	e := newEngineWithOptions(nil, cdb, newRecordingSink(), withDiscoveryDebounce(0))

	var mu sync.Mutex
	var added []schema.GroupVersionKind
	removed := map[schema.GroupVersionKind]bool{}
	ds := newDriverSet()
	// Seed drivers directly (no goroutine); a pre-closed done lets remove's quiesce
	// wait return immediately, and cancel records the stop. Each carries its GVR (the
	// reconciler's repoint key) like a real driver does.
	closedDone := make(chan struct{})
	close(closedDone)
	seed := func(gvk schema.GroupVersionKind, gvr schema.GroupVersionResource) {
		g := gvk
		ds.mu.Lock()
		ds.byGVK[g] = driverHandle{
			driver: newKindDriverWithOptions(&fakeSource{}, nil, g, "1", withGVR(gvr)),
			cancel: func() { mu.Lock(); removed[g] = true; mu.Unlock() },
			done:   closedDone,
		}
		ds.mu.Unlock()
	}
	seed(deploymentGVK, deploymentGVR)
	seed(widgetGVK, widgetGVR)
	startDriver := func(entry gvrEntry, _ func(bool), _ bool) {
		mu.Lock()
		added = append(added, entry.GVK)
		mu.Unlock()
		seed(entry.GVK, entry.GVR) // mirror driverSet.launch registering the driver
	}

	triggers := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() { defer close(done); e.discoveryLoop(ctx, dc, ds, startDriver, triggers) }()

	// Install Gadget and uninstall Widget, then fire the trigger a watch event produces.
	dc.setLists([]*metav1.APIResourceList{deploymentList, widgetList("gadgets", "Gadget")})
	triggers <- struct{}{}

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.Contains(added, gadgetGVK)
	}, "a trigger launches a driver for the newly-installed CRD")
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return removed[widgetGVK]
	}, "a trigger stops the driver for the uninstalled CRD")

	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	require.NotContains(t, added, deploymentGVK, "an already-running kind is never restarted")
	require.False(t, removed[deploymentGVK], "a still-present kind is never stopped")
}

// fakeDebounceTimer is an event-driven stand-in for the debounce timer: the test
// fires the trailing window by sending on fireC, and each Reset (a trigger restarting
// the window) is observed on resets. Stop always reports "was pending" — the test
// never fires the window before a Stop — so debounceTriggers' drain path is never
// taken, and the exact-standard-library drain contract needn't be modelled.
type fakeDebounceTimer struct {
	fireC  chan time.Time
	resets chan struct{}
}

func (f *fakeDebounceTimer) C() <-chan time.Time      { return f.fireC }
func (f *fakeDebounceTimer) Stop() bool               { return true }
func (f *fakeDebounceTimer) Reset(time.Duration) bool { f.resets <- struct{}{}; return true }

// debounceTriggers is a TRAILING window: each further trigger resets it, so a long
// install burst reconciles once after it ends rather than at a fixed delay from the
// first trigger. Event-driven — a fake timer lets the test observe a Reset per trigger
// (the trailing-window invariant) and fire the window deterministically, asserting the
// behavior directly instead of inferring it from wall-clock margins.
func TestDebounceTriggersResetsWindowOnEachTrigger(t *testing.T) {
	ft := &fakeDebounceTimer{fireC: make(chan time.Time, 1), resets: make(chan struct{})}
	e := newEngineWithOptions(nil, migratedCDB(t), newRecordingSink(),
		withDebounceTimer(func(time.Duration) resettableTimer { return ft }))
	triggers := make(chan struct{}, 8)

	done := make(chan bool, 1)
	go func() { done <- e.debounceTriggers(context.Background(), triggers) }()

	// Every trigger must RESTART the window (a Reset), not let a fixed window from the
	// first trigger elapse. Four triggers → four resets, each observed before the next
	// trigger, so the invariant is checked with no wall-clock wait.
	for i := 0; i < 4; i++ {
		triggers <- struct{}{}
		select {
		case <-ft.resets:
		case <-time.After(30 * time.Second):
			t.Fatalf("trigger %d did not reset the debounce window", i)
		}
	}

	// The window hasn't fired (only the test fires it), and the only other exit is
	// ctx cancel — so debounceTriggers must still be waiting, never a fixed delay.
	select {
	case got := <-done:
		t.Fatalf("debounce returned (%v) before the trailing window elapsed", got)
	default:
	}

	// Fire the trailing window → debounce ends and signals one reconcile.
	ft.fireC <- time.Time{}
	select {
	case got := <-done:
		require.True(t, got, "debounce returns true when the window elapses cleanly")
	case <-time.After(30 * time.Second):
		t.Fatal("debounceTriggers did not return after the window fired")
	}
}

// A CRD recreated within the debounce window under the same group/version/kind but a
// DIFFERENT resource plural changes the GVR while the GVK is stable. The reconciler
// keys drivers by GVK, so without comparing the GVR it would keep the old driver
// watching the now-removed endpoint, leaving the kind wedged with no live driver. It
// must instead stop the stale-GVR driver and relaunch against the new endpoint (P2).
func TestReconcileDiscoveryRepointsDriverOnGVRChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)

	widgetGVK := schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"}
	oldGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	newGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "gizmos"}

	// Discovery now serves Widget under a different resource plural (gizmos).
	dc := &fakePreferredDiscovery{lists: []*metav1.APIResourceList{widgetList("gizmos", "Widget")}}
	e := newEngineWithOptions(nil, cdb, newRecordingSink())

	ds := newDriverSet()
	closedDone := make(chan struct{})
	close(closedDone)
	var removed bool
	// A running driver still watching the OLD GVR.
	ds.byGVK[widgetGVK] = driverHandle{
		driver: newKindDriverWithOptions(&fakeSource{}, nil, widgetGVK, "1", withGVR(oldGVR)),
		cancel: func() { removed = true }, done: closedDone,
	}

	var added []gvrEntry
	startDriver := func(entry gvrEntry, retire func(bool), _ bool) {
		added = append(added, entry)
		ds.mu.Lock()
		ds.byGVK[entry.GVK] = driverHandle{
			driver: newKindDriverWithOptions(&fakeSource{}, nil, entry.GVK, "1", withGVR(entry.GVR)),
			cancel: func() {}, done: closedDone, retire: retire,
		}
		ds.mu.Unlock()
	}

	e.reconcileDiscovery(ctx, dc, ds, startDriver)

	require.True(t, removed, "the stale-GVR driver is stopped")
	require.Len(t, added, 1, "the kind is relaunched exactly once")
	require.Equal(t, newGVR, added[0].GVR, "relaunched against the new endpoint")
	gvr, ok := ds.gvr(widgetGVK)
	require.True(t, ok, "a driver for the kind is still running")
	require.Equal(t, newGVR, gvr, "the running driver now watches the new GVR")
}

// A PARTIAL discovery (usable lists + a group error) is a SINGLE pass with no internal
// retry: it launches the kinds it did see and returns, and it does NOT remove or prune
// (an omitted kind may just be in the transiently-unavailable group). The next trigger or
// the discovery poll re-walks and completes the set — correctness rides on the backstop,
// not on any one pass being exact.
func TestReconcileDiscoveryPartialPassLaunchesWithoutRemoving(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	w := cdb.Writer()

	deploymentGVK := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	deploymentList := &metav1.APIResourceList{
		GroupVersion: "apps/v1",
		APIResources: []metav1.APIResource{
			{Name: "deployments", Kind: "Deployment", Namespaced: true, Verbs: []string{"list", "watch"}},
		},
	}
	widgetGVK := schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"}
	widgetGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}

	// A running Widget driver with a cached row; the partial pass omits the widget group,
	// so Widget must be neither removed nor pruned.
	insertObject(t, w, "w1", "example.com/v1", "Widget")
	dc := &fakePreferredDiscovery{
		lists: []*metav1.APIResourceList{deploymentList},
		err:   errors.New("example.com group is unavailable"),
	}
	e := newEngineWithOptions(nil, cdb, newRecordingSink())

	ds := newDriverSet()
	closedDone := make(chan struct{})
	close(closedDone)
	ds.byGVK[widgetGVK] = driverHandle{
		driver: newKindDriverWithOptions(&fakeSource{}, nil, widgetGVK, "1", withGVR(widgetGVR)),
		cancel: func() {}, done: closedDone,
	}

	var added []schema.GroupVersionKind
	startDriver := func(entry gvrEntry, _ func(bool), _ bool) {
		added = append(added, entry.GVK)
		ds.mu.Lock()
		ds.byGVK[entry.GVK] = driverHandle{driver: newKindDriverWithOptions(&fakeSource{}, nil, entry.GVK, "1", withGVR(entry.GVR)), cancel: func() {}, done: closedDone}
		ds.mu.Unlock()
	}

	// Single pass — returns immediately, no retry loop.
	e.reconcileDiscovery(ctx, dc, ds, startDriver)

	require.Equal(t, []schema.GroupVersionKind{deploymentGVK}, added, "the partial pass launches the kind it saw")
	require.True(t, ds.has(widgetGVK), "a partial pass does not remove a kind omitted by the errored group")
	require.Equal(t, 1, countObjectsByKind(t, w, "Widget", "example.com/v1"), "and does not prune its rows")
}

// The authoritative prune runs on EVERY complete pass, not only when this pass
// removed a driver — so orphan rows a prior (failed) prune left behind, whose driver
// is already gone, are still cleaned up rather than lingering until an engine restart
// (P2). Here nothing is removed this pass, yet the pre-existing orphan is pruned.
func TestReconcileDiscoveryPrunesOrphansOnEveryCompletePass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	w := cdb.Writer()

	// An orphan object for a kind discovery won't return and that has no driver — as a
	// prior failed prune (or a CRD uninstalled while the engine was down) would leave.
	insertObject(t, w, "ghost", "example.com/v1", "Widget")
	require.Equal(t, 1, countObjectsByKind(t, w, "Widget", "example.com/v1"))

	deploymentGVK := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	deploymentList := &metav1.APIResourceList{
		GroupVersion: "apps/v1",
		APIResources: []metav1.APIResource{
			{Name: "deployments", Kind: "Deployment", Namespaced: true, Verbs: []string{"list", "watch"}},
		},
	}
	// A complete discovery listing only deployment (no widget) — nothing to remove.
	dc := &fakePreferredDiscovery{lists: []*metav1.APIResourceList{deploymentList}}
	e := newEngineWithOptions(nil, cdb, newRecordingSink())

	ds := newDriverSet()
	closedDone := make(chan struct{})
	close(closedDone)
	deploymentGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	ds.byGVK[deploymentGVK] = driverHandle{
		driver: newKindDriverWithOptions(&fakeSource{}, nil, deploymentGVK, "1", withGVR(deploymentGVR)),
		cancel: func() {}, done: closedDone,
	}

	// Complete pass returns after one iteration; deployment already running, nothing removed.
	e.reconcileDiscovery(ctx, dc, ds, func(gvrEntry, func(bool), bool) {})

	require.Equal(t, 0, countObjectsByKind(t, w, "Widget", "example.com/v1"),
		"the orphan is pruned even though this complete pass removed no driver")
	require.True(t, ds.has(deploymentGVK), "the still-present kind keeps its driver")
}

// A removed initial driver's catch-up retirement runs only AFTER its stale rows are
// pruned: otherwise a removal that closes out the Syncing→Watching milestone would emit
// the permanent catch-up event while the removed kind's rows are still present (P2).
func TestReconcileDiscoveryRetiresRemovedDriverAfterPrune(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	w := cdb.Writer()

	widgetGVK := schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"}
	widgetGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	deploymentGVK := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	deploymentGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	deploymentList := &metav1.APIResourceList{
		GroupVersion: "apps/v1",
		APIResources: []metav1.APIResource{
			{Name: "deployments", Kind: "Deployment", Namespaced: true, Verbs: []string{"list", "watch"}},
		},
	}

	// Widget has cached rows and a running driver; a complete discovery WITHOUT widget
	// removes its driver and prunes its rows.
	insertObject(t, w, "w1", "example.com/v1", "Widget")
	dc := &fakePreferredDiscovery{lists: []*metav1.APIResourceList{deploymentList}}
	e := newEngineWithOptions(nil, cdb, newRecordingSink())

	ds := newDriverSet()
	closedDone := make(chan struct{})
	close(closedDone)
	ds.byGVK[deploymentGVK] = driverHandle{
		driver: newKindDriverWithOptions(&fakeSource{}, nil, deploymentGVK, "1", withGVR(deploymentGVR)),
		cancel: func() {}, done: closedDone,
	}
	rowsAtRetire := -1
	ds.byGVK[widgetGVK] = driverHandle{
		driver: newKindDriverWithOptions(&fakeSource{}, nil, widgetGVK, "1", withGVR(widgetGVR)),
		cancel: func() {}, done: closedDone,
		retire: func(bool) { rowsAtRetire = countObjectsByKind(t, w, "Widget", "example.com/v1") },
	}

	e.reconcileDiscovery(ctx, dc, ds, func(gvrEntry, func(bool), bool) {})

	require.Equal(t, 0, rowsAtRetire, "the retirement callback runs only after the removed kind's rows are pruned")
	require.False(t, ds.has(widgetGVK), "the removed driver is dropped")
	require.Equal(t, 0, countObjectsByKind(t, w, "Widget", "example.com/v1"), "the removed kind's rows are pruned")
}

// A complete discovery sweeps the resume cookie of a vanished kind along with its object
// rows: left behind, a later re-registration of that GVR (same server, unchanged objects)
// would resume its watch from the stale RV and skip the initial LIST, leaving the cache
// permanently empty for that kind. A live kind's cookie is preserved (P1).
func TestReconcileDiscoveryPrunesOrphanedResumeCookies(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)

	deploymentGVK := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	deploymentGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	widgetGVK := schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"}
	deploymentList := &metav1.APIResourceList{
		GroupVersion: "apps/v1",
		APIResources: []metav1.APIResource{
			{Name: "deployments", Kind: "Deployment", Namespaced: true, Verbs: []string{"list", "watch"}},
		},
	}

	// Resume cookies for a live kind (Deployment) and a vanished one (Widget).
	require.NoError(t, persistListRVMeta(ctx, cdb.Writer(), deploymentGVK, "100"))
	require.NoError(t, persistListRVMeta(ctx, cdb.Writer(), widgetGVK, "42"))

	// A complete discovery serving only Deployment (Widget is gone, no driver).
	dc := &fakePreferredDiscovery{lists: []*metav1.APIResourceList{deploymentList}}
	e := newEngineWithOptions(nil, cdb, newRecordingSink())

	ds := newDriverSet()
	closedDone := make(chan struct{})
	close(closedDone)
	ds.byGVK[deploymentGVK] = driverHandle{
		driver: newKindDriverWithOptions(&fakeSource{}, nil, deploymentGVK, "1", withGVR(deploymentGVR)),
		cancel: func() {}, done: closedDone,
	}

	e.reconcileDiscovery(ctx, dc, ds, func(gvrEntry, func(bool), bool) {})

	widgetRV, err := readLastListRV(ctx, cdb.Writer(), widgetGVK)
	require.NoError(t, err)
	require.Equal(t, "", widgetRV, "the vanished kind's resume cookie is swept")

	// The companion last_list_at row is swept too.
	var atCount int
	require.NoError(t, cdb.Writer().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM cluster_meta WHERE key=?`, lastListAtKey(widgetGVK)).Scan(&atCount))
	require.Equal(t, 0, atCount, "the vanished kind's last_list_at is swept too")

	depRV, err := readLastListRV(ctx, cdb.Writer(), deploymentGVK)
	require.NoError(t, err)
	require.Equal(t, "100", depRV, "the live kind's resume cookie is preserved")
}

// A GVR repoint relaunches the driver COLD and DURABLY invalidates the old resume cookie
// first: the old rows survive the repoint (same GVK), so a surviving cookie would let a
// restart in the relaunch window resume the new endpoint from the stale cluster-global RV
// and skip the initial LIST. forceCold keeps this run cold; the cookie delete makes it
// durable across a restart (P2).
func TestReconcileDiscoveryRepointInvalidatesCookieAndRelaunchesCold(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)

	widgetGVK := schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"}
	oldGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}

	// A persisted resume cookie for the kind, as a prior driver would have left.
	require.NoError(t, persistListRVMeta(ctx, cdb.Writer(), widgetGVK, "12345"))

	// Discovery serves Widget under a new resource plural (gizmos) → repoint.
	dc := &fakePreferredDiscovery{lists: []*metav1.APIResourceList{widgetList("gizmos", "Widget")}}
	e := newEngineWithOptions(nil, cdb, newRecordingSink())

	ds := newDriverSet()
	closedDone := make(chan struct{})
	close(closedDone)
	ds.byGVK[widgetGVK] = driverHandle{
		driver: newKindDriverWithOptions(&fakeSource{}, nil, widgetGVK, "1", withGVR(oldGVR)),
		cancel: func() {}, done: closedDone,
	}

	var forceCold []bool
	startDriver := func(entry gvrEntry, _ func(bool), fc bool) {
		forceCold = append(forceCold, fc)
		ds.mu.Lock()
		ds.byGVK[entry.GVK] = driverHandle{driver: newKindDriverWithOptions(&fakeSource{}, nil, entry.GVK, "1", withGVR(entry.GVR)), cancel: func() {}, done: closedDone}
		ds.mu.Unlock()
	}

	e.reconcileDiscovery(ctx, dc, ds, startDriver)

	require.Equal(t, []bool{true}, forceCold, "the repoint relaunches the replacement COLD so it full-LISTs the new endpoint")
	rv, err := readLastListRV(ctx, cdb.Writer(), widgetGVK)
	require.NoError(t, err)
	require.Equal(t, "", rv, "the repoint durably deletes the old resume cookie so a restart can't resume the new endpoint from it")
}

// A GVR repoint TRANSFERS the old driver's catch-up token to the replacement rather than
// firing it: if the old driver was an initial one still pre-watch, its Syncing→Watching
// obligation must carry to the replacement, so the engine can't report Watching (caught up)
// while the cache still holds the old endpoint's rows and the replacement's full LIST hasn't
// finished (P2).
func TestReconcileDiscoveryTransfersCatchUpTokenOnRepoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)

	widgetGVK := schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"}
	oldGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	newGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "gizmos"}

	dc := &fakePreferredDiscovery{lists: []*metav1.APIResourceList{widgetList("gizmos", "Widget")}}
	e := newEngineWithOptions(nil, cdb, newRecordingSink())

	ds := newDriverSet()
	closedDone := make(chan struct{})
	close(closedDone)

	// The old driver's token records every call — it must NOT be fired during the repoint
	// (the obligation is transferred, not discharged).
	var tokenCalls []bool
	token := func(gone bool) { tokenCalls = append(tokenCalls, gone) }
	ds.byGVK[widgetGVK] = driverHandle{
		driver: newKindDriverWithOptions(&fakeSource{}, nil, widgetGVK, "1", withGVR(oldGVR)),
		cancel: func() {}, done: closedDone, retire: token,
	}

	var replacementTokens []func(bool)
	startDriver := func(entry gvrEntry, retire func(bool), _ bool) {
		replacementTokens = append(replacementTokens, retire)
		ds.mu.Lock()
		ds.byGVK[entry.GVK] = driverHandle{
			driver: newKindDriverWithOptions(&fakeSource{}, nil, entry.GVK, "1", withGVR(entry.GVR)),
			cancel: func() {}, done: closedDone, retire: retire,
		}
		ds.mu.Unlock()
	}

	e.reconcileDiscovery(ctx, dc, ds, startDriver)

	require.Empty(t, tokenCalls, "the old driver's catch-up token is NOT fired on a repoint")
	require.Len(t, replacementTokens, 1, "the kind is relaunched once")
	require.NotNil(t, replacementTokens[0], "the replacement inherits the catch-up obligation")
	gvr, ok := ds.gvr(widgetGVK)
	require.True(t, ok)
	require.Equal(t, newGVR, gvr, "the replacement watches the new endpoint")

	// The replacement holds the SAME token: firing it (as its watch phase would) records
	// through to the original, so the milestone was gated on the replacement's catch-up.
	replacementTokens[0](false)
	require.Equal(t, []bool{false}, tokenCalls, "the replacement carries the transferred obligation")
}

// driverSet.remove is synchronous: it cancels the driver and blocks until its
// goroutine has fully stopped (quiesced, so a later object prune can't be undone by an
// in-flight watch event) before returning. It RETURNS the driver's catch-up token
// rather than firing it, so the caller controls its fate — fire it (a vanished kind)
// after pruning, or transfer it to a replacement (a GVR repoint) — P1 / P2.
func TestDriverSetRemoveQuiescesThenReturnsRetire(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, cdb.ID(), podGVK(), cdb.Writer(), cdb)
	fs := &fakeSource{watchers: make(chan *watch.FakeWatcher, 4), listRV: "1"}
	d := newKindDriverWithOptions(fs, store, podGVK(), "1")

	ds := newDriverSet()
	var exited atomic.Bool
	var retiredGone atomic.Int32 // -1 unset; 0 = retire(false); 1 = retire(true)
	retiredGone.Store(-1)
	// onExit must run before remove returns (proving quiesce); the token is the retire.
	ds.launch(ctx, d,
		func(error) { exited.Store(true) },
		func(gone bool) {
			if gone {
				retiredGone.Store(1)
			} else {
				retiredGone.Store(0)
			}
		})

	<-fs.watchers // the driver reached its watch phase (running)
	require.True(t, ds.has(podGVK()))

	retire := ds.remove(podGVK())

	require.True(t, exited.Load(), "remove blocks until the driver goroutine has stopped (quiesced)")
	require.Equal(t, int32(-1), retiredGone.Load(), "remove returns the token WITHOUT firing it — the caller controls timing")
	require.False(t, ds.has(podGVK()), "removed from the set")

	require.NotNil(t, retire, "an initial driver's catch-up token is returned for the caller to fire or transfer")
	retire(true)
	require.Equal(t, int32(1), retiredGone.Load(), "invoking the returned token retires the catch-up obligation")
}

// The trigger watch is a best-effort accelerator: it signals a re-discovery ONLY on a
// delta (a CRD/APIService add/change/remove), for low latency. It does NOT signal on the
// first LIST, a 410, or a plain reconnect — those are left to the discovery poll backstop,
// so an ordinary reconnect never redundantly re-walks discovery. It reuses the driver's
// LIST→RetryWatcher resume idiom.
func TestEngineWatchTriggerSourceSignalsOnDeltaOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fs := &fakeSource{watchers: make(chan *watch.FakeWatcher, 4), listRV: "10"}
	e := newEngineWithOptions(nil, migratedCDB(t), newRecordingSink())

	signals := make(chan struct{}, 16)
	done := make(chan struct{})
	go func() { defer close(done); e.watchTriggerSource(ctx, fs, func() { signals <- struct{}{} }) }()

	// The first watch establishes; the first LIST does NOT signal (the poll covers the
	// startup gap).
	fw := <-fs.watchers
	select {
	case <-signals:
		t.Fatal("the first LIST must not signal — only deltas do")
	case <-time.After(100 * time.Millisecond):
	}

	// A CRD add arrives as a watch delta → signal.
	fw.Add(uObj("crd1", "apiextensions.k8s.io/v1", "CustomResourceDefinition", "", "widgets.example.com", "11"))
	select {
	case <-signals:
	case <-time.After(30 * time.Second):
		t.Fatal("no signal on a CRD delta")
	}

	// A 410 closes the RetryWatcher; the loop re-LISTs and re-watches but does NOT signal
	// (the poll backstop, not this watch, catches anything the reopened watch skipped).
	fw.Error(expiredStatus())
	<-fs.watchers // the re-established watch
	select {
	case <-signals:
		t.Fatal("a 410 re-establishment must not signal — the poll is the backstop")
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	<-done
}

// The trigger-watch seed LIST needs only the collection resourceVersion, so it must
// cap the LIST at one item — otherwise every (re)connect materializes the whole
// CRD/APIService collection (CRD bodies embed 100s-of-KB OpenAPI schemas) just to read
// the list RV.
func TestEngineWatchTriggerSourceSeedListIsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fs := &fakeSource{watchers: make(chan *watch.FakeWatcher, 4), listRV: "10"}
	e := newEngineWithOptions(nil, migratedCDB(t), newRecordingSink())

	done := make(chan struct{})
	go func() { defer close(done); e.watchTriggerSource(ctx, fs, func() {}) }()

	<-fs.watchers // the first watch established, so the seed LIST has run
	cancel()
	<-done

	limits := fs.limitsSeen()
	require.NotEmpty(t, limits)
	require.Equal(t, int64(1), limits[0], "the seed LIST caps at one item — only the list RV is needed")
}

// A trigger source that permits LIST but denies WATCH (an RBAC split) makes
// RetryWatcher terminate without progress. watchTriggerSource must back off between
// re-LIST attempts rather than hot-looping the API server (P1).
func TestEngineWatchTriggerSourceBacksOffWhenWatchDenied(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fs := &fakeSource{
		listRV: "10",
		watchErr: apierrors.NewForbidden(
			schema.GroupResource{Group: "apiextensions.k8s.io", Resource: "customresourcedefinitions"},
			"", errors.New("watch denied")),
	}
	rec, snapshot := recordingSleep()
	e := newEngineWithOptions(nil, migratedCDB(t), newRecordingSink(), withEngineSleep(rec))

	done := make(chan struct{})
	go func() { defer close(done); e.watchTriggerSource(ctx, fs, func() {}) }()

	waitFor(t, func() bool { return len(snapshot()) >= 2 }, "backs off between terminal watch failures")
	sleeps := snapshot()
	require.Equal(t, e.backoffInit, sleeps[0], "first terminal watch failure backs off by the initial")
	require.Equal(t, 2*e.backoffInit, sleeps[1], "and the backoff grows on the next")

	cancel()
	<-done
}

// The discovery poll is the pull-based backstop: it fires the reconcile trigger on a
// fixed cadence regardless of watch activity, so anything the best-effort trigger watches
// miss is still reconciled within one interval.
func TestDiscoveryPollLoopFiresOnInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := newEngineWithOptions(nil, migratedCDB(t), newRecordingSink(), withDiscoveryPoll(20*time.Millisecond))

	signals := make(chan struct{}, 16)
	done := make(chan struct{})
	go func() { defer close(done); e.discoveryPollLoop(ctx, func() { signals <- struct{}{} }) }()

	for i := 0; i < 2; i++ {
		select {
		case <-signals:
		case <-time.After(30 * time.Second):
			t.Fatal("discovery poll did not fire on its interval")
		}
	}
	cancel()
	<-done
}

// The per-driver periodic resync is the object-level backstop: a watch alive past
// resyncPeriod ends itself so Run falls back to a full re-sync, reconciling any drift the
// best-effort watch silently missed. Here a resume (no initial LIST) is followed, after a
// short resync period, by a full LIST.
func TestDriverPeriodicResyncForcesFullResync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, cdb.ID(), podGVK(), cdb.Writer(), cdb)
	fs := &fakeSource{watchers: make(chan *watch.FakeWatcher, 4), listRV: "200"}
	d := newKindDriverWithOptions(fs, store, podGVK(), "100", withResyncPeriod(50*time.Millisecond))

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	<-fs.watchers // the resume watch established
	list, _, _ := fs.counts()
	require.Equal(t, 0, list, "the resume did not LIST")

	// The resync timer ends the quiet watch → Run does a full re-sync (a LIST).
	waitFor(t, func() bool { l, _, _ := fs.counts(); return l >= 1 }, "periodic resync forces a full LIST")

	cancel()
	<-done
}

// objectsStore.ResumeRV is the resume-eligibility guard: it returns the persisted cookie
// only when the cache still holds objects of the kind to apply deltas onto, so a cookie
// that outlived its objects (a kind removed then re-added) returns "" and forces a cold LIST.
func TestObjectsStoreResumeRVGatesOnObjectExistence(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	gvk := schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"}
	s := newObjectsStore(ctx, cdb.ID(), gvk, w, cdb)

	// A persisted cookie but no objects → not resumable (cold).
	require.NoError(t, persistListRVMeta(ctx, w, gvk, "42"))
	rv, err := s.ResumeRV(ctx)
	require.NoError(t, err)
	require.Equal(t, "", rv, "a cookie with no cached objects is ineligible")

	// With an object present, the cookie resumes.
	insertObject(t, w, "w1", "example.com/v1", "Widget")
	rv, err = s.ResumeRV(ctx)
	require.NoError(t, err)
	require.Equal(t, "42", rv, "a cookie backed by cached objects resumes")
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
	drivers := func() []*kindDriver { return []*kindDriver{dPod, dSvc} }
	go func() { defer close(done); e.livenessMonitor(ctx, drivers, caughtUp) }()

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
	drivers := func() []*kindDriver { return []*kindDriver{dPod, dSvc} }
	go func() { defer close(done); e.livenessMonitor(ctx, drivers, caughtUp) }()

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

// staleLaggards sorts its output so that an unchanged stale set compares equal
// across snapshots — the driver set is a map (random iteration order), and without
// the sort livenessMonitor's slices.Equal dedup would treat a mere permutation as a
// change and re-emit the same stale report (P2).
func TestStaleLaggardsSortedForStableDedup(t *testing.T) {
	old := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)
	now := old.Add(10 * time.Minute)
	mk := func(kind string) *kindDriver {
		gvk := schema.GroupVersionKind{Version: "v1", Kind: kind}
		d := newKindDriverWithOptions(&fakeSource{}, nil, gvk, "1", withNow(func() time.Time { return old }))
		d.markLive() // liveAt = old → stale against now past the 5m threshold
		return d
	}
	// Pass drivers in unsorted order (as a map snapshot would); expect sorted output.
	drivers := []*kindDriver{mk("Service"), mk("Pod"), mk("Node"), mk("ConfigMap")}
	got := staleLaggards(drivers, now, 5*time.Minute)
	require.Equal(t, []string{"ConfigMap", "Node", "Pod", "Service"}, got, "laggards are sorted regardless of driver order")
}

// A driver that hasn't entered its watch phase yet (zero liveAt) is judged from its
// createdAt: it gets a bounded startup grace, so a mid-run kind still doing its first
// sync isn't falsely flagged — but a kind that can never LIST/watch is flagged once
// the grace expires, rather than hiding the engine as healthy forever (P2 / P1(b)).
func TestStaleLaggardsBoundsNeverWatchedGrace(t *testing.T) {
	now := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)
	mk := func(kind string, created time.Time) *kindDriver {
		gvk := schema.GroupVersionKind{Version: "v1", Kind: kind}
		d := newKindDriverWithOptions(&fakeSource{}, nil, gvk, "1", withNow(func() time.Time { return created }))
		require.True(t, d.liveAt().IsZero(), "a never-watched driver has zero liveAt")
		return d
	}
	// Created 2m ago, still starting up → within the 5m grace → not flagged.
	starting := mk("Starting", now.Add(-2*time.Minute))
	// Created 10m ago, never reached watch phase → past the grace → flagged.
	stuck := mk("Stuck", now.Add(-10*time.Minute))

	got := staleLaggards([]*kindDriver{starting, stuck}, now, 5*time.Minute)
	require.Equal(t, []string{"Stuck"}, got, "a never-watched driver is flagged only after its startup grace")
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
