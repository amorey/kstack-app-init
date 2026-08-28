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

// Shared fixtures: the pool and the store directory a Service is built over, the api
// server a sweep reads, and the waits a test settles on.
package kubesync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	gobuswatch "github.com/amorey/gobus/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/supervisor"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// --- The pool ---

// fakePool stands in for the connection pool: one lease per context, handed back to
// every claim so a test can vouch for an identity before or after a session takes one.
type fakePool struct {
	mu     sync.Mutex
	leases map[string]*fakeLease
	hub    *gobuswatch.Hub[string, kubeconn.State]
}

func newFakePool() *fakePool {
	return &fakePool{leases: map[string]*fakeLease{}, hub: gobuswatch.New[string, kubeconn.State]()}
}

func (p *fakePool) Acquire(contextName string) kubeconn.Lease {
	l := p.lease(contextName)
	l.mu.Lock()
	l.claims++
	l.mu.Unlock()
	return l
}

// lease returns the claim handed out for a context, building it if nothing has claimed
// one yet — so a test can vouch for an identity before the session arms.
func (p *fakePool) lease(contextName string) *fakeLease {
	p.mu.Lock()
	defer p.mu.Unlock()
	l, ok := p.leases[contextName]
	if !ok {
		l = &fakeLease{contextName: contextName, hub: p.hub, dialed: testutil.NewProbe[string](64)}
		p.leases[contextName] = l
	}
	return l
}

// fakeLease answers ConnFor from whatever the test vouched for, and publishes on the
// same hub kubeconn does, so AwaitConnFor's attach-then-check ordering is exercised for real.
type fakeLease struct {
	contextName string
	hub         *gobuswatch.Hub[string, kubeconn.State]
	// dialed carries every ConnFor: the wake loop looking at a frame, and the runs it wakes.
	dialed *testutil.Probe[string]

	mu       sync.Mutex
	claims   int
	released int
	uid      string
	conn     *kubeconn.Connection
	err      error
	watches  int
}

// refuse makes this lease answer with err — the pool reporting something that is neither
// "no connection yet" nor a mismatched identity.
func (l *fakeLease) refuse(err error) {
	l.mu.Lock()
	l.err = err
	l.mu.Unlock()
	_ = l.hub.Sender().Send(l.contextName, kubeconn.State{})
}

// vouch makes this lease answer for serverUID over an api server serving nothing, for a
// test about arming rather than about what a sweep reads. A real one, because a sweep dials
// whatever the lease hands it.
func (l *fakeLease) vouch(t *testing.T, serverUID string) { l.connect(t, newFakeCluster(t), serverUID) }

// connect makes this lease answer for serverUID over a connection to cluster.
func (l *fakeLease) connect(t *testing.T, cluster *fakeCluster, serverUID string) {
	l.hand(cluster.connection(t), serverUID)
}

// hand publishes a connection and wakes whoever is waiting on this claim. The one it
// replaces is retired, as the pool retires what it rebuilds.
func (l *fakeLease) hand(conn *kubeconn.Connection, serverUID string) {
	l.mu.Lock()
	prev := l.conn
	l.uid, l.conn = serverUID, conn
	l.mu.Unlock()
	if prev != nil {
		prev.Retire()
	}
	_ = l.hub.Sender().Send(l.contextName, kubeconn.State{})
}

// drop takes the connection away, which is what a cluster going unreachable looks like.
func (l *fakeLease) drop() {
	l.mu.Lock()
	prev := l.conn
	l.conn, l.uid = nil, ""
	l.mu.Unlock()
	if prev != nil {
		prev.Retire()
	}
	_ = l.hub.Sender().Send(l.contextName, kubeconn.State{})
}

func (l *fakeLease) held() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.claims - l.released
}

func (l *fakeLease) Conn(context.Context) (*kubeconn.Connection, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return nil, l.err
	}
	if l.conn == nil {
		return nil, kubeconn.ErrNoConnection
	}
	return l.conn, nil
}

func (l *fakeLease) ConnFor(ctx context.Context, serverUID string) (*kubeconn.Connection, error) {
	l.dialed.Fire(serverUID)
	conn, err := l.Conn(ctx)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.uid != serverUID {
		return nil, kubeconn.ErrIdentityMismatch
	}
	return conn, nil
}

func (l *fakeLease) State() kubeconn.State { return kubeconn.State{} }

func (l *fakeLease) WatchState() kubeconn.StateSubscription {
	l.mu.Lock()
	l.watches++
	l.mu.Unlock()
	return l.hub.Watch(l.contextName)
}

// watchers counts who is listening for a connection: a session's own wake loop, plus one
// per worker parked in AwaitConnFor — which is how a test knows a worker got that far.
func (l *fakeLease) watchers() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.watches
}

func (l *fakeLease) Departed() bool { return false }

func (l *fakeLease) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.released++
}

// --- The cluster ---

// fakeCluster is an api server enough of one for a sweep: the two discovery documents, one
// per group-version, and the CRD collection behind IsCRD.
type fakeCluster struct {
	baseURL *url.URL
	client  *http.Client
	dyn     *dynamicfake.FakeDynamicClient

	// transport is what every discovery read goes through: it reports the reads, and parks
	// the one path a test holds. reads and cancelled are its two probes, kept here because
	// that is where a test reaches for them.
	transport *holdTransport
	reads     *testutil.Probe[string]
	cancelled *testutil.Probe[string]
	// listed carries every collection LIST a mirror asks for.
	listed *testutil.Probe[string]

	mu sync.Mutex
	// groups is the non-core groups, in the order /apis serves them; resources is one
	// document per group-version; broken is the paths answering 500; raw is the paths
	// answering a body of the test's own choosing.
	groups    []metav1.APIGroup
	resources map[string]*metav1.APIResourceList
	broken    map[string]bool
	raw       map[string]string
}

func newFakeCluster(t *testing.T) *fakeCluster {
	t.Helper()
	c := &fakeCluster{
		reads:     testutil.NewProbe[string](32),
		cancelled: testutil.NewProbe[string](32),
		listed:    testutil.NewProbe[string](32),
		resources: map[string]*metav1.APIResourceList{},
		broken:    map[string]bool{},
		raw:       map[string]string{},
		dyn:       newFakeDynamic(),
	}

	srv := httptest.NewServer(http.HandlerFunc(c.serveHTTP))
	t.Cleanup(srv.Close)

	base, err := url.Parse(srv.URL)
	require.NoError(t, err)
	c.transport = &holdTransport{
		base: srv.Client().Transport, reads: c.reads, cancelled: c.cancelled,
	}
	c.baseURL, c.client = base, &http.Client{Transport: c.transport}
	return c
}

// connection is what a lease hands a run: the raw client the discovery documents are read
// over, and the dynamic client the lists and watches go through. Built the way the pool
// builds one, so retiring it fires Done for whoever holds it; the clients are then swapped for
// this cluster's own, since its lists and watches are served in memory rather than over HTTP.
func (c *fakeCluster) connection(t *testing.T) *kubeconn.Connection {
	t.Helper()
	conn, err := kubeconn.NewConnection(&rest.Config{Host: c.baseURL.String()})
	require.NoError(t, err)
	conn.HTTPClient, conn.Dynamic = c.client, c.dyn
	return conn
}

// serve declares one group-version's resources, registering the group with /apis when it is
// not the core one.
func (c *fakeCluster) serve(groupVersion string, rs ...metav1.APIResource) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resources[groupVersion] = &metav1.APIResourceList{GroupVersion: groupVersion, APIResources: rs}
	if group, _, found := strings.Cut(groupVersion, "/"); found {
		c.ensureGroupLocked(group, groupVersion)
	}
}

// group declares a group serving several versions, the first of which is preferred. Call it
// before serving them, since serve only fills in a version the group does not name.
func (c *fakeCluster) group(name string, groupVersions ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.registerLocked(name, groupVersions...)
}

// ensureGroupLocked names a group-version in /apis without disturbing which one the group
// prefers.
func (c *fakeCluster) ensureGroupLocked(name, groupVersion string) {
	i := slices.IndexFunc(c.groups, func(g metav1.APIGroup) bool { return g.Name == name })
	if i < 0 {
		c.registerLocked(name, groupVersion)
		return
	}
	if slices.ContainsFunc(c.groups[i].Versions, func(v metav1.GroupVersionForDiscovery) bool {
		return v.GroupVersion == groupVersion
	}) {
		return
	}
	_, version, _ := strings.Cut(groupVersion, "/")
	c.groups[i].Versions = append(c.groups[i].Versions,
		metav1.GroupVersionForDiscovery{GroupVersion: groupVersion, Version: version})
}

func (c *fakeCluster) registerLocked(name string, groupVersions ...string) {
	versions := make([]metav1.GroupVersionForDiscovery, 0, len(groupVersions))
	for _, gv := range groupVersions {
		_, version, _ := strings.Cut(gv, "/")
		versions = append(versions, metav1.GroupVersionForDiscovery{GroupVersion: gv, Version: version})
	}
	group := metav1.APIGroup{Name: name, Versions: versions, PreferredVersion: versions[0]}
	if i := slices.IndexFunc(c.groups, func(g metav1.APIGroup) bool { return g.Name == name }); i >= 0 {
		c.groups[i] = group
		return
	}
	c.groups = append(c.groups, group)
}

// serveRaw makes one path answer body verbatim, for an api server whose answer is not the
// document the sweep is parsing.
func (c *fakeCluster) serveRaw(path, body string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.raw[path] = body
}

// unpreferGroup blanks a group's preferred version, which is what an api server that names
// none looks like.
func (c *fakeCluster) unpreferGroup(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if i := slices.IndexFunc(c.groups, func(g metav1.APIGroup) bool { return g.Name == name }); i >= 0 {
		c.groups[i].PreferredVersion = metav1.GroupVersionForDiscovery{}
	}
}

// breakPath makes one path answer 500 — an aggregated API that is down.
func (c *fakeCluster) breakPath(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.broken[path] = true
}

// hold parks every request for path until the returned func lets it go — how a test holds a
// sweep in flight.
func (c *fakeCluster) hold(path string) func() { return c.transport.hold(path) }

// holdTransport parks a chosen path, and reports what it read and what was cancelled under
// it. **In the caller's own goroutine**, not the handler's: a server notices a client giving
// up whenever its connection closes, which is after the run it belongs to has already
// unwound — so a tripwire there cannot say whether a teardown waited.
type holdTransport struct {
	base http.RoundTripper
	// reads carries every path the sweep asked for, fired before the park so a test can wait
	// for the request it means to hold. cancelled carries the held paths the caller gave up
	// on.
	reads     *testutil.Probe[string]
	cancelled *testutil.Probe[string]

	mu       sync.Mutex
	path     string
	released chan struct{}
}

func (t *holdTransport) hold(path string) func() {
	released := make(chan struct{})
	t.mu.Lock()
	t.path, t.released = path, released
	t.mu.Unlock()
	return sync.OnceFunc(func() { close(released) })
}

func (t *holdTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.reads.Fire(req.URL.Path)

	t.mu.Lock()
	held, released := t.path == req.URL.Path, t.released
	t.mu.Unlock()
	if held {
		select {
		case <-released:
		case <-req.Context().Done():
			t.cancelled.Fire(req.URL.Path)
			// Latency on purpose: the run stays in flight until the test lets go, so
			// whether a teardown WAITED for it has a determinate answer instead of racing
			// how fast a cancelled request unwinds.
			<-released
			return nil, req.Context().Err()
		}
	}
	return t.base.RoundTrip(req)
}

// crd declares a CustomResourceDefinition serving one group's plural.
func (c *fakeCluster) crd(group, plural string) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": plural + "." + group},
		"spec": map[string]any{
			"group": group,
			"names": map[string]any{"plural": plural},
		},
	}}
	_, err := c.dyn.Resource(crdGVR).Create(context.Background(), obj, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}
}

// forbidCRDs refuses the CRD list, the read RBAC commonly denies.
func (c *fakeCluster) forbidCRDs() {
	c.dyn.PrependReactor("list", "customresourcedefinitions",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(crdGVR.GroupResource(), "", nil)
		})
}

// awaitRead blocks until the sweep reads path, so a test asserting on a second sweep knows
// one happened.
func (c *fakeCluster) awaitRead(t *testing.T, path string) {
	t.Helper()
	for {
		if got := c.reads.Await(t, "a sweep reading "+path); got == path {
			return
		}
	}
}

// noRead is the negative assertion: nothing swept in the window. It has no event to wait
// for, so it needs a window of its own rather than the failsafe.
func (c *fakeCluster) noRead(t *testing.T, what string) {
	t.Helper()
	testutil.NoRecv(t, c.reads.Chan(), quietWindow, what)
}

func (c *fakeCluster) serveHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.broken[r.URL.Path] {
		http.Error(w, "the aggregated api server is down", http.StatusInternalServerError)
		return
	}
	if body, ok := c.raw[r.URL.Path]; ok {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
		return
	}

	switch {
	case r.URL.Path == "/api":
		writeJSON(w, metav1.APIVersions{Versions: []string{"v1"}})
	case r.URL.Path == "/apis":
		writeJSON(w, metav1.APIGroupList{Groups: c.groups})
	default:
		gv := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/apis/"), "/api/")
		doc, ok := c.resources[gv]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		writeJSON(w, doc)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// newFakeDynamic is the client every LIST and WATCH goes through. The list kinds are supplied
// explicitly because the scheme carries no types at all, so every collection a test mirrors is
// named here.
func newFakeDynamic() *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			crdGVR:                              "CustomResourceDefinitionList",
			{Version: "v1", Resource: "pods"}:   "PodList",
			{Version: "v1", Resource: "events"}: "EventList",
			{Group: "apps", Version: "v1", Resource: "deployments"}: "DeploymentList",
		})
}

// serveKind declares a kind in the discovery documents. A mirror test needs it because the
// objects read joins through kind_catalog, which the sweep is the only writer of — and in
// production a kind is tracked only because a sweep found it.
func (c *fakeCluster) serveKind(k kubestore.Kind, namespaced bool) {
	c.serve(k.APIVersion, listable(k.Kind, k.Resource, namespaced))
}

// object is one mirrored body: the metadata the store keys on, and nothing else.
func object(apiVersion, kind, name, resourceVersion string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata": map[string]any{
			"name": name, "namespace": "default",
			"uid": "uid-" + name, "resourceVersion": resourceVersion,
		},
	}}
}

// hasObjects makes a collection answer a LIST with these bodies, at listRV. Every call fires
// listed, so a test can tell a relist from a resume.
func (c *fakeCluster) hasObjects(k kubestore.Kind, listRV string, objects ...*unstructured.Unstructured) {
	c.dyn.PrependReactor("list", k.Resource, func(clienttesting.Action) (bool, runtime.Object, error) {
		c.listed.Fire(k.Resource)
		list := &unstructured.UnstructuredList{Object: map[string]any{
			"apiVersion": k.APIVersion, "kind": k.Kind + "List",
			"metadata": map[string]any{"resourceVersion": listRV},
		}}
		for _, o := range objects {
			list.Items = append(list.Items, *o)
		}
		return true, list, nil
	})
}

// failListOnce makes the next LIST of a collection fail and every one after it fall through —
// a transient refusal rather than a standing one.
func (c *fakeCluster) failListOnce(k kubestore.Kind, err error) {
	var once sync.Once
	c.dyn.PrependReactor("list", k.Resource, func(clienttesting.Action) (bool, runtime.Object, error) {
		refused := false
		once.Do(func() { refused = true })
		if refused {
			return true, nil, err
		}
		return false, nil, nil
	})
}

// refuseList makes a collection refuse to be listed.
func (c *fakeCluster) refuseList(k kubestore.Kind, err error) {
	c.dyn.PrependReactor("list", k.Resource, func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, err
	})
}

// streams hands a test the watch its mirror opens: one fresh watcher per open, published as
// it is handed over, so a test can drive the stream and see a re-establish for what it is.
type streams struct {
	opened *testutil.Probe[*watch.RaceFreeFakeWatcher]
	err    error
	held   chan struct{}
}

// streamKind installs the watch for one collection. Call before the worker starts.
func (c *fakeCluster) streamKind(k kubestore.Kind) *streams {
	s := &streams{opened: testutil.NewProbe[*watch.RaceFreeFakeWatcher](8)}
	c.dyn.PrependWatchReactor(k.Resource, func(clienttesting.Action) (bool, watch.Interface, error) {
		if s.err != nil {
			return true, nil, s.err
		}
		if s.held != nil {
			<-s.held
		}
		w := watch.NewRaceFreeFake()
		s.opened.Fire(w)
		return true, w, nil
	})
	return s
}

// refuse makes every later open fail, which is what a server refusing a watch looks like.
func (s *streams) refuse(err error) { s.err = err }

// hold parks every later open until the returned func lets it go — a server slow to
// establish a watch rather than one refusing it.
func (s *streams) hold() func() {
	released := make(chan struct{})
	s.held = released
	return sync.OnceFunc(func() { close(released) })
}

// holdList parks a collection's LIST until the test lets it go, for asserting on what a
// mirror reports while it has nothing yet.
func (c *fakeCluster) holdList(k kubestore.Kind) *heldList {
	h := &heldList{released: make(chan struct{})}
	h.release = sync.OnceFunc(func() { close(h.released) })
	c.dyn.PrependReactor("list", k.Resource, func(clienttesting.Action) (bool, runtime.Object, error) {
		<-h.released
		return false, nil, nil
	})
	return h
}

type heldList struct {
	released chan struct{}
	release  func()
}

// event is one core event body, which the cache keeps in its own table.
func event(name, resourceVersion string) *unstructured.Unstructured {
	return object("v1", "Event", name, resourceVersion)
}

// --- Reading a mirror back ---

// newMirroringService is a service running the real mirror, paced so a test never outwaits a
// production number. Every duration here is small enough to be reached and long enough that a
// loaded machine does not trip it.
func newMirroringService(t *testing.T, cluster *fakeCluster, opts ...func(*pacing)) *Service {
	t.Helper()
	p := defaultPacing()
	p.staleAfter = 200 * time.Millisecond
	p.backoff = supervisor.Backoff{Base: time.Millisecond, Factor: 2, Cap: 5 * time.Millisecond}
	for _, opt := range opts {
		opt(&p)
	}

	svc, pool := newTestService(t, withSyncKindFn(syncKindWith(p)))
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)
	return svc
}

// mirrorKind arms a cache and one kind on it, in the order production reaches them: a kind is
// tracked because a sweep found it, and the objects read joins through the catalog that sweep
// writes.
func mirrorKind(t *testing.T, svc *Service, cacheID int64, k kubestore.Kind) {
	t.Helper()
	svc.TrackDiscovery(cacheID, testParams)
	awaitDiscovered(t, svc, cacheID)
	svc.TrackKind(cacheID, k)
}

// awaitKindReason waits for one kind's verdict to settle somewhere, owning its subscription
// so nothing published before it attached is missed.
func awaitKindReason(t *testing.T, svc *Service, cacheID int64, k kubestore.Kind, reason string) {
	t.Helper()
	awaitKindState(t, svc, cacheID, k, "a mirror settling on "+reason, func(state KindState) bool {
		return state.Reason == reason
	})
}

// awaitKindState waits for one kind's answer to match. Everything a test asserts about a
// verdict belongs in match rather than in a read after it: a run that is down is a state the
// next attempt replaces, so a later read is a different answer rather than the same one again.
func awaitKindState(t *testing.T, svc *Service, cacheID int64, k kubestore.Kind, what string, match func(KindState) bool) {
	t.Helper()
	news := svc.WatchKindNews()
	defer news.Close()

	var last KindState
	settled := assert.Eventually(t, func() bool {
		state, ok := svc.GetKindState(cacheID, k)
		last = state
		return ok && match(state)
	}, testutil.Timeout, time.Millisecond, what)
	if !settled {
		t.Fatalf("the mirror stands on %q: %s", last.Reason, last.Message)
	}
}

func objectNames(t *testing.T, svc *Service, cacheID int64, k kubestore.Kind) []string {
	t.Helper()
	store, ok, err := svc.storeMgr.(*kubestore.Manager).OpenExisting(cacheID)
	require.NoError(t, err)
	require.True(t, ok, "the cache is open")
	defer store.Release()

	rows, err := store.Objects(t.Context(), k.APIVersion, k.Resource)
	require.NoError(t, err)
	names := make([]string, 0, len(rows))
	for _, r := range rows {
		names = append(names, r.Name)
	}
	return names
}

func eventsOf(t *testing.T, svc *Service, cacheID int64) []kubestore.EventRow {
	t.Helper()
	store, ok, err := svc.storeMgr.(*kubestore.Manager).OpenExisting(cacheID)
	require.NoError(t, err)
	require.True(t, ok, "the cache is open")
	defer store.Release()

	rows, err := store.Events(t.Context(), 100)
	require.NoError(t, err)
	return rows
}

func cookieOf(t *testing.T, svc *Service, cacheID int64, k kubestore.Kind) (string, bool) {
	t.Helper()
	store, ok, err := svc.storeMgr.(*kubestore.Manager).OpenExisting(cacheID)
	require.NoError(t, err)
	require.True(t, ok, "the cache is open")
	defer store.Release()

	cookie, held, err := store.Cookie(t.Context(), k.APIVersion, k.Resource)
	require.NoError(t, err)
	return cookie, held
}

// listable is a resource a worker can mirror, which is what most of a catalog looks like.
func listable(kind, plural string, namespaced bool) metav1.APIResource {
	return metav1.APIResource{
		Kind: kind, Name: plural, Namespaced: namespaced,
		Verbs: metav1.Verbs{"get", "list", "watch"},
	}
}

// --- The service ---

// newTestService builds a Service over a fresh pool and a store directory under t, with a
// kind sync that parks until its run ends — the shape a real one has, so nothing re-enters
// and a test drives arming rather than sync. Nothing sweeps until start.
func newTestService(t *testing.T, opts ...option) (*Service, *fakePool) {
	t.Helper()
	pool := newFakePool()
	mgr := kubestore.NewManager(t.TempDir())
	t.Cleanup(func() { _ = mgr.Close() })

	base := []option{withSyncKindFn(func(ctx context.Context, _ kindRun) { <-ctx.Done() })}
	svc := New(pool, mgr, append(base, opts...)...)
	t.Cleanup(func() { _ = svc.Close() })
	return svc, pool
}

// start runs the service and stops it with the test. Registered after newTestService's
// Close, so the cleanups unwind in the order the lifecycle requires.
func start(t *testing.T, svc *Service) {
	t.Helper()
	stop, err := svc.Start(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })
}

// awaitDiscovered settles on a sweep that answered; awaitReason on whichever verdict a test
// is waiting for. News is not a status, so both answer it the way a consumer does: re-read.
//
// Each subscribes for itself and gives the subscription back, so a test asserting on news
// opens its own AFTER settling and never has to tell what the settling consumed from what
// its own sweep published.
func awaitDiscovered(t *testing.T, svc *Service, cacheID int64) {
	t.Helper()
	awaitReason(t, svc, cacheID, ReasonDiscovered)
}

func awaitReason(t *testing.T, svc *Service, cacheID int64, reason string) {
	t.Helper()
	news := svc.WatchDiscoveryNews()
	defer news.Close()
	for {
		if state, ok := svc.GetDiscoveryState(cacheID); ok && state.Reason == reason {
			return
		}
		testutil.Recv(t, news.Chan(), "a sweep settling on "+reason)
	}
}

// catalogOf reads what the sweep wrote, through the door every reader uses. The manager is
// reached off the service because a test owns both ends of this seam.
func catalogOf(t *testing.T, svc *Service, cacheID int64) []kubestore.KindRow {
	t.Helper()
	store, ok, err := svc.storeMgr.(*kubestore.Manager).OpenExisting(cacheID)
	require.NoError(t, err)
	if !ok {
		return nil
	}
	defer store.Release()

	rows, err := store.Kinds(t.Context())
	require.NoError(t, err)
	return rows
}

// awaitDialsQuiet waits until this lease has stopped being dialed, which is the point a test
// can count what happens next. Reading "no run in flight" is not enough — a run the supervisor has
// queued has not started, and dials the moment it does.
//
// The window is bounded because quiet has no event to wait for; each dial restarts it, so a
// cache that never settles fails on the deadline rather than measuring mid-burst.
func awaitDialsQuiet(t *testing.T, l *fakeLease) {
	t.Helper()
	deadline := time.After(testutil.Timeout)
	for {
		l.dialed.Drain()
		select {
		case <-l.dialed.Chan():
		case <-time.After(quietWindow):
			return
		case <-deadline:
			t.Fatal("timed out waiting for the cache to stop dialing")
		}
	}
}

// writeRow puts one object of a kind into the cache, which is how a test rings the store's
// change bus — the same ping a mirror's own write leaves behind.
func writeRow(t *testing.T, svc *Service, cacheID int64, k kubestore.Kind) {
	t.Helper()
	store, ok, err := svc.storeMgr.(*kubestore.Manager).OpenExisting(cacheID)
	require.NoError(t, err)
	require.True(t, ok, "the cache is open")
	defer store.Release()

	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": k.APIVersion,
		"kind":       k.Kind,
		"metadata": map[string]any{
			"name": "one", "uid": "uid-one", "resourceVersion": "1",
		},
	}}
	require.NoError(t, store.ApplyChange(t.Context(), k, watch.Added, obj))
}

// quietWindow bounds a negative assertion — "no news arrived" — which has no event to
// wait for and so needs a window of its own rather than the failsafe every positive wait
// uses. Short: what it is watching for would already have happened.
const quietWindow = 50 * time.Millisecond

// testKind is one kind's identity, spelled the way the store writes rows by.
func testKind(apiVersion, kind, resource string) kubestore.Kind {
	return kubestore.Kind{APIVersion: apiVersion, Kind: kind, Resource: resource}
}
