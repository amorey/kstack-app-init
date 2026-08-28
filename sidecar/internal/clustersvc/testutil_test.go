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

// Fixtures shared across this package's test files. Anything used by one file lives
// beside it instead.
package clustersvc

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive"
	beehivesqlite "github.com/amorey/beehive/sqlite"
	"github.com/amorey/gobus/conflate"
	gobuswatch "github.com/amorey/gobus/watch"
	"github.com/amorey/gochan/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
)

// newTestBeehive returns a beehive over an in-memory store, closed on cleanup. The
// bootstrap New does, minus the disk. opts let a test shrink a cadence it would
// otherwise have to outwait.
func newTestBeehive(t *testing.T, opts ...beehive.Option) *beehive.Beehive {
	t.Helper()
	store, err := beehivesqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	bh, err := beehive.New(store, opts...)
	require.NoError(t, err)
	return bh
}

// newTestDeps returns the shared set over a test beehive — the same newDeps the
// composition root calls, so a test never assembles its own. The controllers are
// registered so a requeue reaches a real reconciler, but beehive is never run, so
// nothing reconciles or collects behind these tests.
//
// One store for every kind, which the owner edges need: beehive refuses an owner in
// another store.
func newTestDeps(t *testing.T) deps {
	t.Helper()
	d, _ := newTestDepsAndBeehive(t)
	return d
}

// fakeKubeconn stands in for the pool: it answers per context from a map and records who
// was asked for. The zero value refuses nothing and knows nothing, which is a fleet whose
// first probe is still owed — what a cluster pass finds unless a test says otherwise.
type fakeKubeconn struct {
	states map[string]kubeconn.State
	// hub is the fleet feed, keyed by context the way the pool keys it. Built on demand so
	// the zero value stays usable.
	once  sync.Once
	hub   *conflate.Hub[string, struct{}]
	asked []string

	// stateOnce/stateHub are the per-claim feed, built on demand for the same reason.
	stateOnce sync.Once
	stateHub  *gobuswatch.Hub[string, kubeconn.State]

	released []string
	retried  []string
	// connErr is what every claim's Conn answers, for a fleet nothing reached.
	connErr error
}

func (f *fakeKubeconn) Acquire(contextName string) kubeconn.Lease {
	f.asked = append(f.asked, contextName)
	return &fakeLease{svc: f, contextName: contextName, state: f.states[contextName], connErr: f.connErr}
}

func (f *fakeKubeconn) Retry(contextName string) { f.retried = append(f.retried, contextName) }

// Subscribe is the fleet feed the trigger reads. publish is the probe landing on it.
func (f *fakeKubeconn) Subscribe() kubeconn.Subscription { return f.moved().Receiver() }

func (f *fakeKubeconn) publish(contextName string) {
	f.moved().Sender().Send(contextName, struct{}{})
}

func (f *fakeKubeconn) moved() *conflate.Hub[string, struct{}] {
	f.once.Do(func() { f.hub = conflate.New[string, struct{}]() })
	return f.hub
}

func (f *fakeKubeconn) stateFeed() *gobuswatch.Hub[string, kubeconn.State] {
	f.stateOnce.Do(func() { f.stateHub = gobuswatch.New[string, kubeconn.State]() })
	return f.stateHub
}

// publishState is a pass landing on one claim, the way the pool's OnPass would.
func (f *fakeKubeconn) publishState(contextName string, st kubeconn.State) {
	f.stateFeed().Sender().Send(contextName, st)
}

// fakeLease is one claim, holding what a probe would have found. It records its release so
// a test can pin the lifetime a pass is meant to keep.
type fakeLease struct {
	svc         *fakeKubeconn
	contextName string
	state       kubeconn.State
	connErr     error
	// departed is what the pool would report for a context the kubeconfig stopped naming.
	departed bool
}

// Conn hands back an empty connection, since the only pass that takes one reaches the
// server through a seam of its own. connErr is what a claim on a cluster nothing reached
// would answer.
func (l *fakeLease) Conn(context.Context) (*kubeconn.Connection, error) {
	if l.connErr != nil {
		return nil, l.connErr
	}
	return &kubeconn.Connection{}, nil
}

// ConnFor refuses everything. Nothing in this package dials, and only kubeconn can stamp
// a connection's identity, so there is no honest way to answer here. Deciding from l.state would be the connection/State correlation the
// design forbids, passing a test against semantics the pool refuses.
func (l *fakeLease) ConnFor(context.Context, string) (*kubeconn.Connection, error) {
	return nil, kubeconn.ErrIdentityMismatch
}

func (l *fakeLease) State() kubeconn.State { return l.state }

func (l *fakeLease) Departed() bool { return l.departed }

// WatchState is the schedule watch's feed; a cluster pass reads State instead.
func (l *fakeLease) WatchState() kubeconn.StateSubscription {
	return l.svc.stateFeed().Watch(l.contextName)
}

func (l *fakeLease) Release() { l.svc.released = append(l.svc.released, l.contextName) }

// fakeKubestore stands in for the store registry: it records what was cleared and
// which caches were deleted, and answers with err. The zero value clears everything
// without complaint.
type fakeKubestore struct {
	// mgr is a real manager over a temp dir, since OpenExisting hands back a concrete
	// store nothing can stand in for.
	mgr *kubestore.Manager

	opened []int64
	// noFile models a cache whose file is not there — a teardown that already ran.
	noFile        bool
	clearedCaches []int64
	removed       []int64
	err           error
	// onClear and onOpen run inside the clears, so a test can assert what had already
	// happened by then — or move the world under one.
	onClear func(cacheID int64)
	onOpen  func(cacheID int64)

	// mu guards the measurement, which a test moves while the gauge reads it.
	mu    sync.Mutex
	stats kubestore.Stats
	// onRemove runs inside Remove, so a test can assert what had already happened by
	// the time the file went.
	onRemove func(cacheID int64)
}

// OpenExisting records the claim and hands back a real store, standing in for a cache
// whose file exists. What the caller then does to a kind is kubestore's own business,
// and its own tests'.
func (f *fakeKubestore) OpenExisting(cacheID int64) (*kubestore.Store, bool, error) {
	if f.onOpen != nil {
		f.onOpen(cacheID)
	}
	// Under the lock: two reconciles can claim a cache at once — a cache-wide read while
	// a kind's own pass clears its rows.
	f.mu.Lock()
	f.opened = append(f.opened, cacheID)
	f.mu.Unlock()
	if f.err != nil {
		return nil, false, f.err
	}
	if f.noFile {
		return nil, false, nil
	}
	store, err := f.mgr.OpenOrCreate(cacheID)
	if err != nil {
		return nil, false, err
	}
	return store, true, nil
}

// newFakeKubestore builds the fake over a real manager, closed with the test.
func newFakeKubestore(t *testing.T) *fakeKubestore {
	t.Helper()
	mgr := kubestore.NewManager(t.TempDir())
	t.Cleanup(func() { assert.NoError(t, mgr.Close()) })
	return &fakeKubestore{mgr: mgr}
}

// kubestoreFake is the store registry a fixture wired, for a test that drives it.
func kubestoreFake(d deps) *fakeKubestore { return d.kubestoreMgr.(*fakeKubestore) }

func (f *fakeKubestore) Clear(cacheID int64) error {
	if f.onClear != nil {
		f.onClear(cacheID)
	}
	f.clearedCaches = append(f.clearedCaches, cacheID)
	if f.err != nil {
		return f.err
	}
	// Through the real manager, so a clear a test drives empties the rows a later read
	// goes looking for — which is the whole of what a re-read reacts to.
	return f.mgr.Clear(cacheID)
}

// Stats is what the gauge measures; setStats moves it under a live subscription, which
// is what the gauge re-emits on.
func (f *fakeKubestore) Stats(context.Context, int64) (kubestore.Stats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats, f.err
}

func (f *fakeKubestore) setStats(s kubestore.Stats) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stats = s
}

// cacheIsOpen asks whether anything holds cacheID's file open — what Manager.Subscribe's
// ok reports. The receiver is closed straight away: this is a question, not a watch.
func cacheIsOpen(m *kubestore.Manager, cacheID int64) bool {
	sub, ok := m.Subscribe(cacheID)
	if ok {
		sub.Close()
	}
	return ok
}

// Subscribe and WatchOpen go to the real manager, so a reader sees whatever a test opened
// through it — and nothing when a test opened nothing, which is the gauge's idle-cache case.
func (f *fakeKubestore) Subscribe(cacheID int64, keys ...string) (kubestore.Subscription, bool) {
	return f.mgr.Subscribe(cacheID, keys...)
}

func (f *fakeKubestore) WatchOpen(cacheID int64) kubestore.OpenSubscription {
	return f.mgr.WatchOpen(cacheID)
}

func (f *fakeKubestore) Remove(cacheID int64) error {
	if f.onRemove != nil {
		f.onRemove(cacheID)
	}
	f.removed = append(f.removed, cacheID)
	return f.err
}

// probedAt is when every fake probe landed. Fixed, so a test asserting the stamp names a
// value rather than reading the clock the code under test would.
var probedAt = time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)

// answering is a pool whose "prod" claim reached the server and read id, or failed with
// err. The shape most tests want; knowing builds anything else.
func answering(id kubeconn.Identity, err error) *fakeKubeconn {
	if err != nil {
		return knowing(kubeconn.State{Connection: failed(err)})
	}
	// A part id leaves empty is left unanswered, which is what a probe that could not read
	// it reports.
	st := kubeconn.State{
		Connection: answeredWith("https://prod.example:6443"),
		Readiness:  answeredWith(kubeconn.ComponentStatus{}),
	}
	if id.ServerUID != "" {
		st.ServerUID = answeredWith(id.ServerUID)
	}
	if id.ServerVersion != "" {
		st.ServerVersion = answeredWith(kubeconn.VersionInfo{GitVersion: id.ServerVersion})
	}
	if id.Username != "" {
		st.Principal = answeredWith(kubeconn.Principal{Username: id.Username})
	}
	return knowing(st)
}

// answeredWith is a check that answered v at probedAt.
func answeredWith[T any](v T) kubeconn.Observation[T] {
	return kubeconn.Observation[T]{
		Value:      v,
		LastSeenAt: probedAt,
		Attempts: kubeconn.Attempts{
			LastAttempt: finished(kubeconn.ReasonSucceeded, ""),
		},
	}
}

// failed is a check whose last attempt did not answer.
func failed(err error) kubeconn.Observation[string] {
	return kubeconn.Observation[string]{
		Attempts: kubeconn.Attempts{
			LastAttempt: finished(kubeconn.ReasonUnreachable, err.Error()),
			Failures:    1, FailingSince: probedAt,
		},
	}
}

// finished is an attempt that ran and ended at probedAt, which is what makes its reason
// readable. The verdict follows the reason, the way a real probe's result would set it.
func finished(reason kubeconn.Reason, msg string) kubeconn.Attempt {
	verdict := kubeconn.VerdictFailed
	if reason == kubeconn.ReasonSucceeded {
		verdict = kubeconn.VerdictSucceeded
	}
	a := kubeconn.Attempt{
		ScheduledAt: probedAt, StartedAt: probedAt, FinishedAt: probedAt,
		Verdict: verdict, Reason: reason, Message: msg,
	}
	if msg != "" {
		a.Err = errors.New(msg)
	}
	return a
}

// knowing is a pool whose "prod" claim holds exactly this state.
func knowing(state kubeconn.State) *fakeKubeconn {
	return &fakeKubeconn{states: map[string]kubeconn.State{"prod": state}}
}

// newTestDepsAndBeehive is newTestDeps plus the beehive behind it, for a test that has
// to write what only a pass can — see newClusterStatusDeps.
func newTestDepsAndBeehive(t *testing.T) (deps, *beehive.Beehive) {
	t.Helper()
	bh := newTestBeehive(t)
	d := newDeps(bh, newTestKubeconfig(t), &fakeKubeconn{}, newFakeKubestore(t), nil)

	_, err := registerControllers(bh, d)
	require.NoError(t, err)

	// The anchors the real service creates at startup: clusterController.Reconcile
	// reads one to declare its dependency edge.
	require.NoError(t, ensureClusterSources(context.Background(), d.sourceClient))
	return d, bh
}

// newClusterStatusDeps returns the shared set plus the client that stores a Cluster
// status from outside a pass, standing in for the connection probe: a fixture needs a
// stored status because the reconciles under test re-read it. Beehive is never run, so
// nothing reconciles behind these tests — which is also the only state an admin write
// is safe in.
func newClusterStatusDeps(t *testing.T) (deps, *beehive.AdminClient[ClusterStatus]) {
	t.Helper()
	bh := newTestBeehive(t)
	return newDeps(bh, newTestKubeconfig(t), &fakeKubeconn{}, newFakeKubestore(t), nil), beehive.NewAdminClient[ClusterStatus](bh, ClusterGroupKind)
}

// newTestKubeconfig returns a started kubeconfig service over an empty temp dir, so
// it has read and every context is absent. Started because a reconcile defers until
// the first read, which Start does synchronously.
func newTestKubeconfig(t *testing.T) *kubeconfig.Service {
	t.Helper()
	svc := kubeconfig.New(filepath.Join(t.TempDir(), "config"), nil)

	stop, err := svc.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, stop(context.Background()))
		assert.NoError(t, svc.Close())
	})
	return svc
}

// newRunningBeehive is newTestBeehive started, so a watch's tail is live. No
// controller is registered: a reconcile writing status would put frames on a watch
// that the test never asked for.
func newRunningBeehive(t *testing.T, opts ...beehive.Option) *beehive.Beehive {
	t.Helper()
	bh := newTestBeehive(t, opts...)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
	return bh
}

// newRunningDeps is the shared set over a running beehive, for the watch tests:
// without one the tail is dead and no change is ever reported. Nothing reconciles, so
// every frame is the test's own doing.
func newRunningDeps(t *testing.T, opts ...beehive.Option) deps {
	t.Helper()
	return newDeps(newRunningBeehive(t, opts...), newTestKubeconfig(t), &fakeKubeconn{}, newFakeKubestore(t), nil)
}

// newReconcilingDeps is the shared set over a running beehive with the controllers
// registered, so a Requeue reaches a real pass. The other running fixture deliberately
// reconciles nothing; this one is for the paths whose whole point is that a pass runs.
func newReconcilingDeps(t *testing.T) (deps, *beehive.AdminClient[ClusterStatus]) {
	t.Helper()
	bh := newTestBeehive(t)
	d := newDeps(bh, newTestKubeconfig(t), &fakeKubeconn{}, newFakeKubestore(t), nil)

	_, err := registerControllers(bh, d)
	require.NoError(t, err)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })

	return d, beehive.NewAdminClient[ClusterStatus](bh, ClusterGroupKind)
}

// fakeKubeconfigSource is a hub the test publishes into, standing in for the
// watcher: same current-on-subscribe contract, driven by hand.
type fakeKubeconfigSource struct{ hub *watch.Hub[*api.Config] }

func newFakeKubeconfigSource(initial *api.Config) *fakeKubeconfigSource {
	return &fakeKubeconfigSource{hub: watch.New(initial)}
}

func (f *fakeKubeconfigSource) Subscribe() kubeconfig.Subscription { return f.hub.Receiver() }

// publish pushes a new snapshot to every subscriber.
func (f *fakeKubeconfigSource) publish(cfg *api.Config) { f.hub.Sender().Send(cfg) }

// close ends every subscription, standing in for the watcher shutting down.
func (f *fakeKubeconfigSource) close() { f.hub.Close() }

// cfgWith builds a snapshot holding one context per name, each naming its own cluster
// and user entry.
func cfgWith(names ...string) *api.Config {
	cfg := &api.Config{Contexts: map[string]*api.Context{}}
	for _, n := range names {
		cfg.Contexts[n] = &api.Context{Cluster: n + "-cluster", AuthInfo: n + "-user"}
	}
	return cfg
}

// cfgCurrent is cfgWith with one of the contexts marked current.
func cfgCurrent(current string, names ...string) *api.Config {
	cfg := cfgWith(names...)
	cfg.CurrentContext = current
	return cfg
}

// forbidden is a check the server answered and refused, which is a grant to fix rather
// than an outage to wait out.
func forbidden(msg string) kubeconn.Observation[string] {
	return kubeconn.Observation[string]{
		Attempts: kubeconn.Attempts{
			LastAttempt: finished(kubeconn.ReasonForbidden, msg),
			Failures:    1, FailingSince: probedAt,
		},
	}
}
