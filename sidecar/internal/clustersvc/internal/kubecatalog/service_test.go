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

package kubecatalog

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/amorey/gobus/conflate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// fakeConns stands in for the pool: every lease's Conn answers connErr or an empty
// connection, and the hub is the fleet feed the bridge reads, driven by hand.
type fakeConns struct {
	hub *conflate.Hub[string, struct{}]

	mu       sync.Mutex
	acquired []string
	released int
	connErr  error
	// serverUID is who every lease says it reached; empty reads as never identified,
	// which the sweep refuses the same way it refuses another cluster's.
	serverUID string
	// conn is the context's one connection, the way the pool holds it: every lease answers
	// with the same object until rebuild replaces it.
	conn *kubeconn.Connection
}

// testUID is the server every fake lease answers as, and the one the tests arm their
// subjects for. A sweep runs only where the two agree.
const testUID = "uid-1"

func newFakeConns() *fakeConns {
	return &fakeConns{hub: conflate.New[string, struct{}](), serverUID: testUID, conn: &kubeconn.Connection{}}
}

// rebuild replaces the context's connection, the way the pool does when credentials move under
// an unchanged server. It returns the new one for a test to compare against.
func (f *fakeConns) rebuild() *kubeconn.Connection {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.conn = &kubeconn.Connection{}
	return f.conn
}

func (f *fakeConns) Acquire(contextName string) kubeconn.Lease {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acquired = append(f.acquired, contextName)
	return &fakeLease{conns: f}
}

func (f *fakeConns) Subscribe() kubeconn.Subscription { return f.hub.Receiver() }

// publish is a connection's news landing on the fleet feed.
func (f *fakeConns) publish(contextName string) { f.hub.Sender().Send(contextName, struct{}{}) }

func (f *fakeConns) setConnErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connErr = err
}

func (f *fakeConns) releaseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.released
}

type fakeLease struct{ conns *fakeConns }

func (l *fakeLease) Conn(context.Context) (*kubeconn.Connection, error) {
	l.conns.mu.Lock()
	defer l.conns.mu.Unlock()
	if l.conns.connErr != nil {
		return nil, l.conns.connErr
	}
	return l.conns.conn, nil
}

// ConnFor answers the way the pool does: the identity comes off the connection, so an
// unidentified one is refused rather than paired with whatever the probes last said.
func (l *fakeLease) ConnFor(ctx context.Context, serverUID string) (*kubeconn.Connection, error) {
	conn, err := l.Conn(ctx)
	if err != nil {
		return nil, err
	}

	l.conns.mu.Lock()
	defer l.conns.mu.Unlock()
	switch {
	case l.conns.serverUID == "":
		return nil, fmt.Errorf("%w: unidentified", kubeconn.ErrIdentityMismatch)
	case l.conns.serverUID != serverUID:
		return nil, fmt.Errorf("%w: reached %s", kubeconn.ErrIdentityMismatch, l.conns.serverUID)
	}
	return conn, nil
}

// State carries no identity on purpose: this package reads it off the connection, and a
// fake that also served it here would let the correlation back in unnoticed.
func (l *fakeLease) State() kubeconn.State { return kubeconn.State{} }

func (f *fakeConns) setServerUID(uid string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.serverUID = uid
}

// WatchState is unused by this package; nothing here watches a single claim.
func (l *fakeLease) WatchState() kubeconn.StateSubscription { return nil }

func (l *fakeLease) Departed() bool { return false }

func (l *fakeLease) Release() {
	l.conns.mu.Lock()
	defer l.conns.mu.Unlock()
	l.conns.released++
}

// newTestService is newWithOptions with the watch endpoint faked by default, so a test that
// says nothing about watching does not reach the real one through a connection it never built.
// A test that cares passes its own withOpener, which wins.
func newTestService(t *testing.T, conns *fakeConns, opts ...option) *Service {
	t.Helper()
	return newWithOptions(conns, append([]option{withOpener(newFakeOpener(t).open)}, opts...)...)
}

// answering is a sweep that always serves these kinds.
func answering(kinds ...Kind) option {
	return withSweep(func(*kubeconn.Connection) (Catalog, error) {
		return Catalog{Kinds: kinds}, nil
	})
}

// startService runs the service for the test's life, stop before Close like the
// composition root.
func startService(t *testing.T, s *Service) {
	t.Helper()
	stop, err := s.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, stop(context.Background()))
		assert.NoError(t, s.Close())
	})
}

// One claim per id, however many passes arm it: the context is fixed for the id's life,
// so a repeat is a no-op.
func TestTrackIsIdempotent(t *testing.T) {
	conns := newFakeConns()
	svc := newTestService(t, conns, answering(pods))

	svc.Track("cachedcatalog/1", "prod", testUID)
	svc.Track("cachedcatalog/1", "prod", testUID)

	assert.Equal(t, []string{"prod"}, conns.acquired)
	_, ok := svc.Read("cachedcatalog/1")
	assert.True(t, ok)
}

// Forget gives back everything Track took: the claim, the subject, and the published
// baseline behind the signal.
func TestForgetReleasesWhatTrackTook(t *testing.T) {
	conns := newFakeConns()
	svc := newTestService(t, conns, answering(pods))
	svc.Track("cachedcatalog/1", "prod", testUID)

	svc.Forget("cachedcatalog/1")

	assert.Equal(t, 1, conns.releaseCount())
	_, ok := svc.Read("cachedcatalog/1")
	assert.False(t, ok)

	svc.Forget("cachedcatalog/1")
	assert.Equal(t, 1, conns.releaseCount(), "idempotent")
}

// Close gives the pool every claim back: the pool closes after this service, and a
// claim outliving its holder is the holder's bug.
func TestCloseReleasesEveryLease(t *testing.T) {
	conns := newFakeConns()
	svc := newTestService(t, conns, answering(pods))
	svc.Track("cachedcatalog/1", "prod", testUID)
	svc.Track("cachedcatalog/2", "staging", testUID)

	require.NoError(t, svc.Close())

	assert.Equal(t, 2, conns.releaseCount())
}

// The whole read path: arming dispatches a sweep, the committed answer signals the
// subscriber, and Read hands it back beside its attempts.
func TestSweepSignalsWhenTheAnswerLands(t *testing.T) {
	conns := newFakeConns()
	svc := newTestService(t, conns, answering(pods))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)

	ev := testutil.Recv(t, sub.Chan(), "the sweep's signal")
	assert.Equal(t, "cachedcatalog/1", ev.Key)
	obs, ok := svc.Read("cachedcatalog/1")
	require.True(t, ok)
	require.True(t, obs.Known())
	assert.Equal(t, []Kind{pods}, obs.Value.Kinds)
	assert.True(t, obs.OK())
}

// A subject whose context resolves to nothing suspends, and the connection bridge is
// what brings it back: the pool saying the context moved re-runs the sweep with no
// cadence to wait out.
func TestConnectionRecoveryWakesASuspendedSweep(t *testing.T) {
	conns := newFakeConns()
	conns.setConnErr(kubeconn.ErrNoConnection)
	svc := newTestService(t, conns, answering(pods))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)
	testutil.Recv(t, sub.Chan(), "the suspension's signal")
	obs, ok := svc.Read("cachedcatalog/1")
	require.True(t, ok)
	require.False(t, obs.Known())

	conns.setConnErr(nil)
	conns.publish("prod")

	testutil.Recv(t, sub.Chan(), "the recovery's signal")
	obs, ok = svc.Read("cachedcatalog/1")
	require.True(t, ok)
	require.True(t, obs.Known())
	assert.Equal(t, []Kind{pods}, obs.Value.Kinds)
}

// --- identity scoping ---

// A context re-pointed at another cluster answers as a server this subject is not for,
// and the pool wakes every subject over that context the moment its identity moves — so
// the superseded cache's sweep is the first thing to run against the new server. Reading
// that server's kinds into this cache is what the check exists to stop.
func TestSweepRefusesAnotherClustersServer(t *testing.T) {
	conns := newFakeConns()
	conns.setServerUID("uid-2")
	svc := newTestService(t, conns, answering(pods))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)

	testutil.Recv(t, sub.Chan(), "the refusal's signal")
	obs, ok := svc.Read("cachedcatalog/1")
	require.True(t, ok)
	assert.False(t, obs.Known(), "nothing was asked, so nothing was committed")
	assert.Equal(t, ReasonIdentityMismatch, obs.LastAttempt.Reason)
	assert.Equal(t, probe.VerdictSuspended, obs.LastAttempt.Verdict)
}

// The standing answer survives the refusal: a cache that swept, then had its context
// re-pointed, keeps the kinds it read from its own server until the record is disarmed.
func TestSweepKeepsItsAnswerWhenTheServerChanges(t *testing.T) {
	conns := newFakeConns()
	svc := newTestService(t, conns, answering(pods))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)
	testutil.Recv(t, sub.Chan(), "the first answer")

	conns.setServerUID("uid-2")
	conns.publish("prod")

	testutil.Recv(t, sub.Chan(), "the refusal's signal")
	obs, ok := svc.Read("cachedcatalog/1")
	require.True(t, ok)
	assert.Equal(t, []Kind{pods}, obs.Value.Kinds, "read from this cache's own server")
	assert.Equal(t, ReasonIdentityMismatch, obs.LastAttempt.Reason)
}

// A server that has not said which cluster it is cannot confirm the subject either, so
// the sweep parks — and the pool reporting the identity is what re-arms it. The gap
// between a connection answering and the UID probe behind it answering.
func TestSweepWaitsForAnUnidentifiedServer(t *testing.T) {
	conns := newFakeConns()
	conns.setServerUID("")
	svc := newTestService(t, conns, answering(pods))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)
	testutil.Recv(t, sub.Chan(), "the wait's signal")
	obs, ok := svc.Read("cachedcatalog/1")
	require.True(t, ok)
	require.Equal(t, ReasonIdentityMismatch, obs.LastAttempt.Reason)

	conns.setServerUID(testUID)
	conns.publish("prod")

	testutil.Recv(t, sub.Chan(), "the sweep once the server is identified")
	obs, ok = svc.Read("cachedcatalog/1")
	require.True(t, ok)
	assert.Equal(t, []Kind{pods}, obs.Value.Kinds)
}

// A cluster nothing reached reports the outage, not the identity it could not have read
// either way — so a cold start reads as connecting rather than as identity trouble.
func TestSweepReportsTheOutageBeforeTheIdentity(t *testing.T) {
	conns := newFakeConns()
	conns.setConnErr(kubeconn.ErrNoConnection)
	conns.setServerUID("")
	svc := newTestService(t, conns, answering(pods))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)

	testutil.Recv(t, sub.Chan(), "the suspension's signal")
	obs, ok := svc.Read("cachedcatalog/1")
	require.True(t, ok)
	assert.Equal(t, ReasonNoConnection, obs.LastAttempt.Reason)
}

// The defect the connection-carried identity exists for: a context re-pointed at another
// cluster commits its new connection before the UID probe re-runs over it, so for a dispatch
// plus a round-trip the pool holds a fresh connection beside the previous one's UID. Asking
// the connection who it reached refuses that; correlating it with the last observation
// accepted it and swept the wrong cluster.
func TestSweepRefusesAConnectionNotYetReidentified(t *testing.T) {
	conns := newFakeConns()
	svc := newTestService(t, conns, answering(pods))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)
	testutil.Recv(t, sub.Chan(), "the first answer")

	// The connection is replaced and nothing has identified the new one yet.
	conns.setServerUID("")
	conns.publish("prod")

	testutil.Recv(t, sub.Chan(), "the refusal's signal")
	obs, ok := svc.Read("cachedcatalog/1")
	require.True(t, ok)
	assert.Equal(t, ReasonIdentityMismatch, obs.LastAttempt.Reason)
	assert.Equal(t, []Kind{pods}, obs.Value.Kinds, "the answer from this cache's own server stands")
}

// --- the watcher's lifetime ---

// The leak a Forget racing a sweep would otherwise cause: Run is on a worker, so Forget can
// stop the watcher and release the lease while the sweep is still in flight, and the sweep
// then finishes and establishes a fresh watcher for an id nothing tracks. Its wakes would be
// harmless no-ops, but the goroutine and its two streams stand until the connection retires —
// indefinitely, for a healthy cluster.
//
// The engine's commit refusal does not cover it: establishment is not a commit.
func TestEnsureWatcherStoresNothingForAnUntrackedID(t *testing.T) {
	conns := newFakeConns()
	svc := newTestService(t, conns, answering(pods))

	svc.ensureWatcher(context.Background(), "cachedcatalog/1", nil)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	assert.Empty(t, svc.watchers, "nothing tracks this id, so nothing may be stored for it")
}

func TestEnsureWatcherStoresOneForATrackedID(t *testing.T) {
	conns := newFakeConns()
	svc := newTestService(t, conns, answering(pods))
	svc.Track("cachedcatalog/1", "prod", testUID)

	svc.ensureWatcher(context.Background(), "cachedcatalog/1", nil)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	assert.Len(t, svc.watchers, 1)
}

// Forget gives back everything Track took, the watcher included — subject membership mirrors
// the record's state, and a watcher outliving it would wake a subject nothing sweeps for.
func TestForgetStopsTheWatcher(t *testing.T) {
	conns := newFakeConns()
	svc := newTestService(t, conns, answering(pods))
	svc.Track("cachedcatalog/1", "prod", testUID)
	svc.ensureWatcher(context.Background(), "cachedcatalog/1", nil)

	svc.Forget("cachedcatalog/1")

	svc.mu.Lock()
	defer svc.mu.Unlock()
	assert.Empty(t, svc.watchers)
}

// noWait is an already-finished context, for a test that establishes a watcher without waiting
// on opens it deliberately parked. Establishment still happens; only the handshake is skipped.
func noWait() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// watcherFor is id's standing watcher, read the way ensureWatcher writes it.
func watcherFor(t *testing.T, s *Service, id string) *watcher {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.watchers[id]
	require.NotNil(t, w, "a watcher for %s", id)
	return w
}

// A watcher whose streams ended is replaced, not read as one still standing. Its own wake is
// what runs the sweep that gets here, so declining to replace it is the end of promptness for
// that cluster: nothing else re-establishes, and discovery falls back to the 10-minute poll
// for the process's life.
func TestEnsureWatcherReplacesASpentWatcher(t *testing.T) {
	conns := newFakeConns()
	f := newFakeOpener(t)
	svc := newTestService(t, conns, answering(pods), withOpener(f.open))
	svc.Track("cachedcatalog/1", "prod", testUID)
	conn := &kubeconn.Connection{}

	// Both streams end before any event, which is a gap: nothing to resume from.
	first, second := f.serve(), f.serve()
	svc.ensureWatcher(context.Background(), "cachedcatalog/1", conn)
	for range 2 {
		f.opens.Await(t, "an open")
	}
	first.Stop()
	second.Stop()
	spent := watcherFor(t, svc, "cachedcatalog/1")
	testutil.Wait(t, spent.ended, "the watcher to end")

	svc.ensureWatcher(context.Background(), "cachedcatalog/1", conn)

	f.opens.Await(t, "the replacement's open")
	assert.NotSame(t, spent, watcherFor(t, svc, "cachedcatalog/1"))
}

// A watcher belongs to the connection it was built on. Credentials or connectivity can replace
// a connection while it still reaches the same server, and the sweep then succeeds over the new
// one — but conn.Done() closing is not something the streams watch, so the old watcher would go
// on holding an HTTP watch over retired credentials and every change would wait for the poll.
func TestEnsureWatcherReplacesAWatcherOverAnotherConnection(t *testing.T) {
	conns := newFakeConns()
	f := newFakeOpener(t)
	svc := newTestService(t, conns, answering(pods), withOpener(f.open))
	svc.Track("cachedcatalog/1", "prod", testUID)

	svc.ensureWatcher(context.Background(), "cachedcatalog/1", &kubeconn.Connection{})
	for range 2 {
		f.opens.Await(t, "an open")
	}
	replaced := watcherFor(t, svc, "cachedcatalog/1")

	rebuilt := &kubeconn.Connection{}
	svc.ensureWatcher(context.Background(), "cachedcatalog/1", rebuilt)

	f.opens.Await(t, "the replacement's open")
	w := watcherFor(t, svc, "cachedcatalog/1")
	assert.NotSame(t, replaced, w)
	assert.Same(t, rebuilt, w.conn, "the watcher stands over the connection the sweep used")
	testutil.WaitReturn(t, replaced.stop, "the replaced watcher's streams to be gone")
}

// The steady state, which is every sweep after the first: a live watcher over the connection
// the sweep just used is left alone, or a healthy cluster would tear down and rebuild two
// streams every ten minutes.
func TestEnsureWatcherKeepsALiveWatcherOverTheSameConnection(t *testing.T) {
	conns := newFakeConns()
	f := newFakeOpener(t)
	svc := newTestService(t, conns, answering(pods), withOpener(f.open))
	svc.Track("cachedcatalog/1", "prod", testUID)
	conn := &kubeconn.Connection{}

	svc.ensureWatcher(context.Background(), "cachedcatalog/1", conn)
	for range 2 {
		f.opens.Await(t, "an open")
	}
	standing := watcherFor(t, svc, "cachedcatalog/1")

	svc.ensureWatcher(context.Background(), "cachedcatalog/1", conn)

	// A negative assertion has no event to wait for, so it needs a bounded window; a
	// replacement opens immediately, so a short one is enough.
	testutil.NoRecv(t, f.opens.Chan(), 50*time.Millisecond, "another open")
	assert.Same(t, standing, watcherFor(t, svc, "cachedcatalog/1"))
}

// Forget drops the subject and its watcher in one critical section, or a sweep landing inside
// the teardown re-establishes against the id it is still able to see. Stopping a watcher waits
// for its streams, so the window is as long as the API server takes to hang up — and what it
// leaves behind is the very leak the tracked check exists to prevent, this time with nothing
// left to notice it.
func TestForgetRemovesTheSubjectAndTheWatcherTogether(t *testing.T) {
	conns := newFakeConns()
	f := newFakeOpener(t)
	f.hold = make(chan struct{})
	release := sync.OnceFunc(func() { close(f.hold) })
	svc := newTestService(t, conns, answering(pods), withOpener(f.open))
	t.Cleanup(func() { assert.NoError(t, svc.Close()) })
	// After the Close, so it runs before it: Cleanup is LIFO, and Close waits for streams still
	// inside the opener.
	t.Cleanup(release)
	svc.Track("cachedcatalog/1", "prod", testUID)

	// Both streams are parked in the opener, so the stop inside Forget cannot return.
	svc.ensureWatcher(noWait(), "cachedcatalog/1", &kubeconn.Connection{})
	for range 2 {
		f.opens.Await(t, "an open")
	}

	forgotten := make(chan struct{})
	go func() {
		defer close(forgotten)
		svc.Forget("cachedcatalog/1")
	}()
	// Forget has passed its critical section and is now blocked stopping the watcher.
	require.Eventually(t, func() bool {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		return svc.watchers["cachedcatalog/1"] == nil
	}, testutil.Timeout, time.Millisecond, "the watcher to be dropped")

	// The sweep that was in flight when Forget landed, finishing.
	svc.ensureWatcher(noWait(), "cachedcatalog/1", &kubeconn.Connection{})

	release()
	testutil.Wait(t, forgotten, "Forget to return")
	svc.mu.Lock()
	defer svc.mu.Unlock()
	assert.Empty(t, svc.watchers, "nothing tracks this id, so nothing may be left watching for it")
}

// A refused watch is refused every time — a namespace-scoped user cannot watch cluster-scoped
// CRDs — so it must cost the promptness and nothing else. Waking the sweep over it closes a
// loop: establish, refused, wake, sweep, establish, at one full discovery pass per turn and no
// committed answer to make it visible.
func TestARefusedWatchDoesNotRerunTheSweep(t *testing.T) {
	conns := newFakeConns()
	f := newFakeOpener(t)
	// Set before anything runs: an opener that refuses has to be arranged first, and the
	// watcher's goroutines read it.
	f.err = errors.New("customresourcedefinitions.apiextensions.k8s.io is forbidden")

	sweeps := testutil.NewProbe[struct{}](8)
	svc := newTestService(t, conns, withOpener(f.open),
		withSweep(func(*kubeconn.Connection) (Catalog, error) {
			sweeps.Fire(struct{}{})
			return Catalog{Kinds: []Kind{pods}}, nil
		}))
	startService(t, svc)

	svc.Track("cachedcatalog/1", "prod", testUID)

	sweeps.Await(t, "the first sweep")
	for range 2 {
		f.opens.Await(t, "a refused open")
	}

	// A negative assertion has no event to wait for, so it needs a bounded window. The loop
	// this guards runs a sweep per refusal with nothing pacing it, so it shows at once.
	testutil.NoRecv(t, sweeps.Chan(), 100*time.Millisecond, "a second sweep")
}

// The watcher belongs to the connection, so it is reconciled against the connection on every
// pass — including the passes whose sweep fails. Credentials can move under an unchanged server
// while discovery is down, and a watcher left on the retired connection then stands for as long
// as the failure lasts, which for an aggregated API server that is down is not a short window.
func TestAFailedSweepStillMovesTheWatcherToTheNewConnection(t *testing.T) {
	conns := newFakeConns()
	f := newFakeOpener(t)

	var mu sync.Mutex
	failing := false
	svc := newTestService(t, conns, withOpener(f.open),
		withSweep(func(*kubeconn.Connection) (Catalog, error) {
			mu.Lock()
			defer mu.Unlock()
			if failing {
				return Catalog{}, errors.New("the server rejected our request")
			}
			return Catalog{Kinds: []Kind{pods}}, nil
		}))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)
	testutil.Recv(t, sub.Chan(), "the first answer")
	for range 2 {
		f.opens.Await(t, "an open")
	}
	stale := watcherFor(t, svc, "cachedcatalog/1")

	mu.Lock()
	failing = true
	mu.Unlock()
	rebuilt := conns.rebuild()
	conns.publish("prod")

	testutil.Recv(t, sub.Chan(), "the failure's signal")
	w := watcherFor(t, svc, "cachedcatalog/1")
	assert.NotSame(t, stale, w, "the watcher over the retired connection is gone")
	assert.Same(t, rebuilt, w.conn)
}

// The sweep does not read discovery until its watches are open. A watch starts from the
// server's current state, so one that opened after the read would emit nothing for a change
// made in between — a change in neither answer, surfacing only at the next poll. Opening first
// is not enough on its own, since the opens are their own goroutines.
func TestTheSweepWaitsForItsWatchesToOpen(t *testing.T) {
	conns := newFakeConns()
	f := newFakeOpener(t)
	// Parked before anything runs, so the opens are still outstanding when the sweep would
	// otherwise start.
	f.hold = make(chan struct{})
	release := sync.OnceFunc(func() { close(f.hold) })

	sweeps := testutil.NewProbe[struct{}](8)
	svc := newTestService(t, conns, withOpener(f.open),
		withSweep(func(*kubeconn.Connection) (Catalog, error) {
			sweeps.Fire(struct{}{})
			return Catalog{Kinds: []Kind{pods}}, nil
		}))
	startService(t, svc)
	// After startService, so it runs before that cleanup: Cleanup is LIFO, and Close waits for
	// streams still inside the opener. Without this a failed assertion here deadlocks.
	t.Cleanup(release)

	svc.Track("cachedcatalog/1", "prod", testUID)

	for range 2 {
		f.opens.Await(t, "an open")
	}
	// A negative assertion has no event to wait for, so it needs a bounded window. Both opens
	// are outstanding, and a sweep that does not wait would already have run.
	testutil.NoRecv(t, sweeps.Chan(), 50*time.Millisecond, "a sweep before its watches are open")

	release()

	sweeps.Await(t, "the sweep, once both watches are open")
}

// A watch the server never answers must not hold the sweep for the run's whole budget: the
// wait is bounded by the run's context, and losing it costs this pass the overlap and nothing
// else — the streams keep their own lifetime and establish in the background.
func TestTheSweepWaitIsBoundedByTheRun(t *testing.T) {
	f := newFakeOpener(t)
	f.hold = make(chan struct{})

	w := startWatcher(context.Background(), nil, f.open, 0, func() {})
	t.Cleanup(w.stop)
	// After the stop, so it runs before it: Cleanup is LIFO, and stop waits for streams still
	// inside the opener.
	t.Cleanup(sync.OnceFunc(func() { close(f.hold) }))
	for range 2 {
		f.opens.Await(t, "an open")
	}

	testutil.WaitReturn(t, func() { w.awaitOpen(noWait()) }, "the wait to give up with the run")
}

// The watcher is established by the sweep that proved the connection usable, so it stands
// over that connection and inherits its identity scoping rather than checking anything itself.
func TestSweepEstablishesTheWatcher(t *testing.T) {
	conns := newFakeConns()
	f := newFakeOpener(t)
	svc := newTestService(t, conns, answering(pods), withOpener(f.open))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)

	testutil.Recv(t, sub.Chan(), "the sweep's signal")
	f.opens.Await(t, "the watch the sweep stood up")
}

// Every refusal takes the watcher with it. conn.Done() does not cover this: a connection that
// goes conflicted is never retired, so a watcher left standing would go on waking a subject
// that can only suspend — a CRD event on the replacement cluster spinning a sweep that will
// never run again.
func TestRefusalStopsTheWatcher(t *testing.T) {
	conns := newFakeConns()
	f := newFakeOpener(t)
	svc := newTestService(t, conns, answering(pods), withOpener(f.open))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)
	testutil.Recv(t, sub.Chan(), "the sweep's signal")
	f.opens.Await(t, "the watch the sweep stood up")

	// The context now answers as another cluster, which is a refusal rather than an outage.
	conns.setServerUID("uid-2")
	conns.publish("prod")
	testutil.Recv(t, sub.Chan(), "the refusal's signal")

	svc.mu.Lock()
	defer svc.mu.Unlock()
	assert.Empty(t, svc.watchers, "a refused run leaves no watch standing")
}
