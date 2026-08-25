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

package kubeconn

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/amorey/gochan/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// fakeKubeconfig resolves each context to a key the test controls, standing in for the user's
// file. err, when set, is what every context fails with.
//
// Every context resolves to the same host, since what a test varies is the key: the key is the
// fingerprint, which is what a rebuild is decided on.
type fakeKubeconfig struct {
	mu     sync.Mutex
	keys   map[string]string
	host   string
	tls    rest.TLSClientConfig
	err    error
	hub    *watch.Hub[*api.Config]
	onRead func()
	// reads names each context read, for a test counting them rather than their answers.
	reads *testutil.Probe[string]
}

// resolving is a kubeconfig where each context resolves to the key beside it, aimed at an
// address nothing answers on.
func resolving(contextKeys ...string) *fakeKubeconfig {
	f := &fakeKubeconfig{keys: map[string]string{}, host: deadHost}
	for i := 0; i < len(contextKeys); i += 2 {
		f.keys[contextKeys[i]] = contextKeys[i+1]
	}
	return f
}

// serving is resolving against a server that answers: every context aims at srv and trusts its
// certificate.
func serving(srv *httptest.Server, contextKeys ...string) *fakeKubeconfig {
	f := resolving(contextKeys...)
	f.host = srv.URL
	f.tls = rest.TLSClientConfig{CAData: caPEM(srv)}
	return f
}

// counting is resolving with the reads probe armed.
func counting(contextKeys ...string) *fakeKubeconfig {
	f := resolving(contextKeys...)
	f.reads = testutil.NewProbe[string](8)
	return f
}

// rotate is the user editing their file: contextName now names different credentials. An
// empty key removes the context.
func (f *fakeKubeconfig) rotate(contextName, key string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if key == "" {
		delete(f.keys, contextName)
		return
	}
	f.keys[contextName] = key
}

func (f *fakeKubeconfig) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.err = err
}

// duringRead runs fn inside the next read, so a test can interleave an event with a read in
// flight. Latency in the code under test on purpose — the test's own assertions stay immediate.
func (f *fakeKubeconfig) duringRead(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.onRead = fn
}

func (f *fakeKubeconfig) RESTConfig(contextName string) (*rest.Config, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.reads != nil {
		f.reads.Fire(contextName)
	}
	if fn := f.onRead; fn != nil {
		f.onRead = nil
		fn()
	}
	if f.err != nil {
		return nil, "", f.err
	}
	key, ok := f.keys[contextName]
	if !ok {
		return nil, "", kubeconfig.ErrContextNotFound
	}
	return &rest.Config{Host: f.host, TLSClientConfig: f.tls}, key, nil
}

func (f *fakeKubeconfig) Subscribe() kubeconfig.Subscription {
	if f.hub == nil {
		f.hub = watch.New[*api.Config](nil)
	}
	return f.hub.Receiver()
}

// changed is the user saving their file: the watch reports it moving.
func (f *fakeKubeconfig) changed() { f.hub.Sender().Send(&api.Config{}) }

// within bounds a wait for something that must arrive, so a regression fails the test rather
// than hanging it until the suite's own deadline.
func within(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// startService runs the pool's engine and watch for a test that needs them, and joins them on
// cleanup.
func startService(t *testing.T, s *Service) {
	t.Helper()
	stop, err := s.Start(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
}

// settled waits until the pool has published a first verdict for the claim, so a test
// subscribing after it hears the news it goes on to cause rather than the first probe's.
func settled(t *testing.T, lease Lease) {
	t.Helper()
	require.Eventually(t, func() bool {
		return lease.State().Connection.LastAttempt.Done()
	}, testutil.Timeout, time.Millisecond, "a first verdict")
}

// awaitState receives one claim's states until cond holds. The hub keeps only the latest value,
// so frames in between may be skipped — cond must be a level, never an edge.
func awaitState(t *testing.T, sub StateSubscription, cond func(State) bool) State {
	t.Helper()
	ctx := within(t)
	for {
		ev, err := sub.RecvContext(ctx)
		require.NoError(t, err, "state frame")
		if cond(ev.Value) {
			return ev.Value
		}
	}
}

// --- claims ---

// A claim is taken whatever the file says. Whether the context resolves is the pool's to find
// out on its own schedule, and a claim is how the holder hears about it — refusing would make a
// transient fact permanent for the caller.
func TestAcquireClaimsAContextTheKubeconfigDoesNotName(t *testing.T) {
	s := New(&fakeKubeconfig{})

	lease := s.Acquire("prod")
	defer lease.Release()

	assert.Len(t, s.claimed, 1)
	assert.Equal(t, State{}, lease.State())
}

// Contexts that resolve alike are deliberately not merged: each gets its own entry, so what is
// read about one belongs to one context.
func TestAcquireBuildsAnEntryPerContext(t *testing.T) {
	s := New(resolving("prod", "key-1", "prod-admin", "key-1"))

	defer s.Acquire("prod").Release()
	defer s.Acquire("prod-admin").Release()

	assert.Len(t, s.claimed, 2)
	assert.NotSame(t, s.claimed["prod"], s.claimed["prod-admin"])
}

// An entry nobody holds is one nothing tracks, so the last release has to drop it.
func TestReleaseDropsTheEntryTheLastHolderHeld(t *testing.T) {
	s := New(resolving("prod", "key-1"))

	s.Acquire("prod").Release()

	assert.Empty(t, s.claimed)
}

// Two holders of one context — the cluster pass and a log tail — are two claims. The first to
// finish must not stop the pool tracking it for the other.
func TestReleaseKeepsTheEntryWhileAnotherHolderHoldsIt(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	pass := s.Acquire("prod")
	tail := s.Acquire("prod")

	pass.Release()
	assert.Len(t, s.claimed, 1)

	tail.Release()
	assert.Empty(t, s.claimed)
}

// Release is idempotent by contract, so it is safe to defer beside a path that also releases.
func TestReleaseIsIdempotent(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	lease := s.Acquire("prod")
	other := s.Acquire("prod")

	lease.Release()
	lease.Release()

	assert.Len(t, s.claimed, 1, "the second release must not drop the other holder's entry")
	other.Release()
}

// A claim released twice — or one whose entry the pool no longer holds — is a no-op rather than
// a decrement into the negative.
func TestReleaseIsANoOpOnceTheEntryIsGone(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	lease := s.Acquire("prod")
	lease.Release()
	require.Empty(t, s.claimed)

	lease.Release()

	assert.Empty(t, s.claimed)
}

// Close drops entries out from under the leases still holding them, so a lease released
// afterwards must not decrement whatever has claimed its name since.
func TestAPreCloseReleaseDoesNotDropALaterClaim(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	stale := s.Acquire("prod")
	require.NoError(t, s.Close())

	fresh := s.Acquire("prod")
	defer fresh.Release()
	stale.Release()

	assert.Len(t, s.claimed, 1, "the later claim still holds its entry")
	assert.False(t, fresh.Departed())
}

// A claim reads as departed once nothing holds its context: there is no entry left to answer
// from, and reporting present would be a claim about a context the pool stopped tracking.
func TestAReleasedClaimReadsAsDeparted(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	lease := s.Acquire("prod")
	require.False(t, lease.Departed())

	lease.Release()

	assert.True(t, lease.Departed())
	assert.Equal(t, State{}, lease.State())
}

// Close drops what the pool holds, so a claim outliving it reads as nothing known rather than
// as a context the pool is still tracking.
func TestCloseDropsWhatThePoolHolds(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	lease := s.Acquire("prod")
	require.False(t, lease.Departed())

	require.NoError(t, s.Close())

	assert.Empty(t, s.claimed)
	assert.True(t, lease.Departed())
}

// --- retiring connections ---

// The probe builds and the pool retires, so a rebuild hands back what it replaced: a run cannot
// retire the connection it is replacing, since its commit lands after it returns and holders
// would reconnect against a Conn still handing out the dead one.
func TestRecordHandsBackTheConnectionItReplaced(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	defer s.Acquire("prod").Release()
	first, second := connTo(t, serveAPI(t)), connTo(t, serveAPI(t))

	stale, held, _ := s.record("prod", first, news{})
	require.True(t, held)
	assert.Nil(t, stale, "the first connection replaces nothing")

	stale, held, _ = s.record("prod", second, news{})

	assert.True(t, held)
	assert.Same(t, first, stale)
}

// The leak this rule exists for: a release landing between the commit and the pass retires the
// entry's connection, which is the *previous* one — the connection the run just built is held by
// an entry that no longer exists, so nothing else can reach it.
func TestRecordHandsBackAConnectionNothingHoldsAnyMore(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	s.Acquire("prod").Release()
	built := connTo(t, serveAPI(t))

	stale, held, _ := s.record("prod", built, news{})

	assert.False(t, held)
	assert.Same(t, built, stale, "nothing else can reach it")
}

// The entry check and the published write are one critical section, so a pass that raced the
// last release files nothing: a baseline left behind for a context nobody holds is one the first
// pass of the next claim compares equal to, telling the fleet nothing about a context that just
// came back.
func TestRecordFilesNothingForAContextNobodyHolds(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	lease := s.Acquire("prod")
	probed := news{phase: PhaseProbed}
	_, _, _ = s.record("prod", nil, probed)
	lease.Release()

	_, held, _ := s.record("prod", nil, probed)

	assert.False(t, held)
	assert.Empty(t, s.published, "no baseline for the claim that comes next")
}

// An entry that goes takes its connection with it, or every released context leaves its sockets
// behind.
func TestReleaseRetiresTheConnectionTheEntryHeld(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	lease := s.Acquire("prod")
	conn := connTo(t, serveAPI(t))
	_, _, _ = s.record("prod", conn, news{})

	lease.Release()

	<-conn.Done()
}

func TestCloseRetiresEveryConnection(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	defer s.Acquire("prod").Release()
	conn := connTo(t, serveAPI(t))
	_, _, _ = s.record("prod", conn, news{})

	require.NoError(t, s.Close())

	<-conn.Done()
}

// --- handing a connection out ---

// What the probe built is what a holder gets, without dialing anything of its own.
func TestConnHandsOutWhatTheProbeBuilt(t *testing.T) {
	srv := serveAPI(t)
	s := New(serving(srv, "prod", "key-1"))
	startService(t, s)
	lease := s.Acquire("prod")
	defer lease.Release()
	watched := lease.WatchState()
	defer watched.Close()
	awaitState(t, watched, func(st State) bool { return st.Phase() == PhaseProbed })

	conn, err := lease.Conn(t.Context())

	require.NoError(t, err)
	assert.Equal(t, srv.URL, conn.BaseURL.String())
}

// A context that resolves to nothing has no connection to hand out, and saying so is what stops
// a holder interpreting a nil.
func TestConnReportsThatThereIsNoConnection(t *testing.T) {
	s := New(&fakeKubeconfig{})
	lease := s.Acquire("prod")
	defer lease.Release()

	conn, err := lease.Conn(t.Context())

	assert.Nil(t, conn)
	assert.ErrorIs(t, err, ErrNoConnection)
}

// --- the news: a context leaving the kubeconfig ---

// The one thing a holder is told without asking. It learns what changed by asking the claim.
func TestDepartureReachesTheHolder(t *testing.T) {
	cfg := counting("prod", "key-1")
	s := New(cfg)
	startService(t, s)
	lease := s.Acquire("prod")
	defer lease.Release()
	require.Equal(t, "prod", cfg.reads.Await(t, "the claim's own read"))
	settled(t, lease)
	news := s.Subscribe()
	defer news.Close()

	cfg.rotate("prod", "")
	cfg.changed()

	ev, err := news.RecvContext(within(t))
	require.NoError(t, err)
	assert.Equal(t, "prod", ev.Key)
	assert.True(t, lease.Departed(), "the state is committed before the signal is sent")
}

// The claim outlives what it is a claim on: the file may name the context again, and this claim
// is how the holder hears about that.
func TestDepartureKeepsTheClaim(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	startService(t, s)
	lease := s.Acquire("prod")
	defer lease.Release()

	cfg.rotate("prod", "")
	cfg.changed()
	require.Eventually(t, lease.Departed, time.Second, time.Millisecond)

	cfg.rotate("prod", "key-1")
	cfg.changed()

	require.Eventually(t, func() bool { return !lease.Departed() },
		time.Second, time.Millisecond, "named again, and the same claim reports it")
	assert.Len(t, s.claimed, 1)
}

// A departed context is announced once. Announcing it on every kubeconfig write would wake its
// holder forever for news it already has.
func TestDepartureIsAnnouncedOnce(t *testing.T) {
	cfg := counting("prod", "key-1")
	s := New(cfg)
	startService(t, s)
	lease := s.Acquire("prod")
	defer lease.Release()
	cfg.reads.Await(t, "the claim's own read")
	settled(t, lease)
	news := s.Subscribe()
	defer news.Close()

	cfg.rotate("prod", "")
	cfg.changed()
	_, err := news.RecvContext(within(t))
	require.NoError(t, err)

	cfg.changed()
	cfg.reads.Await(t, "the re-read the change woke")

	// A negative assertion needs a bounded window: this fails the instant a second one lands.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err = news.RecvContext(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// An unread kubeconfig names nothing. Reporting that as a departure would tell every holder its
// context is gone for as long as the first read takes.
func TestUnreadKubeconfigIsNotADeparture(t *testing.T) {
	cfg := counting("prod", "key-1")
	s := New(cfg)
	startService(t, s)
	lease := s.Acquire("prod")
	defer lease.Release()
	cfg.reads.Await(t, "the claim's own read")

	cfg.setErr(kubeconfig.ErrNotRead)
	cfg.changed()
	cfg.reads.Await(t, "the re-read the change woke")

	assert.False(t, lease.Departed())
}

// A context the file still names but cannot resolve — a cluster entry it points at that is
// gone, credentials that will not load — has not departed. It is the file that is broken, and
// saying otherwise reports a removal the user did not make.
func TestAResolveFailureIsNotADeparture(t *testing.T) {
	cfg := resolving("prod", "key-1")
	cfg.err = errors.New("no such certificate authority file")
	s := New(cfg)
	startService(t, s)
	lease := s.Acquire("prod")
	defer lease.Release()
	watched := lease.WatchState()
	defer watched.Close()

	awaitState(t, watched, func(st State) bool {
		return st.Connection.LastAttempt.Reason == ReasonResolveFailed
	})

	assert.False(t, lease.Departed())
}

// A holder watching its own claim is told too, not just the fleet feed, and the value carries
// why: a probe that suspends has to record its reason or nothing explains the suspension.
func TestDepartureReachesAClaimWatcher(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	startService(t, s)
	lease := s.Acquire("prod")
	defer lease.Release()
	watched := lease.WatchState()
	defer watched.Close()

	cfg.rotate("prod", "")
	cfg.changed()

	// Not any departed frame: one can show a woken re-read still in flight. The settled one is
	// what the suspension is asserted on.
	st := awaitState(t, watched, func(st State) bool {
		return st.Connection.LastAttempt.Reason == ReasonContextNotFound && !st.Connection.InFlight()
	})
	assert.False(t, st.Connection.Scheduled(), "the watch reports the file moving; polling asks nothing new")
}

// --- the asks behind the news ---

// A new context asks for its connection to be probed; the claim does not wait for it. Reading
// the kubeconfig is not work to do on the caller's thread.
func TestAcquireAsksForANewContextsPresence(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	startService(t, s)
	lease := s.Acquire("prod")
	defer lease.Release()
	require.Eventually(t, func() bool { return !lease.Departed() },
		time.Second, time.Millisecond, "presence was never read")

	cfg.rotate("prod", "")
	cfg.changed()

	require.Eventually(t, lease.Departed,
		time.Second, time.Millisecond, "a kubeconfig change never reached the claim")
}

// A later holder joins what the first one's probe found rather than asking for another read.
// Only the engine runs: the kubeconfig watch's subscription seed wakes a re-read this test must
// not count, and coalescing makes that wake land as zero or one extra reads — so it stays off.
func TestAcquireAsksOnlyForANewContext(t *testing.T) {
	cfg := counting("prod", "key-1")
	s := New(cfg)
	stop := s.engine.Start(t.Context())
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
	first := s.Acquire("prod")
	defer first.Release()
	require.Equal(t, "prod", cfg.reads.Await(t, "the first claim's read"))

	second := s.Acquire("prod")
	defer second.Release()

	// A negative assertion needs a bounded window: this fails the instant a read lands.
	testutil.NoRecv(t, cfg.reads.Chan(), 50*time.Millisecond, "a second holder asked for a read")
}

// A kubeconfig change landing while a read is in flight earns a read of its own: the one out
// had already passed the file when it moved.
func TestAChangeArrivingDuringAReadIsReadAgain(t *testing.T) {
	cfg := counting("prod", "key-1")
	s := New(cfg)
	startService(t, s)
	cfg.duringRead(cfg.changed)

	defer s.Acquire("prod").Release()

	assert.Equal(t, "prod", cfg.reads.Await(t, "the claim's read"))
	assert.Equal(t, "prod", cfg.reads.Await(t, "the read the mid-flight change earned"))
}

// The ask a claim makes before Start waits in the engine's queues for its workers, so a context
// that had already gone does not sit reported as present.
func TestAClaimTakenBeforeStartIsChecked(t *testing.T) {
	s := New(resolving("staging", "key-1")) // "prod" is not named
	lease := s.Acquire("prod")
	defer lease.Release()
	require.False(t, lease.Departed(), "nothing has read the file yet")

	startService(t, s)

	require.Eventually(t, lease.Departed, time.Second, time.Millisecond,
		"a claim taken before Start was never checked")
}

// --- the state hub ---

// A receiver is bound to its key for life, and a claim's key is its context — which never moves,
// however the credentials behind it do.
func TestWatchStateIsKeyedByContext(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	lease := s.Acquire("prod")
	defer lease.Release()
	sub := lease.WatchState()
	defer sub.Close()

	s.stateHub.Sender().Send("prod", State{ServerUID: Observation[string]{Value: "uid-1", LastSeen: runAt}})

	ev, err := sub.RecvContext(within(t))
	require.NoError(t, err)
	assert.Equal(t, "uid-1", ev.Value.ServerUID.Value)
}

// A state receiver is the caller's, not the claim's: releasing does not close it, which is why
// the contract says to. One kept past its claim holds a hub slot and reports whatever claims
// that context next.
func TestReleaseDoesNotCloseAStateWatcher(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	lease := s.Acquire("prod")
	watched := lease.WatchState()
	defer watched.Close()

	lease.Release()
	s.stateHub.Sender().Send("prod", State{ServerUID: Observation[string]{Value: "uid-1", LastSeen: runAt}})

	ev, err := watched.RecvContext(within(t))
	require.NoError(t, err, "the receiver is still live, and still the caller's to close")
	assert.Equal(t, "uid-1", ev.Value.ServerUID.Value)
}

// The subscriptions are taken before Start returns, so a config sent straight after it is read
// rather than dropped, and stop joins the loops instead of leaving them running.
func TestStartWatchesTheKubeconfigUntilStopped(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)

	stop, err := s.Start(t.Context())
	require.NoError(t, err)
	require.NotNil(t, cfg.hub, "Start subscribes before it returns")
	cfg.changed()

	require.NoError(t, stop(t.Context()))
}

// A pass can land after the last holder let go — the engine's Remove and this publish are not
// one step. Announcing then would tell watchers on this name about a claim nobody holds, and
// leave news behind for whatever claims it next to be compared against.
func TestAPublishForAReleasedClaimIsDropped(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	lease := s.Acquire("prod")
	v, ok := s.engine.Read("prod")
	require.True(t, ok)
	s.publish("prod", v)
	require.Contains(t, s.published, "prod", "a held claim publishes")

	lease.Release()
	s.publish("prod", v)

	assert.NotContains(t, s.published, "prod")
}

// The feed ending is one of the two ways the watch is meant to finish; the other is its context
// being cancelled. Neither leaves the goroutine parked.
func TestTheKubeconfigWatchEndsWhenItsFeedCloses(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	cfgs := cfg.Subscribe()
	cfgs.Close()

	testutil.WaitReturn(t, func() { s.watchKubeconfig(context.Background(), cfgs) }, "the watch to end")
}
