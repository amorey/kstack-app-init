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
type fakeKubeconfig struct {
	mu     sync.Mutex
	keys   map[string]string
	err    error
	hub    *watch.Hub[*api.Config]
	onRead func()
	// reads names each context read, for a test counting them rather than their answers.
	reads *testutil.Probe[string]
}

// resolving is a kubeconfig where each context resolves to the key beside it.
func resolving(contextKeys ...string) *fakeKubeconfig {
	f := &fakeKubeconfig{keys: map[string]string{}}
	for i := 0; i < len(contextKeys); i += 2 {
		f.keys[contextKeys[i]] = contextKeys[i+1]
	}
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

// duringRead runs fn inside the next read, so a test can interleave a release and a fresh claim
// with one in flight. Latency in the code under test on purpose — the test's own assertions stay
// immediate.
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
	return &rest.Config{Host: "https://" + key}, key, nil
}

func (f *fakeKubeconfig) Subscribe() kubeconfig.Subscription {
	if f.hub == nil {
		f.hub = watch.New[*api.Config](nil)
	}
	return f.hub.Receiver()
}

// A claim is taken whatever the file says. Whether the context resolves is the pool's to find
// out on its own schedule, and a claim is how the holder hears about it — refusing would make a
// transient fact permanent for the caller.
func TestAcquireClaimsAContextTheKubeconfigDoesNotName(t *testing.T) {
	s := New(&fakeKubeconfig{})

	lease := s.Acquire("prod")
	defer lease.Release()

	assert.Len(t, s.claimed, 1)
	assert.Equal(t, PhasePending, lease.State().Phase(), "nothing has looked yet")
	assert.True(t, lease.State().Connection.Scheduled(), "and looking is what the claim asked for")
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

// --- the news: a context leaving the kubeconfig ---

// The one thing this service does. A holder is told rather than left to poll, and it learns what
// changed by asking the claim.
func TestDepartureReachesTheHolder(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	lease := s.Acquire("prod")
	defer lease.Release()
	s.checkConnection("prod")
	require.False(t, lease.Departed())
	news := s.Subscribe()
	defer news.Close()

	cfg.rotate("prod", "")
	s.checkConnection("prod")

	ev, err := news.RecvContext(within(t))
	require.NoError(t, err)
	assert.Equal(t, "prod", ev.Key)
	assert.True(t, lease.Departed())
}

// The claim outlives what it is a claim on: the file may name the context again, and this claim
// is how the holder hears about that.
func TestDepartureKeepsTheClaim(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	lease := s.Acquire("prod")
	defer lease.Release()

	cfg.rotate("prod", "")
	s.checkConnection("prod")
	require.True(t, lease.Departed())

	cfg.rotate("prod", "key-1")
	s.checkConnection("prod")

	assert.False(t, lease.Departed(), "named again, and the same claim reports it")
	assert.Len(t, s.claimed, 1)
}

// A departed context is announced once. Announcing it on every kubeconfig write would wake its
// holder forever for news it already has.
func TestDepartureIsAnnouncedOnce(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	defer s.Acquire("prod").Release()
	s.checkConnection("prod")
	news := s.Subscribe()
	defer news.Close()

	cfg.rotate("prod", "")
	s.checkConnection("prod")
	_, err := news.RecvContext(within(t))
	require.NoError(t, err)

	s.checkConnection("prod")

	// A negative assertion needs a bounded window: this fails the instant a second one lands.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err = news.RecvContext(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// An unread kubeconfig names nothing. Reporting that as a departure would tell every holder its
// context is gone for as long as the first read takes.
func TestUnreadKubeconfigIsNotADeparture(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	lease := s.Acquire("prod")
	defer lease.Release()
	s.checkConnection("prod")

	cfg.mu.Lock()
	cfg.err = kubeconfig.ErrNotRead
	cfg.mu.Unlock()
	s.checkConnection("prod")

	assert.False(t, lease.Departed())
}

// --- the queue behind it ---

// A new context asks for its presence to be read; the claim does not wait for it. Reading the
// kubeconfig is not work to do on the caller's thread.
func TestAcquireAsksForANewContextsPresence(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	startService(t, s)
	lease := s.Acquire("prod")
	defer lease.Release()
	require.Eventually(t, func() bool { return !lease.Departed() },
		time.Second, time.Millisecond, "presence was never read")

	cfg.rotate("prod", "")
	cfg.hub.Sender().Send(&api.Config{})

	require.Eventually(t, func() bool { return lease.Departed() },
		time.Second, time.Millisecond, "a kubeconfig change never reached the claim")
}

// A later holder joins what the first one's check found rather than asking for another.
func TestAcquireAsksOnlyForANewContext(t *testing.T) {
	s := New(resolving("prod", "key-1"))

	first := s.Acquire("prod")
	defer first.Release()
	key, ok := s.checkQ.Next(within(t))
	require.True(t, ok)
	require.Equal(t, connectionOf("prod"), key)
	// Given back first: a key added while held is queued behind the run rather than delivered,
	// which would hide the very ask this test is looking for.
	s.checkQ.Done(key)

	second := s.Acquire("prod")
	defer second.Release()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, ok = s.checkQ.Next(ctx)
	assert.False(t, ok, "a second holder asked for a check")
}

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

// The subscriptions are taken before Start returns, so a config sent straight after it is read
// rather than dropped, and stop joins the loops instead of leaving them running.
func TestStartWatchesTheKubeconfigUntilStopped(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)

	stop, err := s.Start(t.Context())
	require.NoError(t, err)
	require.NotNil(t, cfg.hub, "Start subscribes before it returns")
	cfg.hub.Sender().Send(&api.Config{})

	require.NoError(t, stop(t.Context()))
}

// within bounds a wait for something that must arrive, so a regression fails the test rather
// than hanging it until the suite's own deadline.
func within(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// startService runs the pool's loops for a test that needs them, and joins them on cleanup.
func startService(t *testing.T, s *Service) {
	t.Helper()
	stop, err := s.Start(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
}

// connectionOf is the queue key a claim and a kubeconfig change ask for.
func connectionOf(contextName string) checkKey {
	return checkKey{contextName: contextName, check: checkConnection}
}

// checkConnection runs the connection check the way a worker would, on the test's goroutine, so
// a test asserts on settled state with no loops running.
func (s *Service) checkConnection(contextName string) {
	s.runCheck(context.Background(), connectionOf(contextName))
}

// A holder watching its own claim is told too, not just the fleet feed, and the value carries
// why: a check that suspends has to record its reason or nothing explains the suspension.
func TestDepartureReachesAClaimWatcher(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	lease := s.Acquire("prod")
	defer lease.Release()
	s.checkConnection("prod")
	watched := lease.WatchState()
	defer watched.Close()

	cfg.rotate("prod", "")
	s.checkConnection("prod")

	ev, err := watched.RecvContext(within(t))
	require.NoError(t, err)
	assert.Equal(t, ReasonContextNotFound, ev.Value.Connection.LastAttempt.Reason)
	assert.False(t, ev.Value.Connection.Scheduled(), "nothing to reach, so nothing is due")
}

// A kubeconfig change reaches a holder through the loops, not only through a direct read: the
// watch asks, the queue carries, and the claim reports.
func TestAKubeconfigChangeReachesTheHolder(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	startService(t, s)
	lease := s.Acquire("prod")
	defer lease.Release()
	require.Eventually(t, func() bool { return !lease.Departed() },
		time.Second, time.Millisecond, "the first presence read never landed")

	cfg.rotate("prod", "")
	cfg.hub.Sender().Send(&api.Config{})

	require.Eventually(t, lease.Departed,
		time.Second, time.Millisecond, "the change never reached the claim")
}

// A claim reads as departed once nothing holds its context: there is no entry left to answer
// from, and reporting present would be a claim about a context the pool stopped tracking.
func TestAReleasedClaimReadsAsDeparted(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	lease := s.Acquire("prod")
	s.checkConnection("prod")
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
	s.checkConnection("prod")
	require.False(t, lease.Departed())

	require.NoError(t, s.Close())

	assert.Empty(t, s.claimed)
	assert.True(t, lease.Departed())
}

// A context the file still names but cannot resolve — a cluster entry it points at that is
// gone, credentials that will not load — has not departed. It is the file that is broken, and
// saying otherwise reports a removal the user did not make.
func TestAResolveFailureIsNotADeparture(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	lease := s.Acquire("prod")
	defer lease.Release()
	s.checkConnection("prod")
	require.False(t, lease.Departed())

	cfg.mu.Lock()
	cfg.err = errors.New("no such certificate authority file")
	cfg.mu.Unlock()
	s.checkConnection("prod")

	assert.False(t, lease.Departed())
}

// The last holder can release while a read is in flight and another caller re-claim the same
// name. The entry it gets is a different one, and an answer that predates it is not about it.
func TestAReadIsNotCommittedToAReplacementClaim(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	first := s.Acquire("prod")
	cfg.rotate("prod", "") // so the read now in flight will answer "gone"

	var second Lease
	cfg.duringRead(func() {
		first.Release()
		second = s.Acquire("prod")
	})
	s.checkConnection("prod")

	require.NotNil(t, second)
	defer second.Release()
	assert.False(t, second.Departed(), "answered about the claim that went away, not this one")
}

// The ask a claim makes before Start waits in the queue for the worker Start takes. Asserted on
// the queue directly, because end to end it is also covered by the kubeconfig watch: subscribing
// to a gochan hub delivers its seed, so Start enqueues every claimed context anyway. That is a
// second mechanism in another package, not a guarantee this one makes.
func TestAClaimTakenBeforeStartStaysQueued(t *testing.T) {
	s := New(resolving("staging", "key-1")) // "prod" is not named

	defer s.Acquire("prod").Release()

	key, ok := s.checkQ.Next(within(t))
	require.True(t, ok)
	assert.Equal(t, connectionOf("prod"), key)
}

// An ask that lands while a read is in flight gets a read of its own. That read had already
// passed the file when the change happened, so answering the ask with it would report a file
// nothing observed.
func TestAnAskArrivingDuringAReadIsReadAgain(t *testing.T) {
	cfg := resolving("prod", "key-1")
	cfg.reads = testutil.NewProbe[string](4)
	s := New(cfg)
	startService(t, s)

	cfg.duringRead(func() { s.checkQ.Add(connectionOf("prod")) })
	defer s.Acquire("prod").Release()

	assert.Equal(t, "prod", cfg.reads.Await(t, "the claim's read"))
	assert.Equal(t, "prod", cfg.reads.Await(t, "the read the mid-flight ask earned"))
}

// And it is read once the loop runs, so a context that had already gone does not sit reported
// as present.
func TestAClaimTakenBeforeStartIsChecked(t *testing.T) {
	s := New(resolving("staging", "key-1"))
	lease := s.Acquire("prod")
	defer lease.Release()
	require.False(t, lease.Departed(), "nothing has read the file yet")

	startService(t, s)

	require.Eventually(t, lease.Departed, time.Second, time.Millisecond,
		"a claim taken before Start was never checked")
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
