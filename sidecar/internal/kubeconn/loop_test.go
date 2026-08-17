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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// Building a connection is not a request to validate it. A negative assertion, so it needs
// a bounded window — several times the shrunk cadence, and it fails the moment a probe
// arrives rather than at the end of the wait.
func TestEntryForAloneProbesNothing(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{}))

	_, err := f.entryFor(testConfig(), "key-a")
	require.NoError(t, err)

	testutil.NoRecv(t, f.probes.Chan(), 20*testCadence, "building a connection must not probe it")
	assert.False(t, f.running("key-a"))
}

func TestProbeNowProbesOnce(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{ServerUID: "uid-1"}))

	require.NoError(t, f.ProbeNow(testConfig(), "key-a"))
	f.probes.Await(t, "probe")

	assert.Equal(t, "uid-1", f.awaitResult(t, "key-a").Identity.ServerUID)
}

// The cadence waits for a claim: one probe was asked for, and one is what it gets.
func TestProbeNowParksAfterItsProbe(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{}))

	require.NoError(t, f.ProbeNow(testConfig(), "key-a"))
	f.probes.Await(t, "probe")

	testutil.NoRecv(t, f.probes.Chan(), 20*testCadence, "an unclaimed key must not re-probe")
	require.Eventually(t, func() bool { return !f.running("key-a") }, testutil.Timeout, testCadence)
}

// A claim is what arms the cadence: once one is held the loop keeps going on its own.
func TestALeaseArmsTheCadence(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{}))

	f.acquire(t, "key-a")
	f.probes.Await(t, "the claimed probe")
	f.probes.Await(t, "cadence probe")
}

// Both wakes are "run again" requests, so a second while one is pending adds nothing — and
// neither may block the caller, which holds the service's lock.
func TestKickAndNudgeCoalesce(t *testing.T) {
	e := &entry{wake: make(chan struct{}, 1), idle: make(chan struct{}, 1)}

	e.kick()
	e.kick()
	e.nudge()
	e.nudge()

	assert.Len(t, e.wake, 1)
	assert.Len(t, e.idle, 1)
}

// A shutdown landing between entryFor's re-check and this call is the one way demand sees
// it: the lock is released in between, so the guard cannot be dropped. Its refusal has to
// reach the caller, or Acquire hands back a claim nothing will ever answer.
func TestDemandRefusesOnceStopped(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{}))

	e, err := f.entryFor(testConfig(), "key-a")
	require.NoError(t, err)

	f.mu.Lock()
	f.stopped = true
	err = f.demand("key-a", e)
	leases := e.leases
	f.mu.Unlock()

	require.ErrorIs(t, err, ErrClosed)
	assert.False(t, f.running("key-a"))
	assert.Zero(t, leases, "a refused claim must not be counted")
}

// Shutdown has to reach a loop parked between probes, not only one mid-probe.
func TestShutdownEndsALoopParkedOnTheCadence(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{}))
	// A cadence nothing will reach, so the loop is provably parked on it and the
	// cancellation below is the only thing that can wake it.
	f.budget.Cadence = time.Hour

	f.acquire(t, "key-a")
	f.probes.Await(t, "the claimed probe")
	require.True(t, f.running("key-a"))

	f.stopLoops()
	require.Eventually(t, func() bool { return !f.running("key-a") }, testutil.Timeout, testCadence)
}

// Releasing the last claim while the loop waits out the cadence must end it there, not one
// probe later: that probe has nobody waiting for it, and for credentials that dial through
// a helper it is a subprocess too.
func TestReleasingTheLastLeaseSkipsTheNextProbe(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{}))

	lease := f.acquire(t, "key-a")
	f.probes.Await(t, "the claimed probe")
	// Parked on the cadence now, so the release below is what ends the loop.
	lease.Release()

	require.Eventually(t, func() bool { return !f.running("key-a") }, testutil.Timeout, testCadence)
	testutil.NoRecv(t, f.probes.Chan(), 20*testCadence, "a released key must not probe on the way out")
}

// A re-check that finds the loop still wanted leaves it running. Nudged directly: the
// count reaching zero is what sends one, and a claim arriving back in that window is too
// narrow to drive from a test.
func TestAnIdleRecheckKeepsAWantedLoop(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{}))

	f.acquire(t, "key-a")
	f.probes.Await(t, "the claimed probe")

	f.mu.Lock()
	e := f.conns["key-a"]
	f.mu.Unlock()
	e.nudge()

	f.probes.Await(t, "the cadence, still running")
	assert.True(t, f.running("key-a"))
}

// The loop is demand's, so it ends with the last claim rather than idling until shutdown.
func TestTheLoopEndsWithTheLastLease(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{}))

	lease := f.acquire(t, "key-a")
	f.probes.Await(t, "the claimed probe")
	require.True(t, f.running("key-a"))

	lease.Release()
	require.Eventually(t, func() bool { return !f.running("key-a") }, testutil.Timeout, testCadence)
}

// A second claim holds the cadence open when the first goes, so one caller releasing
// cannot stop credentials another is still using.
func TestTheCadenceOutlivesOneOfTwoLeases(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{}))

	first, err := f.Acquire(testConfig(), "key-a")
	require.NoError(t, err)
	f.acquire(t, "key-a")

	first.Release()
	// Release is idempotent, so a caller that defers it and releases early cannot
	// double-count the drop and take the other claim down with it.
	first.Release()

	f.probes.Await(t, "the claimed probe")
	f.probes.Await(t, "cadence probe")
}

// A claim arriving as the loop winds down must not fall between the two: the loop's exit
// and the claim are decided under one lock, so the claim either keeps the loop alive or
// starts a fresh one.
func TestAClaimAfterTheLoopEndsStartsAnother(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{ServerUID: "uid-1"}))

	first, err := f.Acquire(testConfig(), "key-a")
	require.NoError(t, err)
	f.probes.Await(t, "the claimed probe")
	first.Release()
	require.Eventually(t, func() bool { return !f.running("key-a") }, testutil.Timeout, testCadence)

	second := f.acquire(t, "key-a")
	f.probes.Await(t, "the probe the new claim asked for")

	conn, err := second.Conn(t.Context())
	require.NoError(t, err)
	require.NotNil(t, conn)
}

// The whole point of one entry per credential key: two callers under one key share the
// probe, rather than each running their own against the same server.
func TestTwoLeasesOnOneKeyShareOneProbe(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{}))

	f.acquire(t, "key-a")
	f.acquire(t, "key-a")
	f.probes.Await(t, "the shared probe")

	// One loop, so probes arrive at the cadence rather than two at a time. A negative
	// assertion against a doubled rate, bounded well inside two cadences.
	f.probes.Await(t, "the next cadence probe")
	testutil.NoRecv(t, f.probes.Chan(), testCadence/2, "one key must run one probe loop, however many claims it has")
}

// A caller holding no claim still has to hear that a probe landed, or nothing it wrote
// down would ever be brought up to date.
func TestAProbeAnnouncesNewsToObservers(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{ServerUID: "uid-1"}))

	announced := f.watchNews(t, "key-a")

	require.NoError(t, f.ProbeNow(testConfig(), "key-a"))
	assert.Equal(t, "key-a", testutil.Recv(t, announced.Chan(), "the announcement"))
}

// A user retrying a cluster that is still down has watched Probing go true, and only the
// probe they asked for turns it back off. Nothing else would tell them it finished.
func TestAnAskIsAnsweredEvenWhenTheVerdictRepeats(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{ServerUID: "uid-1"}))
	announced := f.watchNews(t, "key-a")

	require.NoError(t, f.ProbeNow(testConfig(), "key-a"))
	assert.Equal(t, "key-a", testutil.Recv(t, announced.Chan(), "the first ask"))
	f.probes.Await(t, "the first probe")

	// Same answer, so nothing about the cluster changed — but the ask still has to land.
	require.NoError(t, f.ProbeNow(testConfig(), "key-a"))
	assert.Equal(t, "key-a", testutil.Recv(t, announced.Chan(), "the repeat ask"))
}

// A shutdown taken mid-probe must not leave the key claiming to be probing for good.
func TestShutdownClearsProbing(t *testing.T) {
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	f := newProbeFixture(t, func(context.Context, *Connection) (Identity, error) {
		<-release
		return Identity{}, nil
	})
	t.Cleanup(releaseOnce)

	require.NoError(t, f.ProbeNow(testConfig(), "key-a"))
	f.probes.Await(t, "the probe now parked")
	require.True(t, f.State("key-a").Probing)

	f.stopLoops()
	releaseOnce()

	require.Eventually(t, func() bool { return !f.State("key-a").Probing }, testutil.Timeout, testCadence)
}

// A probe that learned nothing new must not announce: that pass can only conclude nothing
// moved, and it would run per key on every cadence.
func TestAnUnchangedResultDoesNotAnnounce(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{ServerUID: "uid-1"}))

	// Read the bus here rather than through the fixture's reader: this asserts that
	// nothing was published, and a reader on its own goroutine could always be one hop
	// behind a publish that did happen.
	sub := f.Subscribe("key-a")
	defer sub.Close()

	f.acquire(t, "key-a")

	// A first probe publishes twice — that it started, and its result — and the bus may
	// coalesce those into one. So take whatever it left rather than counting, once it has
	// finished: the next probe starting proves that, since one loop runs them in turn.
	f.probes.Await(t, "the claimed probe")
	f.probes.Await(t, "a later probe")
	for {
		if _, err := sub.TryRecv(); err != nil {
			break
		}
	}

	// From here every probe repeats an answer already published.
	f.probes.Await(t, "another later probe")
	f.probes.Await(t, "and another")

	_, err := sub.TryRecv()
	assert.Error(t, err, "an identical result must not announce")
}

// Probing is asked-for-and-unanswered, not running-right-now: a caller that asks and then
// reads State must not be told nothing is happening while its request sits behind the
// semaphore.
func TestProbingCoversTheWaitForASlot(t *testing.T) {
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	f := newProbeFixture(t, func(context.Context, *Connection) (Identity, error) {
		<-release
		return Identity{}, nil
	})
	t.Cleanup(releaseOnce)

	// Every slot held by a probe that has provably started, so the next key can only be
	// queued behind them.
	for i := range testConcurrency {
		require.NoError(t, f.ProbeNow(testConfig(), "busy-"+strconv.Itoa(i)))
		f.probes.Await(t, "a probe taking a slot")
	}

	require.NoError(t, f.ProbeNow(testConfig(), "queued"))
	// Its loop has taken the kick, so it is inside the probe and waiting on the semaphore.
	require.Eventually(t, func() bool { return !f.asked("queued") }, testutil.Timeout, testCadence)

	state := f.State("queued")
	assert.True(t, state.Probing, "asked for and unanswered, however long the wait")
	assert.Nil(t, state.Last, "nothing is known yet, which is the state being reported")
}

// A probe that lands after shutdown began describes the shutdown, not the cluster, so it
// must not be stored as what these credentials reach.
func TestAProbeCancelledByShutdownIsDropped(t *testing.T) {
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	f := newProbeFixture(t, func(context.Context, *Connection) (Identity, error) {
		<-release
		return Identity{ServerUID: "uid-1"}, nil
	})
	t.Cleanup(releaseOnce)

	require.NoError(t, f.ProbeNow(testConfig(), "key-a"))
	f.probes.Await(t, "the probe now parked")

	// Cancelled directly rather than through the stop func, which would race the release
	// below: this is the same cancellation, ordered.
	f.stopLoops()
	releaseOnce()

	require.Eventually(t, func() bool { return !f.running("key-a") }, testutil.Timeout, testCadence)
	assert.Nil(t, f.State("key-a").Last, "a probe cancelled with the loop says nothing")
}

// A slot is held for a probe's whole run, so a server that completes the handshake and
// then never answers would hold one of the four until the process ended — and four of them
// would starve every other key.
func TestAProbeIsBounded(t *testing.T) {
	f := newProbeFixture(t, func(ctx context.Context, _ *Connection) (Identity, error) {
		<-ctx.Done()
		return Identity{}, ctx.Err()
	})

	require.NoError(t, f.ProbeNow(testConfig(), "key-a"))
	require.Eventually(t, func() bool {
		last := f.State("key-a").Last
		return last != nil && errors.Is(last.Err, context.DeadlineExceeded)
	}, testutil.Timeout, testCadence)
}

// A first install asks for a probe per set of credentials in one pass, and each may run a
// credential helper. The semaphore is what keeps that from being 30 keychain prompts in
// one second.
func TestFirstProbesAreBounded(t *testing.T) {
	var inFlight, peak atomic.Int32
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	f := newProbeFixture(t, func(context.Context, *Connection) (Identity, error) {
		n := inFlight.Add(1)
		for {
			was := peak.Load()
			if n <= was || peak.CompareAndSwap(was, n) {
				break
			}
		}
		<-release
		inFlight.Add(-1)
		return Identity{}, nil
	})

	// Registered after the service's own cleanup, so it runs before it: cleanups are LIFO,
	// and a failed assertion that left the probes parked would deadlock the service's
	// drain and swallow the failure.
	t.Cleanup(releaseOnce)

	const keys = 3 * testConcurrency
	for i := range keys {
		require.NoError(t, f.ProbeNow(testConfig(), "key-"+strconv.Itoa(i)))
	}
	// Every slot busy is the state worth measuring; the rest are queued behind it.
	require.Eventually(t, func() bool { return inFlight.Load() == testConcurrency }, testutil.Timeout, testCadence)
	releaseOnce()

	for range keys {
		f.probes.Await(t, "probe")
	}
	require.LessOrEqual(t, peak.Load(), int32(testConcurrency))
}

// Shutdown must reach a key still queued behind the semaphore, not only the ones already
// probing — otherwise the drain waits on a goroutine nothing will release.
func TestShutdownReleasesAKeyWaitingForASlot(t *testing.T) {
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	f := newProbeFixture(t, func(context.Context, *Connection) (Identity, error) {
		<-release
		return Identity{}, nil
	})
	t.Cleanup(releaseOnce)

	// Every slot held by a probe that has provably started, so the next key parks on the
	// semaphore rather than racing the cancellation to reach it.
	for i := range testConcurrency {
		require.NoError(t, f.ProbeNow(testConfig(), "busy-"+strconv.Itoa(i)))
		f.probes.Await(t, "a probe taking a slot")
	}
	require.NoError(t, f.ProbeNow(testConfig(), "queued"))
	// Its loop has taken the kick, so it is inside the probe and on the semaphore — where
	// the cancellation below is the only arm of its select that can be ready.
	require.Eventually(t, func() bool { return !f.asked("queued") }, testutil.Timeout, testCadence)

	// The same cancellation the stop func makes, ordered against the release below.
	f.stopLoops()
	releaseOnce()

	require.Eventually(t, func() bool { return !f.running("queued") }, testutil.Timeout, testCadence)
	assert.Nil(t, f.State("queued").Last, "the key never got a slot, so it never probed")
}

// Demand arriving mid-shutdown must not add a goroutine to a WaitGroup already being
// waited on.
func TestDemandAfterShutdownIsRefused(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{}))

	stop, err := f.Service.Start(context.Background())
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))

	// Refused rather than accepted-and-ignored: no loop will start after this, so a claim
	// taken here would hold a Conn that waits on a result nothing is going to produce.
	require.ErrorIs(t, f.ProbeNow(testConfig(), "key-a"), ErrClosed)
	lease, err := f.Acquire(testConfig(), "key-a")
	require.ErrorIs(t, err, ErrClosed)
	assert.Nil(t, lease)

	assert.False(t, f.running("key-a"))
}
