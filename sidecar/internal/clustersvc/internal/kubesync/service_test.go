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

package kubesync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// fakeConns stands in for the pool: Acquire hands out a lease that only counts
// releases, which is all this leaf's connService interface needs.
type fakeConns struct {
	mu       sync.Mutex
	acquired []string
	released int
}

func (f *fakeConns) Acquire(contextName string) kubeconn.Lease {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acquired = append(f.acquired, contextName)
	return &fakeLease{conns: f}
}

func (f *fakeConns) releaseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.released
}

type fakeLease struct{ conns *fakeConns }

func (l *fakeLease) Conn(context.Context) (*kubeconn.Connection, error) { return nil, nil }
func (l *fakeLease) ConnFor(context.Context, string) (*kubeconn.Connection, error) {
	return nil, nil
}
func (l *fakeLease) State() kubeconn.State                  { return kubeconn.State{} }
func (l *fakeLease) WatchState() kubeconn.StateSubscription { return nil }
func (l *fakeLease) Departed() bool                         { return false }
func (l *fakeLease) Release() {
	l.conns.mu.Lock()
	defer l.conns.mu.Unlock()
	l.conns.released++
}

// run is one call into a seamed sync body: the params it was started with, the
// ctx it runs on, and the commit func to call — captured so a test can commit
// after the body would otherwise have exited, or after the subject was forgotten.
type run struct {
	ctx    context.Context
	p      Params
	commit func(Observation)
}

// fakeSync hands every worker's run to the test via starts, and blocks each
// worker until its ctx ends — the seam every behavior in this file drives by
// hand, never by racing the production sync loop.
type fakeSync struct {
	starts *testutil.Probe[*run]
}

func newFakeSync() *fakeSync {
	return &fakeSync{starts: testutil.NewProbe[*run](8)}
}

func (f *fakeSync) body(ctx context.Context, p Params, commit func(Observation)) {
	f.starts.Fire(&run{ctx: ctx, p: p, commit: commit})
	<-ctx.Done()
}

func testParams(cacheID int64) Params {
	return Params{
		CacheID:     cacheID,
		ContextName: "prod",
		ServerUID:   "uid-1",
		APIVersion:  "v1",
		Resource:    "pods",
		Namespaced:  true,
	}
}

func newTestService(conns connService, f *fakeSync) *Service {
	return newWithOptions(conns, withSync(f.body))
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

// Track starts exactly one worker, with the params given, and acquires a lease
// for the context they name.
func TestTrackStartsOneWorker(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newTestService(conns, f)
	startService(t, svc)

	p := testParams(1)
	svc.Track("resource/1", p)

	r := f.starts.Await(t, "the worker's start")
	assert.Equal(t, p, r.p)
	assert.Equal(t, []string{"prod"}, conns.acquired)
}

// Track of an already-tracked id with the same params is a no-op: the body does
// not run again, and no second lease is taken.
func TestTrackOfATrackedIDIsANoOp(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newTestService(conns, f)
	startService(t, svc)

	p := testParams(1)
	svc.Track("resource/1", p)
	f.starts.Await(t, "the first worker's start")

	svc.Track("resource/1", p)

	// A negative assertion has no event to wait for, so it needs a bounded window;
	// a second worker starting immediately calls fakeSync.body synchronously.
	testutil.NoRecv(t, f.starts.Chan(), testutil.Timeout/50, "a second worker start")
	assert.Equal(t, []string{"prod"}, conns.acquired, "no second lease")
}

// Track with params that moved replaces the worker: the old one is cancelled, its
// lease released, and the new one runs with the new params over the new context.
func TestTrackWithChangedParamsReplacesTheWorker(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newTestService(conns, f)
	startService(t, svc)

	svc.Track("resource/1", testParams(1))
	first := f.starts.Await(t, "the first worker's start")
	first.commit(Observation{Reason: ReasonWatching, ObjectCount: 5})

	next := testParams(1)
	next.ContextName = "staging"
	next.Namespaced = false
	svc.Track("resource/1", next)

	assert.Error(t, first.ctx.Err(), "the worker syncing the old params was not cancelled")
	second := f.starts.Await(t, "the replacement worker's start")
	assert.Equal(t, next, second.p)
	assert.Equal(t, []string{"prod", "staging"}, conns.acquired, "a lease on the new context")
	assert.Equal(t, 1, conns.releaseCount(), "the old context's lease released")

	// A different sync, so the replaced worker's answer does not carry over.
	_, ok := svc.Read("resource/1")
	assert.False(t, ok, "the observation survived a params change")
}

// Before any commit, Read reports zero/false for a tracked id and false for an
// unknown one. A commit lands the Observation.
func TestReadBeforeAndAfterCommit(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newTestService(conns, f)
	startService(t, svc)

	_, ok := svc.Read("unknown")
	assert.False(t, ok, "an unknown id")

	svc.Track("resource/1", testParams(1))
	r := f.starts.Await(t, "the worker's start")

	obs, ok := svc.Read("resource/1")
	assert.False(t, ok, "tracked but not yet committed")
	assert.Equal(t, Observation{}, obs)

	// Subscribed before the commit, so its signal proves the write landed before
	// Read is asked to see it.
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	want := Observation{Reason: ReasonWatching, ObjectCount: 3}
	r.commit(want)
	testutil.Recv(t, sub.Chan(), "the commit's signal")

	obs, ok = svc.Read("resource/1")
	assert.True(t, ok)
	assert.Equal(t, want, obs)
}

// The signal fires when the news moves, and only then: a repeat commit with the
// same Reason is silent, and a Reason change signals again.
func TestSignalFiresOnlyWhenTheReasonMoves(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newTestService(conns, f)
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("resource/1", testParams(1))
	r := f.starts.Await(t, "the worker's start")

	r.commit(Observation{Reason: ReasonSyncing, ObjectCount: 1})
	ev := testutil.Recv(t, sub.Chan(), "the first commit's signal")
	assert.Equal(t, "resource/1", ev.Key)

	// Same Reason, different counts/timestamps: no signal.
	r.commit(Observation{Reason: ReasonSyncing, ObjectCount: 2})

	// A new Reason signals again — asserted by ordering: the next event received
	// after the quiet commit is this one, proving the quiet commit produced none.
	r.commit(Observation{Reason: ReasonWatching, ObjectCount: 2})
	ev = testutil.Recv(t, sub.Chan(), "the reason-changing commit's signal")
	assert.Equal(t, "resource/1", ev.Key)

	obs, ok := svc.Read("resource/1")
	require.True(t, ok)
	assert.Equal(t, ReasonWatching, obs.Reason)
}

// Forget cancels the worker's ctx, waits for the body to exit, releases the
// lease, and drops the observation. Forget of an unknown id is a no-op.
func TestForgetCancelsWaitsReleasesAndDrops(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newTestService(conns, f)
	startService(t, svc)

	svc.Track("resource/1", testParams(1))
	r := f.starts.Await(t, "the worker's start")
	r.commit(Observation{Reason: ReasonWatching})

	bodyDone := make(chan struct{})
	go func() {
		<-r.ctx.Done()
		close(bodyDone)
	}()

	svc.Forget("resource/1")

	testutil.Wait(t, bodyDone, "the body's ctx to end")
	assert.Equal(t, 1, conns.releaseCount())
	_, ok := svc.Read("resource/1")
	assert.False(t, ok)

	svc.Forget("resource/1")
	assert.Equal(t, 1, conns.releaseCount(), "idempotent")
}

// A commit that lands after Forget must not resurrect the id.
func TestCommitAfterForgetDoesNotResurrect(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newTestService(conns, f)
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("resource/1", testParams(1))
	r := f.starts.Await(t, "the worker's start")

	svc.Forget("resource/1")

	r.commit(Observation{Reason: ReasonWatching})

	_, ok := svc.Read("resource/1")
	assert.False(t, ok, "the stale commit must not resurrect the id")

	// A negative assertion has no event to wait for, so it needs a bounded window.
	testutil.NoRecv(t, sub.Chan(), testutil.Timeout/50, "a signal from the stale commit")
}

// Bounce cancels the first worker's ctx and, only after it exits, starts a new
// worker with the same params; the last observation survives.
func TestBounceRestartsInPlaceKeepingTheObservation(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newTestService(conns, f)
	startService(t, svc)

	p := testParams(1)
	svc.Track("resource/1", p)
	first := f.starts.Await(t, "the first worker's start")
	first.commit(Observation{Reason: ReasonWatching, ObjectCount: 5})

	bodyDone := make(chan struct{})
	go func() {
		<-first.ctx.Done()
		close(bodyDone)
	}()

	svc.Bounce("resource/1")

	testutil.Wait(t, bodyDone, "the first body's ctx to end")
	second := f.starts.Await(t, "the second worker's start")
	assert.Equal(t, p, second.p)
	// A plain interface compare, never assert.NotEqual: that walks into
	// context.Context's internals via reflection, racing the live second worker's
	// concurrent mutation of its own cancelCtx state.
	assert.True(t, first.ctx != second.ctx, "a fresh ctx for the restarted worker")

	obs, ok := svc.Read("resource/1")
	require.True(t, ok)
	assert.Equal(t, Observation{Reason: ReasonWatching, ObjectCount: 5}, obs, "the observation survives the bounce")

	// Bounce of an unknown id is a no-op.
	svc.Bounce("unknown")
	testutil.NoRecv(t, f.starts.Chan(), testutil.Timeout/50, "a worker start for an unknown id")
}

// BounceCache bounces exactly the subjects whose Params.CacheID matches;
// others keep their original worker running.
func TestBounceCacheBouncesOnlyMatchingSubjects(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newTestService(conns, f)
	startService(t, svc)

	svc.Track("resource/1", testParams(1))
	r1 := f.starts.Await(t, "resource/1's start")
	svc.Track("resource/2", testParams(2))
	r2 := f.starts.Await(t, "resource/2's start")

	r1done := make(chan struct{})
	go func() { <-r1.ctx.Done(); close(r1done) }()

	svc.BounceCache(1)

	testutil.Wait(t, r1done, "resource/1's ctx to end")
	r1restart := f.starts.Await(t, "resource/1's restart")
	assert.Equal(t, int64(1), r1restart.p.CacheID)

	// resource/2's worker was never cancelled and never restarted.
	select {
	case <-r2.ctx.Done():
		t.Fatal("resource/2's worker was cancelled by a BounceCache for another cache")
	default:
	}
}

// The stop func returned by Start cancels every worker's ctx and waits for the
// bodies to exit.
func TestStopCancelsEveryWorkerAndWaits(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newWithOptions(conns, withSync(f.body))
	stop, err := svc.Start(context.Background())
	require.NoError(t, err)

	svc.Track("resource/1", testParams(1))
	r := f.starts.Await(t, "the worker's start")

	require.NoError(t, stop(context.Background()))

	select {
	case <-r.ctx.Done():
	default:
		t.Fatal("stop did not cancel the worker's ctx")
	}

	require.NoError(t, svc.Close())
}

// Close releases every tracked lease and closes the signal hub.
func TestCloseReleasesLeasesAndClosesHub(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newWithOptions(conns, withSync(f.body))
	stop, err := svc.Start(context.Background())
	require.NoError(t, err)

	svc.Track("resource/1", testParams(1))
	f.starts.Await(t, "the worker's start")
	svc.Track("resource/2", testParams(2))
	f.starts.Await(t, "the second worker's start")

	sub := svc.Subscribe()

	require.NoError(t, stop(context.Background()))
	require.NoError(t, svc.Close())

	assert.Equal(t, 2, conns.releaseCount())
	testutil.RecvClosed(t, sub.Chan(), "the signal hub, after Close")
}

// Two Bounces racing over one subject leave exactly one worker running. Without
// the generation guard both callers spawn from the same waited-on worker, and the
// subject holds only the later one's cancel — so the other syncs on forever.
func TestConcurrentBouncesLeaveOneWorkerRunning(t *testing.T) {
	conns := &fakeConns{}

	started := testutil.NewProbe[struct{}](8)
	var mu sync.Mutex
	var runs []context.Context
	// Latency injected into the worker on purpose: the first body holds its exit
	// until both callers are inside Bounce, so they wait on the same done channel
	// rather than one bouncing after the other has finished.
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	body := func(ctx context.Context, _ Params, _ func(Observation)) {
		mu.Lock()
		runs = append(runs, ctx)
		mu.Unlock()
		started.Fire(struct{}{})
		<-ctx.Done()
		once.Do(func() {
			<-entered
			<-entered
			close(release)
		})
		<-release
	}

	svc := newWithOptions(conns, withSync(body))
	startService(t, svc)

	svc.Track("resource/1", testParams(1))
	started.Await(t, "the first worker's start")

	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			entered <- struct{}{}
			svc.Bounce("resource/1")
		})
	}
	testutil.WaitReturn(t, wg.Wait, "both Bounce calls to return")

	// Polled, not awaited on a start: the last Bounce spawns outside the call, and
	// two callers that did not overlap legitimately restart twice. Either way the
	// settled count is one — the broken shape leaves two workers live for good.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		live := 0
		for _, ctx := range runs {
			if ctx.Err() == nil {
				live++
			}
		}
		return live == 1
	}, testutil.Timeout, time.Millisecond, "exactly one worker left running after the racing bounces")
}

// ForgetCache disarms exactly the subjects syncing into that cache, waits for their
// workers, and releases their leases; another cache's worker keeps running.
func TestForgetCacheDisarmsOnlyMatchingSubjects(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newTestService(conns, f)
	startService(t, svc)

	svc.Track("resource/1", testParams(1))
	r1 := f.starts.Await(t, "resource/1's start")
	svc.Track("resource/2", testParams(1))
	r2 := f.starts.Await(t, "resource/2's start")
	svc.Track("resource/3", testParams(2))
	r3 := f.starts.Await(t, "resource/3's start")

	svc.ForgetCache(1)

	// ForgetCache returns only once those workers have exited, so this is settled.
	assert.Error(t, r1.ctx.Err(), "resource/1's worker outlived ForgetCache")
	assert.Error(t, r2.ctx.Err(), "resource/2's worker outlived ForgetCache")
	assert.NoError(t, r3.ctx.Err(), "another cache's worker was disarmed")

	_, ok := svc.Read("resource/1")
	assert.False(t, ok, "a forgotten subject")
	_, ok = svc.Read("resource/3")
	assert.False(t, ok, "resource/3 has committed nothing")
	assert.Equal(t, 2, conns.releaseCount(), "the disarmed subjects' leases")
}
