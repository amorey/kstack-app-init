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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
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

// fakeStores is the store manager as this leaf reaches it: a real manager over a temp
// dir, plus what the test needs to see — which caches were opened, and a substitutable
// error for the torn-down case.
type fakeStores struct {
	mgr *kubestore.Manager

	mu     sync.Mutex
	opened []int64
	err    error
}

func newFakeStores(t *testing.T) *fakeStores {
	mgr := kubestore.NewManager(t.TempDir())
	t.Cleanup(func() { assert.NoError(t, mgr.Close()) })
	return &fakeStores{mgr: mgr}
}

func (f *fakeStores) OpenOrCreate(cacheID int64) (*kubestore.Store, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opened = append(f.opened, cacheID)
	if f.err != nil {
		return nil, f.err
	}
	return f.mgr.OpenOrCreate(cacheID)
}

// failWith replaces what Acquire answers, so a test can let a store open again.
func (f *fakeStores) failWith(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeStores) openedCaches() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.opened...)
}

// run is one call into a seamed sync body: the params it was started with, the
// ctx it runs on, the store handle it was handed, and the commit func to call —
// captured so a test can commit after the body would otherwise have exited, or
// after the subject was forgotten.
type run struct {
	ctx    context.Context
	p      Params
	store  *kubestore.Store
	commit func(Observation)
}

// fakeSync hands every worker's run to the test via starts, and blocks each
// worker until its ctx ends — the seam every behavior in this file drives by
// hand, never by racing the production sync loop.
type fakeSync struct {
	starts *testutil.Probe[*run]
	// mu guards onStop, which a test sets from its own goroutine while bodies run.
	mu sync.Mutex
	// onStop runs as a worker exits, which is where Track's replace path is blocked —
	// the window a test needs to land a hold in.
	onStop func()
}

func (f *fakeSync) stopWith(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onStop = fn
}

func (f *fakeSync) stopped() func() {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn := f.onStop
	f.onStop = nil
	return fn
}

func newFakeSync() *fakeSync {
	return &fakeSync{starts: testutil.NewProbe[*run](8)}
}

func (f *fakeSync) body(ctx context.Context, p Params, _ kubeconn.Lease, store *kubestore.Store, commit func(Observation)) {
	f.starts.Fire(&run{ctx: ctx, p: p, store: store, commit: commit})
	<-ctx.Done()
	if fn := f.stopped(); fn != nil {
		fn()
	}
}

func testParams(cacheID int64) Params {
	return Params{
		CacheID:     cacheID,
		ContextName: "prod",
		ServerUID:   "uid-1",
		APIVersion:  "v1",
		Kind:        "Pod",
		Resource:    "pods",
	}
}

// newTestService is the service under test: the seamed sync body, over a real store
// registry in a temp dir — a worker holds a handle for its run, so nothing here can
// stand in for one.
func newTestService(t *testing.T, conns connService, f *fakeSync) *Service {
	t.Helper()
	return newWithOptions(conns, newFakeStores(t), withSync(f.body))
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
	svc := newTestService(t, conns, f)
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
	svc := newTestService(t, conns, f)
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
	svc := newTestService(t, conns, f)
	startService(t, svc)

	svc.Track("resource/1", testParams(1))
	first := f.starts.Await(t, "the first worker's start")
	first.commit(Observation{Reason: ReasonWatching, ObjectCount: 5})

	next := testParams(1)
	next.ContextName = "staging"
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
	svc := newTestService(t, conns, f)
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
	svc := newTestService(t, conns, f)
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
	svc := newTestService(t, conns, f)
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
	svc := newTestService(t, conns, f)
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

// A restart cancels the first worker's ctx and, only after it exits, starts a new
// worker with the same params; the last observation survives.
func TestRestartIsInPlaceAndKeepsTheObservation(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newTestService(t, conns, f)
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

	svc.restart("resource/1")

	testutil.Wait(t, bodyDone, "the first body's ctx to end")
	second := f.starts.Await(t, "the second worker's start")
	assert.Equal(t, p, second.p)
	// A plain interface compare, never assert.NotEqual: that walks into
	// context.Context's internals via reflection, racing the live second worker's
	// concurrent mutation of its own cancelCtx state.
	assert.True(t, first.ctx != second.ctx, "a fresh ctx for the restarted worker")

	obs, ok := svc.Read("resource/1")
	require.True(t, ok)
	assert.Equal(t, Observation{Reason: ReasonWatching, ObjectCount: 5}, obs, "the observation survives the restart")

	// A restart of an unknown id is a no-op.
	svc.restart("unknown")
	testutil.NoRecv(t, f.starts.Chan(), testutil.Timeout/50, "a worker start for an unknown id")
}

// The stop func returned by Start cancels every worker's ctx and waits for the
// bodies to exit.
func TestStopCancelsEveryWorkerAndWaits(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newWithOptions(conns, newFakeStores(t), withSync(f.body))
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
	svc := newWithOptions(conns, newFakeStores(t), withSync(f.body))
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

// Two restarts racing over one subject leave exactly one worker running. Without
// the generation guard both callers spawn from the same waited-on worker, and the
// subject holds only the later one's cancel — so the other syncs on forever.
func TestConcurrentRestartsLeaveOneWorkerRunning(t *testing.T) {
	conns := &fakeConns{}

	started := testutil.NewProbe[struct{}](8)
	var mu sync.Mutex
	var runs []context.Context
	// Latency injected into the worker on purpose: the first body holds its exit
	// until both callers are inside restart, so they wait on the same done channel
	// rather than one restarting after the other has finished.
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	body := func(ctx context.Context, _ Params, _ kubeconn.Lease, _ *kubestore.Store, _ func(Observation)) {
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

	svc := newWithOptions(conns, newFakeStores(t), withSync(body))
	startService(t, svc)

	svc.Track("resource/1", testParams(1))
	started.Await(t, "the first worker's start")

	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			entered <- struct{}{}
			svc.restart("resource/1")
		})
	}
	testutil.WaitReturn(t, wg.Wait, "both restart calls to return")

	// Polled, not awaited on a start: the last restart spawns outside the call, and
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
	}, testutil.Timeout, time.Millisecond, "exactly one worker left running after the racing restarts")
}

// ForgetCache disarms exactly the subjects syncing into that cache, waits for their
// workers, and releases their leases; another cache's worker keeps running.
func TestForgetCacheDisarmsOnlyMatchingSubjects(t *testing.T) {
	conns := &fakeConns{}
	f := newFakeSync()
	svc := newTestService(t, conns, f)
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

// The worker syncs into its cache's store, so the handle is taken as the run starts
// and given back when it exits — never held for a subject that is not running.
func TestTrackAcquiresTheCachesStoreForTheRun(t *testing.T) {
	f := newFakeSync()
	stores := newFakeStores(t)
	svc := newWithOptions(&fakeConns{}, stores, withSync(f.body))
	startService(t, svc)

	svc.Track("resource/1", testParams(7))

	r := f.starts.Await(t, "the worker's start")
	require.NotNil(t, r.store)
	assert.NotNil(t, r.store)
	assert.Equal(t, []int64{7}, stores.openedCaches())
}

// A store the registry has retired means the cache is being torn down: the worker
// parks rather than publishing, since its Forget is on the way and there is nothing
// left to sync into.
func TestAWorkerWhoseStoreIsDeletedParksWithoutPublishing(t *testing.T) {
	f := newFakeSync()
	stores := newFakeStores(t)
	stores.err = kubestore.ErrRemoved
	svc := newWithOptions(&fakeConns{}, stores, withSync(f.body))
	startService(t, svc)

	svc.Track("resource/1", testParams(1))

	// A negative assertion needs a bounded window; a body that ran would fire at once.
	testutil.NoRecv(t, f.starts.Chan(), testutil.Timeout/50, "a sync body")
	_, ok := svc.Read("resource/1")
	assert.False(t, ok, "a parked worker published an observation")
	// The subject is still tracked and still stoppable.
	svc.Forget("resource/1")
}

// The health fold reads the whole fleet at once, grouped by cache — so the snapshot
// carries each subject's params beside its answer, in one critical section.
func TestObservationsSnapshotsTheTrackedFleet(t *testing.T) {
	f := newFakeSync()
	svc := newTestService(t, &fakeConns{}, f)
	startService(t, svc)

	svc.Track("resource/1", testParams(1))
	first := f.starts.Await(t, "the first worker's start")
	svc.Track("resource/2", testParams(2))
	f.starts.Await(t, "the second worker's start")

	sub := svc.Subscribe()
	t.Cleanup(sub.Close)
	first.commit(Observation{Reason: ReasonWatching, ObjectCount: 4})
	testutil.Recv(t, sub.Chan(), "the commit's signal")

	got := svc.Observations()

	require.Len(t, got, 2)
	byID := map[string]SubjectObservation{}
	for _, o := range got {
		byID[o.ID] = o
	}
	assert.True(t, byID["resource/1"].Known)
	assert.Equal(t, ReasonWatching, byID["resource/1"].Observation.Reason)
	assert.Equal(t, int64(1), byID["resource/1"].Params.CacheID)
	assert.False(t, byID["resource/2"].Known, "a subject with no answer yet")
}

// A poke resumes every cache at once — a warm restart in place, the cookies intact.
func TestRestartAllRestartsEverySubject(t *testing.T) {
	f := newFakeSync()
	svc := newTestService(t, &fakeConns{}, f)
	startService(t, svc)

	svc.Track("resource/1", testParams(1))
	first := f.starts.Await(t, "the first worker's start")
	svc.Track("resource/2", testParams(2))
	second := f.starts.Await(t, "the second worker's start")

	svc.RestartAll()

	assert.Error(t, first.ctx.Err(), "the first worker was not restarted")
	assert.Error(t, second.ctx.Err(), "the second worker was not restarted")
	f.starts.Await(t, "a replacement worker's start")
	f.starts.Await(t, "the other replacement worker's start")
}

// Clearing a cache is stop-then-touch-the-file, and a pass that arms a worker in between
// would leave one resuming a watch into the file the clear is about to empty — deltas
// into an empty database, with no cold list to fill it. The hold closes that gap: the
// subjects are stopped before it runs and nothing may arm one until it returns.
func TestWhileCacheStoppedRefusesTracksForThatCache(t *testing.T) {
	f := newFakeSync()
	svc := newTestService(t, &fakeConns{}, f)
	startService(t, svc)
	svc.Track("resource/1", testParams(1))
	first := f.starts.Await(t, "the worker's start")

	require.NoError(t, svc.WhileCacheStopped(1, func() error {
		assert.Error(t, first.ctx.Err(), "the clear ran while a worker was still writing")

		// The pass that races the clear.
		svc.Track("resource/1", testParams(1))
		testutil.NoRecv(t, f.starts.Chan(), testutil.Timeout/50, "a worker armed during the clear")

		// Another cache is nobody's business but its own.
		svc.Track("resource/2", testParams(2))
		f.starts.Await(t, "the other cache's worker")
		return nil
	}))
}

// The hold ends with the clear, so the record's own pass — which the caller requeues —
// arms the worker again.
func TestWhileCacheStoppedReleasesTheHold(t *testing.T) {
	f := newFakeSync()
	svc := newTestService(t, &fakeConns{}, f)
	startService(t, svc)

	require.NoError(t, svc.WhileCacheStopped(1, func() error { return nil }))
	svc.Track("resource/1", testParams(1))

	f.starts.Await(t, "the re-armed worker")
}

// A failing clear releases the hold too: the caller reports it and requeues, and a cache
// nothing may arm is worse than one whose rows are still there.
func TestWhileCacheStoppedReleasesTheHoldOnFailure(t *testing.T) {
	f := newFakeSync()
	svc := newTestService(t, &fakeConns{}, f)
	startService(t, svc)
	boom := errors.New("disk full")

	require.ErrorIs(t, svc.WhileCacheStopped(1, func() error { return boom }), boom)

	svc.Track("resource/1", testParams(1))
	f.starts.Await(t, "the re-armed worker")
}

// The per-kind clear owes the same, one subject wide: its rows and its cookie go, so a
// worker that resumed in between would watch on and never re-list.
func TestWhileStoppedRefusesTracksForThatSubject(t *testing.T) {
	f := newFakeSync()
	svc := newTestService(t, &fakeConns{}, f)
	startService(t, svc)
	svc.Track("resource/1", testParams(1))
	first := f.starts.Await(t, "the worker's start")

	require.NoError(t, svc.WhileStopped("resource/1", 1, func() error {
		assert.Error(t, first.ctx.Err(), "the clear ran while the worker was still writing")

		svc.Track("resource/1", testParams(1))
		testutil.NoRecv(t, f.starts.Chan(), testutil.Timeout/50, "a worker armed during the clear")

		// Another kind in the same cache keeps syncing: only its own rows are going.
		svc.Track("resource/2", testParams(1))
		f.starts.Await(t, "the other kind's worker")
		return nil
	}))
}

// The hold has to survive Track's replace path: it drops the lock to wait for the old
// worker, and a clear taking the hold in that window would otherwise be invisible —
// leaving a worker spawned for a cache whose file is about to be closed and swapped.
func TestTrackWithChangedParamsRespectsAHoldTakenWhileItWaited(t *testing.T) {
	f := newFakeSync()
	svc := newTestService(t, &fakeConns{}, f)
	startService(t, svc)
	svc.Track("resource/1", testParams(1))
	f.starts.Await(t, "the worker's start")

	// The hold is taken while Track is blocked waiting for the worker it just forgot —
	// the window in which it holds no lock — and stands until the test releases it.
	taken, release := make(chan struct{}), make(chan struct{})
	cleared := make(chan struct{})
	f.stopWith(func() {
		go func() {
			defer close(cleared)
			assert.NoError(t, svc.WhileCacheStopped(1, func() error {
				close(taken)
				<-release
				return nil
			}))
		}()
		<-taken
	})

	next := testParams(1)
	next.ContextName = "staging"
	svc.Track("resource/1", next)

	testutil.NoRecv(t, f.starts.Chan(), testutil.Timeout/50, "a worker armed under a hold")
	close(release)
	testutil.Wait(t, cleared, "the clear to finish")
}

// A file that will not open is usually transient — a descriptor limit, a full disk — and
// the record cannot re-arm the worker: its params have not moved, so Track is a no-op.
// The worker retries it up its own ladder rather than parking on the first failure.
func TestAWorkerRetriesAStoreThatWillNotOpen(t *testing.T) {
	f := newFakeSync()
	stores := newFakeStores(t)
	stores.err = errors.New("too many open files")
	svc := newWithOptions(&fakeConns{}, stores, withSync(f.body), withOpenRetry(time.Millisecond, 2*time.Millisecond))
	startService(t, svc)

	svc.Track("resource/1", testParams(1))

	require.Eventually(t, func() bool {
		return len(stores.openedCaches()) > 1
	}, testutil.Timeout, time.Millisecond, "the worker parked on the first failed open")

	// And it syncs once the store opens again.
	stores.failWith(nil)
	f.starts.Await(t, "the worker's start once the store opened")
}

// Holding is what keeps a clear from reading as a cache that stopped syncing, so it has
// to cover the per-kind clear too: stopping the only armed kind in a cache empties the
// fold exactly as a cache-wide clear does.
func TestHoldingCoversAPerKindClear(t *testing.T) {
	f := newFakeSync()
	svc := newTestService(t, &fakeConns{}, f)
	startService(t, svc)
	svc.Track("resource/1", testParams(7))
	f.starts.Await(t, "the worker's start")

	require.NoError(t, svc.WhileStopped("resource/1", 7, func() error {
		assert.True(t, svc.Holding(7), "a per-kind clear does not report its cache")
		return nil
	}))

	assert.False(t, svc.Holding(7), "the hold outlived the clear")
}

// A poke restarts every worker in place, and it must not walk through a clear's hold:
// the store is being closed and swapped, so a worker respawned there would write into
// the file going away, or resume a watch into the fresh one with no cold list behind it.
//
// The race is driven from the exiting worker, which is where restart is blocked when the
// hold lands.
func TestRestartDoesNotRespawnUnderAHold(t *testing.T) {
	f := newFakeSync()
	svc := newTestService(t, &fakeConns{}, f)
	startService(t, svc)
	svc.Track("resource/1", testParams(1))
	f.starts.Await(t, "the worker's start")

	release, cleared := make(chan struct{}), make(chan struct{})
	f.stopWith(func() {
		go func() {
			defer close(cleared)
			assert.NoError(t, svc.WhileCacheStopped(1, func() error {
				<-release
				return nil
			}))
		}()
		// The hold is registered before the clear stops anything, which is the window
		// restart returns into. Waiting for the clear to reach its own body would
		// deadlock — its ForgetCache waits for this very worker — so this waits for the
		// hold alone and leaves the rest of the interleaving to the scheduler.
		require.Eventually(t, func() bool { return svc.Holding(1) },
			testutil.Timeout, time.Millisecond, "the clear to take the hold")
	})

	svc.RestartAll()

	testutil.NoRecv(t, f.starts.Chan(), testutil.Timeout/50, "a worker respawned under a hold")
	close(release)
	testutil.Wait(t, cleared, "the clear to finish")
}
