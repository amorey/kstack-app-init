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

package cluster

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/beehive"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/kubesync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/objectsync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// These tests cover the controller's own responsibilities — when a kind's worker runs,
// what a report becomes, and what a deletion takes with it. The sync itself belongs to
// the objectsync/kubesync packages and is tested there, so the worker here is a fake.

var testGVRSyncSpec = ClusterCacheGVRSyncSpec{
	Enabled:    true,
	APIVersion: "apps/v1",
	Kind:       "Deployment",
	Resource:   "deployments",
	Namespaced: true,
}

// fakeWorker records its lifecycle and hands back the sink so a test can drive reports.
// It announces itself on Start rather than at construction, so a worker a test takes off
// the factory channel is one that is definitely running — no separate started flag to read
// (and race on) afterwards.
type fakeWorker struct {
	factory *workerFactory
	sink    kubesync.Sink

	mu      sync.Mutex
	stopped bool
	// stopErr, when set, fails the drain — the deletion path must then keep the finalizer.
	stopErr error
}

func (w *fakeWorker) Start() {
	w.factory.newC.Fire(w)
}

func (w *fakeWorker) Stop(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopErr != nil {
		return w.stopErr
	}
	w.stopped = true
	return nil
}

func (w *fakeWorker) isStopped() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stopped
}

// workerFactory hands out fakeWorkers and remembers every one it built, so a test can
// assert on restarts (a second worker) as well as the live one.
type workerFactory struct {
	mu      sync.Mutex
	built   []*fakeWorker
	newC    *testutil.Probe[*fakeWorker]
	stopErr error
}

func newWorkerFactory() *workerFactory {
	return &workerFactory{newC: testutil.NewProbe[*fakeWorker](8)}
}

func (f *workerFactory) build(_ context.Context, _ *rest.Config, _ *store.ClusterDB, sink kubesync.Sink) (workerHandle, error) {
	f.mu.Lock()
	w := &fakeWorker{factory: f, sink: sink, stopErr: f.stopErr}
	f.built = append(f.built, w)
	f.mu.Unlock()
	return w, nil
}

// await returns the next worker the controller built AND started, failing the test if none
// arrives.
func (f *workerFactory) await(t *testing.T) *fakeWorker {
	t.Helper()
	return f.newC.Await(t, "the controller to start a worker")
}

func (f *workerFactory) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.built)
}

// gvrWorkerFactory hands out fakeWorkers for the per-kind sync, recording the Kind each
// was built for so a test can check the spec reached the worker intact.
type gvrWorkerFactory struct {
	*workerFactory
	kinds *testutil.Probe[objectsync.Kind]
	// limiters records the LIST-phase budget each worker was built with, so a test can
	// assert every kind of one cache shares one.
	limiters *testutil.Probe[kubesync.ListLimiter]
}

// awaitKind returns the kind the next worker was built for.
func (f *gvrWorkerFactory) awaitKind(t *testing.T) objectsync.Kind {
	t.Helper()
	return f.kinds.Await(t, "the kind a worker was built for")
}

// awaitLimiter returns the LIST budget the next worker was built with.
func (f *gvrWorkerFactory) awaitLimiter(t *testing.T) kubesync.ListLimiter {
	t.Helper()
	return f.limiters.Await(t, "the limiter a worker was built with")
}

func newGVRWorkerFactory() *gvrWorkerFactory {
	return &gvrWorkerFactory{
		workerFactory: newWorkerFactory(),
		kinds:         testutil.NewProbe[objectsync.Kind](8),
		limiters:      testutil.NewProbe[kubesync.ListLimiter](8),
	}
}

func (f *gvrWorkerFactory) build(ctx context.Context, cfg *rest.Config, cdb *store.ClusterDB, kind objectsync.Kind, limiter kubesync.ListLimiter, sink kubesync.Sink) (workerHandle, error) {
	f.kinds.Fire(kind)
	f.limiters.Fire(limiter)
	return f.workerFactory.build(ctx, cfg, cdb, sink)
}

// gvrSyncFixture is a running control plane holding the four kinds of the owner chain,
// with the GVR-sync controller real and its worker faked.
type gvrSyncFixture struct {
	ctrl       *ClusterCacheGVRSyncController
	client     beehive.Client[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus]
	factory    *gvrWorkerFactory
	connMgr    *ConnectionManager
	cacheMgr   *store.Manager
	clusterCC  beehive.ControllerClient[ClusterStatus]
	clusterID  ClusterID
	cacheID    beehive.ObjectID
	discovery  beehive.ObjectID
	cacheRefID store.CacheRef
}

// newGVRSyncFixture wires Cluster → ClusterCache → ClusterCacheGVRDiscovery →
// ClusterCacheGVRSync. The three ancestors get no-op controllers: only the owner edges
// matter here (they name the cache file and key the credentials).
func newGVRSyncFixture(t *testing.T) *gvrSyncFixture {
	t.Helper()
	ctx := context.Background()
	bh := NewTestBeehiveUnstarted(t)

	connMgr := NewConnectionManager()
	// The pools must close before TempDir's RemoveAll: on Windows an open file can't
	// be unlinked, so a leaked cache handle fails the cleanup.
	cacheMgr := store.NewManager(t.TempDir())
	t.Cleanup(func() { _ = cacheMgr.Shutdown(context.Background()) })
	rt := &controllerRuntime{bh: bh, connMgr: connMgr, cacheManager: cacheMgr}

	ctrl := NewClusterCacheGVRSyncController(rt)
	factory := newGVRWorkerFactory()
	ctrl.newWorker = factory.build

	clusterCC, err := beehive.Register(bh, ClusterGroupKind, &NoopController[ClusterSpec, ClusterStatus]{})
	require.NoError(t, err)
	_, err = beehive.Register(bh, ClusterCacheGroupKind, &NoopController[ClusterCacheSpec, ClusterCacheStatus]{})
	require.NoError(t, err)
	_, err = beehive.Register(bh, ClusterCacheGVRDiscoveryGroupKind,
		&NoopController[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus]{})
	require.NoError(t, err)
	cc, err := beehive.Register(bh, ClusterCacheGVRSyncGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)
	discoveryClient := beehive.NewClient[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus](
		bh, ClusterCacheGVRDiscoveryGroupKind)

	clusterObj, err := coreClient.Create(ctx, kubeconfigName("alpha"), eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	cacheObj, err := cacheClient.Create(ctx,
		ClusterCacheName(ClusterID(clusterObj.ID), testCacheUID),
		ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)
	discoveryObj, err := discoveryClient.Create(ctx,
		ClusterCacheGVRDiscoveryName(cacheObj.ID),
		ClusterCacheGVRDiscoverySpec{Enabled: true},
		beehive.WithOwner(cacheObj.ID))
	require.NoError(t, err)

	return &gvrSyncFixture{
		ctrl:       ctrl,
		client:     beehive.NewClient[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus](bh, ClusterCacheGVRSyncGroupKind),
		factory:    factory,
		connMgr:    connMgr,
		cacheMgr:   cacheMgr,
		clusterCC:  clusterCC,
		clusterID:  ClusterID(clusterObj.ID),
		cacheID:    cacheObj.ID,
		discovery:  discoveryObj.ID,
		cacheRefID: newCacheRef(clusterObj.ID, cacheObj.ID),
	}
}

func (f *gvrSyncFixture) connect(fingerprint string) {
	f.connMgr.Set(f.clusterID, &rest.Config{Host: "https://example"}, fingerprint)
}

// probe does what the core controller's converge does on a successful probe: fill the
// ConnectionManager, then write the parent Cluster's status — the wake that reaches a
// waiting worker over the DependsOn edge.
func (f *gvrSyncFixture) probe(t *testing.T, fingerprint string) {
	t.Helper()
	f.connect(fingerprint)
	version := fingerprint
	require.NoError(t, f.clusterCC.UpdateStatus(context.Background(), beehive.ObjectID(f.clusterID), 1,
		ClusterStatus{Server: ClusterServer{Version: &version}}))
}

// createChild creates the sync object the discovery controller would, owned by the
// discovery anchor and carrying the drain finalizer.
func (f *gvrSyncFixture) createChild(t *testing.T, spec ClusterCacheGVRSyncSpec) *beehive.Object[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus] {
	t.Helper()
	obj, err := f.client.Create(context.Background(),
		ClusterCacheGVRSyncName(f.discovery, spec.APIVersion, spec.Resource),
		spec,
		beehive.WithOwner(f.discovery),
		beehive.WithFinalizers(gvrSyncDrainFinalizer))
	require.NoError(t, err)
	return obj
}

// awaitRequeueAtLeast blocks until the scheduler's next reconcile of obj is at least min
// away — the requeue a pass actually asked for, read from beehive rather than from our own
// constant. It waits rather than sampling once because the stream is current-on-subscribe
// and an earlier pass's backoff retry may still be the scheduled one.
func (f *gvrSyncFixture) awaitRequeueAtLeast(t *testing.T, id beehive.ObjectID, min time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := f.client.WatchSchedule(ctx, id)
	require.NoError(t, err)
	var last time.Duration
	for {
		select {
		case sched, ok := <-ch:
			require.True(t, ok, "schedule stream ended before a requeue was scheduled")
			if sched.NextRequeueAt.IsZero() {
				continue
			}
			if last = time.Until(sched.NextRequeueAt); last >= min {
				return
			}
		case <-ctx.Done():
			t.Fatalf("no requeue at least %s out was ever scheduled (last was %s)", min, last)
		}
	}
}

// awaitRestartWaiters blocks until n sequences hold or are queued on cacheID's restart gate.
// It is how a test knows a second RestartCacheWorkers has reached its wait rather than
// merely been spawned. Safe to call off the test goroutine: it reports nothing itself, and a
// timeout surfaces as the caller's own assertion failing.
func (f *gvrSyncFixture) awaitRestartWaiters(t *testing.T, cacheID int64, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f.ctrl.workers.mu.Lock()
		g := f.ctrl.workers.restartGates[cacheID]
		waiters := 0
		if g != nil {
			waiters = g.waiters
		}
		f.ctrl.workers.mu.Unlock()
		if waiters >= n {
			return
		}
		runtime.Gosched()
	}
}

func (f *gvrSyncFixture) awaitSyncedReason(t *testing.T, id beehive.ObjectID, want string) Condition {
	t.Helper()
	return awaitConditionReason(t, f.client, id, ConditionSynced, want)
}

// cachedKinds reads the cache's kind catalog, which is where a running sync advertises
// its kind to the dashboard.
func (f *gvrSyncFixture) cachedKinds(t *testing.T) []store.KindRow {
	t.Helper()
	cdb := f.cacheMgr.Lookup(f.cacheRefID.CacheID)
	require.NotNil(t, cdb, "the controller must have opened the cache")
	kinds, err := cdb.Kinds(context.Background())
	require.NoError(t, err)
	return kinds
}

// TestGVRSyncEnabledStartsWorker covers the happy path: an enabled child whose cluster has
// credentials gets a worker, built for the kind its spec names.
func TestGVRSyncEnabledStartsWorker(t *testing.T) {
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	f.createChild(t, testGVRSyncSpec)

	f.factory.await(t) // arriving here means built and started

	kind, ok := f.factory.kinds.TryAwait()
	require.True(t, ok, "no kind recorded")
	assert.Equal(t, objectsync.Kind{
		APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Namespaced: true,
	}, kind, "the spec's GVR identity must reach the worker verbatim")
}

// TestGVRSyncWorkersOfOneCacheShareOneListLimiter pins the bound that keeps a cold sync's
// cost flat in the kind count. Every kind's worker starts concurrently (a reconcile only
// starts one and returns), and each goes straight to a full LIST, so they must contend for
// ONE budget — a per-worker limiter would bound nothing.
func TestGVRSyncWorkersOfOneCacheShareOneListLimiter(t *testing.T) {
	f := newGVRSyncFixture(t)
	f.connect("fp-1")

	f.createChild(t, testGVRSyncSpec)
	f.factory.await(t)
	f.createChild(t, ClusterCacheGVRSyncSpec{
		Enabled: true, APIVersion: "v1", Kind: "Pod", Resource: "pods", Namespaced: true,
	})
	f.factory.await(t)

	first := f.factory.awaitLimiter(t)
	second := f.factory.awaitLimiter(t)
	require.NotNil(t, first, "workers must be built with a real limiter, not an unbounded nil")
	assert.Equal(t, cacheListConcurrency, cap(first))
	assert.Equal(t, first, second, "both kinds of one cache must share one LIST budget")
}

// "Clear cache" deletes the .db file every one of a cache's workers is holding, so those
// workers must be rebuilt against the new handle. Nothing else would do it: a reconcile
// leaves a running worker alone while its connection and kind are unchanged, so the cache
// would stay empty until a resync poke or a process restart.
//
// It is cache-SCOPED: another cache's workers are not this operation's to disturb.
func TestGVRSyncRestartCacheWorkersIsScopedToOneCache(t *testing.T) {
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	f.createChild(t, testGVRSyncSpec)

	mine := f.factory.await(t)
	f.factory.awaitKind(t)
	entry := f.ctrl.workers.entries()[0]

	// A worker belonging to some other cache, registered directly: this fixture drives one
	// cache, and what matters here is the ref the restart filters on.
	other := &syncEntry{
		objID: 9999,
		ref:   store.CacheRef{ClusterID: 42, CacheID: 42},
		cfg:   entry.cfg, cdb: entry.cdb, kind: entry.kind,
		fingerprint: entry.fingerprint,
		worker:      &fakeWorker{factory: f.factory.workerFactory},
	}
	ok, err := f.ctrl.workers.putIfAbsent(other)
	require.NoError(t, err)
	require.True(t, ok)

	// The teardown runs INSIDE the restart: drained first, so nothing is mid-write when
	// the file goes, and rebuilt after against a fresh handle.
	var deletedWhileStopped bool
	require.NoError(t, f.ctrl.RestartCacheWorkers(context.Background(), entry.ref, func() error {
		deletedWhileStopped = mine.isStopped()
		return f.cacheMgr.DeleteCacheFiles(context.Background(), entry.ref)
	}))
	assert.True(t, deletedWhileStopped, "the cache file must not be deleted under a live worker")

	rebuilt := f.factory.await(t)
	f.factory.awaitKind(t)
	assert.NotSame(t, mine, rebuilt, "the cleared cache's worker must be rebuilt")
	assert.True(t, mine.isStopped())
	assert.False(t, other.worker.(*fakeWorker).isStopped(), "another cache's worker must be left alone")

	// And on a handle that WORKS. DeleteCacheFiles closed the one the drained worker held,
	// so a rebuild reusing it would leave every database operation failing while the worker
	// stayed registered — and a registered worker is what stops a reconcile from replacing
	// it, so the cleared cache would never refill.
	next := f.ctrl.workers.get(entry.objID)
	require.NotNil(t, next)
	assert.NotSame(t, entry.cdb, next.cdb, "the restart must reopen the cache, not reuse the closed handle")
	assert.NoError(t, next.cdb.Writer().PingContext(context.Background()),
		"the rebuilt worker's handle must be usable")
}

// A worker whose earlier stop timed out stays registered, flagged draining — which is what
// keeps the deletion barrier honest. But it is NOT gone: its goroutine may still be
// writing. Treating "not current" as "somebody else handled it" skipped it silently, so
// between() (DeleteCacheFiles) removed the .db out from under a live writer — the exact
// case the abort exists to prevent — and the kind never restarted either, since it was
// never drained.
func TestGVRSyncRestartCacheWorkersAbortsOnAnAlreadyDrainingWorker(t *testing.T) {
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	f.createChild(t, testGVRSyncSpec)

	stuck := f.factory.await(t)
	f.factory.awaitKind(t)
	entry := f.ctrl.workers.entries()[0]

	// An earlier stop that timed out: the entry stays in the set, marked draining.
	stuck.mu.Lock()
	stuck.stopErr = errors.New("wedged mid-write")
	stuck.mu.Unlock()
	require.Error(t, f.ctrl.workers.stopBounded(context.Background(), entry.objID))
	require.True(t, entry.draining.Load())
	require.False(t, f.ctrl.workers.isCurrent(entry), "a draining entry is not the live worker")

	var ran bool
	err := f.ctrl.RestartCacheWorkers(context.Background(), entry.ref, func() error {
		ran = true
		return nil
	})
	require.Error(t, err, "a worker still draining must abort the sequence, not be skipped")
	assert.False(t, ran, "the teardown must not run while a worker is still writing")
}

// The drain covers a SNAPSHOT of the running workers, which leaves a hole: a reconcile
// that has opened the cache but not yet registered its worker is in no snapshot, and the
// lifecycle lock it holds is one the restart never knows to take. It would then register a
// worker on the handle between() just closed — and since the fingerprint and kind still
// match, every later reconcile no-ops on that dead entry and the kind never syncs again.
//
// Registration is therefore refused for the whole sequence, which is what makes "drained
// or refused" exhaustive.
func TestGVRSyncRestartCacheWorkersRefusesARegistrationMidSequence(t *testing.T) {
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	f.createChild(t, testGVRSyncSpec)

	f.factory.await(t)
	f.factory.awaitKind(t)
	entry := f.ctrl.workers.entries()[0]

	// Stand in for the reconcile caught between Open and registration: it built its worker
	// against the doomed handle before the restart began, and tries to register during it.
	inFlight := &syncEntry{
		objID: 4242,
		ref:   entry.ref,
		cfg:   entry.cfg, cdb: entry.cdb, kind: entry.kind,
		fingerprint: entry.fingerprint,
		worker:      &fakeWorker{factory: f.factory.workerFactory},
	}

	var registered bool
	var regErr error
	require.NoError(t, f.ctrl.RestartCacheWorkers(context.Background(), entry.ref, func() error {
		registered, regErr = f.ctrl.workers.putIfAbsent(inFlight)
		return f.cacheMgr.DeleteCacheFiles(context.Background(), entry.ref)
	}))

	assert.False(t, registered, "a worker built on the doomed handle must not join the set")
	require.ErrorIs(t, regErr, errCacheRestarting,
		"the refusal must be an error, so the object gets another reconcile")
	assert.Nil(t, f.ctrl.workers.get(inFlight.objID))

	// And the barrier lifts: the drained workers come back, and later registrations work.
	f.factory.await(t)
	assert.NotNil(t, f.ctrl.workers.get(entry.objID))
}

// Two sequences can overlap on one cache — two clears, or a clear beside a deletion. The
// barrier that stops a reconcile registering a worker on the doomed handle was a flag, so
// whichever finished first reopened the window the other was still relying on.
func TestGVRSyncOverlappingCacheRestartsHoldTheBarrier(t *testing.T) {
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	f.createChild(t, testGVRSyncSpec)
	f.factory.await(t)
	f.factory.awaitKind(t)
	entry := f.ctrl.workers.entries()[0]

	// A second sequence takes its hold while the first is inside between(), and keeps it
	// after that first sequence has finished and released its own.
	require.NoError(t, f.ctrl.RestartCacheWorkers(context.Background(), entry.ref, func() error {
		f.ctrl.workers.beginCacheRestart(entry.ref.CacheID)
		return nil
	}))

	_, err := f.ctrl.workers.putIfAbsent(&syncEntry{
		objID: 4242, ref: entry.ref, kind: entry.kind,
		worker: &fakeWorker{factory: f.factory.workerFactory},
	})
	require.ErrorIs(t, err, errCacheRestarting,
		"the second sequence's hold must outlive the first's release")

	// Once it releases too, registration works again.
	f.ctrl.workers.endCacheRestart(entry.ref.CacheID)
	ok, err := f.ctrl.workers.putIfAbsent(&syncEntry{
		objID: 4243, ref: entry.ref, kind: entry.kind,
		worker: &fakeWorker{factory: f.factory.workerFactory},
	})
	require.NoError(t, err)
	require.True(t, ok)

	f.ctrl.workers.mu.Lock()
	held := f.ctrl.workers.restarting[entry.ref.CacheID]
	f.ctrl.workers.mu.Unlock()
	assert.Zero(t, held, "every hold must be released exactly once")
}

// Two clears of one cache (two windows, or a double-click) must run one after the other,
// each off its OWN snapshot. Letting them interleave was wrong two ways: they take every
// affected object's lifecycle lock in map-iteration order and hold them to the end, so a
// disagreeing order deadlocks permanently (sync.Mutex ignores the clear's timeout); and
// when the orders agree, the second sequence's snapshot is stale by the time it runs — the
// first already drained and replaced every entry in it — so holds() rejects them all, it
// drains nothing, and its teardown deletes the .db under the first sequence's fresh
// workers.
func TestGVRSyncOverlappingCacheRestartsAreSerialized(t *testing.T) {
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	f.createChild(t, testGVRSyncSpec)
	first := f.factory.await(t)
	f.factory.awaitKind(t)
	entry := f.ctrl.workers.entries()[0]

	// The second sequence starts while the first is inside its teardown, and must wait
	// there — not proceed on a snapshot the first is about to invalidate.
	release := make(chan struct{})
	secondDone := make(chan error, 1)
	var liveDuringSecond []*syncEntry

	go func() {
		f.awaitRestartWaiters(t, entry.ref.CacheID, 2)
		close(release)
	}()

	require.NoError(t, f.ctrl.RestartCacheWorkers(context.Background(), entry.ref, func() error {
		go func() {
			secondDone <- f.ctrl.RestartCacheWorkers(context.Background(), entry.ref, func() error {
				liveDuringSecond = f.ctrl.workers.entries()
				return nil
			})
		}()
		<-release
		return nil
	}))

	rebuilt := f.factory.await(t) // the first sequence's restart
	require.NoError(t, <-secondDone)

	assert.Empty(t, liveDuringSecond,
		"the second sequence must drain the workers the first restarted, off a fresh snapshot")
	assert.True(t, first.isStopped())
	assert.True(t, rebuilt.isStopped(), "the second sequence's teardown ran with nothing writing")

	// And it restarts what it drained, so the cache is left with a live worker.
	f.factory.await(t)
	assert.NotNil(t, f.ctrl.workers.get(entry.objID))
}

// The whole point of draining first is that nothing is mid-write when the teardown runs.
// A worker that will NOT drain therefore has to abort the sequence: for ClearCache the
// teardown is DeleteCacheFiles, so running it anyway would remove the .db/-wal/-shm out
// from under a live writer — exactly what the drain finalizer exists to prevent.
func TestGVRSyncRestartCacheWorkersSkipsTeardownWhenADrainFails(t *testing.T) {
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	f.createChild(t, testGVRSyncSpec)

	stuck := f.factory.await(t)
	f.factory.awaitKind(t)
	entry := f.ctrl.workers.entries()[0]

	stuck.mu.Lock()
	stuck.stopErr = errors.New("wedged mid-write")
	stuck.mu.Unlock()

	var ran bool
	err := f.ctrl.RestartCacheWorkers(context.Background(), entry.ref, func() error {
		ran = true
		return nil
	})
	require.Error(t, err, "a failed drain must be reported, not swallowed")
	assert.False(t, ran, "the teardown must not run while a worker is still writing")

	// And the entry stays in the set, still draining — which is what keeps the deletion
	// barrier real and makes the next reconcile retry the drain.
	still := f.ctrl.workers.get(entry.objID)
	require.NotNil(t, still)
	assert.True(t, still.draining.Load())
}

// A drain that times out leaves the entry in the set, flagged draining — deliberately,
// since forgetting it would let the deletion barrier clear while a wedged goroutine still
// writes to the cache file. But that entry is not a running worker: its sink drops every
// report. A later reconcile must therefore rebuild it, not match on its identity and
// no-op, which would silently end this kind's syncing for the process lifetime.
func TestGVRSyncRebuildsAfterAWedgedDrain(t *testing.T) {
	ctx := context.Background()
	f := newGVRSyncFixture(t)
	f.connect("fp-1")

	obj := f.createChild(t, testGVRSyncSpec)
	first := f.factory.await(t)
	f.factory.awaitKind(t)

	// Wedge the drain: the stop fails, so the entry stays behind marked draining.
	first.mu.Lock()
	first.stopErr = errors.New("drain timed out")
	first.mu.Unlock()
	require.Error(t, f.ctrl.workers.stopBounded(ctx, obj.ID))

	entry := f.ctrl.workers.get(obj.ID)
	require.NotNil(t, entry, "a failed drain must keep the entry — that is the barrier")
	require.True(t, entry.draining.Load())

	// The next reconcile: same connection, same kind, so the identity matches exactly. It
	// must still rebuild, because what it matches is dead.
	first.mu.Lock()
	first.stopErr = nil // the retry drains cleanly
	first.mu.Unlock()

	live, err := f.client.Get(ctx, obj.ID)
	require.NoError(t, err)
	_, err = f.ctrl.Reconcile(ctx, f.ctrl.ctrlClient, live)
	require.NoError(t, err)

	second := f.factory.await(t)
	assert.NotSame(t, first, second, "the wedged worker must be replaced, not matched and skipped")
	assert.True(t, first.isStopped(), "the retry must drain the wedged worker")
}

// The resync poke samples the running workers, then bounces each one. A worker the
// reconcile has since stopped — a pause, or a deletion that has already cleared the drain
// finalizer and released the cache file — must NOT come back: it would write into a file
// being deleted, for an object nothing will reconcile again.
//
// The lifecycle lock is what closes the timing window; this pins the re-check that makes a
// stale sample safe once the lock is held.
func TestGVRSyncPokeDoesNotResurrectAStoppedWorker(t *testing.T) {
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	obj := f.createChild(t, testGVRSyncSpec)

	worker := f.factory.await(t)
	f.factory.awaitKind(t)
	entry := f.ctrl.workers.get(obj.ID)
	require.NotNil(t, entry)

	// The reconcile's side of the race, already done: the worker is drained and forgotten.
	require.NoError(t, f.ctrl.workers.stopBounded(context.Background(), obj.ID))
	require.True(t, worker.isStopped())
	require.Nil(t, f.ctrl.workers.get(obj.ID))

	// The poke, still holding the entry it sampled before that happened.
	f.ctrl.restartWorker(context.Background(), entry)

	select {
	case <-f.factory.newC.Chan():
		t.Fatal("the poke resurrected a worker the reconcile had stopped")
	case <-time.After(200 * time.Millisecond):
	}
	assert.Nil(t, f.ctrl.workers.get(obj.ID), "the object must still have no worker")
}

// The poke's whole point is to re-establish live watches after a wake, so a genuinely
// running worker must still be bounced.
func TestGVRSyncPokeRestartsARunningWorker(t *testing.T) {
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	obj := f.createChild(t, testGVRSyncSpec)

	first := f.factory.await(t)
	f.factory.awaitKind(t)

	f.ctrl.restartWorkers(context.Background())

	second := f.factory.await(t)
	f.factory.awaitKind(t)
	assert.NotSame(t, first, second, "the poke must rebuild the running worker")
	assert.True(t, first.isStopped(), "the superseded worker must be drained")
	assert.NotNil(t, f.ctrl.workers.get(obj.ID))
}

// A child is named for its (apiVersion, resource) alone, so a CRD deleted and recreated
// with the same plural but a different Kind keeps the SAME child — discovery rewrites its
// spec in place. The worker's identity has to include that kind, or the old worker keeps
// running: still listing the right REST path, but writing rows under the obsolete Kind (the
// objects table is keyed by kind), holding a stale catalog entry, and reporting against a
// generation that has moved on.
func TestGVRSyncRestartsWorkerWhenTheKindChanges(t *testing.T) {
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	obj := f.createChild(t, testGVRSyncSpec)

	first := f.factory.await(t)
	require.Equal(t, objectsync.Kind{
		APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Namespaced: true,
	}, f.factory.awaitKind(t))

	// The CRD comes back under the same apiVersion/resource with a different Kind — the
	// same child, respecified. The connection is untouched.
	remapped := testGVRSyncSpec
	remapped.Kind = "Rollout"
	_, err := f.client.Update(context.Background(), obj.ID, remapped)
	require.NoError(t, err)

	second := f.factory.await(t)
	assert.NotSame(t, first, second, "the respecified child must get a new worker")
	assert.Eventually(t, first.isStopped, 2*time.Second, 5*time.Millisecond,
		"the worker built from the superseded kind must be drained")
	assert.Equal(t, objectsync.Kind{
		APIVersion: "apps/v1", Kind: "Rollout", Resource: "deployments", Namespaced: true,
	}, f.factory.awaitKind(t), "the replacement must carry the new identity")
}

// A steady re-reconcile — the 30s liveness recheck, a poke, an unchanged spec re-apply —
// must NOT churn the worker: restarting a hundred kinds' watches every half minute would
// re-list the whole cluster. The identity comparison is what keeps that true.
func TestGVRSyncKeepsWorkerWhenNothingChanged(t *testing.T) {
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	obj := f.createChild(t, testGVRSyncSpec)

	first := f.factory.await(t)
	f.factory.awaitKind(t)

	// Re-apply the identical spec: beehive suppresses the write, and an out-of-band
	// reconcile re-runs converge against the same connection.
	_, err := f.client.Update(context.Background(), obj.ID, testGVRSyncSpec)
	require.NoError(t, err)

	select {
	case <-f.factory.newC.Chan():
		t.Fatal("an unchanged spec must not rebuild the worker")
	case <-time.After(200 * time.Millisecond):
	}
	assert.False(t, first.isStopped(), "the running worker must be left alone")
}

// TestGVRSyncDisabledRunsNoWorker verifies the pause semantics: a disabled child is live
// but idle, keeping its rows so an unpause resumes rather than re-listing.
func TestGVRSyncDisabledRunsNoWorker(t *testing.T) {
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	spec := testGVRSyncSpec
	spec.Enabled = false
	obj := f.createChild(t, spec)

	cond := f.awaitSyncedReason(t, obj.ID, ReasonPaused)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Zero(t, f.factory.count(), "a paused child must not run a worker")
}

// TestGVRSyncWaitsForConnection verifies the normal startup ordering: discovery can only
// have run after a connection, but the child may still be reconciled before the
// credentials are in memory (a restart), and that is a wait, not a failure.
//
// The wake is the DependsOn edge on the Cluster — the probe below fills the
// ConnectionManager and writes the parent's status in one step, exactly as converge does.
// The requeue is only the backstop behind it, and must stay coarse: an offline cluster has
// one of these children per served kind (100-150), so a few-second poll is tens of
// reconciles per second per offline cluster, forever.
func TestGVRSyncWaitsForConnection(t *testing.T) {
	f := newGVRSyncFixture(t)
	obj := f.createChild(t, testGVRSyncSpec)

	cond := f.awaitSyncedReason(t, obj.ID, ReasonNoConnection)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Zero(t, f.factory.count())

	// Read the scheduler's own answer rather than the constant: what matters is the requeue
	// the reconcile actually asked for.
	// A waiting child must not poll; the parent's status write is what wakes it.
	f.awaitRequeueAtLeast(t, obj.ID, 30*time.Second)

	f.probe(t, "fp-1")
	f.factory.await(t)
}

// TestGVRSyncFoldsWorkerReports verifies the report path: a catch-up becomes a Synced
// condition plus one entry on this kind's own event timeline, and the steady heartbeat
// writes nothing at all.
//
// That last part matters more here than anywhere else: a cache runs a hundred of these
// workers, so a heartbeat that bumped resource_version would wake the parent cache a
// hundred times every 30 seconds.
func TestGVRSyncFoldsWorkerReports(t *testing.T) {
	ctx := context.Background()
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	obj := f.createChild(t, testGVRSyncSpec)
	w := f.factory.await(t)

	syncedAt := time.Now().UTC().Truncate(time.Second)
	w.sink.Report(kubesync.Status{
		State: kubesync.StateWatching, ColdStart: true,
		SyncedItems: 7, CaughtUpIn: 2 * time.Second, LastUpdateAt: &syncedAt, LastLiveAt: &syncedAt,
	})

	cond := f.awaitSyncedReason(t, obj.ID, ReasonWatching)
	assert.Equal(t, ConditionTrue, cond.Status)

	stored, err := f.client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Nil(t, stored.Status, "status is never written")

	events, err := f.client.ListEvents(ctx, obj.ID)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, ReasonSyncComplete, events[0].Reason)
	assert.Contains(t, events[0].Message, "cached 7 deployments")

	before, err := f.client.Get(ctx, obj.ID)
	require.NoError(t, err)
	later := syncedAt.Add(30 * time.Second)
	w.sink.Report(kubesync.Status{State: kubesync.StateWatching, ColdStart: true, LastLiveAt: &later})

	// Nothing observable changes, so wait out a window in which a write would have landed.
	require.Never(t, func() bool {
		got, err := f.client.Get(ctx, obj.ID)
		return err == nil && got.ResourceVersion != before.ResourceVersion
	}, 300*time.Millisecond, 25*time.Millisecond,
		"a heartbeat that changes no condition must not bump resource_version — with a "+
			"hundred workers per cache that is a hundred wakes every 30s")

	events, err = f.client.ListEvents(ctx, obj.ID)
	require.NoError(t, err)
	assert.Len(t, events, 1, "a heartbeat is not a transition and must not append an event run")
}

// TestGVRSyncDeletionDrainsAndForgetsKind covers the deletion path, which the discovery
// controller triggers when the cluster stops serving a kind (an uninstalled CRD). The
// worker drains before the finalizer clears, and the kind leaves the cache entirely —
// otherwise the dashboard would keep listing it, frozen at whenever the sync stopped.
func TestGVRSyncDeletionDrainsAndForgetsKind(t *testing.T) {
	ctx := context.Background()
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	obj := f.createChild(t, testGVRSyncSpec)
	w := f.factory.await(t)

	// The real worker registers its kind on start; the fake doesn't, so seed the catalog
	// row directly — what matters here is that the deletion path removes it.
	cdb := f.cacheMgr.Lookup(f.cacheRefID.CacheID)
	require.NotNil(t, cdb)
	_, err := cdb.Writer().ExecContext(ctx,
		`INSERT INTO kind_catalog (api_version, kind, resource, scope, is_crd)
		 VALUES ('apps/v1', 'Deployment', 'deployments', 'Namespaced', 0)`)
	require.NoError(t, err)
	require.Len(t, f.cachedKinds(t), 1)

	require.NoError(t, f.client.Delete(ctx, obj.ID))

	require.Eventually(t, func() bool {
		_, getErr := f.client.Get(ctx, obj.ID)
		return errors.Is(getErr, beehive.ErrNotFound)
	}, 2*time.Second, 10*time.Millisecond, "the object must be collected once its finalizer clears")
	assert.True(t, w.isStopped(), "the worker must have drained before the finalizer cleared")
	assert.Empty(t, f.cachedKinds(t), "a kind that is no longer synced must leave the catalog")
}

// The per-object lifecycle locks are keyed by ClusterCacheGVRSync ids, which are
// AUTOINCREMENT and never reused — so unlike the core controller's per-cluster map (bounded
// by the kube-contexts ever seen), this one grows without bound: every server-UID
// migration, cluster delete/recreate or CRD version bump mints a fresh set of ~150.
//
// The deletion path is where they are reclaimed, and it is the only place that can be: it
// holds the lock and runs after the object has been collected, so no later caller exists.
func TestGVRSyncDeletionReclaimsTheLifecycleLock(t *testing.T) {
	ctx := context.Background()
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	obj := f.createChild(t, testGVRSyncSpec)
	f.factory.await(t)

	f.ctrl.workers.mu.Lock()
	held := len(f.ctrl.workers.lifecycle)
	f.ctrl.workers.mu.Unlock()
	require.NotZero(t, held, "the reconcile must have taken a lifecycle lock")

	require.NoError(t, f.client.Delete(ctx, obj.ID))
	require.Eventually(t, func() bool {
		_, getErr := f.client.Get(ctx, obj.ID)
		return errors.Is(getErr, beehive.ErrNotFound)
	}, 2*time.Second, 10*time.Millisecond, "the object must be collected once its finalizer clears")

	require.Eventually(t, func() bool {
		f.ctrl.workers.mu.Lock()
		defer f.ctrl.workers.mu.Unlock()
		return len(f.ctrl.workers.lifecycle) == 0
	}, 2*time.Second, 10*time.Millisecond, "a collected object's lock must not outlive it")
}

// TestGVRSyncDeletionKeepsFinalizerWhileDrainFails verifies the barrier holds: while the
// worker can't be drained the object stays deletion-pending, so its parent discovery
// anchor — and transitively the cache's own stop-before-delete wait — keeps waiting
// instead of the .db file being deleted under a live writer.
func TestGVRSyncDeletionKeepsFinalizerWhileDrainFails(t *testing.T) {
	ctx := context.Background()
	f := newGVRSyncFixture(t)
	f.factory.stopErr = errors.New("drain wedged")
	f.connect("fp-1")
	obj := f.createChild(t, testGVRSyncSpec)
	f.factory.await(t)

	require.NoError(t, f.client.Delete(ctx, obj.ID))

	require.Never(t, func() bool {
		_, err := f.client.Get(ctx, obj.ID)
		return errors.Is(err, beehive.ErrNotFound)
	}, 500*time.Millisecond, 25*time.Millisecond, "a failed drain must not release the finalizer")

	stored, err := f.client.Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Contains(t, stored.Finalizers, gvrSyncDrainFinalizer)
}

// TestGVRSyncPokeRestartsWorkers covers the resync path. On OS resume or network-on the
// host pokes the sidecar, and long-lived watches have to re-establish: the TCP connection
// under a watch is dead but the client can take a while to notice, so without this a
// cache's kinds sit on dead streams until kubesync's own backoff or its 30m re-list
// notices. A poke restarts them at once.
func TestGVRSyncPokeRestartsWorkers(t *testing.T) {
	f := newGVRSyncFixture(t)
	f.connect("fp-1")
	f.createChild(t, testGVRSyncSpec)
	first := f.factory.await(t)

	f.ctrl.restartWorkers(context.Background())

	second := f.factory.await(t)
	assert.NotSame(t, first, second, "a poke must build a fresh worker")
	assert.True(t, first.isStopped(), "the superseded worker must be drained")
}

// TestGVRSyncPokeIgnoresStoppedWorkers pins that a poke only touches what is running: a
// paused or waiting-on-credentials kind has no worker, and a poke must not start one
// behind the reconcile's back.
func TestGVRSyncPokeIgnoresStoppedWorkers(t *testing.T) {
	f := newGVRSyncFixture(t)
	spec := testGVRSyncSpec
	spec.Enabled = false
	obj := f.createChild(t, spec)
	f.awaitSyncedReason(t, obj.ID, ReasonPaused)

	f.ctrl.restartWorkers(context.Background())

	assert.Zero(t, f.factory.count(), "a poke must not start a worker the reconcile stopped")
}
