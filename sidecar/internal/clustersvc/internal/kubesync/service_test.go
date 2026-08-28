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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/supervisor"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

var testParams = Params{ContextName: "prod", ServerUID: "uid-1"}

func TestTrackDiscoveryClaimsTheContextAndForgetGivesItBack(t *testing.T) {
	svc, pool := newTestService(t)

	svc.TrackDiscovery(1, testParams)
	require.Equal(t, 1, pool.lease("prod").held(), "the session holds a claim on its context")

	svc.ForgetDiscovery(1)
	require.Equal(t, 0, pool.lease("prod").held(), "forgetting gives the claim back")
}

func TestTrackDiscoveryIsIdempotentForUnchangedParams(t *testing.T) {
	svc, pool := newTestService(t)

	svc.TrackDiscovery(1, testParams)
	svc.TrackDiscovery(1, testParams)

	require.Equal(t, 1, pool.lease("prod").held(), "a repeat pass re-arms nothing")
}

func TestTrackDiscoveryRebuildsWhenParamsMove(t *testing.T) {
	svc, pool := newTestService(t)

	svc.TrackDiscovery(1, testParams)
	svc.TrackDiscovery(1, Params{ContextName: "staging", ServerUID: "uid-2"})

	assert.Equal(t, 0, pool.lease("prod").held(), "the superseded context is released")
	assert.Equal(t, 1, pool.lease("staging").held(), "the session moves to the new context")
}

func TestKindTrackedAgainstAnUnarmedCacheIsHeldUntilItIsArmed(t *testing.T) {
	fake := newFakeKindSync()
	svc, pool := newTestService(t, fake.option())
	pool.lease("prod").vouch(t, "uid-1")
	start(t, svc)

	// The record's pass may land before the cache's, so the registration is held rather
	// than refused.
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))
	_, running := fake.runs.TryAwait()
	require.False(t, running, "a kind on an unarmed cache runs nothing")

	svc.TrackDiscovery(1, testParams)
	fake.runs.Await(t, "the held kind starts when its cache is armed")
}

func TestARegistrationOutlivesItsCacheBeingForgotten(t *testing.T) {
	fake := newFakeKindSync()
	svc, pool := newTestService(t, fake.option())
	pool.lease("prod").vouch(t, "uid-1")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))
	fake.runs.Await(t, "the kind runs while its cache is armed")

	// Pausing is one call and resuming is one call: nothing re-registers the kinds.
	svc.ForgetDiscovery(1)
	svc.TrackDiscovery(1, testParams)
	fake.runs.Await(t, "resuming restarts every kind still registered")
}

func TestAKindWaitsForAConnectionVouchingForItsServerUID(t *testing.T) {
	fake := newFakeKindSync()
	svc, pool := newTestService(t, fake.option())
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))

	pool.lease("prod").vouch(t, "uid-other")
	_, running := fake.runs.TryAwait()
	require.False(t, running, "a connection answering as another cluster arms nothing")

	pool.lease("prod").vouch(t, "uid-1")
	fake.runs.Await(t, "the kind runs once the connection vouches for its identity")
}

func TestForgetKindWaitsForItsWorker(t *testing.T) {
	fake := newFakeKindSync()
	svc, pool := newTestService(t, fake.option())
	pool.lease("prod").vouch(t, "uid-1")
	start(t, svc)

	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)
	fake.runs.Await(t, "the sync runs")

	svc.ForgetKind(1, kind)
	_, done := fake.returned.TryAwait()
	require.True(t, done, "ForgetKind returns only once the kind cannot still write")
}

func TestForgetDiscoveryWaitsForEveryWorkerUnderIt(t *testing.T) {
	fake := newFakeKindSync()
	svc, pool := newTestService(t, fake.option())
	pool.lease("prod").vouch(t, "uid-1")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))
	fake.runs.Await(t, "the sync runs")

	svc.ForgetDiscovery(1)
	fake.returned.Await(t, "the kind body is drained")
}

func TestGetStateReportsNothingBeforeARunCommits(t *testing.T) {
	svc, pool := newTestService(t)
	pool.lease("prod").vouch(t, "uid-1")
	kind := testKind("apps/v1", "Deployment", "deployments")

	_, ok := svc.GetDiscoveryState(1)
	assert.False(t, ok, "a cache nobody has armed has no answer")

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)

	_, ok = svc.GetKindState(1, kind)
	assert.False(t, ok, "a kind that has committed nothing has no answer")
}

func TestDiscoveryNewsIsKeyedByCacheID(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	news := svc.WatchDiscoveryNews()
	t.Cleanup(news.Close)
	start(t, svc)

	svc.TrackDiscovery(7, testParams)
	ev := testutil.Recv(t, news.Chan(), "the cache is woken")
	assert.Equal(t, int64(7), ev.Key, "news is keyed by cache id and carries nothing else")
}

func TestKindNewsIsKeyedByCacheAndKind(t *testing.T) {
	fake := newFakeKindSync()
	svc, pool := newTestService(t, fake.option())
	runs := fake.runs
	pool.lease("prod").vouch(t, "uid-1")
	start(t, svc)

	news := svc.WatchKindNews()
	t.Cleanup(news.Close)

	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)
	r := runs.Await(t, "the sync runs")

	r.Report(ReasonWatching)
	ev := testutil.Recv(t, news.Chan(), "the kind's record is woken")
	assert.Equal(t, KindKey{CacheID: 1, Kind: kind}, ev.Key)

	got, ok := svc.GetKindState(1, kind)
	require.True(t, ok)
	assert.Equal(t, ReasonWatching, got.Reason)
}

func TestReplacingAKindDropsTheStoppedWorkersVerdict(t *testing.T) {
	fake := newFakeKindSync()
	svc, pool := newTestService(t, fake.option())
	runs := fake.runs
	pool.lease("prod").vouch(t, "uid-1")
	start(t, svc)

	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)
	runs.Await(t, "the sync runs").Report(ReasonWatching)

	// The plural is unchanged, so this is the same collection under a new Kind name. The
	// worker that answered Watching is stopped, and the one replacing it may still be
	// waiting for a connection or cold-listing.
	renamed := testKind("apps/v1", "Deploy", "deployments")
	svc.TrackKind(1, renamed)
	_, ok := svc.GetKindState(1, renamed)
	assert.False(t, ok, "a replacement starts with no answer")

	runs.Await(t, "the replacement runs").Report(ReasonSyncing)
	got, ok := svc.GetKindState(1, renamed)
	require.True(t, ok)
	assert.Equal(t, ReasonSyncing, got.Reason)
}

func TestRestartAllReEntersEveryArmedKind(t *testing.T) {
	fake := newFakeKindSync()
	svc, pool := newTestService(t, fake.option())
	pool.lease("prod").vouch(t, "uid-1")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))
	// The stream, not merely the run: a restart cancels what is standing, and one arriving
	// while the first run is still establishing has nothing to cancel — correctly, since that
	// run is already building a fresh stream.
	fake.established.Await(t, "a stream to be standing")

	// A watch that died under a sleeping machine reports nothing, so a resume poke
	// restarts the run rather than waiting for one to notice.
	svc.RestartAll()
	fake.runs.Await(t, "the sync runs again")
}

func TestStopDrainsEveryWorker(t *testing.T) {
	fake := newFakeKindSync()
	svc, pool := newTestService(t, fake.option())
	pool.lease("prod").vouch(t, "uid-1")

	stop, err := svc.Start(t.Context())
	require.NoError(t, err)

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))
	fake.runs.Await(t, "the sync runs")

	require.NoError(t, stop(t.Context()))
	fake.returned.Await(t, "the sync is drained")

	require.NoError(t, svc.Close())
	assert.Equal(t, 0, pool.lease("prod").held(), "Close releases the claims")
}

func TestACacheWhoseStoreWillNotOpenArmsNothing(t *testing.T) {
	pool := newFakePool()
	stores := &refusingStores{}
	svc := New(pool, stores, newFakeKindSync().option())
	t.Cleanup(func() { _ = svc.Close() })

	svc.TrackDiscovery(1, testParams)

	assert.Equal(t, 0, pool.lease("prod").held(), "the context is claimed only once the file is open")
	_, ok := svc.GetDiscoveryState(1)
	assert.False(t, ok, "a cache that did not arm has no answer")

	svc.TrackDiscovery(1, testParams)
	assert.Equal(t, 2, stores.attempts, "the next pass retries rather than reading the cache as armed")
}

// refusingStores is a store manager that will not open a file, which is what a cache
// directory gone read-only looks like. Every call is from the test's own goroutine.
type refusingStores struct{ attempts int }

func (r *refusingStores) OpenOrCreate(int64) (*kubestore.Store, error) {
	r.attempts++
	return nil, errors.New("open cache store")
}

func TestForgettingACacheNobodyArmedIsANoOp(t *testing.T) {
	svc, _ := newTestService(t)

	svc.ForgetDiscovery(7)
	svc.ForgetKind(7, testKind("apps/v1", "Deployment", "deployments"))
}

func TestAStoppedServiceArmsNothing(t *testing.T) {
	svc, pool := newTestService(t)
	require.NoError(t, svc.Close())

	svc.TrackDiscovery(1, testParams)

	assert.Equal(t, 0, pool.lease("prod").held(), "a stopped service takes no claim")
	assert.Nil(t, svc.sessionOf(1), "and arms nothing")
}

func TestAKindStateIsAnsweredOnlyForAKindTheCacheTracks(t *testing.T) {
	svc, _ := newTestService(t)
	kind := testKind("apps/v1", "Deployment", "deployments")

	_, ok := svc.GetKindState(1, kind)
	assert.False(t, ok, "a cache nobody has armed has no answer")

	svc.TrackDiscovery(1, testParams)
	_, ok = svc.GetKindState(1, kind)
	assert.False(t, ok, "nor does a kind the cache does not track")
}

func TestAPassPublishesNothingForASubjectThatIsNotACache(t *testing.T) {
	svc, _ := newTestService(t)

	// The supervisor names its own subjects, so both of these are reads the seam must survive
	// rather than states it can reach: one not this package's, one whose cache is gone.
	svc.publishDiscovery("not-a-cache", supervisor.Snapshot{})
	svc.publishDiscovery(discoverySubject(404), supervisor.Snapshot{})
}

func TestAStoppedSessionsAnswerIsDropped(t *testing.T) {
	svc, _ := newTestService(t)
	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	sess := svc.sessionOf(1)

	// The session a run committed against is gone, which is what a commit racing its own
	// teardown looks like: the answer belongs to nobody and is dropped.
	svc.ForgetDiscovery(1)
	svc.commitDiscovery(sess, DiscoveryState{Reason: ReasonDiscovered})

	svc.TrackDiscovery(1, testParams)
	_, ok := svc.GetDiscoveryState(1)
	assert.False(t, ok, "the re-armed cache carries no answer from the session before it")
	_, ok = svc.GetKindState(1, kind)
	assert.False(t, ok, "nor one for its kinds")
}

func TestStoppingWithNoTimeLeftReportsTheEngineRefusing(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))
	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")

	stop, err := svc.Start(t.Context())
	require.NoError(t, err)

	release := cluster.hold("/api/v1")
	defer release()
	svc.TrackDiscovery(1, testParams)
	cluster.awaitRead(t, "/api/v1")

	// A sweep is parked mid-request, so the supervisor cannot drain inside a deadline that has
	// already passed and says so rather than blocking the caller.
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	assert.Error(t, stop(expired), "a stop with no time left reports the supervisor refusing")
}

// ForgetKind cancels the run in flight rather than waiting it out — a cold list of a large kind
// is minutes, and forgetting is synchronous. The join is outside armMu, so arming another cache
// is not held behind it either.
func TestForgetKindCancelsTheRunInFlightRatherThanWaitingItOut(t *testing.T) {
	fake := newParkingKindSync()
	svc, pool := newTestService(t, fake.option())
	pool.lease("prod").vouch(t, "uid-1")
	start(t, svc)

	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)
	fake.runs.Await(t, "the run to be out")

	// The run never returns on its own, so a forget that waited for it would never return.
	testutil.WaitReturn(t, func() { svc.ForgetKind(1, kind) }, "ForgetKind to cancel the run in flight")
	fake.returned.Await(t, "the run to have unwound before ForgetKind returned")

	// And another cache arms while nothing holds armMu behind the join.
	svc.TrackDiscovery(2, testParams)
	svc.TrackKind(2, kind)
	fake.runs.Await(t, "another cache to arm")
}

// Two generations for one kind must never be able to write at once: both drive the same
// collection, and a rename gives them different singulars to key rows by. A generation is a run
// and the stream it starts, and it ends when that stream is joined.
func TestARenameNeverRunsTwoGenerationsAtOnce(t *testing.T) {
	kinds := newParkingKindSync()
	svc, pool := newTestService(t, kinds.option())
	pool.lease("prod").vouch(t, "uid-1")
	start(t, svc)

	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)
	kinds.runs.Await(t, "the run to be out")

	svc.TrackKind(1, testKind("apps/v1", "Deploy", "deployments"))
	kinds.runs.Await(t, "the replacement to run")

	assert.False(t, kinds.sawOverlap(), "the old generation is gone before the replacement starts")
}

// ForgetKind promises that past its return nothing can still write through the kind. A re-track
// landing while the withdrawn run is unwinding must not undo that: if the kind reads as tracked
// again, commitKind stops refusing that run, and its last act is published as the new
// registration's answer.
func TestARetrackDuringAForgetCannotReviveTheWithdrawnRunsWrite(t *testing.T) {
	kinds := newParkingKindSync()
	svc, pool := newTestService(t, kinds.option())
	pool.lease("prod").vouch(t, "uid-1")
	start(t, svc)

	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)
	kinds.runs.Await(t, "the run to be out")

	forgotten := make(chan struct{})
	go func() { defer close(forgotten); svc.ForgetKind(1, kind) }()

	// Aimed at the window: the run has been cancelled and is unwinding, which is exactly when
	// a forget that let go of its arming lock early would admit a re-track.
	kinds.exiting.Await(t, "the withdrawn run to begin unwinding")
	svc.TrackKind(1, kind)
	testutil.Wait(t, forgotten, "the forget to return")
	kinds.runs.Await(t, "the re-tracked kind to run")

	state, ok := svc.GetKindState(1, kind)
	assert.False(t, ok && state.Reason == reasonWithdrawnWrite,
		"a withdrawn run's write is refused however the kind is registered afterwards")
	assert.False(t, kinds.sawOverlap(), "and it never runs beside its replacement")
}

// A rename leaves the kind TRACKED — same plural, new singular — so nothing refuses what the old
// generation reports on its way down. Its verdict must still not be served for the generation
// that replaced it: the rows are keyed by the new singular and nothing has listed them yet.
func TestARenameDropsTheOldGenerationsVerdictThoughTheKindStaysTracked(t *testing.T) {
	fake := newFakeKindSync()
	fake.reportOnExit = true
	svc, pool := newTestService(t, fake.option())
	pool.lease("prod").vouch(t, "uid-1")
	start(t, svc)

	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)
	fake.established.Await(t, "a stream to be standing")

	svc.TrackKind(1, testKind("apps/v1", "Deploy", "deployments"))

	state, ok := svc.GetKindState(1, kind)
	assert.False(t, ok && state.Reason == reasonWithdrawnWrite,
		"the withdrawn generation's report is not left standing under the kind that replaced it")
}
