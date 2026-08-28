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
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
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
	entered := testutil.NewProbe[struct{}](4)
	svc, pool := newTestService(t, withSyncKindFn(func(ctx context.Context, _ kindRun) {
		entered.Fire(struct{}{})
		<-ctx.Done()
	}))
	pool.lease("prod").vouch(t, "uid-1")

	// The record's pass may land before the cache's, so the registration is held rather
	// than refused.
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))
	_, running := entered.TryAwait()
	require.False(t, running, "a kind on an unarmed cache runs nothing")

	svc.TrackDiscovery(1, testParams)
	entered.Await(t, "the held kind starts when its cache is armed")
}

func TestARegistrationOutlivesItsCacheBeingForgotten(t *testing.T) {
	entered := testutil.NewProbe[struct{}](4)
	svc, pool := newTestService(t, withSyncKindFn(func(ctx context.Context, _ kindRun) {
		entered.Fire(struct{}{})
		<-ctx.Done()
	}))
	pool.lease("prod").vouch(t, "uid-1")

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))
	entered.Await(t, "the kind runs while its cache is armed")

	// Pausing is one call and resuming is one call: nothing re-registers the kinds.
	svc.ForgetDiscovery(1)
	svc.TrackDiscovery(1, testParams)
	entered.Await(t, "resuming restarts every kind still registered")
}

func TestAKindWaitsForAConnectionVouchingForItsServerUID(t *testing.T) {
	entered := testutil.NewProbe[struct{}](4)
	svc, pool := newTestService(t, withSyncKindFn(func(ctx context.Context, _ kindRun) {
		entered.Fire(struct{}{})
		<-ctx.Done()
	}))

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))

	pool.lease("prod").vouch(t, "uid-other")
	_, running := entered.TryAwait()
	require.False(t, running, "a connection answering as another cluster arms nothing")

	pool.lease("prod").vouch(t, "uid-1")
	entered.Await(t, "the worker starts once the connection vouches for its identity")
}

func TestForgetKindWaitsForItsWorker(t *testing.T) {
	entered, returned := testutil.NewProbe[struct{}](4), testutil.NewProbe[struct{}](4)
	svc, pool := newTestService(t, withSyncKindFn(func(ctx context.Context, _ kindRun) {
		entered.Fire(struct{}{})
		<-ctx.Done()
		returned.Fire(struct{}{})
	}))
	pool.lease("prod").vouch(t, "uid-1")

	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)
	entered.Await(t, "the sync runs")

	svc.ForgetKind(1, kind)
	_, done := returned.TryAwait()
	require.True(t, done, "ForgetKind returns only once the worker cannot still write")
}

func TestForgetDiscoveryWaitsForEveryWorkerUnderIt(t *testing.T) {
	entered, returned := testutil.NewProbe[struct{}](8), testutil.NewProbe[struct{}](8)
	svc, pool := newTestService(t, withSyncKindFn(func(ctx context.Context, _ kindRun) {
		entered.Fire(struct{}{})
		<-ctx.Done()
		returned.Fire(struct{}{})
	}))
	pool.lease("prod").vouch(t, "uid-1")

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))
	entered.Await(t, "the sync runs")

	svc.ForgetDiscovery(1)
	returned.Await(t, "the kind body is drained")
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
	assert.False(t, ok, "a kind whose worker has committed nothing has no answer")
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
	runs := testutil.NewProbe[kindRun](4)
	svc, pool := newTestService(t, withSyncKindFn(func(ctx context.Context, r kindRun) {
		runs.Fire(r)
		<-ctx.Done()
	}))
	pool.lease("prod").vouch(t, "uid-1")

	news := svc.WatchKindNews()
	t.Cleanup(news.Close)

	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)
	r := runs.Await(t, "the sync runs")

	r.Commit(KindState{Reason: ReasonWatching})
	ev := testutil.Recv(t, news.Chan(), "the kind's record is woken")
	assert.Equal(t, KindKey{CacheID: 1, Kind: kind}, ev.Key)

	got, ok := svc.GetKindState(1, kind)
	require.True(t, ok)
	assert.Equal(t, ReasonWatching, got.Reason)
}

func TestReplacingAKindDropsTheStoppedWorkersVerdict(t *testing.T) {
	runs := testutil.NewProbe[kindRun](4)
	svc, pool := newTestService(t, withSyncKindFn(func(ctx context.Context, r kindRun) {
		runs.Fire(r)
		<-ctx.Done()
	}))
	pool.lease("prod").vouch(t, "uid-1")

	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)
	runs.Await(t, "the sync runs").Commit(KindState{Reason: ReasonWatching})

	// The plural is unchanged, so this is the same collection under a new Kind name. The
	// worker that answered Watching is stopped, and the one replacing it may still be
	// waiting for a connection or cold-listing.
	renamed := testKind("apps/v1", "Deploy", "deployments")
	svc.TrackKind(1, renamed)
	_, ok := svc.GetKindState(1, renamed)
	assert.False(t, ok, "a replacement worker starts with no answer")

	runs.Await(t, "the replacement runs").Commit(KindState{Reason: ReasonSyncing})
	got, ok := svc.GetKindState(1, renamed)
	require.True(t, ok)
	assert.Equal(t, ReasonSyncing, got.Reason)
}

func TestRestartAllReEntersEveryArmedMirror(t *testing.T) {
	entered := testutil.NewProbe[struct{}](8)
	svc, pool := newTestService(t, withSyncKindFn(func(ctx context.Context, _ kindRun) {
		entered.Fire(struct{}{})
		<-ctx.Done()
	}))
	pool.lease("prod").vouch(t, "uid-1")

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))
	entered.Await(t, "the sync runs")

	// A watch that died under a sleeping machine reports nothing, so a resume poke
	// restarts the run rather than waiting for one to notice.
	svc.RestartAll()
	entered.Await(t, "the sync runs again")
}

func TestStopDrainsEveryWorker(t *testing.T) {
	entered, returned := testutil.NewProbe[struct{}](8), testutil.NewProbe[struct{}](8)
	svc, pool := newTestService(t, withSyncKindFn(func(ctx context.Context, _ kindRun) {
		entered.Fire(struct{}{})
		<-ctx.Done()
		returned.Fire(struct{}{})
	}))
	pool.lease("prod").vouch(t, "uid-1")

	stop, err := svc.Start(t.Context())
	require.NoError(t, err)

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))
	entered.Await(t, "the sync runs")

	require.NoError(t, stop(t.Context()))
	returned.Await(t, "the sync is drained")

	require.NoError(t, svc.Close())
	assert.Equal(t, 0, pool.lease("prod").held(), "Close releases the claims")
}

func TestACacheWhoseStoreWillNotOpenArmsNothing(t *testing.T) {
	pool := newFakePool()
	stores := &refusingStores{}
	svc := New(pool, stores, withSyncKindFn(func(ctx context.Context, _ kindRun) { <-ctx.Done() }))
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

	// The engine names its own subjects, so both of these are reads the seam must survive
	// rather than states it can reach: one not this package's, one whose cache is gone.
	svc.publishDiscovery("not-a-cache", probe.Snapshot{})
	svc.publishDiscovery(subjectOf(404), probe.Snapshot{})
}

func TestAStoppedWorkersAnswerIsDropped(t *testing.T) {
	svc, _ := newTestService(t)
	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	sess := svc.sessionOf(1)

	// The session a worker committed against is gone, which is what a commit racing its own
	// teardown looks like: the answer belongs to nobody and is dropped.
	svc.ForgetDiscovery(1)
	svc.commitDiscovery(sess, DiscoveryState{Reason: ReasonDiscovered})
	svc.commitKind(sess, kind, KindState{Reason: ReasonWatching})

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

	// A sweep is parked mid-request, so the engine cannot drain inside a deadline that has
	// already passed and says so rather than blocking the caller.
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	assert.Error(t, stop(expired), "a stop with no time left reports the engine refusing")
}
