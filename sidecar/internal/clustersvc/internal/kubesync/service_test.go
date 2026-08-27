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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	svc, pool := newTestService(t, withKindBody(func(ctx context.Context, _ kindRun) {
		entered.Fire(struct{}{})
		<-ctx.Done()
	}))
	pool.lease("prod").vouch("uid-1")

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
	svc, pool := newTestService(t, withKindBody(func(ctx context.Context, _ kindRun) {
		entered.Fire(struct{}{})
		<-ctx.Done()
	}))
	pool.lease("prod").vouch("uid-1")

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
	svc, pool := newTestService(t, withKindBody(func(ctx context.Context, _ kindRun) {
		entered.Fire(struct{}{})
		<-ctx.Done()
	}))

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))

	pool.lease("prod").vouch("uid-other")
	_, running := entered.TryAwait()
	require.False(t, running, "a connection answering as another cluster arms nothing")

	pool.lease("prod").vouch("uid-1")
	entered.Await(t, "the worker starts once the connection vouches for its identity")
}

func TestForgetKindWaitsForItsWorker(t *testing.T) {
	entered, returned := testutil.NewProbe[struct{}](4), testutil.NewProbe[struct{}](4)
	svc, pool := newTestService(t, withKindBody(func(ctx context.Context, _ kindRun) {
		entered.Fire(struct{}{})
		<-ctx.Done()
		returned.Fire(struct{}{})
	}))
	pool.lease("prod").vouch("uid-1")

	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)
	entered.Await(t, "the mirror runs")

	svc.ForgetKind(1, kind)
	_, done := returned.TryAwait()
	require.True(t, done, "ForgetKind returns only once the worker cannot still write")
}

func TestForgetDiscoveryWaitsForEveryWorkerUnderIt(t *testing.T) {
	entered, returned := testutil.NewProbe[struct{}](8), testutil.NewProbe[struct{}](8)
	body := func(ctx context.Context) {
		entered.Fire(struct{}{})
		<-ctx.Done()
		returned.Fire(struct{}{})
	}
	svc, pool := newTestService(t,
		withDiscoveryBody(func(ctx context.Context, _ discoveryRun) { body(ctx) }),
		withKindBody(func(ctx context.Context, _ kindRun) { body(ctx) }))
	pool.lease("prod").vouch("uid-1")

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))
	entered.Await(t, "the sweep runs")
	entered.Await(t, "the mirror runs")

	svc.ForgetDiscovery(1)
	returned.Await(t, "the discovery body is drained")
	returned.Await(t, "the kind body is drained")
}

func TestGetStateReportsNothingBeforeARunCommits(t *testing.T) {
	svc, pool := newTestService(t)
	pool.lease("prod").vouch("uid-1")
	kind := testKind("apps/v1", "Deployment", "deployments")

	_, ok := svc.GetDiscoveryState(1)
	assert.False(t, ok, "a cache nobody has armed has no answer")

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)

	_, ok = svc.GetDiscoveryState(1)
	assert.False(t, ok, "an armed cache whose sweep has committed nothing has no answer")
	_, ok = svc.GetKindState(1, kind)
	assert.False(t, ok, "a kind whose worker has committed nothing has no answer")
}

func TestACommittedStateIsReadableAndWakesItsRecord(t *testing.T) {
	committed := testutil.NewProbe[func(DiscoveryState)](4)
	svc, _ := newTestService(t, withDiscoveryBody(func(ctx context.Context, r discoveryRun) {
		committed.Fire(r.Commit)
		<-ctx.Done()
	}))

	news := svc.WatchDiscoveryNews()
	t.Cleanup(news.Close)

	svc.TrackDiscovery(1, testParams)
	commit := committed.Await(t, "the sweep body runs")

	commit(DiscoveryState{Reason: ReasonDiscovered, Message: "14 group-versions"})
	ev := testutil.Recv(t, news.Chan(), "the cache is woken")
	assert.Equal(t, int64(1), ev.Key, "news is keyed by cache id and carries nothing else")

	got, ok := svc.GetDiscoveryState(1)
	require.True(t, ok)
	assert.Equal(t, ReasonDiscovered, got.Reason)
	assert.Equal(t, "14 group-versions", got.Message)
}

func TestNewsFiresOnAReasonThatMovedAndOnACatalogThatCommitted(t *testing.T) {
	runs := testutil.NewProbe[discoveryRun](4)
	svc, _ := newTestService(t, withDiscoveryBody(func(ctx context.Context, r discoveryRun) {
		runs.Fire(r)
		<-ctx.Done()
	}))

	news := svc.WatchDiscoveryNews()
	t.Cleanup(news.Close)

	svc.TrackDiscovery(1, testParams)
	r := runs.Await(t, "the sweep body runs")

	r.Commit(DiscoveryState{Reason: ReasonDiscovered})
	testutil.Recv(t, news.Chan(), "the first answer is news")

	// A resume is not news: nothing a reader can act on moved.
	r.Commit(DiscoveryState{Reason: ReasonDiscovered, Message: "re-read, unchanged"})
	testutil.NoRecv(t, news.Chan(), quietWindow, "an unmoved reason wakes nobody")

	// The one publication a reason cannot carry: two sweeps both settling on Discovered
	// with a kind appearing between them.
	r.Announce()
	testutil.Recv(t, news.Chan(), "a catalog that committed is news")
}

func TestKindNewsIsKeyedByCacheAndKind(t *testing.T) {
	runs := testutil.NewProbe[kindRun](4)
	svc, pool := newTestService(t, withKindBody(func(ctx context.Context, r kindRun) {
		runs.Fire(r)
		<-ctx.Done()
	}))
	pool.lease("prod").vouch("uid-1")

	news := svc.WatchKindNews()
	t.Cleanup(news.Close)

	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)
	r := runs.Await(t, "the mirror body runs")

	r.Commit(KindState{Reason: ReasonWatching})
	ev := testutil.Recv(t, news.Chan(), "the kind's record is woken")
	assert.Equal(t, KindKey{CacheID: 1, Kind: kind}, ev.Key)

	got, ok := svc.GetKindState(1, kind)
	require.True(t, ok)
	assert.Equal(t, ReasonWatching, got.Reason)
}

func TestReplacingAKindDropsTheStoppedWorkersVerdict(t *testing.T) {
	runs := testutil.NewProbe[kindRun](4)
	svc, pool := newTestService(t, withKindBody(func(ctx context.Context, r kindRun) {
		runs.Fire(r)
		<-ctx.Done()
	}))
	pool.lease("prod").vouch("uid-1")

	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)
	runs.Await(t, "the mirror runs").Commit(KindState{Reason: ReasonWatching})

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

func TestRestartAllReEntersEveryArmedBody(t *testing.T) {
	entered := testutil.NewProbe[struct{}](8)
	body := func(ctx context.Context) {
		entered.Fire(struct{}{})
		<-ctx.Done()
	}
	svc, pool := newTestService(t,
		withDiscoveryBody(func(ctx context.Context, _ discoveryRun) { body(ctx) }),
		withKindBody(func(ctx context.Context, _ kindRun) { body(ctx) }))
	pool.lease("prod").vouch("uid-1")

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))
	entered.Await(t, "the sweep runs")
	entered.Await(t, "the mirror runs")

	// A watch that died under a sleeping machine reports nothing, so a resume poke
	// restarts the runs rather than waiting for one to notice.
	svc.RestartAll()
	entered.Await(t, "the sweep runs again")
	entered.Await(t, "the mirror runs again")
}

func TestStopDrainsEveryWorker(t *testing.T) {
	entered, returned := testutil.NewProbe[struct{}](8), testutil.NewProbe[struct{}](8)
	body := func(ctx context.Context) {
		entered.Fire(struct{}{})
		<-ctx.Done()
		returned.Fire(struct{}{})
	}
	svc, pool := newTestService(t,
		withDiscoveryBody(func(ctx context.Context, _ discoveryRun) { body(ctx) }),
		withKindBody(func(ctx context.Context, _ kindRun) { body(ctx) }))
	pool.lease("prod").vouch("uid-1")

	stop, err := svc.Start(t.Context())
	require.NoError(t, err)

	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, testKind("apps/v1", "Deployment", "deployments"))
	entered.Await(t, "the sweep runs")
	entered.Await(t, "the mirror runs")

	require.NoError(t, stop(t.Context()))
	returned.Await(t, "the sweep is drained")
	returned.Await(t, "the mirror is drained")

	require.NoError(t, svc.Close())
	assert.Equal(t, 0, pool.lease("prod").held(), "Close releases the claims")
}
