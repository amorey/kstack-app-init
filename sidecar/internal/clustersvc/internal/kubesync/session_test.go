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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

func TestASessionTakesItsClaimsInStartAndGivesThemBackInClose(t *testing.T) {
	svc, pool := newTestService(t)

	sess := newSession(svc, 1, testParams)
	require.Equal(t, 0, pool.lease("prod").held(), "a session holds nothing before it starts")

	require.NoError(t, sess.start())
	assert.Equal(t, 1, pool.lease("prod").held(), "start claims the context")
	assert.NotNil(t, sess.store, "start opens the cache file")

	sess.close()
	assert.Equal(t, 0, pool.lease("prod").held(), "close gives the claim back")
}

func TestStartPutsTheCacheOnTheDiscoveryEngineAndCloseTakesItOff(t *testing.T) {
	svc, _ := newTestService(t)

	sess := newSession(svc, 1, testParams)
	_, ok := svc.discoveryEngine.Read(sess.subject())
	require.False(t, ok, "nothing sweeps for a session that has not started")

	require.NoError(t, sess.start())
	_, ok = svc.discoveryEngine.Read(sess.subject())
	assert.True(t, ok, "start adds the cache's subject")

	sess.close()
	_, ok = svc.discoveryEngine.Read(sess.subject())
	assert.False(t, ok, "close drops it again")
}

func TestACatalogSubscriptionEndingLeavesTheSessionRunning(t *testing.T) {
	svc, _ := newTestService(t)
	svc.TrackDiscovery(1, testParams)

	// Clearing the cache ends the file the subscription belongs to, which is what the wake
	// loop sees: it returns, and the session — whose sweep still runs on the interval —
	// tears down as usual.
	require.NoError(t, svc.storeMgr.(*kubestore.Manager).Clear(1))

	svc.ForgetDiscovery(1)
}

func TestASweepIsParkedOnlyWhileSomethingHasNotBeenScheduled(t *testing.T) {
	svc, _ := newTestService(t)

	assert.False(t, svc.sweepParked("cache/99"), "a subject nobody tracks is not a parked sweep")
}

func TestAKindWorkerGivesUpWhenItsSessionEnds(t *testing.T) {
	entered := testutil.NewProbe[struct{}](1)
	returned := testutil.NewProbe[struct{}](1)
	svc, pool := newTestService(t, withSyncKindFn(func(context.Context, kindRun) { entered.Fire(struct{}{}) }))

	sess := newSession(svc, 1, testParams)
	require.NoError(t, sess.start())

	// Nothing vouches for the ServerUID, so the worker parks in AwaitConnFor rather than
	// reaching the sync. Waiting for its watch is what makes the close below land on a
	// worker that got that far, rather than on one whose loop never entered the body.
	sess.startKind(testKind("apps/v1", "Deployment", "deployments"))
	require.Eventually(t, func() bool { return pool.lease("prod").watchers() > 1 },
		testutil.Timeout, time.Millisecond, "the worker to be waiting for a connection")

	go func() { sess.close(); returned.Fire(struct{}{}) }()

	testutil.Wait(t, returned.Chan(), "the worker to give up once its session ends")
	testutil.NoRecv(t, entered.Chan(), quietWindow, "a kind syncs without a connection vouching for it")
}

func TestASweepRegistersAgainstItsSessionOnlyWhileOneIsArmed(t *testing.T) {
	svc, _ := newTestService(t)

	_, ok := svc.enterSweep(1)
	assert.False(t, ok, "a cache nobody has armed registers no run")

	svc.TrackDiscovery(1, testParams)
	sess, ok := svc.enterSweep(1)
	require.True(t, ok, "an armed cache registers its run")
	sess.leaveSweep()

	// Closing the door is what a teardown does before it joins, so a run that has not
	// registered by then never will.
	svc.mu.Lock()
	sess.stopping = true
	svc.mu.Unlock()

	_, ok = svc.enterSweep(1)
	assert.False(t, ok, "a cache already stopping registers no run")
}

func TestAConnectionAnsweringAsAnotherClusterWakesNothing(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))
	svc, pool := newTestService(t)
	start(t, svc)
	svc.TrackDiscovery(1, testParams)

	// A connection arrives, so the wake loop runs — but it answers for another cluster, so
	// it is not the connection this cache waits for and the sweep stays parked.
	pool.lease("prod").connect(cluster, "another-uid")

	cluster.noRead(t, "a sweep dialing a connection that answers as another cluster")
}

func TestASweepIsParkedWhileAnythingUnderItIsUnscheduled(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))
	svc, pool := newTestService(t)
	start(t, svc)

	// Nothing vouches yet, so every probe suspends and schedules nothing.
	svc.TrackDiscovery(1, testParams)
	sess := svc.sessionOf(1)
	require.Eventually(t, func() bool { return svc.sweepParked(sess.subject()) },
		testutil.Timeout, time.Millisecond, "a suspended sweep is parked")

	// The wake loop hands it a connection, and a settled sweep is scheduled again.
	pool.lease("prod").connect(cluster, "uid-1")
	require.Eventually(t, func() bool { return !svc.sweepParked(sess.subject()) },
		testutil.Timeout, time.Millisecond, "a settled sweep is not parked")
}

func TestASettledSweepReportsAConnectionItLost(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))
	svc, pool := newTestService(t)
	pool.lease("prod").connect(cluster, "uid-1")
	start(t, svc)
	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	// A settled sweep is scheduled rather than parked, so nothing but this wake replaces a
	// verdict the connection just made wrong.
	pool.lease("prod").drop()

	awaitReason(t, svc, 1, ReasonNoConnection)
}

func TestASettledSweepReportsAConnectionThatStartedAnsweringAsAnotherCluster(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))
	svc, pool := newTestService(t)
	pool.lease("prod").connect(cluster, "uid-1")
	start(t, svc)
	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	pool.lease("prod").connect(cluster, "another-uid")

	awaitReason(t, svc, 1, ReasonIdentityMismatch)
}

func TestALostConnectionReportedOnceIsNotWokenAgain(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))
	svc, pool := newTestService(t)
	pool.lease("prod").connect(cluster, "uid-1")
	start(t, svc)
	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	pool.lease("prod").drop()
	awaitReason(t, svc, 1, ReasonNoConnection)
	// One probe committing the verdict is not all three finishing: the other two dial on
	// their way out, and a baseline drained before them would count those as the re-wake.
	awaitSweepSettled(t, svc, 1)

	// The feed publishes every pass, not only the ones that changed something. A verdict
	// that already names the connection is not news, and re-waking on each frame would be
	// the poll a suspended sweep exists to avoid. One dial per frame is the wake loop
	// looking; a further one is a run it woke.
	lease := pool.lease("prod")
	awaitSweepSettled(t, svc, 1)
	lease.dialed.Drain()
	lease.drop()

	lease.dialed.Await(t, "the wake loop to look at the frame")
	testutil.NoRecv(t, lease.dialed.Chan(), quietWindow,
		"a sweep re-woken by a connection whose verdict is already reported")
}
