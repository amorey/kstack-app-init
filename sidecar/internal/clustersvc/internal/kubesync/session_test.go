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
	_, ok := svc.discoverySupervisor.Read(sess.discoverySubject())
	require.False(t, ok, "nothing sweeps for a session that has not started")

	require.NoError(t, sess.start())
	_, ok = svc.discoverySupervisor.Read(sess.discoverySubject())
	assert.True(t, ok, "start adds the cache's subject")

	sess.close()
	_, ok = svc.discoverySupervisor.Read(sess.discoverySubject())
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

func TestASubjectNobodyTracksIsNotAParkedSweep(t *testing.T) {
	svc, _ := newTestService(t)

	assert.False(t, svc.sweepParked("cache/99"), "a subject nobody tracks is not a parked sweep")
}

// The fan-out skips while neither document has answered, which schedules nothing either. Reading
// that as parked would wake the whole sweep on every connection state frame, and the failing
// documents would be re-dispatched ahead of the ladder they should be climbing.
func TestASweepWaitingOnItsOwnDocumentsIsNotParked(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.breakPath("/api")
	cluster.breakPath("/apis")

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)
	svc.TrackDiscovery(1, testParams)
	awaitReason(t, svc, 1, ReasonDiscoveryFailed)

	assert.False(t, svc.sweepParked(svc.sessionOf(1).discoverySubject()),
		"a sweep whose documents will not load waits on its own retries, not on a wake")
}

// A run holds a supervisor worker, so a kind whose connection does not vouch records why and
// suspends rather than waiting at the gate. Nothing syncs past it, and the session ends without
// anything to join.
func TestAKindSuspendedAtTheGateSyncsNothingAndEndsWithItsSession(t *testing.T) {
	fake := newFakeKindSync()
	svc, _ := newTestService(t, fake.option())
	start(t, svc)

	kind := testKind("apps/v1", "Deployment", "deployments")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, kind)
	awaitKindReason(t, svc, 1, kind, ReasonNoConnection)

	// The gate is ahead of anything the body reads, so a run reaching it has read nothing.
	testutil.NoRecv(t, fake.runs.Chan(), quietWindow, "a kind syncs without a connection vouching for it")

	returned := testutil.NewProbe[struct{}](1)
	go func() { svc.ForgetDiscovery(1); returned.Fire(struct{}{}) }()
	testutil.Wait(t, returned.Chan(), "the session to end")
}

func TestASweepRegistersAgainstItsSessionOnlyWhileOneIsArmed(t *testing.T) {
	svc, _ := newTestService(t)

	_, ok := svc.enterRun(1)
	assert.False(t, ok, "a cache nobody has armed registers no run")

	svc.TrackDiscovery(1, testParams)
	sess, ok := svc.enterRun(1)
	require.True(t, ok, "an armed cache registers its run")
	sess.leaveRun()

	// Closing the door is what a teardown does before it joins, so a run that has not
	// registered by then never will.
	svc.mu.Lock()
	sess.stopping = true
	svc.mu.Unlock()

	_, ok = svc.enterRun(1)
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
	pool.lease("prod").connect(t, cluster, "another-uid")

	cluster.noRead(t, "a sweep dialing a connection that answers as another cluster")
}

func TestASweepIsParkedWhileAnythingUnderItIsSuspended(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))
	svc, pool := newTestService(t)
	start(t, svc)

	// Nothing vouches yet, so every probe suspends and schedules nothing.
	svc.TrackDiscovery(1, testParams)
	sess := svc.sessionOf(1)
	require.Eventually(t, func() bool { return svc.sweepParked(sess.discoverySubject()) },
		testutil.Timeout, time.Millisecond, "a suspended sweep is parked")

	// The wake loop hands it a connection, and a settled sweep is scheduled again.
	pool.lease("prod").connect(t, cluster, "uid-1")
	require.Eventually(t, func() bool { return !svc.sweepParked(sess.discoverySubject()) },
		testutil.Timeout, time.Millisecond, "a settled sweep is not parked")
}

func TestASettledSweepReportsAConnectionItLost(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))
	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
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
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)
	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	pool.lease("prod").connect(t, cluster, "another-uid")

	awaitReason(t, svc, 1, ReasonIdentityMismatch)
}

func TestALostConnectionReportedOnceIsNotWokenAgain(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))
	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)
	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	pool.lease("prod").drop()
	awaitReason(t, svc, 1, ReasonNoConnection)

	// The feed publishes every pass, not only the ones that changed something. A verdict
	// that already names the connection is not news, and re-waking on each frame would be
	// the poll a suspended sweep exists to avoid. One dial per frame is the wake loop
	// looking; a further one is a run it woke — so the baseline is the point the cache has
	// stopped dialing, not the point one probe committed.
	lease := pool.lease("prod")
	awaitDialsQuiet(t, lease)
	lease.drop()

	lease.dialed.Await(t, "the wake loop to look at the frame")
	testutil.NoRecv(t, lease.dialed.Chan(), quietWindow,
		"a sweep re-woken by a connection whose verdict is already reported")
}

func TestAKindParkedAtTheGateReportsWhyItWaits(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10")
	cluster.streamKind(podKind)

	svc := newSyncingService(t, cluster)
	syncKind(t, svc, 1, podKind)
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)

	// Each refusal is the kind's own news, in the order the pool moved through them, and
	// the retry countdown does not survive into the wait: nothing is retrying at the gate.
	lease := svc.connSvc.(*fakePool).lease("prod")
	lease.drop()
	awaitKindReason(t, svc, 1, podKind, ReasonNoConnection)
	state, _ := svc.GetKindState(1, podKind)
	assert.True(t, state.NextRetryAt.IsZero())

	lease.connect(t, cluster, "another-uid")
	awaitKindReason(t, svc, 1, podKind, ReasonIdentityMismatch)

	lease.connect(t, cluster, "uid-1")
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)
}

func TestACacheStandsBehindNoReasonUntilARunCommitsOne(t *testing.T) {
	svc, _ := newTestService(t)

	sess := newSession(svc, 1, testParams)
	assert.Empty(t, sess.discoveryReason(),
		"a cache whose sweep has not answered names no reason, so the first frame is news")
}

// The admission is one critical section: it checks the cache is armed and reads the kind
// together. Split apart, a ForgetKind landing between the two would leave the run listing rows
// for a kind nobody tracks — the relist-behind-a-clear race ForgetKind is ordered before
// ClearKind to rule out.
func TestAKindRunIsAdmittedWithItsWholeKind(t *testing.T) {
	svc, pool := newTestService(t)
	pool.lease("prod").vouch(t, "uid-1")
	svc.TrackDiscovery(1, testParams)
	svc.TrackKind(1, podKind)

	sess, k, ok := svc.enterKindRun(1, idOf(podKind))
	require.True(t, ok, "an armed cache with the kind tracked admits the run")
	assert.Equal(t, podKind, k, "the run is handed the whole value, singular included")

	sess.leaveRun()
}

func TestAKindRunIsRefusedForACacheOrAKindThatIsGone(t *testing.T) {
	svc, pool := newTestService(t)
	pool.lease("prod").vouch(t, "uid-1")

	_, _, ok := svc.enterKindRun(1, idOf(podKind))
	assert.False(t, ok, "nothing has armed this cache")

	svc.TrackDiscovery(1, testParams)
	_, _, ok = svc.enterKindRun(1, idOf(podKind))
	assert.False(t, ok, "the cache is armed but nothing tracks this kind")
}
