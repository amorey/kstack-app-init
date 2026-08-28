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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

var podKind = kubestore.Kind{APIVersion: "v1", Kind: "Pod", Resource: "pods"}

func TestAColdKindIsListedIntoTheCacheAndThenWatched(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10", object("v1", "Pod", "one", "1"), object("v1", "Pod", "two", "2"))
	stream := cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)

	awaitKindReason(t, svc, 1, podKind, ReasonWatching)
	stream.opened.Await(t, "the watch to open")

	assert.ElementsMatch(t, []string{"one", "two"}, objectNames(t, svc, 1, podKind),
		"the cold list is on disk")
	cookie, ok := cookieOf(t, svc, 1, podKind)
	require.True(t, ok, "a completed list leaves the position a watch resumes from")
	assert.Equal(t, "10", cookie)
}

func TestAColdKindSaysItIsSyncingBeforeItHasAnything(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.streamKind(podKind)
	held := cluster.holdList(podKind)
	defer held.release()

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)

	// A cold start genuinely has no rows, so it says so rather than holding a reason it
	// cannot stand behind.
	awaitKindReason(t, svc, 1, podKind, ReasonSyncing)
	held.release()
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)
}

func TestADeltaLandsInTheCacheAndProvesTheStreamLive(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10")
	stream := cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)

	before, _ := svc.GetKindState(1, podKind)
	stream.opened.Await(t, "the watch to open").Add(object("v1", "Pod", "three", "11"))

	require.Eventually(t, func() bool {
		return len(objectNames(t, svc, 1, podKind)) == 1
	}, testutil.Timeout, time.Millisecond, "the delta to land")

	after, _ := svc.GetKindState(1, podKind)
	assert.True(t, after.LastUpdateAt.After(before.LastUpdateAt), "data arriving is stamped")
	assert.True(t, after.LastLiveAt.After(before.LastLiveAt), "and proves the stream live")
}

func TestABookmarkProvesTheStreamLiveWithoutData(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10")
	stream := cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)
	before, _ := svc.GetKindState(1, podKind)

	// Bookmarks exist to make an idle watch prove itself: no rows move, but the position
	// does, so a restart resumes from it rather than cold-listing again.
	stream.opened.Await(t, "the watch to open").Action(watch.Bookmark, object("v1", "Pod", "", "42"))

	require.Eventually(t, func() bool {
		cookie, ok := cookieOf(t, svc, 1, podKind)
		return ok && cookie == "42"
	}, testutil.Timeout, time.Millisecond, "the bookmark to move the position")

	after, _ := svc.GetKindState(1, podKind)
	assert.True(t, after.LastLiveAt.After(before.LastLiveAt), "a bookmark proves the stream live")
	assert.Equal(t, before.LastUpdateAt, after.LastUpdateAt, "and is not data")
}

func TestAWatchThatStopsProvingItselfReadsStale(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10")
	cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)

	awaitKindReason(t, svc, 1, podKind, ReasonWatching)
	awaitKindReason(t, svc, 1, podKind, ReasonStale)
}

func TestAResumeHoldsTheReasonTheRunBeforeItCommitted(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10", object("v1", "Pod", "one", "1"))
	stream := cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)
	stream.opened.Await(t, "the watch to open")

	// A resume poke on a cache of hundreds must not walk every kind through Syncing: the
	// rows are still there, so the verdict stands while the stream re-establishes.
	svc.RestartAll()
	stream.opened.Await(t, "the watch to be re-established")

	state, ok := svc.GetKindState(1, podKind)
	require.True(t, ok)
	assert.Equal(t, ReasonWatching, state.Reason, "a resume holds its reason")
	assert.ElementsMatch(t, []string{"one"}, objectNames(t, svc, 1, podKind),
		"and re-lists nothing: the cookie is what it resumes from")
}

func TestAKindThatWillNotListRetriesAndSaysWhy(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.refuseList(podKind, errors.New("the collection is forbidden"))
	cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)

	awaitKindState(t, svc, 1, podKind, "a run down for a reason it names", func(state KindState) bool {
		return state.Reason == ReasonSyncFailed &&
			strings.Contains(state.Message, "the collection is forbidden") &&
			!state.NextRetryAt.IsZero()
	})
	awaitKindState(t, svc, 1, podKind, "the run to be retried", func(state KindState) bool {
		return state.Restarts >= 2
	})
}

// The apiserver ends a watch on its own timeout, which is not a kind going down: reporting one
// would walk every collection through SyncFailed every few minutes.
func TestARotatedStreamIsRebuiltWithoutReportingAFailure(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10")
	stream := cluster.streamKind(podKind)

	// Long enough that nothing but a verdict this test provokes can move the reason.
	svc := newMirroringService(t, cluster, func(p *pacing) { p.staleAfter = time.Minute })
	mirrorKind(t, svc, 1, podKind)
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)

	stream.opened.Await(t, "the watch to open").Stop()
	stream.opened.Await(t, "the watch to be rebuilt")

	awaitKindState(t, svc, 1, podKind, "the reopen to be paced under the standing verdict",
		func(state KindState) bool {
			return state.Reason == ReasonWatching && !state.NextRetryAt.IsZero()
		})
}

// An open the server accepts and drops has proven nothing, so it must not pass for a settled
// stream — a run clearing its streak on the open alone would sit at the base delay forever.
func TestTheRetryStreakClearsOnAFrameRatherThanTheOpen(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10")
	stream := cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster, func(p *pacing) { p.staleAfter = time.Minute })
	mirrorKind(t, svc, 1, podKind)

	// Two closures with nothing between them: one streak, two rungs.
	for range 2 {
		stream.opened.Await(t, "the watch to open").Stop()
	}
	awaitKindState(t, svc, 1, podKind, "the ladder to climb", func(state KindState) bool {
		return state.Restarts >= 2
	})

	stream.opened.Await(t, "the watch to be rebuilt").Add(object("v1", "Pod", "one", "11"))

	awaitKindState(t, svc, 1, podKind, "the frame to clear the streak", func(state KindState) bool {
		return state.Restarts == 0 && state.NextRetryAt.IsZero()
	})
}

func TestEventsAgeOutOnACadenceRatherThanEveryDelta(t *testing.T) {
	eventKind := kubestore.Kind{APIVersion: "v1", Kind: "Event", Resource: "events"}
	cluster := newFakeCluster(t)
	cluster.serveKind(eventKind, true)
	cluster.hasObjects(eventKind, "10")
	stream := cluster.streamKind(eventKind)

	svc := newMirroringService(t, cluster, func(p *pacing) { p.eventsWindow, p.eventsEvery = 1, 3 })
	mirrorKind(t, svc, 1, eventKind)
	awaitKindReason(t, svc, 1, eventKind, ReasonWatching)

	// Events are the one collection the server never deletes from, so the cache is what
	// bounds them. Both rows standing is the proof the cadence held: the window is one, so a
	// prune per delta would already have taken the first.
	watcher := stream.opened.Await(t, "the watch to open")
	watcher.Add(event("one", "11"))
	watcher.Add(event("two", "12"))
	require.Eventually(t, func() bool {
		return len(eventsOf(t, svc, 1)) == 2
	}, testutil.Timeout, time.Millisecond, "both events to land unpruned")

	watcher.Add(event("three", "13"))
	require.Eventually(t, func() bool {
		return len(eventsOf(t, svc, 1)) == 1
	}, testutil.Timeout, time.Millisecond, "the window to be enforced once the cadence comes round")
}

func TestAWatchRefusedIsRetried(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10")
	stream := cluster.streamKind(podKind)
	stream.refuse(errors.New("the watch is forbidden"))

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)

	awaitKindState(t, svc, 1, podKind, "a run down for the watch it was refused", func(state KindState) bool {
		return state.Reason == ReasonSyncFailed && strings.Contains(state.Message, "the watch is forbidden")
	})
}

func TestAWatchThatErrorsOutIsRetried(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10")
	stream := cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)

	// The server ends a watch it will not serve by sending a Status, not by closing.
	stream.opened.Await(t, "the watch to open").Error(&metav1.Status{Message: "too old resource version"})

	awaitKindState(t, svc, 1, podKind, "a run down for the frame that ended it", func(state KindState) bool {
		return state.Reason == ReasonSyncFailed && strings.Contains(state.Message, "too old resource version")
	})
}

func TestAFrameThatIsNotAnObjectEndsTheRun(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10")
	stream := cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)

	// Nothing else can be applied to the rows, and treating it as data would book progress
	// for a delta that never landed.
	stream.opened.Await(t, "the watch to open").Action(watch.Added, &metav1.Status{Message: "not an object"})

	awaitKindState(t, svc, 1, podKind, "a run down for a frame it cannot apply", func(state KindState) bool {
		return state.Reason == ReasonSyncFailed && strings.Contains(state.Message, "unexpected")
	})
}

func TestAColdListWaitingOnTheGateGivesUpWithItsRun(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.streamKind(podKind)
	held := cluster.holdList(podKind)
	defer held.release()

	// One slot, taken by another kind's relist that is parked, so this one waits for the
	// gate rather than for the server.
	otherKind := kubestore.Kind{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments"}
	cluster.serveKind(otherKind, true)
	cluster.streamKind(otherKind)
	holdingSlot := cluster.holdList(otherKind)
	defer holdingSlot.release()

	svc := newMirroringService(t, cluster, func(p *pacing) { p.coldLists = make(chan struct{}, 1) })
	mirrorKind(t, svc, 1, otherKind)
	awaitKindReason(t, svc, 1, otherKind, ReasonSyncing)
	svc.TrackKind(1, podKind)
	awaitKindReason(t, svc, 1, podKind, ReasonSyncing)

	// Forgetting is synchronous, and a worker queued behind the gate has to give up for it
	// to be: nothing has reached the server on its behalf yet.
	svc.ForgetKind(1, podKind)
}

func TestAConnectionRetiredUnderAMirrorIsReplacedRatherThanRetried(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10", object("v1", "Pod", "one", "1"))
	stream := cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)
	stream.opened.Await(t, "the watch to open")

	// The pool rebuilt the connection — credentials moved — and the mirror is blocked in a
	// read on the one it retired. Nothing it reads can tell it, so the run has to end for the
	// worker to reach the replacement.
	lease := svc.connSvc.(*fakePool).lease("prod")
	lease.connect(t, cluster, "uid-1")

	stream.opened.Await(t, "the watch to be re-established over the replacement")
	state, _ := svc.GetKindState(1, podKind)
	assert.Equal(t, ReasonWatching, state.Reason, "a resume off the cookie holds its reason")
	assert.Zero(t, state.Restarts, "and a rebuild is not a run that is down")
}

func TestAKindWhoseAPIVersionIsUnparseableStillNamesItsCollection(t *testing.T) {
	gvr := gvrOf(kubestore.Kind{APIVersion: "a/b/c", Kind: "Broken", Resource: "brokens"})
	assert.Equal(t, "brokens", gvr.Resource, "a corrupt catalog row is not a panic")
}

func TestASlowResumeSaysSoOnceItHasStoppedBeingCurrent(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10", object("v1", "Pod", "one", "1"))
	stream := cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)
	stream.opened.Await(t, "the watch to open")

	// The next open never lands, so the resume outlasts the window its rows stay current
	// for — the one resume worth publishing.
	release := stream.hold()
	defer release()
	svc.RestartAll()

	awaitKindReason(t, svc, 1, podKind, ReasonResuming)
}

func TestACacheRemovedUnderAMirrorFailsItsRunRatherThanWritingOn(t *testing.T) {
	for _, tc := range []struct {
		name string
		send func(*watch.RaceFreeFakeWatcher)
	}{
		{"a delta", func(w *watch.RaceFreeFakeWatcher) { w.Add(object("v1", "Pod", "three", "11")) }},
		{"a bookmark", func(w *watch.RaceFreeFakeWatcher) { w.Action(watch.Bookmark, object("v1", "Pod", "", "42")) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster := newFakeCluster(t)
			cluster.serveKind(podKind, true)
			cluster.hasObjects(podKind, "10")
			stream := cluster.streamKind(podKind)

			svc := newMirroringService(t, cluster)
			mirrorKind(t, svc, 1, podKind)
			awaitKindReason(t, svc, 1, podKind, ReasonWatching)
			watcher := stream.opened.Await(t, "the watch to open")

			// The file is retired under the run, which is what a cache deleted while its
			// mirrors are up looks like. Nothing may land in it afterwards.
			require.NoError(t, svc.storeMgr.(*kubestore.Manager).Remove(1))
			tc.send(watcher)

			awaitKindReason(t, svc, 1, podKind, ReasonSyncFailed)
		})
	}
}

func TestAKindTrackedAgainstARemovedCacheFailsBeforeItReadsAnything(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10")
	cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	// The file is gone before this kind's mirror starts, so the position it would resume
	// from cannot even be read.
	require.NoError(t, svc.storeMgr.(*kubestore.Manager).Remove(1))
	svc.TrackKind(1, podKind)

	awaitKindReason(t, svc, 1, podKind, ReasonSyncFailed)
}

func TestAPositionTheServerHasDroppedIsColdListedRatherThanResumedForever(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10", object("v1", "Pod", "one", "1"))
	stream := cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)
	watcher := stream.opened.Await(t, "the watch to open")

	// The cluster has moved past the position the cookie names, which is the one failure a
	// resume cannot retry its way out of: the cookie is what it would retry with. The relist
	// is held so the verdict it runs under can be read before it lands.
	cluster.hasObjects(podKind, "20", object("v1", "Pod", "two", "2"))
	cluster.listed.Drain()
	held := cluster.holdList(podKind)
	defer held.release()
	watcher.Error(&metav1.Status{
		Reason: metav1.StatusReasonExpired, Code: 410, Message: "too old resource version",
	})

	// The rows are served throughout, so this is not the cold start Syncing names.
	awaitKindReason(t, svc, 1, podKind, ReasonResyncing)
	held.release()
	cluster.listed.Await(t, "the collection to be listed again")
	require.Eventually(t, func() bool {
		names := objectNames(t, svc, 1, podKind)
		return len(names) == 1 && names[0] == "two"
	}, testutil.Timeout, time.Millisecond, "the cold list to replace what the cache held")
}

func TestAStreamThatSettlesBetweenClosuresRetriesAtTheFloor(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10")
	stream := cluster.streamKind(podKind)

	// Wide enough that a ladder climbing over the cycles below is unmistakable in the delay
	// each failure reports.
	const floor = 100 * time.Millisecond
	svc := newMirroringService(t, cluster, func(p *pacing) {
		p.staleAfter = time.Minute
		p.backoff = probe.Backoff{Base: floor, Factor: 2, Cap: 10 * time.Second}
	})
	mirrorKind(t, svc, 1, podKind)

	// Three closures, each over a stream that delivered a frame first. Every one is the first
	// failure of its own streak, so each waits the floor — a worker whose ladder never reset
	// would be at four times that by the third.
	var delay time.Duration
	for range 3 {
		watcher := stream.opened.Await(t, "the watch to open")
		watcher.Add(object("v1", "Pod", "one", "11"))
		awaitKindState(t, svc, 1, podKind, "the frame to settle the stream", func(state KindState) bool {
			return state.Restarts == 0
		})

		before := time.Now()
		watcher.Stop()
		awaitKindState(t, svc, 1, podKind, "the reopen to be paced", func(state KindState) bool {
			if state.Restarts == 0 {
				return false
			}
			delay = state.NextRetryAt.Sub(before)
			return true
		})
	}
	assert.Less(t, delay, 2*floor, "a stream that settled clears the streak its ladder is climbing")
}

// The delta cadence counts within one run, so a mirror restarted more often than it comes round
// would never reach it — and a resume takes no list, which is the other thing that prunes.
func TestAResumingEventsCollectionIsCappedOnTheWayBackIn(t *testing.T) {
	eventKind := kubestore.Kind{APIVersion: "v1", Kind: "Event", Resource: "events"}
	cluster := newFakeCluster(t)
	cluster.serveKind(eventKind, true)
	cluster.hasObjects(eventKind, "10")
	stream := cluster.streamKind(eventKind)

	svc := newMirroringService(t, cluster, func(p *pacing) {
		p.staleAfter = time.Minute
		p.eventsWindow, p.eventsEvery = 1, 1000
	})
	mirrorKind(t, svc, 1, eventKind)
	awaitKindReason(t, svc, 1, eventKind, ReasonWatching)

	watcher := stream.opened.Await(t, "the watch to open")
	watcher.Add(event("one", "11"))
	watcher.Add(event("two", "12"))
	require.Eventually(t, func() bool {
		return len(eventsOf(t, svc, 1)) == 2
	}, testutil.Timeout, time.Millisecond, "both events to land with the cadence out of reach")

	// A rotation sends the mirror back through establish, which resumes from the cookie the
	// list left rather than taking another one.
	watcher.Stop()

	require.Eventually(t, func() bool {
		return len(eventsOf(t, svc, 1)) == 1
	}, testutil.Timeout, time.Millisecond, "the window to be enforced on the way back in")
}

func TestARelistedEventsCollectionIsCappedBeforeAnythingArrives(t *testing.T) {
	eventKind := kubestore.Kind{APIVersion: "v1", Kind: "Event", Resource: "events"}
	cluster := newFakeCluster(t)
	cluster.serveKind(eventKind, true)
	cluster.hasObjects(eventKind, "10", event("one", "1"), event("two", "2"), event("three", "3"))
	cluster.streamKind(eventKind)

	// A cadence long enough that no delta reaches it — an idle cluster's, where the LIST is
	// the only thing that ever wrote.
	svc := newMirroringService(t, cluster, func(p *pacing) { p.eventsWindow, p.eventsEvery = 1, 1000 })
	mirrorKind(t, svc, 1, eventKind)
	awaitKindReason(t, svc, 1, eventKind, ReasonWatching)

	require.Eventually(t, func() bool {
		return len(eventsOf(t, svc, 1)) == 1
	}, testutil.Timeout, time.Millisecond, "the window to bound what the list carried")
}

func TestASlowResumeThatFinishesReportsTheStreamItEstablished(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10", object("v1", "Pod", "one", "1"))
	stream := cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)
	stream.opened.Await(t, "the watch to open")

	release := stream.hold()
	svc.RestartAll()
	awaitKindReason(t, svc, 1, podKind, ReasonResuming)

	// The announcement is the resume being slow, not the stream being down: once it lands,
	// the verdict is the stream's and stays there.
	release()
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)
	state, _ := svc.GetKindState(1, podKind)
	assert.Equal(t, ReasonWatching, state.Reason, "the announcement does not outlive what it announced")
}

func TestAResumeParkedOnItsOpenStillGivesUpWithItsRun(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10", object("v1", "Pod", "one", "1"))
	stream := cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)
	stream.opened.Await(t, "the watch to open")

	// An open that never lands, held past the announcement so the worker is waiting on it
	// rather than on the window before it.
	release := stream.hold()
	defer release()
	svc.RestartAll()
	awaitKindReason(t, svc, 1, podKind, ReasonResuming)

	// Forgetting a kind waits for its worker. Whether the open ever unwinds is the server's
	// business, so a worker that waits on one unconditionally cannot be joined.
	done := make(chan struct{})
	go func() { defer close(done); svc.ForgetKind(1, podKind) }()
	testutil.Wait(t, done, "the teardown to return while the open is still held")
}

func TestARelistRefusedOnceIsRetriedAsARelist(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveKind(podKind, true)
	cluster.hasObjects(podKind, "10", object("v1", "Pod", "one", "1"))
	stream := cluster.streamKind(podKind)

	svc := newMirroringService(t, cluster)
	mirrorKind(t, svc, 1, podKind)
	awaitKindReason(t, svc, 1, podKind, ReasonWatching)
	watcher := stream.opened.Await(t, "the watch to open")

	// The collection moves on, the position is dropped, and the replacement list is refused
	// once. The cookie only goes when a page lands, so a retry that forgot it must relist
	// would resume from a position the server has already refused.
	cluster.hasObjects(podKind, "20", object("v1", "Pod", "two", "2"))
	cluster.failListOnce(podKind, errors.New("the api server is busy"))
	watcher.Error(&metav1.Status{
		Reason: metav1.StatusReasonExpired, Code: 410, Message: "too old resource version",
	})

	require.Eventually(t, func() bool {
		names := objectNames(t, svc, 1, podKind)
		return len(names) == 1 && names[0] == "two"
	}, testutil.Timeout, time.Millisecond, "the relist to be retried until it lands")
}
