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

// White-box (package cluster): the service test seeds beehive objects directly and
// exercises the data/mutation/watch surface in isolation from the (network-touching)
// real controllers, using the shared helpers in testutil_test.go.
package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// WatchCacheSyncHealth folds a cache's per-kind records into one verdict — the reading an
// always-mounted consumer needs, since the per-kind stream is a hundred-plus records per
// cache and no single child's verdict is the cache's.
//
// The fold must be dominated by the worst kind: ninety-nine healthy kinds and one whose
// watch is wedged is not a healthy cache, and reading any one child would call it either
// way at random.
func TestServiceWatchCacheSyncHealthFoldsWorstKind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _, syncCC, _ := newServiceTestSync(t)
	id := seedCluster(t, s, "alpha")

	cacheID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")
	discoveryID := seedGVRDiscovery(t, s, cacheID)
	healthy := seedGVRSync(t, s, discoveryID, "apps/v1", "deployments")
	wedged := seedGVRSync(t, s, discoveryID, "example.com/v1", "widgets")

	require.NoError(t, syncCC.SetConditions(ctx, healthy, []domain.Condition{
		domain.LiveCondition(domain.ConditionSynced, domain.ConditionTrue, domain.ReasonWatching, ""),
	}))
	require.NoError(t, syncCC.SetConditions(ctx, wedged, []domain.Condition{
		domain.LiveCondition(domain.ConditionSynced, domain.ConditionFalse, domain.ReasonStale, "the watch has stalled"),
	}))

	st, err := s.Caches().WatchSyncHealth(ctx)
	require.NoError(t, err)
	ch := st.Frames

	deadline := time.After(3 * time.Second)
	for {
		h := recvBy(t, ch, deadline)
		if h.CacheID != domain.ClusterCacheID(cacheID) || h.Reason != domain.ReasonStale {
			continue // an earlier fold, before both verdicts had landed
		}
		assert.Equal(t, domain.ConditionFalse, h.Status)
		assert.Equal(t, 2, h.TotalKinds)
		assert.Equal(t, 1, h.UnhealthyKinds)
		require.Len(t, h.UnhealthyKindRefs, 1, "the verdict must name the kind behind it")
		assert.Equal(t, "widgets", h.UnhealthyKindRefs[0].Resource)
		assert.Equal(t, "example.com/v1", h.UnhealthyKindRefs[0].APIVersion,
			"with its api group, since the plural alone doesn't identify a kind")
		return
	}
}

// A kind whose verdict nobody has observed yet keeps the whole cache out of Watching. The
// end-to-end half of TestSyncHealthVerdict: a sync record exists but carries no Synced
// condition, which is what every kind looks like between its record being created and its
// worker's first report — and, after a restart, what every kind looks like at once.
func TestServiceWatchCacheSyncHealthWaitsForEveryKind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _, syncCC, _ := newServiceTestSync(t)
	id := seedCluster(t, s, "alpha")

	cacheID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")
	discoveryID := seedGVRDiscovery(t, s, cacheID)
	reported := seedGVRSync(t, s, discoveryID, "apps/v1", "deployments")
	silent := seedGVRSync(t, s, discoveryID, "v1", "pods")

	// One kind reports healthy; the other has not reported at all.
	require.NoError(t, syncCC.SetConditions(ctx, reported, []domain.Condition{
		domain.LiveCondition(domain.ConditionSynced, domain.ConditionTrue, domain.ReasonWatching, ""),
	}))

	st, err := s.Caches().WatchSyncHealth(ctx)
	require.NoError(t, err)
	ch := st.Frames

	deadline := time.After(3 * time.Second)
	for {
		h := recvBy(t, ch, deadline)
		if h.CacheID != domain.ClusterCacheID(cacheID) || h.TotalKinds != 2 {
			continue // an earlier fold, before both records had landed
		}
		require.Equal(t, domain.ConditionUnknown, h.Status,
			"one unobserved kind must keep the cache out of a healthy verdict")
		assert.Equal(t, domain.ReasonSyncing, h.Reason)
		break
	}

	// Once the last kind reports, the cache is healthy.
	require.NoError(t, syncCC.SetConditions(ctx, silent, []domain.Condition{
		domain.LiveCondition(domain.ConditionSynced, domain.ConditionTrue, domain.ReasonWatching, ""),
	}))
	for {
		h := recvBy(t, ch, deadline)
		if h.CacheID != domain.ClusterCacheID(cacheID) || h.Reason != domain.ReasonWatching {
			continue
		}
		assert.Equal(t, domain.ConditionTrue, h.Status)
		assert.Equal(t, 2, h.TotalKinds)
		return
	}
}

// A cache whose kinds are all healthy reports healthy, and one whose discovery has not
// landed reports Unknown rather than either verdict — nobody has observed anything yet.
func TestServiceWatchCacheSyncHealthHealthyAndUnknown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _, syncCC, _ := newServiceTestSync(t)
	id := seedCluster(t, s, "alpha")

	cacheID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")
	discoveryID := seedGVRDiscovery(t, s, cacheID)

	st, err := s.Caches().WatchSyncHealth(ctx)
	require.NoError(t, err)
	ch := st.Frames

	first := recvBy(t, ch, time.After(3*time.Second))
	assert.Equal(t, domain.ConditionUnknown, first.Status, "no kinds yet is neither healthy nor broken")
	assert.Zero(t, first.TotalKinds)

	only := seedGVRSync(t, s, discoveryID, "apps/v1", "deployments")
	require.NoError(t, syncCC.SetConditions(ctx, only, []domain.Condition{
		domain.LiveCondition(domain.ConditionSynced, domain.ConditionTrue, domain.ReasonWatching, ""),
	}))

	deadline := time.After(3 * time.Second)
	for {
		h := recvBy(t, ch, deadline)
		if h.Status != domain.ConditionTrue {
			continue
		}
		assert.Equal(t, domain.ReasonWatching, h.Reason)
		assert.Equal(t, 1, h.TotalKinds)
		assert.Zero(t, h.UnhealthyKinds)
		return
	}
}

// TestServiceWatchCacheSyncHealthSharesOneFold pins that the fold is process-wide, not
// per-subscriber. Every window computes the same verdict from the same two watches, so a
// second subscriber must attach to the running fold rather than start its own — otherwise
// each open window costs two more beehive watches, another ticker, another copy of every
// per-kind record, and another acquisition of the sync controller's writeMu per flush.
func TestServiceWatchCacheSyncHealthSharesOneFold(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _, syncCC, _ := newServiceTestSync(t)
	id := seedCluster(t, s, "alpha")
	cacheID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")
	discoveryID := seedGVRDiscovery(t, s, cacheID)
	only := seedGVRSync(t, s, discoveryID, "apps/v1", "deployments")
	require.NoError(t, syncCC.SetConditions(ctx, only, []domain.Condition{
		domain.LiveCondition(domain.ConditionSynced, domain.ConditionTrue, domain.ReasonWatching, ""),
	}))

	firstSt, err := s.Caches().WatchSyncHealth(ctx)
	require.NoError(t, err)
	first := firstSt.Frames
	awaitSyncHealth(t, first, domain.ClusterCacheID(cacheID), domain.ReasonWatching)

	hub := s.syncHealth
	require.NotNil(t, hub, "the first subscriber starts the fold")

	// A second subscriber must reuse it AND be served the current verdict at once — a
	// settled cache emits nothing, so a window that only saw future frames would render
	// "not reported yet" forever.
	secondSt, err := s.Caches().WatchSyncHealth(ctx)
	require.NoError(t, err)
	second := secondSt.Frames
	awaitSyncHealth(t, second, domain.ClusterCacheID(cacheID), domain.ReasonWatching)
	assert.Same(t, hub, s.syncHealth, "a second subscriber must not start a second fold")
}

// TestServiceWatchCacheSyncHealthClosesOnShutdown pins the teardown. The fold outlives
// every subscriber, so nothing a subscriber does can end it — only the service can, and
// when it does every open stream has to close rather than hang on a hub nobody will
// publish to again.
func TestServiceWatchCacheSyncHealthClosesOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _, _, _ := newServiceTestSync(t)
	id := seedCluster(t, s, "alpha")
	cacheID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")
	seedGVRDiscovery(t, s, cacheID)

	st, err := s.Caches().WatchSyncHealth(ctx)
	require.NoError(t, err)
	ch := st.Frames
	awaitSyncHealth(t, ch, domain.ClusterCacheID(cacheID), domain.ReasonSyncing) // no kinds yet

	s.stopSyncHealthFold(context.Background())

	require.Eventually(t, func() bool {
		select {
		case _, ok := <-ch:
			return !ok
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "shutting the fold down must close its subscribers")

	assert.NoError(t, st.Err(), "an orderly shutdown is not a fault")
}

// The verdict is a gauge, but a failable one — the fold reads two beehive watches of its
// own, and they die for the same reasons their sibling delta watches do. A subscriber
// whose receiver just closed must be able to tell that from an orderly shutdown, or the
// always-mounted stream reconnect-loops with nothing shown.
//
// The reason is planted in the fold's slot rather than provoked from a real watch:
// beehive's streamFail is unexported, so a fake source cannot fail. What is under test is
// the half that is ours — slot to subscriber. recordFoldWatchEnd covers the other half.
func TestServiceWatchCacheSyncHealthReportsAFoldWatchThatDied(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _, _, _ := newServiceTestSync(t)
	id := seedCluster(t, s, "alpha")
	cacheID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")
	seedGVRDiscovery(t, s, cacheID)

	st, err := s.Caches().WatchSyncHealth(ctx)
	require.NoError(t, err)
	awaitSyncHealth(t, st.Frames, domain.ClusterCacheID(cacheID), domain.ReasonSyncing)

	boom := errors.New("sync-health ClusterCacheGVRSync watch ended: watch too old")
	s.syncHealthMu.Lock()
	s.syncHealthErr.Store(&boom)
	s.syncHealthMu.Unlock()
	s.stopSyncHealthFold(context.Background())

	testutil.WaitClosed(t, st.Frames, "the subscriber's stream once the fold ended")
	assert.Equal(t, boom, st.Err(), "a dead fold watch must reach the subscriber as a reason")
}

// The guard on the way in: only a real fault is recorded. A nil Err, or the fold's own
// context ending, is an orderly stop — recording either would report every shutdown as a
// broken watch.
func TestRecordFoldWatchEndOnlyRecordsARealFault(t *testing.T) {
	live := context.Background()
	dead, cancel := context.WithCancel(context.Background())
	cancel()

	var slot atomic.Pointer[error]
	recordFoldWatchEnd(live, &slot, "ClusterCacheGVRSync", nil)
	assert.Nil(t, slot.Load(), "a clean end is not a fault")

	recordFoldWatchEnd(dead, &slot, "ClusterCacheGVRSync", errors.New("watch too old"))
	assert.Nil(t, slot.Load(), "our own teardown races the source's; that is not a fault")

	recordFoldWatchEnd(live, &slot, "ClusterCacheGVRSync", errors.New("watch too old"))
	require.NotNil(t, slot.Load())
	assert.Contains(t, (*slot.Load()).Error(), "ClusterCacheGVRSync",
		"the reason must name which of the fold's two watches died")
}

// Stopping the fold JOINS it. Cancelling alone only asks it to stop, and the two
// fleet-wide WatchList leases it holds come back in its defers — so a stop that returned
// early let beehive begin draining while the fold was still running, the interleaving
// "the fold goes first" exists to prevent.
func TestServiceStopSyncHealthFoldWaitsForIt(t *testing.T) {
	s, coreCC, _, _, _ := newServiceTestSync(t)
	id := seedCluster(t, s, "alpha")
	cacheID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")
	seedGVRDiscovery(t, s, cacheID)

	// A fold that takes a moment to unwind. Whether the stop waits is otherwise decided by
	// goroutine scheduling, which would make either answer look right often enough.
	// Latency injected INTO the code under test, not the test waiting before it asserts —
	// the one shape no-magic-sleeps permits (root CLAUDE.md).
	var unwound atomic.Bool
	s.syncHealthFoldExit = func() {
		time.Sleep(50 * time.Millisecond)
		unwound.Store(true)
	}

	_, _, err := s.syncHealthReceiver()
	require.NoError(t, err)

	s.stopSyncHealthFold(context.Background())
	require.True(t, unwound.Load(), "stop returned while the fold was still unwinding")
}
