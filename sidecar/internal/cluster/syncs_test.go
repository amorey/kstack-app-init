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
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// TestWatchGVRSyncsIsScopedToOneCache pins the scoping that makes this stream usable. A
// cache has one sync record per served kind — a hundred or more — so unlike the other
// object watches this one is opened per cache, and must not leak another cache's kinds
// into it.
// Get is the query entrypoint into one synced kind's record (the GraphQL
// clusterCacheGVRSync field), so it must resolve the owner edge the watch relies on —
// DiscoveryID is what ties the record to its cache. An unknown id is (nil, nil).
func TestSyncsGet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)

	id := seedCluster(t, s, "alpha")
	cacheID := seedActiveCache(t, s, coreCC, id, "uid-alpha")
	discoveryID := seedGVRDiscovery(t, s, cacheID)
	syncID := seedGVRSync(t, s, discoveryID, "apps/v1", "deployments")

	got, err := s.Syncs().Get(ctx, domain.ClusterCacheGVRSyncID(syncID))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, domain.ClusterCacheGVRSyncID(syncID), got.ID)
	assert.EqualValues(t, discoveryID, got.DiscoveryID) // resolved from the owner edge
	assert.Equal(t, "deployments", got.Spec.Resource)

	missing, err := s.Syncs().Get(ctx, domain.ClusterCacheGVRSyncID(syncID+9999))
	require.NoError(t, err)
	assert.Nil(t, missing)
}

// List is the query path down to a cache's per-kind records (the GraphQL
// ClusterCache.syncs field). It is keyed by the CACHE, resolving the discovery
// anchor itself exactly as Watch does — the anchor is one per cache and never a
// caller's argument. The underlying store is fleet-wide, so scoping is the contract.
func TestSyncsList(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)

	mine := seedCluster(t, s, "alpha")
	other := seedCluster(t, s, "beta")
	myCache := seedActiveCache(t, s, coreCC, mine, "uid-alpha")
	otherCache := seedActiveCache(t, s, coreCC, other, "uid-beta")

	myCacheID := domain.ClusterCacheID(myCache)

	// A cache whose discovery pass has never run owns no records — empty, not an error.
	empty, err := s.Syncs().List(ctx, &myCacheID)
	require.NoError(t, err)
	assert.Empty(t, empty, "no anchor yet means no records")

	myDiscovery := seedGVRDiscovery(t, s, myCache)
	otherDiscovery := seedGVRDiscovery(t, s, otherCache)
	seedGVRSync(t, s, myDiscovery, "apps/v1", "deployments")
	seedGVRSync(t, s, myDiscovery, "v1", "pods")
	seedGVRSync(t, s, otherDiscovery, "apps/v1", "deployments")

	got, err := s.Syncs().List(ctx, &myCacheID)
	require.NoError(t, err)
	require.Len(t, got, 2, "this cache's kinds only")

	resources := []string{got[0].Spec.Resource, got[1].Spec.Resource}
	assert.ElementsMatch(t, []string{"deployments", "pods"}, resources)
	for _, gs := range got {
		assert.EqualValues(t, myDiscovery, gs.DiscoveryID) // resolved from the owner edge
	}

	// Unscoped: every cache's records, which is what the plural root field serves.
	all, err := s.Syncs().List(ctx, nil)
	require.NoError(t, err)
	assert.Len(t, all, 3, "this cache's two plus the other cache's one")
}

func TestWatchGVRSyncsIsScopedToOneCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)

	mine := seedCluster(t, s, "alpha")
	other := seedCluster(t, s, "beta")
	myCache := seedActiveCache(t, s, coreCC, mine, "uid-alpha")
	otherCache := seedActiveCache(t, s, coreCC, other, "uid-beta")

	myDiscovery := seedGVRDiscovery(t, s, myCache)
	otherDiscovery := seedGVRDiscovery(t, s, otherCache)
	seedGVRSync(t, s, myDiscovery, "apps/v1", "deployments")
	seedGVRSync(t, s, otherDiscovery, "apps/v1", "deployments")

	ch, err := s.Syncs().Watch(ctx, domain.ClusterCacheID(myCache))
	require.NoError(t, err)

	got := recvGVRSyncFrame(t, ch)
	assert.Equal(t, domain.DeltaFrameAdded, got.Type)
	assert.Equal(t, "deployments", got.Sync.Spec.Resource)
	assert.Equal(t, domain.ClusterCacheGVRDiscoveryID(myDiscovery), got.Sync.DiscoveryID,
		"the record must carry its owning discovery anchor, the key a client joins on")
	bm := recvGVRSyncFrame(t, ch)
	requireBookmark(t, bm.Type, bm.Sync)

	// The other cache's identically-named kind must not arrive.
	select {
	case extra := <-ch:
		t.Fatalf("another cache's sync leaked into the stream: %+v", extra.Sync)
	case <-time.After(300 * time.Millisecond):
	}
}

// A cache that has no discovery anchor yet is the normal state of one just created — it
// gains one within a reconcile. Resolving the anchor once at subscribe latched an empty
// stream for the subscription's whole life, so a sync dialog opened a moment too early
// showed nothing until the user closed and reopened it.

// A cache that has no discovery anchor yet is the normal state of one just created — it
// gains one within a reconcile. Resolving the anchor once at subscribe latched an empty
// stream for the subscription's whole life, so a sync dialog opened a moment too early
// showed nothing until the user closed and reopened it.
func TestWatchGVRSyncsResolvesAnAnchorCreatedAfterSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)

	id := seedCluster(t, s, "alpha")
	cacheID := seedActiveCache(t, s, coreCC, id, "uid-alpha")

	// Subscribe BEFORE the cache has an anchor. Nothing exists yet, so the initial state
	// is empty and its Bookmark arrives at once.
	ch, err := s.Syncs().Watch(ctx, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)
	bm := recvGVRSyncFrame(t, ch)
	requireBookmark(t, bm.Type, bm.Sync)

	discoveryID := seedGVRDiscovery(t, s, cacheID)
	seedGVRSync(t, s, discoveryID, "apps/v1", "deployments")

	got := recvGVRSyncFrame(t, ch)
	assert.Equal(t, domain.DeltaFrameAdded, got.Type)
	assert.Equal(t, "deployments", got.Sync.Spec.Resource)
	assert.Equal(t, domain.ClusterCacheGVRDiscoveryID(discoveryID), got.Sync.DiscoveryID)
}

func recvGVRSyncFrame(t *testing.T, ch <-chan domain.ClusterCacheGVRSyncWatchFrame) domain.ClusterCacheGVRSyncWatchFrame {
	t.Helper()
	return testutil.Recv(t, ch, "a gvr sync frame")
}

// seedGVRDiscovery creates the discovery anchor the cache controller would.
// catalogSubscribe fans two brokers into one channel, so it owns a goroutine and two
// registrations. Closing its output when both brokers close is how a caller learns through
// the ping path that the db went away — the same signal a bare broker subscription gives —
// and it is what lets the caller release the composite rather than dropping it on the floor.

// The per-kind watch is cache-scoped but rides a FLEET-wide stream, so its filter runs on
// every sync record of every cache. While our own anchor is unresolved there is nothing
// cached to compare against, so each of those frames cost its own point query — ~1500
// lookups to drain a ten-cluster snapshot.
//
// One lookup per DISTINCT anchor is enough: a verdict never flips, and an anchor created
// after we looked cannot be one we already rejected (ids are AUTOINCREMENT).
func TestGVRSyncAnchorFilterLooksUpEachAnchorOnce(t *testing.T) {
	var lookups int
	var ours beehive.ObjectID // no anchor yet — the state that made every frame cost one
	keep := gvrSyncAnchorFilter(func() (beehive.ObjectID, error) {
		lookups++
		return ours, nil
	}, func(error) { t.Fatal("no read failed") })

	// Another cache's kinds, streaming past us before we have an anchor of our own.
	for range 50 {
		require.Empty(t, keep(gvrSyncFrameOwnedBy(77)))
	}
	require.Equal(t, 1, lookups, "one lookup decided the whole cache, not one per record")

	// A second cache joins: one more lookup, then memoized too.
	for range 50 {
		require.Empty(t, keep(gvrSyncFrameOwnedBy(78)))
	}
	require.Equal(t, 2, lookups)

	// Ours appears. It is a fresh id, so it cannot collide with anything ruled out.
	ours = 91
	require.Len(t, keep(gvrSyncFrameOwnedBy(91)), 1)
	require.Empty(t, keep(gvrSyncFrameOwnedBy(77)), "a rejected anchor stays rejected")
}

// A failed read is undecidable, not a rejection: memoizing it would drop that cache's
// records for the stream's whole life on the strength of one transient error.
//
// Nor may the frame itself be dropped. beehive re-emits an object only when it changes, so
// a kind whose one frame landed in a "database is locked" moment during cold start would
// stay invisible to this subscription for as long as it lives. The frame is held and
// released once a read can judge it.

// A failed read is undecidable, not a rejection: memoizing it would drop that cache's
// records for the stream's whole life on the strength of one transient error.
//
// Nor may the frame itself be dropped. beehive re-emits an object only when it changes, so
// a kind whose one frame landed in a "database is locked" moment during cold start would
// stay invisible to this subscription for as long as it lives. The frame is held and
// released once a read can judge it.
func TestGVRSyncAnchorFilterHoldsFramesUntilAReadSucceeds(t *testing.T) {
	var fail bool
	var errs int
	keep := gvrSyncAnchorFilter(func() (beehive.ObjectID, error) {
		if fail {
			return 0, errors.New("read failed")
		}
		return 91, nil
	}, func(error) { errs++ })

	fail = true
	require.Empty(t, keep(gvrSyncFrameOwnedBy(91)), "undecidable, so nothing is emitted yet")
	require.Empty(t, keep(gvrSyncFrameOwnedBy(91)))
	require.Equal(t, 2, errs)

	// The read works: the two held frames come out ahead of the one being judged.
	fail = false
	require.Len(t, keep(gvrSyncFrameOwnedBy(91)), 3,
		"frames held during the outage must be released, not lost")

	// Nothing is held twice.
	require.Len(t, keep(gvrSyncFrameOwnedBy(91)), 1)
}

// A frame held during an outage that turns out to belong to ANOTHER cache is dropped when
// it is finally judged — holding it never made it ours.

// A frame held during an outage that turns out to belong to ANOTHER cache is dropped when
// it is finally judged — holding it never made it ours.
func TestGVRSyncAnchorFilterDropsHeldFramesOfOtherCaches(t *testing.T) {
	var fail bool
	keep := gvrSyncAnchorFilter(func() (beehive.ObjectID, error) {
		if fail {
			return 0, errors.New("read failed")
		}
		return 91, nil
	}, func(error) {})

	fail = true
	require.Empty(t, keep(gvrSyncFrameOwnedBy(77)))

	fail = false
	require.Len(t, keep(gvrSyncFrameOwnedBy(91)), 1, "only our own frame comes out")
}

// The release of held frames must not depend on what the NEXT frame happens to be. On a
// multi-cluster fleet almost every frame after the read recovers belongs to an anchor
// already ruled out, and the memo answered those before the drain ran — so a kind whose one
// frame landed in the error window stayed held for the subscription's whole life, invisible
// in the sync panel, because beehive re-emits an object only when it changes.

// The release of held frames must not depend on what the NEXT frame happens to be. On a
// multi-cluster fleet almost every frame after the read recovers belongs to an anchor
// already ruled out, and the memo answered those before the drain ran — so a kind whose one
// frame landed in the error window stayed held for the subscription's whole life, invisible
// in the sync panel, because beehive re-emits an object only when it changes.
func TestGVRSyncAnchorFilterReleasesHeldFramesOnAnAlreadyRejectedAnchor(t *testing.T) {
	var fail bool
	keep := gvrSyncAnchorFilter(func() (beehive.ObjectID, error) {
		if fail {
			return 0, errors.New("read failed")
		}
		return 91, nil
	}, func(error) {})

	// Rule an anchor out while the read works — the steady state the memo exists for.
	require.Empty(t, keep(gvrSyncFrameOwnedBy(77)))

	// Our own kind's only frame lands during an outage.
	fail = true
	require.Empty(t, keep(gvrSyncFrameOwnedBy(91)))

	// The read recovers, but the fleet's next frame is one of the rejected cache's.
	fail = false
	require.Len(t, keep(gvrSyncFrameOwnedBy(77)), 1,
		"the held frame must come out; nothing else will ever ask for it")
	require.Empty(t, keep(gvrSyncFrameOwnedBy(77)), "and only once")
}

// A frame from an already-rejected anchor that arrives DURING an outage is simply dropped:
// it was decided before the read broke, so holding it would only grow the buffer.

// A frame from an already-rejected anchor that arrives DURING an outage is simply dropped:
// it was decided before the read broke, so holding it would only grow the buffer.
func TestGVRSyncAnchorFilterDoesNotHoldFramesItAlreadyRejected(t *testing.T) {
	var fail bool
	keep := gvrSyncAnchorFilter(func() (beehive.ObjectID, error) {
		if fail {
			return 0, errors.New("read failed")
		}
		return 91, nil
	}, func(error) {})

	require.Empty(t, keep(gvrSyncFrameOwnedBy(77)))
	fail = true
	require.Empty(t, keep(gvrSyncFrameOwnedBy(77)))

	fail = false
	require.Empty(t, keep(gvrSyncFrameOwnedBy(77)), "nothing was held, so nothing is released")
}

// A hard Deleted carries no owner edge, so it can't be attributed — forwarded on its id
// alone, which the client keys removal on.

// A hard Deleted carries no owner edge, so it can't be attributed — forwarded on its id
// alone, which the client keys removal on.
func TestGVRSyncAnchorFilterForwardsAnUnattributableDelete(t *testing.T) {
	keep := gvrSyncAnchorFilter(
		func() (beehive.ObjectID, error) { t.Fatal("must not need a lookup"); return 0, nil },
		func(error) { t.Fatal("no read failed") },
	)
	require.Len(t, keep(gvrSyncFrameOwnedBy(0)), 1)
}

// The Bookmark needs no anchor, so with nothing held it passes straight through.
func TestGVRSyncAnchorFilterForwardsTheBookmark(t *testing.T) {
	keep := gvrSyncAnchorFilter(
		func() (beehive.ObjectID, error) { t.Fatal("must not need a lookup"); return 0, nil },
		func(error) { t.Fatal("no read failed") },
	)
	out := keep(domain.ClusterCacheGVRSyncWatchFrame{Type: domain.DeltaFrameBookmark})
	require.Len(t, out, 1)
	require.Equal(t, domain.DeltaFrameBookmark, out[0].Type)
}

// The Bookmark must never overtake frames held during a read outage: it says the initial
// state is complete, which is false while part of that state is still undecided. It queues
// behind them and is released in order.
func TestGVRSyncAnchorFilterHoldsTheBookmarkBehindUndecidedFrames(t *testing.T) {
	var fail bool
	keep := gvrSyncAnchorFilter(func() (beehive.ObjectID, error) {
		if fail {
			return 0, errors.New("read failed")
		}
		return 91, nil
	}, func(error) {})

	fail = true
	require.Empty(t, keep(gvrSyncFrameOwnedBy(91)), "undecidable, so held")
	require.Empty(t, keep(domain.ClusterCacheGVRSyncWatchFrame{Type: domain.DeltaFrameBookmark}),
		"the Bookmark waits behind the snapshot frame it would otherwise close over")

	// The read recovers: the held frame comes out first, its Bookmark behind it.
	fail = false
	out := keep(gvrSyncFrameOwnedBy(91))
	require.Len(t, out, 3)
	require.Equal(t, domain.DeltaFrameAdded, out[0].Type)
	require.Equal(t, domain.DeltaFrameBookmark, out[1].Type)
	require.Equal(t, domain.DeltaFrameAdded, out[2].Type)
}

func gvrSyncFrameOwnedBy(discoveryID beehive.ObjectID) domain.ClusterCacheGVRSyncWatchFrame {
	return domain.ClusterCacheGVRSyncWatchFrame{
		Type: domain.DeltaFrameAdded,
		Sync: &domain.ClusterCacheGVRSync{DiscoveryID: domain.ClusterCacheGVRDiscoveryID(discoveryID)},
	}
}
