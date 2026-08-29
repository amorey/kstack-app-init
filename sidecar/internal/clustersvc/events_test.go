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

package clustersvc

import (
	"context"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// addEvent appends one run to id's timeline from outside a pass, standing in for the
// controller that writes it. Generic over the kind's status because an AdminClient is
// scoped to one kind, even though the read under test is not.
func addEvent[Status any](t *testing.T, admin *beehive.AdminClient[Status], id beehive.ObjectID, category, reason string) {
	t.Helper()
	require.NoError(t, admin.AddEvent(context.Background(), id, beehive.EventSpec{
		Category: category,
		Type:     beehive.EventNormal,
		Reason:   reason,
		Message:  reason + " happened",
	}))
}

// ptr takes the address of a literal, for the two optional arguments under test.
func ptr[T any](v T) *T { return &v }

// reasonsOf reads the machine codes out of a timeline, which is what these tests
// assert on: the run's identity, not its sampled message.
func reasonsOf(events []Event) []string {
	reasons := make([]string, 0, len(events))
	for _, ev := range events {
		reasons = append(reasons, ev.Reason)
	}
	return reasons
}

// One reader serves all three kinds: an event carries no kind of its own, so the read
// is by id and a per-kind branch would be three copies of one call.
func TestListEventsReadsEveryKindsTimeline(t *testing.T) {
	d, bh := newTestDepsAndBeehive(t)
	ctx := context.Background()
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	kind := createKind(t, d, cache.ID, deploymentsSpec)

	addEvent(t, beehive.NewAdminClient[ClusterStatus](bh, ClusterGroupKind),
		cluster.ID, ConnectionEventCategory, "Connected")
	addEvent(t, beehive.NewAdminClient[ClusterCacheStatus](bh, ClusterCacheGroupKind),
		cache.ID, categoryDiscovery, "Discovered")
	addEvent(t, beehive.NewAdminClient[ClusterCachedKindStatus](bh, ClusterCachedKindGroupKind),
		kind.ID, categorySync, "SyncComplete")

	svc := serviceOver(t, d)
	for name, tc := range map[string]struct {
		id       beehive.ObjectID
		category string
		reason   string
	}{
		"cluster":    {cluster.ID, ConnectionEventCategory, "Connected"},
		"cache":      {cache.ID, categoryDiscovery, "Discovered"},
		"cachedKind": {kind.ID, categorySync, "SyncComplete"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := svc.ListEvents(ctx, ObjectID(tc.id), nil, nil)

			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.NotZero(t, got[0].ID, "the run's own id, the client's upsert key")
			assert.Equal(t, tc.category, got[0].Category)
			assert.Equal(t, tc.reason, got[0].Reason)
			assert.Equal(t, beehive.EventNormal, got[0].Type)
			assert.Equal(t, tc.reason+" happened", got[0].Message)
			assert.Equal(t, 1, got[0].Count)
			assert.False(t, got[0].FirstAt.IsZero())
			assert.False(t, got[0].LastAt.IsZero())
		})
	}
}

// The two arguments are beehive options, and neither maps by passing the value
// through: a nil category must add no option at all, since WithEventCategory("")
// selects the default timeline — which nothing in this package ever writes to.
func TestListEventsMapsCategoryAndLimit(t *testing.T) {
	d, bh := newTestDepsAndBeehive(t)
	ctx := context.Background()
	cluster := createCluster(t, d.clusterClient, "prod")
	admin := beehive.NewAdminClient[ClusterStatus](bh, ClusterGroupKind)
	addEvent(t, admin, cluster.ID, ConnectionEventCategory, "Connecting")
	addEvent(t, admin, cluster.ID, ConnectionEventCategory, "Connected")
	addEvent(t, admin, cluster.ID, categoryDiscovery, "Discovered")

	svc := serviceOver(t, d)
	id := ObjectID(cluster.ID)

	t.Run("a nil category reads every timeline", func(t *testing.T) {
		got, err := svc.ListEvents(ctx, id, nil, nil)

		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"Connecting", "Connected", "Discovered"}, reasonsOf(got))
	})

	t.Run("a category selects its own", func(t *testing.T) {
		got, err := svc.ListEvents(ctx, id, ptr(ConnectionEventCategory), nil)

		require.NoError(t, err)
		assert.Equal(t, []string{"Connected", "Connecting"}, reasonsOf(got), "newest run first")
	})

	t.Run("a limit bounds the read", func(t *testing.T) {
		got, err := svc.ListEvents(ctx, id, ptr(ConnectionEventCategory), ptr(1))

		require.NoError(t, err)
		assert.Equal(t, []string{"Connected"}, reasonsOf(got), "the newest run survives the bound")
	})
}

// harnessCategory is a timeline no controller writes. The watch tests run with the
// reconcilers live, so scoping to it keeps every frame the test's own doing.
const harnessCategory = "harness"

// awaitRun takes the next frame and requires it to be a run.
func awaitRun(t *testing.T, stream *Stream[EventWatchFrame], what string) Event {
	t.Helper()
	f := testutil.Recv(t, stream.Frames, what)
	require.Equal(t, EventFrameRun, f.Type)
	require.NotNil(t, f.Event)
	return *f.Event
}

// The snapshot arrives as runs, one Bookmark closes it, and what the log grows by
// follows. The bookmark is what tells an empty timeline from one still arriving.
func TestWatchEventsSnapshotsThenBookmarksThenTails(t *testing.T) {
	d, bh := newRunningRegisteredDeps(t)
	ctx := t.Context()
	cluster := createCluster(t, d.clusterClient, "prod")
	admin := beehive.NewAdminClient[ClusterStatus](bh, ClusterGroupKind)
	addEvent(t, admin, cluster.ID, harnessCategory, "Stored")

	stream, err := serviceOver(t, d).WatchEvents(ctx, ObjectID(cluster.ID), ptr(harnessCategory))

	require.NoError(t, err)
	assert.Equal(t, "Stored", awaitRun(t, stream, "the snapshot").Reason)
	assert.Equal(t, EventFrameBookmark,
		testutil.Recv(t, stream.Frames, "the bookmark").Type, "one bookmark closes the snapshot")

	addEvent(t, admin, cluster.ID, harnessCategory, "Grew")
	assert.Equal(t, "Grew", awaitRun(t, stream, "the tail").Reason)
}

// An empty timeline bookmarks too: without it a client cannot tell "no events" from a
// snapshot still on its way.
func TestWatchEventsBookmarksAnEmptyTimeline(t *testing.T) {
	d, _ := newRunningRegisteredDeps(t)
	cluster := createCluster(t, d.clusterClient, "prod")

	stream, err := serviceOver(t, d).WatchEvents(t.Context(), ObjectID(cluster.ID), ptr(harnessCategory))

	require.NoError(t, err)
	assert.Equal(t, EventFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// A record collected while its timeline watch is open ends the stream CLEANLY. beehive
// files the vanished row as ErrNotFound and its reader treats that as terminal, but the
// deletion is the answer here, not a failure — forwarded, it would put an error in
// front of a user who cleared a cache, once per open kind timeline.
//
// Two cadences are shrunk, and both are load-bearing: GC is what actually collects the
// row, and the watch floor is how the reader notices — only an event WRITE wakes it,
// and collection is not one.
func TestWatchEventsEndsCleanlyWhenTheRecordIsCollected(t *testing.T) {
	d, bh := newRunningRegisteredDeps(t,
		beehive.WithGCInterval(time.Millisecond),
		beehive.WithWatchFloorInterval(time.Millisecond))
	ctx := t.Context()
	cluster := createCluster(t, d.clusterClient, "prod")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")
	kind := createKind(t, d, cache.ID, deploymentsSpec)
	addEvent(t, beehive.NewAdminClient[ClusterCachedKindStatus](bh, ClusterCachedKindGroupKind),
		kind.ID, harnessCategory, "SyncComplete")

	stream, err := serviceOver(t, d).WatchEvents(ctx, ObjectID(kind.ID), ptr(harnessCategory))
	require.NoError(t, err)
	require.Equal(t, "SyncComplete", awaitRun(t, stream, "the snapshot").Reason)

	require.NoError(t, d.kindClient.Delete(context.Background(), kind.ID))

	testutil.WaitClosed(t, stream.Frames, "the watch to end")
	assert.NoError(t, stream.Err(), "the deletion is the answer, not a broken watch")
}

// The other half of the filter, so the carve-out above cannot grow into "a dead watch
// is always fine". A guard rather than a red test: nobody writes this errors.Is from
// "Err is forwarded", and without it the silent-broken-watch bug ships unnoticed.
func TestTerminalErrKeepsTheReasonsAConsumerCanActOn(t *testing.T) {
	assert.NoError(t, terminalErr(beehive.ErrNotFound), "a collected record is a clean end")
	assert.NoError(t, terminalErr(nil))
	assert.ErrorIs(t, terminalErr(beehive.ErrWatchTooOld), beehive.ErrWatchTooOld,
		"runs were lost; a resubscribe is what makes the client correct")
	assert.ErrorIs(t, terminalErr(beehive.ErrStopped), beehive.ErrStopped)
}
