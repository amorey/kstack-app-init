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
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// frame is what the tests fold each change into: the type plus the id, so a
// Bookmark (which carries no object) is distinguishable from an object change.
type frame struct {
	typ domain.DeltaFrameType
	id  beehive.ObjectID
}

// testSpec/testStatus stand in for any kind's shapes — watchListChan is generic over
// them and reads neither.
type (
	testSpec   struct{}
	testStatus struct{}
)

// runWatchList pumps a beehive stream through watchListChan, folding each change to a
// frame. The returned channel is the stream under test.
func runWatchList(
	ctx context.Context,
	snapIDs []beehive.ObjectID,
	src <-chan beehive.ObjectChange[testSpec, testStatus],
) <-chan frame {
	stream := &beehive.ObjectListStream[testSpec, testStatus]{Changes: src}
	for _, id := range snapIDs {
		stream.Objects = append(stream.Objects, &beehive.Object[testSpec, testStatus]{ID: id})
	}
	return watchListChan(ctx, "Test", stream,
		func(t domain.DeltaFrameType, id beehive.ObjectID, _ *beehive.Object[testSpec, testStatus]) frame {
			return frame{typ: t, id: id}
		})
}

// The snapshot replays as Added in order, then exactly one Bookmark, then live
// changes. The Bookmark's position IS the contract — a client renders an empty state
// off it, so a snapshot object arriving after one would land in a set already
// reported complete.
func TestWatchListChanBookmarksBetweenSnapshotAndDeltas(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := make(chan beehive.ObjectChange[testSpec, testStatus])
	out := runWatchList(ctx, []beehive.ObjectID{1, 2}, src)

	assert.Equal(t, frame{domain.DeltaFrameAdded, 1}, recv(t, out))
	assert.Equal(t, frame{domain.DeltaFrameAdded, 2}, recv(t, out))
	assert.Equal(t, frame{typ: domain.DeltaFrameBookmark}, recv(t, out), "the snapshot is closed before any delta")

	src <- beehive.ObjectChange[testSpec, testStatus]{
		Type: beehive.Modified, ID: 2, Object: &beehive.Object[testSpec, testStatus]{ID: 2},
	}
	assert.Equal(t, frame{domain.DeltaFrameModified, 2}, recv(t, out))
}

// An empty collection is the case the Bookmark exists for: with nothing to replay it
// is the stream's first frame, which is what tells a client "empty" rather than
// "not yet".
func TestWatchListChanBookmarksAnEmptySnapshotImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := runWatchList(ctx, nil, make(chan beehive.ObjectChange[testSpec, testStatus]))
	assert.Equal(t, frame{typ: domain.DeltaFrameBookmark}, recv(t, out))
}

// A deletion-pending object is collapsed to Deleted: List/Get hide tombstones, so the
// watch must remove it at once rather than showing a row no read can return.
func TestWatchListChanCollapsesDeletionPendingToDeleted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := make(chan beehive.ObjectChange[testSpec, testStatus])
	out := runWatchList(ctx, nil, src)
	require.Equal(t, frame{typ: domain.DeltaFrameBookmark}, recv(t, out))

	at := time.Now()
	src <- beehive.ObjectChange[testSpec, testStatus]{
		Type: beehive.Modified, ID: 5,
		Object: &beehive.Object[testSpec, testStatus]{ID: 5, DeletionRequestedAt: &at},
	}
	assert.Equal(t, frame{domain.DeltaFrameDeleted, 5}, recv(t, out),
		"a soft-delete reads as Modified upstream but must reach the client as Deleted")
}

// A beehive stream that fails closes its change channel and reports the reason from
// Err(), so the pump ends with it rather than hanging on a source that will never
// deliver again.
func TestWatchListChanEndsWhenTheSourceCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := make(chan beehive.ObjectChange[testSpec, testStatus])
	out := runWatchList(ctx, nil, src)
	require.Equal(t, frame{typ: domain.DeltaFrameBookmark}, recv(t, out))

	close(src)
	testutil.RecvClosed(t, out, "the stream when its source closes")
}

// The stream closes when the subscriber goes away, whatever the source is doing.
func TestWatchListChanClosesOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := runWatchList(ctx, nil, make(chan beehive.ObjectChange[testSpec, testStatus]))
	require.Equal(t, frame{typ: domain.DeltaFrameBookmark}, recv(t, out))

	cancel()
	testutil.RecvClosed(t, out, "the stream on ctx cancel")
}
