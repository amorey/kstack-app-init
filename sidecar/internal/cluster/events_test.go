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
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The generic event reader maps beehive's coalesced runs to the domain Event wire
// shape, newest-run-first, honoring the category filter and limit. beehive coalesces
// same-(category,type,reason) occurrences into one run, so a repeated reason bumps
// Count and a changed reason starts a new run.
func TestServiceEventsReadsTimeline(t *testing.T) {
	ctx := context.Background()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")
	oid := beehive.ObjectID(id)

	const cat = "test-timeline"
	// run A (failure), repeated → one run, Count 2
	for range 2 {
		require.NoError(t, coreCC.AddEvent(ctx, oid, beehive.EventSpec{
			Category: cat, Type: beehive.EventWarning, Reason: "ReasonA", Message: "boom",
		}))
	}
	// run B (success) → new run, Count 1
	require.NoError(t, coreCC.AddEvent(ctx, oid, beehive.EventSpec{
		Category: cat, Type: beehive.EventNormal, Reason: "ReasonB",
	}))

	category := cat
	evs, err := s.events(ctx, s.coreClient, oid, &category, nil)
	require.NoError(t, err)
	require.Len(t, evs, 2, "two runs: A coalesced, B new")

	// newest run first
	assert.Equal(t, "ReasonB", evs[0].Reason)
	assert.Equal(t, beehive.EventNormal, evs[0].Type)
	assert.Equal(t, 1, evs[0].Count)

	assert.Equal(t, "ReasonA", evs[1].Reason)
	assert.Equal(t, beehive.EventWarning, evs[1].Type)
	assert.Equal(t, "boom", evs[1].Message)
	assert.Equal(t, 2, evs[1].Count)

	assert.NotEqual(t, evs[0].ID, evs[1].ID, "distinct run ids")
	assert.NotZero(t, evs[0].ID)
	assert.False(t, evs[0].FirstAt.IsZero())
	assert.False(t, evs[0].LastAt.IsZero())

	// category filter: a non-matching category yields no runs
	other := "no-such-category"
	none, err := s.events(ctx, s.coreClient, oid, &other, nil)
	require.NoError(t, err)
	assert.Empty(t, none)

	// limit bounds the read to the newest run
	limit := 1
	limited, err := s.events(ctx, s.coreClient, oid, &category, &limit)
	require.NoError(t, err)
	require.Len(t, limited, 1)
	assert.Equal(t, "ReasonB", limited[0].Reason)
}

// watchEvents streams bare runs (mirroring beehive's WatchEvents): the snapshot replays
// existing runs, a repeated same-outcome occurrence re-delivers the run with a bumped
// count under the same id (the consumer upserts), and a changed reason delivers a fresh
// run with a distinct id.

// watchEvents streams bare runs (mirroring beehive's WatchEvents): the snapshot replays
// existing runs, a repeated same-outcome occurrence re-delivers the run with a bumped
// count under the same id (the consumer upserts), and a changed reason delivers a fresh
// run with a distinct id.
func TestServiceWatchEventsStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")
	oid := beehive.ObjectID(id)

	const cat = "test-watch"
	// one existing run before subscribe → replayed in the snapshot
	require.NoError(t, coreCC.AddEvent(ctx, oid, beehive.EventSpec{
		Category: cat, Type: beehive.EventWarning, Reason: "ReasonA", Message: "boom",
	}))

	category := cat
	ch, err := s.watchEvents(ctx, s.coreClient, oid, &category)
	require.NoError(t, err)

	// snapshot: run A
	e := recv(t, ch)
	assert.Equal(t, "ReasonA", e.Reason)
	assert.Equal(t, beehive.EventWarning, e.Type)
	assert.Equal(t, 1, e.Count)
	runA := e.ID
	assert.NotZero(t, runA)

	// extend run A → re-delivered with the same id, count 2
	require.NoError(t, coreCC.AddEvent(ctx, oid, beehive.EventSpec{
		Category: cat, Type: beehive.EventWarning, Reason: "ReasonA", Message: "boom",
	}))
	e = recv(t, ch)
	assert.Equal(t, runA, e.ID)
	assert.Equal(t, 2, e.Count)

	// changed reason → new run, distinct id
	require.NoError(t, coreCC.AddEvent(ctx, oid, beehive.EventSpec{
		Category: cat, Type: beehive.EventNormal, Reason: "ReasonB",
	}))
	e = recv(t, ch)
	assert.Equal(t, "ReasonB", e.Reason)
	assert.NotEqual(t, runA, e.ID)

	// ctx cancel closes the stream
	cancel()
	assert.Eventually(t, func() bool {
		select {
		case _, ok := <-ch:
			return !ok
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "stream should close on ctx cancel")
}

// The kind-scoped public surface (ClusterEvents / ClusterEventsWatch), reached through
// the ClusterService interface, delegates to the generic reader/watch against the
// Cluster kind client. Asserting via the interface value locks the public API shape.
