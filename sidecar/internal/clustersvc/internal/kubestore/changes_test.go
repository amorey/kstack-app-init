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

package kubestore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// uids is what a changes read is asserted on: which rows moved, in the order they came.
func uids(rows []ObjectRow) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.UID)
	}
	return out
}

// The whole point of the stamp: a reader asks for what moved past its cursor instead of
// re-reading the collection, so a write to one row of a hundred returns one row.
func TestObjectChangesReturnsOnlyWhatMovedPastTheCursor(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow}, true, 7))
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "42")))
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-2", "api-1", "42")))
	at := storeHead(t, s)

	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Modified, pod("uid-2", "api-1", "43")))

	got, err := s.ObjectChanges(ctx, "v1", "pods", at)
	require.NoError(t, err)
	assert.Equal(t, []string{"uid-2"}, uids(got.Written))
	assert.Equal(t, storeHead(t, s), got.At.Seq, "the read is current at the head it returns")
	assert.True(t, got.KindResolved)
}

// A cursor at the head has nothing above it. The reader takes that answer and sends no
// frames, which is what a ping for a kind that did not move costs.
func TestObjectChangesAtTheHeadReturnsNothing(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow}, true, 7))
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "42")))

	got, err := s.ObjectChanges(ctx, "v1", "pods", storeHead(t, s))

	require.NoError(t, err)
	assert.Empty(t, got.Written)
	assert.Empty(t, got.Deleted)
}

// The rows a relist wrote and the deletes it pruned are one transaction at one position, so
// a reader whose cursor sits below it takes both halves of that relist together.
func TestObjectChangesCarriesBothHalvesOfARelist(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow}, true, 7))
	first := beginReplace(t, s, podKind)
	require.NoError(t, first.WritePage(ctx, []*unstructured.Unstructured{
		pod("uid-1", "api-0", "42"), pod("uid-2", "api-1", "42"),
	}))
	_, err := first.Commit(ctx, "100")
	require.NoError(t, err)
	at := storeHead(t, s)

	// uid-2 is not in the second list, so it is pruned; uid-3 is new.
	second := beginReplace(t, s, podKind)
	require.NoError(t, second.WritePage(ctx, []*unstructured.Unstructured{
		pod("uid-1", "api-0", "42"), pod("uid-3", "api-2", "43"),
	}))
	_, err = second.Commit(ctx, "101")
	require.NoError(t, err)

	got, err := s.ObjectChanges(ctx, "v1", "pods", at)
	require.NoError(t, err)
	assert.Equal(t, []string{"uid-3"}, uids(got.Written), "uid-1 was rewritten unchanged")
	assert.Equal(t, []string{"uid-2"}, got.Deleted)
}

// The mark rides the read, out of the same transaction as the rows: a reader compares it
// against the cursor it came in with to know whether the deletes above that cursor are all
// still in the log, and a mark read separately could be raised between the two.
func TestObjectChangesCarriesTheKindsTrimMark(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow}, true, 7))
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "42")))
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Deleted, pod("uid-1", "api-0", "42")))
	// Everything in the log is older than an hour from now, so the trim takes it all.
	require.NoError(t, trimDeletes(ctx, openFileOf(t, s), time.Now().Add(time.Hour).UnixMilli()))

	got, err := s.ObjectChanges(ctx, "v1", "pods", 0)

	require.NoError(t, err)
	assert.Equal(t, trimmedMark(t, s, "Pod"), got.Trimmed)
	assert.Positive(t, got.Trimmed)
}

// A kind whose catalog row is gone must report that, not read as an empty kind. The rows
// are keyed by Kind and the caller names the plural, so an unresolved name would match
// nothing in either range and the reader would be told nothing moved — over a kind whose
// rows it should drop.
func TestObjectChangesReportsAKindItCannotResolve(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	got, err := s.ObjectChanges(ctx, "v1", "pods", 0)

	require.NoError(t, err)
	assert.False(t, got.KindResolved)
}

// Events are one collection with one cursor, and their deletes are logged under the fixed
// ('v1', 'Event') every event delete uses.
func TestEventChangesReturnsWhatMovedAndWhatWent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.ApplyChange(ctx, eventsKind, watch.Added, firing("ev-1", "10", 1)))
	require.NoError(t, s.ApplyChange(ctx, eventsKind, watch.Added, firing("ev-2", "10", 1)))
	at := storeHead(t, s)

	require.NoError(t, s.ApplyChange(ctx, eventsKind, watch.Modified, firing("ev-1", "11", 2)))
	require.NoError(t, s.ApplyChange(ctx, eventsKind, watch.Deleted, firing("ev-2", "10", 1)))

	got, err := s.EventChanges(ctx, at)
	require.NoError(t, err)
	require.Len(t, got.Written, 1)
	assert.Equal(t, "ev-1", got.Written[0].UID)
	assert.Equal(t, []string{"ev-2"}, got.Deleted)
	assert.Equal(t, storeHead(t, s), got.At.Seq)
	assert.True(t, got.KindResolved, "the events collection is always addressable")
}
