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
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/watch"
)

func TestExtractEventReadsTheCoreSpelling(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata":       map[string]any{"uid": "ev-1"},
		"involvedObject": map[string]any{"uid": "pod-1", "kind": "Pod", "namespace": "prod", "name": "api-0"},
		"type":           "Warning", "reason": "BackOff", "message": "restarting",
		"count":          int64(3),
		"firstTimestamp": "2026-08-01T00:00:00Z",
		"lastTimestamp":  "2026-08-01T00:05:00Z",
	})

	row, err := extractEvent(u)

	require.NoError(t, err)
	assert.Equal(t, "ev-1", row.UID)
	assert.Equal(t, "pod-1", row.InvolvedUID)
	assert.Equal(t, "Pod", row.InvolvedKind)
	assert.Equal(t, "prod", row.InvolvedNS)
	assert.Equal(t, "api-0", row.InvolvedName)
	assert.Equal(t, "Warning", row.Type)
	assert.Equal(t, "restarting", row.Message)
	assert.Equal(t, 3, row.Count)
	assert.Equal(t, time.Date(2026, 8, 1, 0, 5, 0, 0, time.UTC).UnixMilli(), row.LastSeen)
}

// The events.k8s.io spelling of the same data: `regarding`, `note`, `series.count`.
func TestExtractEventReadsTheNewSpelling(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "events.k8s.io/v1", "kind": "Event",
		"metadata":  map[string]any{"uid": "ev-2"},
		"regarding": map[string]any{"uid": "pod-2", "kind": "Pod", "name": "api-1"},
		"note":      "pulled image",
		"series":    map[string]any{"count": int64(5), "lastObservedTime": "2026-08-01T00:09:00Z"},
	})

	row, err := extractEvent(u)

	require.NoError(t, err)
	assert.Equal(t, "pod-2", row.InvolvedUID)
	assert.Equal(t, "pulled image", row.Message)
	assert.Equal(t, 5, row.Count)
	assert.Equal(t, time.Date(2026, 8, 1, 0, 9, 0, 0, time.UTC).UnixMilli(), row.LastSeen)
}

// Which field group is present decides the spelling, never the UID: involvedObject.uid
// is optional, so a missing one must not be read as the `regarding` shape.
func TestExtractEventPrefersTheGroupThatIsPresent(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata":       map[string]any{"uid": "ev-3"},
		"involvedObject": map[string]any{"kind": "Pod", "name": "api-2"},
		"regarding":      map[string]any{"uid": "wrong", "name": "wrong"},
	})

	row, err := extractEvent(u)

	require.NoError(t, err)
	assert.Empty(t, row.InvolvedUID)
	assert.Equal(t, "api-2", row.InvolvedName)
}

// The first spelling PRESENT wins, so a genuine 0 is not read as absent.
func TestExtractEventKeepsAnExplicitZeroCount(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"uid": "ev-4"},
		"count":    int64(0),
		"series":   map[string]any{"count": int64(9)},
	})

	row, err := extractEvent(u)

	require.NoError(t, err)
	assert.Equal(t, 0, row.Count)
}

// An event carrying no time at all still needs a count; nothing else defaults it.
func TestExtractEventDefaultsTheCountToOne(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"uid": "ev-5"},
	})

	row, err := extractEvent(u)

	require.NoError(t, err)
	assert.Equal(t, 1, row.Count)
}

// Advancing past an unparseable — not merely absent — time is load-bearing: a
// malformed series.lastObservedTime would otherwise store a null last_seen and sort
// the cluster's newest event below every timestamped row.
func TestExtractEventSkipsAnUnparseableTime(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata":  map[string]any{"uid": "ev-6"},
		"series":    map[string]any{"lastObservedTime": "not-a-time"},
		"eventTime": "2026-08-01T01:00:00Z",
	})

	row, err := extractEvent(u)

	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC).UnixMilli(), row.LastSeen)
	// first_seen falls back to last_seen rather than reading as 1970.
	assert.Equal(t, row.LastSeen, row.FirstSeen)
}

func TestExtractEventRefusesAnEventWithNoUID(t *testing.T) {
	_, err := extractEvent(obj(map[string]any{"apiVersion": "v1", "kind": "Event"}))

	assert.Error(t, err)
}

// An event's body is stored like any other: server noise stripped. Events are the
// highest-volume table, so managedFields kept there is the most bytes nothing reads —
// and a body shaped unlike every other kind's is one a reader has to special-case.
func TestExtractEventStripsServerNoise(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{
			"uid": "ev-1", "name": "api-0.17f",
			"managedFields": []any{map[string]any{"manager": "kube-controller-manager"}},
			"annotations": map[string]any{
				"kubectl.kubernetes.io/last-applied-configuration": "{}",
			},
		},
		"involvedObject": map[string]any{"kind": "Pod", "name": "api-0"},
		"reason":         "BackOff",
	})

	row, err := extractEvent(u)

	require.NoError(t, err)
	var stored map[string]any
	require.NoError(t, json.Unmarshal(row.RawJSON, &stored))
	meta := stored["metadata"].(map[string]any)
	assert.NotContains(t, meta, "managedFields")
	assert.NotContains(t, meta, "annotations")
}

// A nil body would panic on the projection, taking the worker's goroutine with it.
func TestExtractEventRefusesAnEmptyBody(t *testing.T) {
	_, err := extractEvent(nil)

	assert.ErrorIs(t, err, errUnprojectable)
}

// An event carrying a value JSON cannot represent is skipped like any other unprojectable
// body — events are the highest-volume stream, and one bad body must not stop the rest.
func TestExtractEventRefusesABodyThatWillNotMarshal(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"uid": "ev-1"},
		"count":    math.NaN(),
	})

	_, err := extractEvent(u)

	assert.ErrorIs(t, err, errUnprojectable)
}

// An event with no times stores NULL rather than the epoch, so "never seen" is absence
// and not a 1970 instant sorting to the bottom of every window.
func TestAnEventWithNoTimesStoresNull(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	u := obj(map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata":       map[string]any{"uid": "ev-1"},
		"involvedObject": map[string]any{"uid": "pod-1"},
		"reason":         "BackOff",
	})

	require.NoError(t, s.ApplyChange(ctx, eventsKind, watch.Added, u))

	assert.Equal(t, 1, countRows(t, s,
		`SELECT COUNT(*) FROM events WHERE first_seen IS NULL AND last_seen IS NULL`))
}
