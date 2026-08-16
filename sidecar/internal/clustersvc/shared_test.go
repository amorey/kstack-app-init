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
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ObjectID rides the wire as a quoted decimal string, so a client never sees an
// int64 it cannot represent (JSON numbers are float64 in every JS runtime).
func TestObjectIDMarshalsAsAQuotedDecimalString(t *testing.T) {
	var buf bytes.Buffer
	ObjectID(9007199254740993).MarshalGQL(&buf) // 2^53+1: unrepresentable as a float64
	assert.Equal(t, `"9007199254740993"`, buf.String())
}

// UnmarshalGQL accepts every form a client id arrives in: the string it was served
// as, a json.Number from a JSON variable, and the int Go decodes an inline literal
// to. All four must land on the same id.
func TestObjectIDUnmarshalsEveryWireForm(t *testing.T) {
	for _, v := range []any{"42", json.Number("42"), int64(42), 42} {
		var id ObjectID
		require.NoError(t, id.UnmarshalGQL(v), "%T", v)
		assert.Equal(t, ObjectID(42), id, "%T", v)
	}
}

// A malformed id is a client error, not a zero id: silently reading garbage as 0
// would resolve to "not found" instead of saying what was wrong.
func TestObjectIDUnmarshalRejectsMalformedInput(t *testing.T) {
	for _, v := range []any{"not-a-number", "", json.Number("nope"), true, 1.5} {
		var id ObjectID
		assert.Error(t, id.UnmarshalGQL(v), "%#v should not parse", v)
	}
}

// RawJSON marshals its stored bytes verbatim — the sidecar forwards a cached
// object's raw_json as-is, without re-serializing a parsed map — so field order,
// spacing, and nesting are whatever the store holds.
func TestRawJSONMarshalGQLVerbatim(t *testing.T) {
	// A non-canonical body (unsorted keys, extra spacing) must survive byte-for-byte.
	body := `{"kind":"Pod","metadata":{"name":"p","namespace":"ns"},"spec":{"a":1}}`

	var buf bytes.Buffer
	RawJSON(body).MarshalGQL(&buf)

	assert.Equal(t, body, buf.String())
}

// An empty RawJSON is the absent body — it serializes as JSON null, not "" or an
// empty object, so consumers that omitted the resolver-gated field read null.
func TestRawJSONMarshalGQLEmptyIsNull(t *testing.T) {
	var buf bytes.Buffer
	RawJSON("").MarshalGQL(&buf)

	assert.Equal(t, "null", buf.String())
}

// RawJSON is a full round-trip scalar: UnmarshalGQL re-serializes an arbitrary
// decoded value back to JSON so the type also works as an input, and the result
// re-marshals to an equivalent value.
func TestRawJSONUnmarshalGQLRoundTrip(t *testing.T) {
	var r RawJSON
	require.NoError(t, r.UnmarshalGQL(map[string]any{"name": "p", "n": float64(1)}))

	var buf bytes.Buffer
	r.MarshalGQL(&buf)
	assert.JSONEq(t, `{"name":"p","n":1}`, buf.String())
}

// A nil input is the absent value — it unmarshals to the empty RawJSON, which in
// turn marshals back to null.
func TestRawJSONUnmarshalGQLNil(t *testing.T) {
	var r RawJSON
	require.NoError(t, r.UnmarshalGQL(nil))
	assert.Equal(t, RawJSON(""), r)
}

// A value JSON cannot represent is an error rather than a silently empty body,
// which would read downstream as the absent-body case.
func TestRawJSONUnmarshalGQLRejectsUnserializableInput(t *testing.T) {
	var r RawJSON
	assert.Error(t, r.UnmarshalGQL(make(chan int)))
	assert.Equal(t, RawJSON(""), r, "a rejected value must not be stored")
}

// Messages come from unbounded sources (raw client-go errors, kilobyte /readyz
// bodies) and are re-serialized to every watcher on every frame.
func TestTruncateMessageCapsOverlongInput(t *testing.T) {
	assert.Equal(t, "short", TruncateMessage("short"))

	exact := strings.Repeat("x", MaxMessageLen)
	assert.Equal(t, exact, TruncateMessage(exact), "a message at the cap is untouched")

	got := TruncateMessage(strings.Repeat("x", MaxMessageLen+50))
	assert.Equal(t, strings.Repeat("x", MaxMessageLen)+"…", got)
}

// LiveCondition is the sole constructor, which is what makes the message cap and
// the liveness flag unskippable.
func TestLiveConditionCapsItsMessageAndMarksLiveness(t *testing.T) {
	c := LiveCondition(ConditionConnected, ConditionFalse, ReasonProbeFailed, strings.Repeat("x", MaxMessageLen+1))
	assert.Equal(t, string(ConditionConnected), c.Type)
	assert.Equal(t, ConditionFalse, c.Status)
	assert.Equal(t, ReasonProbeFailed, c.Reason)
	assert.True(t, c.Liveness, "every condition here is process-scoped")
	assert.Equal(t, MaxMessageLen+len("…"), len(c.Message))
}

// FindCondition returns a pointer INTO the slice, so a caller can read the live row
// rather than a copy.
func TestFindCondition(t *testing.T) {
	conds := []Condition{
		{Type: string(ConditionConnected), Reason: ReasonConnected},
		{Type: string(ConditionHealthy), Reason: ReasonReady},
	}
	got := FindCondition(conds, ConditionHealthy)
	require.NotNil(t, got)
	assert.Equal(t, ReasonReady, got.Reason)
	assert.Same(t, &conds[1], got)

	assert.Nil(t, FindCondition(conds, ConditionSynced), "an absent type is nil, not a zero row")
	assert.Nil(t, FindCondition(nil, ConditionConnected))
}

// The sync-health fold and the cluster-status comparison both use this to decide
// whether a record actually changed. Comparing by instant, not by pointer or by
// wall-clock struct, is what keeps two readings of the same stamp from looking
// different — a monotonic-clock reading or a differing location would.
func TestTimePtrEqual(t *testing.T) {
	at := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	other := at.In(time.FixedZone("elsewhere", 3600))
	later := at.Add(time.Second)

	assert.True(t, TimePtrEqual(nil, nil), "both absent is equal")
	assert.True(t, TimePtrEqual(&at, &at))
	assert.True(t, TimePtrEqual(&at, &other), "same instant, different location")
	assert.False(t, TimePtrEqual(&at, &later))
	assert.False(t, TimePtrEqual(&at, nil))
	assert.False(t, TimePtrEqual(nil, &at))
}

// --- toOwnerRef ---

// A collected object's outgoing edges go with it, so a departure frame carries no
// owner. The zero ref is what lets that frame reach a consumer at all: an error would
// null the entity, and a change with no entity is dropped rather than folded — so the
// removal would never land and the record would sit on screen for the life of the
// subscription.
func TestToOwnerRefWithNoOwnerIsTheZeroRef(t *testing.T) {
	ctx := context.Background()
	client := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](newTestBeehive(t), ClusterCacheGroupKind)
	_, err := client.Create(ctx, "orphan", ClusterCacheSpec{})
	require.NoError(t, err)

	obj, err := client.GetByName(ctx, "orphan", beehive.LoadOwner())
	require.NoError(t, err)

	ref, err := toOwnerRef(obj)

	require.NoError(t, err)
	assert.Equal(t, ObjectRef{}, ref)
}

// Forgetting beehive.LoadOwner is a caller bug, not a state the store can be in, so it
// stays an error rather than reading as an absent owner.
func TestToOwnerRefReportsAnUnloadedEdge(t *testing.T) {
	_, err := toOwnerRef(&beehive.Object[ClusterCacheSpec, ClusterCacheStatus]{ID: 7, Kind: "ClusterCache"})

	assert.ErrorIs(t, err, beehive.ErrNotLoaded)
}
