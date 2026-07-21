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

package engine

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// A Secret's raw_json must never persist plaintext — neither in .data/.stringData
// nor in the kubectl last-applied annotation, which holds the full applied
// manifest (data and all) for Secrets created with `kubectl apply`.
func TestExtractObjectRedactsSecret(t *testing.T) {
	secretValue := "c3VwZXItc2VjcmV0" // base64("super-secret")
	lastApplied := `{"apiVersion":"v1","kind":"Secret","data":{"password":"` + secretValue + `"}}`

	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":      "creds",
			"namespace": "default",
			"uid":       "uid-1",
			"annotations": map[string]any{
				lastAppliedAnnotation: lastApplied,
				"unrelated":           "keep-me",
			},
		},
		"data":       map[string]any{"password": secretValue},
		"stringData": map[string]any{"token": "plain-token"},
	}}

	row, isEvent, err := extractObject(u)
	require.NoError(t, err)
	require.False(t, isEvent)

	// Plaintext must not survive anywhere in the persisted JSON (json.Marshal
	// escapes "<redacted>" as <redacted>, so assert structurally).
	require.NotContains(t, string(row.RawJSON), secretValue, "secret value leaked")
	require.NotContains(t, string(row.RawJSON), "plain-token", "stringData value leaked")

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(row.RawJSON, &decoded))
	data := decoded["data"].(map[string]any)
	require.Equal(t, "<redacted>", data["password"])
	_, hasKey := data["password"]
	require.True(t, hasKey, "data keys preserved (only values redacted)")
	require.Equal(t, "<redacted>", decoded["stringData"].(map[string]any)["token"])

	// The last-applied manifest (which for a Secret holds the full applied body) is
	// dropped outright by stripServerNoise, not merely placeholdered.
	anns := decoded["metadata"].(map[string]any)["annotations"].(map[string]any)
	_, hasLastApplied := anns[lastAppliedAnnotation]
	require.False(t, hasLastApplied, "last-applied manifest stripped")
	require.Equal(t, "keep-me", anns["unrelated"], "unrelated annotations preserved")

	// The caller's in-memory object must be untouched — we only redact the
	// persisted copy.
	require.Equal(t, secretValue, u.Object["data"].(map[string]any)["password"])
	origAnns := u.Object["metadata"].(map[string]any)["annotations"].(map[string]any)
	require.Equal(t, lastApplied, origAnns[lastAppliedAnnotation], "source annotation not mutated")
}

// A Secret with no kubectl annotation still gets .data/.stringData scrubbed and
// shares its metadata untouched (no needless copy).
func TestExtractObjectRedactsSecretNoAnnotation(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]any{"name": "creds", "uid": "uid-2"},
		"data":       map[string]any{"key": "dmFsdWU="},
	}}

	row, _, err := extractObject(u)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(row.RawJSON, &decoded))
	require.Equal(t, "<redacted>", decoded["data"].(map[string]any)["key"])
	require.False(t, strings.Contains(string(row.RawJSON), "dmFsdWU="))
}

// Every object's raw_json must drop server-side bookkeeping a monitoring UI never
// surfaces: metadata.managedFields (server-side-apply ownership) and the kubectl
// last-applied-configuration annotation (a full duplicate of the applied manifest).
// This runs for all kinds, not just Secrets, and roughly halves stored bytes.
func TestExtractObjectStripsServerNoise(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      "web",
			"namespace": "default",
			"uid":       "uid-dep",
			"managedFields": []any{
				map[string]any{"manager": "kubectl", "operation": "Apply"},
			},
			"annotations": map[string]any{
				lastAppliedAnnotation: `{"apiVersion":"apps/v1","kind":"Deployment"}`,
				"team":                "platform",
			},
		},
		"spec": map[string]any{"replicas": int64(3)},
	}}

	row, isEvent, err := extractObject(u)
	require.NoError(t, err)
	require.False(t, isEvent)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(row.RawJSON, &decoded))
	meta := decoded["metadata"].(map[string]any)
	_, hasManaged := meta["managedFields"]
	require.False(t, hasManaged, "managedFields stripped")

	anns := meta["annotations"].(map[string]any)
	_, hasLastApplied := anns[lastAppliedAnnotation]
	require.False(t, hasLastApplied, "last-applied annotation stripped")
	require.Equal(t, "platform", anns["team"], "unrelated annotations preserved")

	// Non-metadata fields survive untouched.
	require.Equal(t, "web", meta["name"])
	require.Equal(t, float64(3), decoded["spec"].(map[string]any)["replicas"])

	// The caller's in-memory object must be untouched — we only strip the copy.
	origMeta := u.Object["metadata"].(map[string]any)
	require.Contains(t, origMeta, "managedFields", "source managedFields not mutated")
	origAnns := origMeta["annotations"].(map[string]any)
	require.Contains(t, origAnns, lastAppliedAnnotation, "source annotation not mutated")
}

// managedFields is stripped even when the object carries no annotations map (no
// nil-map panic on the annotations path).
func TestExtractObjectStripsManagedFieldsNoAnnotations(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":          "cfg",
			"uid":           "uid-cm",
			"managedFields": []any{map[string]any{"manager": "kube-controller-manager"}},
		},
	}}

	row, _, err := extractObject(u)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(row.RawJSON, &decoded))
	_, hasManaged := decoded["metadata"].(map[string]any)["managedFields"]
	require.False(t, hasManaged, "managedFields stripped")
}

// extractEvent must read count from whichever spelling is actually present
// rather than treating a genuine 0 as "field absent", and default to 1 only
// when no spelling carries the field at all.
func TestExtractEventCount(t *testing.T) {
	event := func(extra map[string]any) *unstructured.Unstructured {
		obj := map[string]any{
			"apiVersion": "v1",
			"kind":       "Event",
			"metadata":   map[string]any{"name": "evt", "uid": "evt-uid"},
		}
		for k, v := range extra {
			obj[k] = v
		}
		return &unstructured.Unstructured{Object: obj}
	}

	cases := []struct {
		name string
		obj  map[string]any
		want int
	}{
		{"core/v1 count present", map[string]any{"count": int64(5)}, 5},
		{"core/v1 count zero preserved", map[string]any{"count": int64(0)}, 0},
		{"no count field defaults to 1", map[string]any{}, 1},
		{"events.k8s.io series.count", map[string]any{"series": map[string]any{"count": int64(3)}}, 3},
		{"deprecatedCount fallback", map[string]any{"deprecatedCount": int64(7)}, 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row, err := extractEvent(event(tc.obj))
			require.NoError(t, err)
			require.Equal(t, tc.want, row.Count)
		})
	}
}

// extractEvent must read involved-object identity from the field group that is
// actually present. involvedObject.uid is optional, so a name-only core/v1
// reference must keep its kind/namespace/name and not be mistaken for an
// events.k8s.io object (whose `regarding` field is absent and would clobber it).
func TestExtractEventInvolvedObject(t *testing.T) {
	t.Run("core/v1 name-only reference is preserved", func(t *testing.T) {
		u := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Event",
			"metadata":   map[string]any{"name": "evt", "uid": "evt-uid"},
			"involvedObject": map[string]any{
				"kind":      "Pod",
				"namespace": "default",
				"name":      "mypod",
				// no uid — valid; ObjectReference.uid is optional
			},
		}}
		row, err := extractEvent(u)
		require.NoError(t, err)
		require.Empty(t, row.InvolvedUID)
		require.Equal(t, "Pod", row.InvolvedKind)
		require.Equal(t, "default", row.InvolvedNS)
		require.Equal(t, "mypod", row.InvolvedName)
	})

	t.Run("events.k8s.io regarding is read", func(t *testing.T) {
		u := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "events.k8s.io/v1",
			"kind":       "Event",
			"metadata":   map[string]any{"name": "evt", "uid": "evt-uid"},
			"regarding": map[string]any{
				"uid":       "pod-uid",
				"kind":      "Pod",
				"namespace": "default",
				"name":      "mypod",
			},
		}}
		row, err := extractEvent(u)
		require.NoError(t, err)
		require.Equal(t, "pod-uid", row.InvolvedUID)
		require.Equal(t, "Pod", row.InvolvedKind)
		require.Equal(t, "default", row.InvolvedNS)
		require.Equal(t, "mypod", row.InvolvedName)
	})
}

func cond(t, status, reason, lastTransitionTime string) map[string]any {
	m := map[string]any{"type": t, "status": status}
	if reason != "" {
		m["reason"] = reason
	}
	if lastTransitionTime != "" {
		m["lastTransitionTime"] = lastTransitionTime
	}
	return m
}

func crdWithConditions(conds ...map[string]any) *unstructured.Unstructured {
	slice := make([]any, len(conds))
	for i, c := range conds {
		slice[i] = c
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"conditions": slice},
	}}
}

func TestStatusFromConditions(t *testing.T) {
	t.Run("most-recently-true condition wins", func(t *testing.T) {
		u := crdWithConditions(
			cond("Ready", "True", "", "2021-01-01T00:00:00Z"),
			cond("Synced", "True", "", "2021-02-01T00:00:00Z"),
		)
		require.Equal(t, "Synced", statusFromConditions(u))
	})

	t.Run("True with no timestamp is still surfaced", func(t *testing.T) {
		// lastTransitionTime omitted: the True condition must not be dropped in
		// favor of a False/empty fallback just because its timestamp is zero.
		u := crdWithConditions(
			cond("Degraded", "False", "AllGood", ""),
			cond("Ready", "True", "", ""),
		)
		require.Equal(t, "Ready", statusFromConditions(u))
	})

	t.Run("True with unparsable timestamp is still surfaced", func(t *testing.T) {
		u := crdWithConditions(cond("Ready", "True", "", "not-a-timestamp"))
		require.Equal(t, "Ready", statusFromConditions(u))
	})

	t.Run("False condition reports type and reason", func(t *testing.T) {
		u := crdWithConditions(cond("Synced", "False", "OutOfSync", "2021-01-01T00:00:00Z"))
		require.Equal(t, "Synced (OutOfSync)", statusFromConditions(u))
	})
}
