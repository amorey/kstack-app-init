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

package objectsync

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

func cond(condType, status, reason, lastTransitionTime string) map[string]any {
	m := map[string]any{"type": condType, "status": status}
	if reason != "" {
		m["reason"] = reason
	}
	if lastTransitionTime != "" {
		m["lastTransitionTime"] = lastTransitionTime
	}
	return m
}

func objWithConditions(kind string, conds ...map[string]any) *unstructured.Unstructured {
	slice := make([]any, len(conds))
	for i, c := range conds {
		slice[i] = c
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "example.io/v1",
		"kind":       kind,
		"status":     map[string]any{"conditions": slice},
	}}
}

// The generic fallback is what covers the long tail — most CRDs (ArgoCD, Cert-Manager,
// Crossplane, Knative) follow the conventional .status.conditions form, and nothing else
// in this file knows their kinds.
func TestStatusFromConditions(t *testing.T) {
	t.Run("most-recently-true condition wins", func(t *testing.T) {
		u := objWithConditions("Application",
			cond("Ready", "True", "", "2021-01-01T00:00:00Z"),
			cond("Synced", "True", "", "2021-02-01T00:00:00Z"),
		)
		assert.Equal(t, "Synced", statusFromConditions(u))
	})

	t.Run("True with no timestamp is still surfaced", func(t *testing.T) {
		// lastTransitionTime omitted: the True condition must not be dropped in favor of
		// a False/empty fallback just because its timestamp is zero.
		u := objWithConditions("Application",
			cond("Degraded", "False", "AllGood", ""),
			cond("Ready", "True", "", ""),
		)
		assert.Equal(t, "Ready", statusFromConditions(u))
	})

	t.Run("True with unparsable timestamp is still surfaced", func(t *testing.T) {
		u := objWithConditions("Application", cond("Ready", "True", "", "not-a-timestamp"))
		assert.Equal(t, "Ready", statusFromConditions(u))
	})

	t.Run("False condition reports type and reason", func(t *testing.T) {
		u := objWithConditions("Application", cond("Synced", "False", "OutOfSync", "2021-01-01T00:00:00Z"))
		assert.Equal(t, "Synced (OutOfSync)", statusFromConditions(u))
	})

	t.Run("an unknown kind routes to the fallback", func(t *testing.T) {
		u := objWithConditions("Application", cond("Ready", "True", "", ""))
		got := extractStatus(u)
		assert.Equal(t, "Ready", got.Summary)
		assert.False(t, got.Ready.Valid, "a count that doesn't apply must stay NULL, not 0")
		assert.False(t, got.Total.Valid)
		assert.False(t, got.Restart.Valid)
		assert.Empty(t, got.Host)
	})
}

// "Pod", "Job" and "Node" are not reserved words. A CRD carrying one of those names in its
// own group is that user's kind, and must reach the conditions fallback rather than have a
// built-in's reader hunt for fields it doesn't have — the same rule the write path's Secret
// check and the controller's isEventsKind follow.
func TestExtractStatusKeysOnGroupNotKindName(t *testing.T) {
	crdPod := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "acme.io/v1",
		"kind":       "Pod",
		"spec":       map[string]any{"nodeName": "not-a-real-node"},
		"status": map[string]any{
			"phase":      "Running",
			"conditions": []any{cond("Ready", "True", "", "")},
		},
	}}

	got := extractStatus(crdPod)

	assert.Equal(t, "Ready", got.Summary, "a CRD named Pod must use the conditions fallback")
	assert.False(t, got.Ready.Valid, "the built-in Pod reader must not have run")
	assert.Empty(t, got.Host, "a CRD's spec.nodeName is not a scheduling host")
}

// The per-kind readings are what make the cache queryable in SQL ("which pods are not
// ready"). Each column is nullable, so a count that doesn't apply to a kind must stay nil
// rather than become a 0 that reads as "none ready".
func TestExtractStatusPerKind(t *testing.T) {
	tests := []struct {
		name    string
		obj     map[string]any
		summary string
		ready   sql.NullInt64
		total   sql.NullInt64
		host    string
	}{
		{
			name: "a running pod counts its ready containers and records its node",
			obj: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"spec":       map[string]any{"nodeName": "node-1"},
				"status": map[string]any{
					"phase": "Running",
					"containerStatuses": []any{
						map[string]any{"ready": true, "restartCount": int64(0)},
						map[string]any{"ready": false, "restartCount": int64(3)},
					},
				},
			},
			summary: "Running", ready: count(1), total: count(2), host: "node-1",
		},
		{
			name: "a deployment reports ready over DESIRED replicas, not observed",
			obj: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"spec":       map[string]any{"replicas": int64(3)},
				"status":     map[string]any{"readyReplicas": int64(2)},
			},
			summary: "Progressing 2/3", ready: count(2), total: count(3),
		},
		{
			name: "a namespace reports its phase and no counts",
			obj: map[string]any{
				"apiVersion": "v1",
				"kind":       "Namespace",
				"status":     map[string]any{"phase": "Terminating"},
			},
			summary: "Terminating",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractStatus(&unstructured.Unstructured{Object: tc.obj})
			assert.Equal(t, tc.summary, got.Summary)
			assert.Equal(t, tc.ready, got.Ready)
			assert.Equal(t, tc.total, got.Total)
			assert.Equal(t, tc.host, got.Host)
		})
	}
}

// The columns are only worth computing if they reach the table — this is the end-to-end
// half, against real SQLite.
func TestApplyWritesMaterializedStatusColumns(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newObjectStore(cdb, deploymentsKind)
	require.NoError(t, st.EnsureCatalog(ctx))

	dep := newDeployment("dep-1", "one", func(obj map[string]any) {
		obj["spec"] = map[string]any{"replicas": int64(3)}
		obj["status"] = map[string]any{"readyReplicas": int64(2)}
	})
	require.NoError(t, st.ApplyChange(ctx, watch.Added, dep))

	var summary sql.NullString
	var ready, total sql.NullInt64
	var host sql.NullString
	require.NoError(t, cdb.Reader().QueryRow(
		`SELECT status_summary, ready_count, total_count, host FROM objects WHERE uid=?`, "dep-1",
	).Scan(&summary, &ready, &total, &host))

	assert.Equal(t, "Progressing 2/3", summary.String)
	assert.Equal(t, int64(2), ready.Int64)
	assert.Equal(t, int64(3), total.Int64)
	assert.False(t, host.Valid, "a Deployment has no node, so host must be NULL not \"\"")

	// The query these columns exist for: answerable without unpacking a body.
	var notReady int
	require.NoError(t, cdb.Reader().QueryRow(
		`SELECT COUNT(*) FROM objects WHERE kind='Deployment' AND ready_count < total_count`,
	).Scan(&notReady))
	assert.Equal(t, 1, notReady)
}
