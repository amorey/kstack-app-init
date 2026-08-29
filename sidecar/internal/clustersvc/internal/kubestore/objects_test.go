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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// body decodes a projected row's stored JSON back into a map.
func body(t *testing.T, row objectRow) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal(row.RawJSON, &out))
	return out
}

func TestProjectReadsTheIdentityColumns(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"uid": "uid-1", "name": "api-0", "namespace": "prod",
			"resourceVersion": "42", "generation": int64(3),
			"creationTimestamp": "2026-08-01T00:00:00Z",
			"labels":            map[string]any{"app": "api"},
		},
	})

	row, err := projectObject(u)

	require.NoError(t, err)
	assert.Equal(t, "uid-1", row.UID)
	assert.Equal(t, "prod", row.Namespace)
	assert.Equal(t, "api-0", row.Name)
	assert.Equal(t, "42", row.ResourceVersion)
	assert.Equal(t, int64(3), row.Generation)
	assert.NotZero(t, row.CreatedAt)
	assert.Equal(t, map[string]string{"app": "api"}, row.Labels)
}

// managedFields and the kubectl last-applied annotation are roughly half a typical
// object's bytes and nothing reads them — stripping is what lets the stored body be
// served verbatim.
func TestProjectStripsServerNoise(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{
			"uid": "uid-1", "name": "cm",
			"managedFields": []any{map[string]any{"manager": "kubectl"}},
			"annotations": map[string]any{
				"kubectl.kubernetes.io/last-applied-configuration": "{}",
			},
		},
	})

	row, err := projectObject(u)

	require.NoError(t, err)
	meta := body(t, row)["metadata"].(map[string]any)
	assert.NotContains(t, meta, "managedFields")
	// The annotations map emptied by that removal is noise of its own.
	assert.NotContains(t, meta, "annotations")
}

// The cache file must never hold the cluster's credentials. Keys survive so a UI can
// list what a Secret holds.
func TestProjectRedactsSecretValues(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata":   map[string]any{"uid": "uid-1", "name": "creds"},
		"data":       map[string]any{"password": "aHVudGVyMg=="},
		"stringData": map[string]any{"password": "hunter2"},
	})

	row, err := projectObject(u)

	require.NoError(t, err)
	stored := body(t, row)
	assert.Equal(t, map[string]any{"password": redactedValue}, stored["data"])
	assert.NotContains(t, stored, "stringData")
	// The caller's body is the live watch object; projecting must not mutate it.
	assert.Equal(t, "hunter2", u.Object["stringData"].(map[string]any)["password"])
}

// Redaction reads the BODY's own kind, so it cannot be bypassed by how the collection
// was addressed.
func TestProjectRedactsOnlyCoreSecrets(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "example.com/v1", "kind": "Secret",
		"metadata": map[string]any{"uid": "uid-1", "name": "not-a-secret"},
		"data":     map[string]any{"password": "kept"},
	})

	row, err := projectObject(u)

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"password": "kept"}, body(t, row)["data"])
}

func TestProjectKeepsOnlyOwnerRefsThatCarryAUID(t *testing.T) {
	yes := true
	u := obj(map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"uid": "uid-1", "name": "api-0", "ownerReferences": []any{
			map[string]any{"uid": "owner-1", "controller": yes},
			map[string]any{"name": "no-uid"},
		}},
	})

	row, err := projectObject(u)

	require.NoError(t, err)
	assert.Equal(t, []ownerRef{{UID: "owner-1", IsController: true}}, row.OwnerRefs)
}

// A body with no UID is unkeyable: nothing could address the row later, and a delta
// for it would book progress against a row that was never written.
func TestProjectRefusesAnObjectWithNoUID(t *testing.T) {
	_, err := projectObject(obj(map[string]any{"apiVersion": "v1", "kind": "Pod"}))

	assert.Error(t, err)
}

func TestProjectRefusesAnEmptyBody(t *testing.T) {
	_, err := projectObject(&unstructured.Unstructured{})

	assert.Error(t, err)
}

// podWith builds a Pod carrying the given owner references and labels. A nil for either
// is the shape apimachinery hands back for an object that has none.
func podWith(uid string, owners []any, labels map[string]any) *unstructured.Unstructured {
	meta := map[string]any{"uid": uid, "name": "api-0", "namespace": "prod", "resourceVersion": "1"}
	if owners != nil {
		meta["ownerReferences"] = owners
	}
	if labels != nil {
		meta["labels"] = labels
	}
	return obj(map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": meta})
}

// owner is one ownerReferences entry.
func owner(uid string, controller bool) map[string]any {
	return map[string]any{"uid": uid, "kind": "ReplicaSet", "name": "rs", "controller": controller}
}

func TestInsertRoundTripsEveryOwnerRef(t *testing.T) {
	s := newTestStore(t)
	u := podWith("uid-1", []any{owner("o-1", true), owner("o-2", false), owner("o-3", false)}, nil)

	require.NoError(t, s.ApplyChange(context.Background(), podKind, watch.Added, u))

	assert.Equal(t, 3, countRows(t, s, `SELECT COUNT(*) FROM owner_refs WHERE child_uid='uid-1'`))
	assert.Equal(t, 1, countRows(t, s,
		`SELECT is_controller FROM owner_refs WHERE child_uid='uid-1' AND owner_uid='o-1'`))
	assert.Zero(t, countRows(t, s,
		`SELECT is_controller FROM owner_refs WHERE child_uid='uid-1' AND owner_uid='o-2'`))
}

// The insert upserts, so only the DELETE ahead of it can retire an edge the object dropped.
func TestInsertDropsAnOwnerRefTheObjectNoLongerCarries(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added,
		podWith("uid-1", []any{owner("o-1", true), owner("o-2", false)}, nil)))

	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Modified,
		podWith("uid-1", []any{owner("o-1", true)}, nil)))

	assert.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM owner_refs WHERE child_uid='uid-1'`))
}

// The nil guard: OwnerRefs is built by append, so an object with no owner marshals to
// `null`, and json_each('null') yields one all-NULL row — a NOT NULL violation under
// STRICT, inside the relist page's transaction, for every unowned object in the cluster.
func TestInsertTakesAnObjectWithNoOwnerRefs(t *testing.T) {
	s := newTestStore(t)

	require.NoError(t, s.ApplyChange(context.Background(), podKind, watch.Added,
		podWith("uid-1", nil, nil)))

	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM owner_refs WHERE child_uid='uid-1'`))
}

func TestInsertRoundTripsEveryLabel(t *testing.T) {
	s := newTestStore(t)
	u := podWith("uid-1", nil, map[string]any{"app": "api", "tier": "web", "env": "prod"})

	require.NoError(t, s.ApplyChange(context.Background(), podKind, watch.Added, u))

	assert.Equal(t, 3, countRows(t, s, `SELECT COUNT(*) FROM labels WHERE uid='uid-1'`))
	assert.Equal(t, 1, countRows(t, s,
		`SELECT COUNT(*) FROM labels WHERE uid='uid-1' AND key='tier' AND value='web'`))
}

// The insert upserts, so only the DELETE ahead of it can retire a label the object dropped.
func TestInsertDropsALabelTheObjectNoLongerCarries(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added,
		podWith("uid-1", nil, map[string]any{"app": "api", "tier": "web"})))

	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Modified,
		podWith("uid-1", nil, map[string]any{"app": "api"})))

	assert.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM labels WHERE uid='uid-1'`))
}

// The other half of the nil guard: an unlabelled object's Labels is the nil map
// apimachinery returns, which marshals to `null` exactly as the ref slice does.
func TestInsertTakesAnObjectWithNoLabels(t *testing.T) {
	s := newTestStore(t)

	require.NoError(t, s.ApplyChange(context.Background(), podKind, watch.Added,
		podWith("uid-1", nil, nil)))

	assert.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM labels WHERE uid='uid-1'`))
}
