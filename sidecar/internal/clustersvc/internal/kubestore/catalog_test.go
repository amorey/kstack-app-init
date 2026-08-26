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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	podRow        = KindRow{APIVersion: "v1", Kind: "Pod", Resource: "pods", Scope: ScopeNamespaced}
	deploymentRow = KindRow{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Scope: ScopeNamespaced}
)

// catalogRows reads the table back in a deterministic order.
func catalogRows(t *testing.T, s *Store) []KindRow {
	t.Helper()
	rows, err := db(t, s).QueryContext(context.Background(),
		`SELECT api_version, kind, resource, scope, is_crd FROM kind_catalog ORDER BY api_version, resource`)
	require.NoError(t, err)
	defer rows.Close()

	var out []KindRow
	for rows.Next() {
		var r KindRow
		require.NoError(t, rows.Scan(&r.APIVersion, &r.Kind, &r.Resource, &r.Scope, &r.IsCRD))
		out = append(out, r)
	}
	require.NoError(t, rows.Err())
	return out
}

// A sweep's answer lands as the table's rows.
func TestSyncKindsWritesTheAnswer(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow, deploymentRow}, true))

	assert.Equal(t, []KindRow{deploymentRow, podRow}, catalogRows(t, s))
}

// Pruning is the caller's call, because a partial answer has not seen every group: a kind
// missing from an incomplete sweep has not stopped being served.
func TestSyncKindsPrunesOnlyWhenAsked(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow, deploymentRow}, true))

	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow}, false))
	assert.Len(t, catalogRows(t, s), 2, "an incomplete answer dropped a kind")

	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow}, true))
	assert.Equal(t, []KindRow{podRow}, catalogRows(t, s))
}

// The plural is unique too, so a Kind renamed under an unchanged plural collides with the
// row it replaces — on the index rather than on the primary key, which is the case a
// single-target upsert cannot resolve.
func TestSyncKindsResolvesARenamedKind(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	old := KindRow{APIVersion: "example.com/v1", Kind: "Widget", Resource: "widgets", Scope: ScopeNamespaced, IsCRD: true}
	require.NoError(t, s.SyncKinds(ctx, []KindRow{old}, true))

	renamed := old
	renamed.Kind = "Gadget"
	require.NoError(t, s.SyncKinds(ctx, []KindRow{renamed}, false))

	assert.Equal(t, []KindRow{renamed}, catalogRows(t, s), "the rename's loser survived")
}

// Nothing on this path fills schema_json, so an upsert must leave it alone: writing NULL
// on every sweep would quietly make the column unusable to whoever fills it.
func TestSyncKindsLeavesTheSchemaAlone(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow}, true))
	_, err := db(t, s).ExecContext(ctx,
		`UPDATE kind_catalog SET schema_json = '{"x":1}' WHERE api_version = 'v1' AND kind = 'Pod'`)
	require.NoError(t, err)

	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow}, true))

	var got *string
	require.NoError(t, db(t, s).QueryRowContext(ctx,
		`SELECT schema_json FROM kind_catalog WHERE api_version = 'v1' AND kind = 'Pod'`).Scan(&got))
	require.NotNil(t, got)
	assert.JSONEq(t, `{"x":1}`, *got)
}
