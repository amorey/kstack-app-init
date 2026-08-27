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

	"k8s.io/apimachinery/pkg/watch"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The watches re-read on every ping, so a read must not queue behind the single-connection
// writer: a transaction held open would otherwise stall every open watch on the cache.
func TestAReadDoesNotQueueBehindAHeldWriteTransaction(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow}, true))

	f, err := s.file()
	require.NoError(t, err)
	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck // the writer is held on purpose

	// The reader pool is separate, so this answers rather than blocking until the
	// transaction ends.
	kinds, err := s.Kinds(ctx)
	require.NoError(t, err)
	assert.Len(t, kinds, 1)
}

// The catalog is what the cluster serves and the counts are what is cached, so the join is
// outer: an advertised kind with nothing synced yet reads with Count 0 rather than vanishing
// from the nav.
func TestKindsJoinsCountsAndKeepsAnEmptyKind(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow, deploymentRow}, true))
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "42")))

	got, err := s.Kinds(ctx)
	require.NoError(t, err)

	assert.Equal(t, []KindRow{
		{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Scope: ScopeNamespaced},
		{APIVersion: "v1", Kind: "Pod", Resource: "pods", Scope: ScopeNamespaced, Count: 1},
	}, got, "ordered by (api_version, kind) for stable display")
}

// last_seen has one-second resolution, so ties straddle the limit. Ordering them by rowid
// would make a relist's re-inserted rows read as phantom Deleted/Added, so the uid tiebreak
// is what keeps the window stable across two reads.
func TestEventsBreaksLastSeenTiesByUID(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	for _, uid := range []string{"uid-a", "uid-c", "uid-b"} {
		require.NoError(t, s.ApplyChange(ctx, eventsKind, watch.Added, event(uid, "2026-08-26T10:00:00Z")))
	}

	got, err := s.Events(ctx, 2)
	require.NoError(t, err)

	require.Len(t, got, 2)
	assert.Equal(t, []string{"uid-c", "uid-b"}, []string{got[0].UID, got[1].UID},
		"the window is not stable across reads")
}

// A non-positive limit is the whole window rather than nothing.
func TestEventsDefaultsItsLimit(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.ApplyChange(ctx, eventsKind, watch.Added, event("uid-a", "2026-08-26T10:00:00Z")))

	got, err := s.Events(ctx, 0)
	require.NoError(t, err)
	assert.Len(t, got, 1)
}

// Objects are keyed by Kind while a watch names the plural, so the read translates through
// kind_catalog. A kind the sweep has not registered resolves to nothing — which is why the
// catalog's rows are load-bearing for this family rather than decoration.
func TestObjectsResolveThePluralThroughTheCatalog(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "42")))

	got, err := s.Objects(ctx, "v1", "pods")
	require.NoError(t, err)
	assert.Empty(t, got, "no catalog row, so the plural names no Kind")

	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow}, true))

	got, err = s.Objects(ctx, "v1", "pods")
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "uid-1", got[0].UID)
	assert.Equal(t, "42", got[0].ResourceVersion)
}

// The body comes back as stored, so an unchanged row is never inflated: the diff keys on
// (uid, resource_version) and only the rows that become frames are decompressed.
func TestObjectsReturnTheBodyStillCompressed(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow}, true))
	require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-1", "api-0", "42")))

	got, err := s.Objects(ctx, "v1", "pods")
	require.NoError(t, err)
	require.Len(t, got, 1)

	body, err := got[0].Body()
	require.NoError(t, err)
	assert.Contains(t, string(body), `"uid":"uid-1"`)
}

// Ordered by (namespace, name), which is what the table renders and what the index serves.
func TestObjectsAreOrderedByNamespaceAndName(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.SyncKinds(ctx, []KindRow{podRow}, true))
	for _, name := range []string{"api-2", "api-0", "api-1"} {
		require.NoError(t, s.ApplyChange(ctx, podKind, watch.Added, pod("uid-"+name, name, "42")))
	}

	got, err := s.Objects(ctx, "v1", "pods")
	require.NoError(t, err)

	require.Len(t, got, 3)
	assert.Equal(t, []string{"api-0", "api-1", "api-2"}, []string{got[0].Name, got[1].Name, got[2].Name})
}
