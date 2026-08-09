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

package eventsync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// openTestCache opens a real (temp-dir) cache db, the same one the events worker writes
// into in production.
func openTestCache(t *testing.T) *store.ClusterDB {
	t.Helper()
	mgr := store.NewManager(t.TempDir())
	cdb, err := mgr.Open(context.Background(), store.CacheRef{ClusterID: 1, CacheID: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	return cdb
}

// newTestEvent builds a core/v1 Event body.
func newTestEvent(uid, name, rv string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Event",
		"metadata": map[string]any{
			"uid":             uid,
			"name":            name,
			"namespace":       "default",
			"resourceVersion": rv,
		},
		"involvedObject": map[string]any{
			"kind":      "Pod",
			"namespace": "default",
			"name":      "web-1",
			"uid":       "pod-uid",
		},
		"type":           "Warning",
		"reason":         "BackOff",
		"message":        "Back-off restarting failed container",
		"count":          int64(3),
		"firstTimestamp": "2026-08-07T10:00:00Z",
		"lastTimestamp":  "2026-08-07T10:05:00Z",
	}}
}

// TestExtractEventCoreV1 covers the spelling the worker actually lists.
func TestExtractEventCoreV1(t *testing.T) {
	row, err := extractEvent(newTestEvent("uid-1", "e1", "10"))
	require.NoError(t, err)

	assert.Equal(t, "uid-1", row.UID)
	assert.Equal(t, "pod-uid", row.InvolvedUID)
	assert.Equal(t, "Pod", row.InvolvedKind)
	assert.Equal(t, "default", row.InvolvedNS)
	assert.Equal(t, "web-1", row.InvolvedName)
	assert.Equal(t, "Warning", row.Type)
	assert.Equal(t, "BackOff", row.Reason)
	assert.Equal(t, 3, row.Count)
	assert.Equal(t, time.Date(2026, 8, 7, 10, 0, 0, 0, time.UTC).UnixMilli(), row.FirstSeen)
	assert.Equal(t, time.Date(2026, 8, 7, 10, 5, 0, 0, time.UTC).UnixMilli(), row.LastSeen)
}

// TestExtractEventEventsK8sIO covers the other spelling. The worker lists core/v1, but a
// body can still reach the projection through either, and the two carry the same data
// under different field names — so both must land in the same columns.
func TestExtractEventEventsK8sIO(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "events.k8s.io/v1",
		"kind":       "Event",
		"metadata":   map[string]any{"uid": "uid-2", "name": "e2", "namespace": "kube-system"},
		"regarding": map[string]any{
			"kind": "Node", "name": "node-1", "uid": "node-uid",
		},
		"type":   "Normal",
		"reason": "NodeReady",
		"note":   "Node is ready",
		"series": map[string]any{
			"count":            int64(7),
			"lastObservedTime": "2026-08-07T11:00:00Z",
		},
	}}

	row, err := extractEvent(u)
	require.NoError(t, err)

	assert.Equal(t, "uid-2", row.UID)
	assert.Equal(t, "node-uid", row.InvolvedUID, "regarding must map to the involved columns")
	assert.Equal(t, "Node", row.InvolvedKind)
	assert.Equal(t, "Node is ready", row.Message, "note is the events.k8s.io spelling of message")
	assert.Equal(t, 7, row.Count, "series.count is the events.k8s.io spelling of count")
	assert.Equal(t, time.Date(2026, 8, 7, 11, 0, 0, 0, time.UTC).UnixMilli(), row.LastSeen)
	assert.Equal(t, row.LastSeen, row.FirstSeen, "an event with only a last-seen time reads as a point in time")
}

// The four spellings of an Event's time are a fallback CHAIN, and a value that doesn't
// parse has to fall through it like an absent one. Branching on presence alone let a
// malformed earlier spelling end the chain: last_seen stored NULL, and since the events
// read orders by last_seen DESC, the newest event in the cluster sorted below every
// timestamped row and never entered the dashboard's window.
func TestExtractEventFallsPastAnUnparseableTimestamp(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "events.k8s.io/v1",
		"kind":       "Event",
		"metadata":   map[string]any{"uid": "uid-9", "name": "e9"},
		"regarding":  map[string]any{"kind": "Pod", "name": "p"},
		"series":     map[string]any{"lastObservedTime": "07 Aug 26 11:00 UTC"},
		"eventTime":  "2026-08-07T11:00:00.123456Z",
	}}

	row, err := extractEvent(u)
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 8, 7, 11, 0, 0, 123456000, time.UTC).UnixMilli(), row.LastSeen,
		"a spelling that cannot be read must not shadow one that can")
}

// TestExtractEventNameOnlyInvolvedObject pins the branch that must not be confused for the
// other spelling: involvedObject.uid is optional, so a name-only reference is valid and
// must keep its identity rather than falling through to `regarding` and reading empty.
func TestExtractEventNameOnlyInvolvedObject(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion":     "v1",
		"kind":           "Event",
		"metadata":       map[string]any{"uid": "uid-3", "name": "e3"},
		"involvedObject": map[string]any{"kind": "Pod", "name": "not-yet-observed"},
		"reason":         "Scheduled",
	}}

	row, err := extractEvent(u)
	require.NoError(t, err)
	assert.Empty(t, row.InvolvedUID)
	assert.Equal(t, "Pod", row.InvolvedKind)
	assert.Equal(t, "not-yet-observed", row.InvolvedName)
	assert.Equal(t, 1, row.Count, "an event carrying no count spelling counts once")
	assert.Zero(t, row.LastSeen, "no timestamp must read as absent, not as the epoch")
}

// TestExtractEventRequiresUID: a body with no uid can't be keyed, so it's rejected rather
// than written under an empty primary key (which would collide with the next such body).
func TestExtractEventRequiresUID(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": "no-uid"},
	}}
	_, err := extractEvent(u)
	assert.Error(t, err)
}

// TestStoreUpsertIsIdempotent verifies a re-observed event updates its row rather than
// duplicating it — the property the api server's repeated Modified deltas depend on.
func TestStoreUpsertIsIdempotent(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newEventStore(cdb)

	require.NoError(t, st.ApplyChange(ctx, watch.Added, newTestEvent("uid-1", "e1", "10")))
	require.NoError(t, st.ApplyChange(ctx, watch.Modified, newTestEvent("uid-1", "e1", "11")))

	rows, err := cdb.Events(ctx, 100)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "uid-1", rows[0].UID)
}

// TestReplaceSessionClearsCookieOnFirstPage pins the invariant that makes a resume cookie
// mean "a full LIST completed on disk": the cookie is dropped in the same transaction as
// the first page's rows, so a pass that dies mid-pagination can never leave partial rows
// beside a cookie that would resume past them.
func TestReplaceSessionClearsCookieOnFirstPage(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newEventStore(cdb)

	require.NoError(t, st.PersistRV(ctx, "old-rv"))

	sess := st.BeginReplace()
	require.NoError(t, sess.WritePage(ctx, []*unstructured.Unstructured{newTestEvent("uid-1", "e1", "10")}))

	rv, err := st.ResumeRV(ctx)
	require.NoError(t, err)
	assert.Empty(t, rv, "the first written page must invalidate the stale cookie")

	// Abandoning the session here (no Commit) is exactly the failed-pass case: the row is
	// durable, the cookie is gone, so the next start cold-LISTs and prunes it if needed.
	require.NoError(t, commitErr(sess.Commit(ctx, "new-rv")))
	rv, err = st.ResumeRV(ctx)
	require.NoError(t, err)
	assert.Equal(t, "new-rv", rv, "only a successful Commit rewrites the cookie")
}

// TestEnsureCatalogRegistersEventKind covers the Event kind's row in the cache's kind
// catalog. The catalog says what a cache HOLDS, and it holds events — but every other
// entry is written by an objectsync worker, and events don't have one (they are
// deliberately excluded from the per-GVR sync). Without this the ('v1','Event') count the
// events triggers maintain is unreachable: store.Kinds is a kind_catalog LEFT JOIN, so no
// catalog row means no count, and the dashboard's curated Events row shows none.
func TestEnsureCatalogRegistersEventKind(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newEventStore(cdb)

	require.NoError(t, st.EnsureCatalog(ctx))
	require.NoError(t, st.EnsureCatalog(ctx), "re-registering on every worker start must be a no-op")
	require.NoError(t, st.ApplyChange(ctx, watch.Added, newTestEvent("e1", "one", "10")))

	kinds, err := cdb.Kinds(ctx)
	require.NoError(t, err)
	require.Len(t, kinds, 1)
	assert.Equal(t, "v1", kinds[0].APIVersion)
	assert.Equal(t, "Event", kinds[0].Kind)
	assert.Equal(t, "events", kinds[0].Resource)
	assert.Equal(t, "Namespaced", kinds[0].Scope)
	assert.False(t, kinds[0].IsCRD)
	assert.Equal(t, 1, kinds[0].Count, "the trigger-maintained count must now be reachable")
}

// TestReplacePrunesRowsWrittenInTheSameMillisecond pins the sweep boundary. The relist
// reconciles by mark and sweep, and updated_at has millisecond resolution — so a row
// written in the same tick the relist began carries the session's own start stamp, and with
// an inclusive boundary it would masquerade as a row the pass wrote and survive a prune it
// deserved. A relist immediately after a write is the common shape (a 410 on a busy
// cluster), so this must not depend on the clock ticking.
func TestReplacePrunesRowsWrittenInTheSameMillisecond(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newEventStore(cdb)

	// Freeze the clock so the write and the relist genuinely share a millisecond — left to
	// the real clock this passes or fails on whether the tick happened to land.
	st.now = func() int64 { return 1_700_000_000_000 }

	require.NoError(t, st.ApplyChange(ctx, watch.Added, newTestEvent("uid-gone", "gone", "10")))

	sess := st.BeginReplace()
	require.NoError(t, sess.WritePage(ctx, []*unstructured.Unstructured{newTestEvent("uid-kept", "kept", "11")}))
	require.NoError(t, commitErr(sess.Commit(ctx, "500")))

	rows, err := cdb.Events(ctx, 100)
	require.NoError(t, err)
	require.Len(t, rows, 1, "an event the relist did not carry must be swept however recently it was written")
	assert.Equal(t, "uid-kept", rows[0].UID)
}

// commitErr drops a replace session's prune count, for the tests that only care that the
// commit succeeded.
func commitErr(_ int, err error) error { return err }

// A delete carrying no uid cannot be applied — there is no row to key. Reporting success
// for it told the driver a delta had landed, and the driver books that as progress and lets
// a bookmark advance past it, while the delta's own cookie write never happened. A crash in
// that window resumed the watch from an older position and replayed.
func TestDeleteWithoutAUIDIsAnError(t *testing.T) {
	cdb := openTestCache(t)
	st, err := NewStore(cdb)
	require.NoError(t, err)
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"name": "nameless"},
	}}
	require.Error(t, st.ApplyChange(context.Background(), watch.Deleted, u))
}
