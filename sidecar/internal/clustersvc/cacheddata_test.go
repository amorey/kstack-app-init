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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// dataFixture is a cluster with a cache, and that cache's real store to seed.
type dataFixture struct {
	t         *testing.T
	d         deps
	clusterID ClusterID
	cacheID   ClusterCacheID
	store     *kubestore.Store
}

// newDataFixture builds the record chain the gate walks, and opens the cache's file the
// way a worker would.
func newDataFixture(t *testing.T) *dataFixture {
	t.Helper()
	d, status := newClusterStatusDeps(t)
	cluster := storedCluster(t, d, status, true, "uid-1")
	cache := createCache(t, d.cacheClient, ClusterID(cluster.ID), "uid-1")

	store, err := kubestoreFake(d).mgr.OpenOrCreate(int64(cache.ID))
	require.NoError(t, err)
	t.Cleanup(func() {
		if store != nil {
			store.Release()
		}
	})

	return &dataFixture{
		t:         t,
		d:         d,
		clusterID: ClusterID(cluster.ID),
		cacheID:   ClusterCacheID(cache.ID),
		store:     store,
	}
}

// data is the family under test.
func (f *dataFixture) data() CachedData { return serviceOver(f.t, f.d).CachedData() }

// stopWorkers releases the cache's only claim, which is what a pause does: the workers are
// forgotten, their claims go, and the last release closes the file. The rows stay on disk.
func (f *dataFixture) stopWorkers() {
	f.t.Helper()
	require.NotNil(f.t, f.store)
	f.store.Release()
	f.store = nil
	require.False(f.t, cacheIsOpen(kubestoreFake(f.d).mgr, int64(f.cacheID)), "the file stayed open")
}

// A paused cache still holds its rows, and reading them is not what pausing stops. Nothing
// keeps its file open once the workers are forgotten, so a read that bound only to an
// already-open file would show an empty nav over a full cache — on a pause, and on every
// restart until the first worker arms.
func TestCachedDataReadsACacheWhoseWorkersStopped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newDataFixture(t)
	require.NoError(t, f.store.SyncKinds(ctx, []kubestore.KindRow{
		{APIVersion: "v1", Kind: "Pod", Resource: "pods", Scope: kubestore.ScopeNamespaced},
	}, true, 7))
	require.NoError(t, f.store.ApplyChange(ctx, kubestore.Kind{
		APIVersion: "v1", Kind: "Pod", Resource: "pods",
	}, watch.Added, dataPod("uid-1", "api-0", "42")))

	f.stopWorkers()

	kinds, err := f.data().ListKinds(ctx, f.clusterID, f.cacheID)
	require.NoError(t, err)
	require.Len(t, kinds, 1, "ListKinds lost a paused cache's catalog")
	assert.Equal(t, 1, kinds[0].Count)

	stream, err := f.data().WatchObjects(ctx, f.clusterID, f.cacheID, "v1", "pods")
	require.NoError(t, err)
	first := testutil.Recv(t, stream.Frames, "the object")
	require.NotNil(t, first.Object, "WatchObjects lost a paused cache's rows")
	assert.Equal(t, "uid-1", first.Object.UID)
}

// pod is a Pod body the store will accept.
func dataPod(uid, name, rv string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"uid": uid, "name": name, "namespace": "prod", "resourceVersion": rv,
		},
	}}
}

// dataEvent is an Event body the store will accept, at a given resourceVersion and count.
func dataEvent(uid, rv string, count int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata":       map[string]any{"uid": uid, "name": uid, "resourceVersion": rv},
		"involvedObject": map[string]any{"uid": "pod-1", "kind": "Pod", "name": "api-0"},
		"reason":         "BackOff", "message": "restarting", "type": "Warning", "count": count,
	}}
}

// A pair that does not resolve is definitively empty rather than an error or a wait: the
// cache is gone, or was never this cluster's, and no row will ever arrive for it.
func TestCachedDataGatesAnUnknownPair(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newDataFixture(t)

	kinds, err := f.data().ListKinds(ctx, f.clusterID, 9999)
	require.NoError(t, err)
	assert.Empty(t, kinds)

	stream, err := f.data().WatchKinds(ctx, f.clusterID, 9999)
	require.NoError(t, err)
	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
	_, open := <-stream.Frames
	assert.False(t, open, "a scope that can never be filled stayed open")
}

// A cache owned by another cluster is the same answer: the gate is the pair, not the id.
func TestCachedDataGatesACacheOfAnotherCluster(t *testing.T) {
	ctx := context.Background()
	f := newDataFixture(t)

	kinds, err := f.data().ListKinds(ctx, 9999, f.cacheID)
	require.NoError(t, err)
	assert.Empty(t, kinds)
}

// A cache whose file exists but will not open — corrupt, unreadable, out of descriptors —
// is a storage failure, not an empty cache. Answering "no kinds" would report the cluster
// as serving nothing and give a caller nothing to act on.
func TestListKindsReportsAStoreThatWillNotOpen(t *testing.T) {
	ctx := context.Background()
	f := newDataFixture(t)
	boom := errors.New("permission denied")
	kubestoreFake(f.d).err = boom

	_, err := f.data().ListKinds(ctx, f.clusterID, f.cacheID)

	assert.ErrorIs(t, err, boom)
}

// ListKinds is one read: what ClusterCache.kinds resolves.
func TestListKindsReadsTheCatalog(t *testing.T) {
	ctx := context.Background()
	f := newDataFixture(t)
	require.NoError(t, f.store.SyncKinds(ctx, []kubestore.KindRow{
		{APIVersion: "v1", Kind: "Pod", Resource: "pods", Scope: kubestore.ScopeNamespaced},
	}, true, 7))

	got, err := f.data().ListKinds(ctx, f.clusterID, f.cacheID)

	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, ClusterCachedDataKind{
		APIVersion: "v1", Kind: "Pod", Resource: "pods", Scope: "Namespaced",
	}, got[0])
}

// An advertised kind shows before its worker has synced anything — Count 0, not absent,
// which is what the nav promises.
func TestWatchKindsStreamsTheCatalogWithCounts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newDataFixture(t)
	require.NoError(t, f.store.SyncKinds(ctx, []kubestore.KindRow{
		{APIVersion: "v1", Kind: "Pod", Resource: "pods", Scope: kubestore.ScopeNamespaced},
	}, true, 7))

	stream, err := f.data().WatchKinds(ctx, f.clusterID, f.cacheID)
	require.NoError(t, err)

	first := testutil.Recv(t, stream.Frames, "the kind")
	require.NotNil(t, first.Kind)
	assert.Equal(t, 0, first.Kind.Count)
	assert.Equal(t, f.cacheID, first.CacheID, "the frame carries no provenance")
	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)

	require.NoError(t, f.store.ApplyChange(ctx, kubestore.Kind{
		APIVersion: "v1", Kind: "Pod", Resource: "pods",
	}, watch.Added, dataPod("uid-1", "api-0", "42")))

	moved := testutil.Recv(t, stream.Frames, "the count moving")
	assert.Equal(t, DeltaFrameModified, moved.Type)
	assert.Equal(t, 1, moved.Kind.Count)
}

// The store carries a CRD's printer columns as JSON, and this projection is where they become
// the typed value the wire serves.
func TestWatchKindsDecodesPrinterColumns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newDataFixture(t)
	require.NoError(t, f.store.SyncKinds(ctx, []kubestore.KindRow{{
		APIVersion: "example.com/v1", Kind: "Widget", Resource: "widgets",
		Scope: kubestore.ScopeNamespaced, IsCRD: true,
		PrinterColumns: `[{"name":"Replicas","type":"integer","jsonPath":".spec.replicas","priority":1}]`,
	}}, true, 7))

	stream, err := f.data().WatchKinds(ctx, f.clusterID, f.cacheID)
	require.NoError(t, err)

	first := testutil.Recv(t, stream.Frames, "the kind")
	require.NotNil(t, first.Kind)
	assert.Equal(t, []PrinterColumn{
		{Name: "Replicas", Type: "integer", JSONPath: ".spec.replicas", Priority: 1},
	}, first.Kind.PrinterColumns)
}

// A projection has no error path, so a blob that will not parse yields no columns rather than
// dropping the kind out of the nav. The sidecar is the only writer, so it should never happen —
// the branch is there so "never happens" has an answer.
func TestAMalformedPrinterColumnBlobYieldsNoColumns(t *testing.T) {
	got := toCachedDataKind(kubestore.KindRow{
		APIVersion: "example.com/v1", Kind: "Widget", Resource: "widgets", PrinterColumns: "{not json",
	})

	assert.Equal(t, "Widget", got.Kind)
	assert.Empty(t, got.PrinterColumns)
}

// The kinds watch diffs whole rows, which is what makes an edited CRD reach a client already
// sitting on the dashboard. Decoding anywhere before the diff would leave the columns out of the
// compared value, and the table would only pick them up on the next reconnect.
func TestWatchKindsReportsAColumnsOnlyChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newDataFixture(t)
	row := kubestore.KindRow{
		APIVersion: "example.com/v1", Kind: "Widget", Resource: "widgets",
		Scope: kubestore.ScopeNamespaced, IsCRD: true,
	}
	require.NoError(t, f.store.SyncKinds(ctx, []kubestore.KindRow{row}, true, 7))

	stream, err := f.data().WatchKinds(ctx, f.clusterID, f.cacheID)
	require.NoError(t, err)
	testutil.Recv(t, stream.Frames, "the kind")
	require.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)

	edited := row
	edited.PrinterColumns = `[{"name":"Phase","type":"string","jsonPath":".status.phase","priority":0}]`
	require.NoError(t, f.store.SyncKinds(ctx, []kubestore.KindRow{edited}, true, 8))

	moved := testutil.Recv(t, stream.Frames, "the edited CRD")
	assert.Equal(t, DeltaFrameModified, moved.Type)
	assert.Equal(t, "Phase", moved.Kind.PrinterColumns[0].Name)
}

// The objects watch carries the kind as provenance beside the cache, since one client
// switching resources within a cache has to reject the previous subscription's stragglers.
func TestWatchObjectsCarriesItsKindAsProvenance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newDataFixture(t)
	require.NoError(t, f.store.SyncKinds(ctx, []kubestore.KindRow{
		{APIVersion: "v1", Kind: "Pod", Resource: "pods", Scope: kubestore.ScopeNamespaced},
	}, true, 7))
	require.NoError(t, f.store.ApplyChange(ctx, kubestore.Kind{
		APIVersion: "v1", Kind: "Pod", Resource: "pods",
	}, watch.Added, dataPod("uid-1", "api-0", "42")))

	stream, err := f.data().WatchObjects(ctx, f.clusterID, f.cacheID, "v1", "pods")
	require.NoError(t, err)

	first := testutil.Recv(t, stream.Frames, "the object")
	require.NotNil(t, first.Object)
	assert.Equal(t, "uid-1", first.Object.UID)
	assert.Equal(t, f.cacheID, first.CacheID)
	assert.Equal(t, "v1", first.APIVersion)
	assert.Equal(t, "pods", first.Resource)
	assert.Contains(t, string(first.Object.RawJSON), `"uid":"uid-1"`, "an Added frame carries the body")
}

// A Deleted frame's object is the key the client removes by, so it stays non-null — but its
// body is dead weight: applyChange deletes by uid and never looks at the entity. Nothing
// else would catch a hydrate that fired on every frame type.
func TestWatchObjectsSendsADeletedFrameWithoutABody(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newDataFixture(t)
	podKind := kubestore.Kind{APIVersion: "v1", Kind: "Pod", Resource: "pods"}
	require.NoError(t, f.store.SyncKinds(ctx, []kubestore.KindRow{
		{APIVersion: "v1", Kind: "Pod", Resource: "pods", Scope: kubestore.ScopeNamespaced},
	}, true, 7))
	require.NoError(t, f.store.ApplyChange(ctx, podKind, watch.Added, dataPod("uid-1", "api-0", "42")))

	stream, err := f.data().WatchObjects(ctx, f.clusterID, f.cacheID, "v1", "pods")
	require.NoError(t, err)
	testutil.Recv(t, stream.Frames, "the object")
	require.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)

	require.NoError(t, f.store.ApplyChange(ctx, podKind, watch.Deleted, dataPod("uid-1", "api-0", "42")))

	gone := testutil.Recv(t, stream.Frames, "the removal")
	assert.Equal(t, DeltaFrameDeleted, gone.Type)
	require.NotNil(t, gone.Object, "the client keys the removal off the object")
	assert.Equal(t, "uid-1", gone.Object.UID)
	assert.Empty(t, gone.Object.RawJSON, "a removal carries no body")
}

// A guard on the split's cost: the diff read and the body fetch are two queries against a
// moving table, so a row can go between them. The frame serves a null body and the watch
// carries on — the next resync's Deleted frame is the real answer, and failing the stream
// would be reporting a race as a breakage.
func TestHydrateObjectServesNoBodyForARowThatIsGone(t *testing.T) {
	ctx := context.Background()
	f := newDataFixture(t)

	obj := hydrateObject(ctx, f.store, DeltaFrameAdded, kubestore.ObjectRow{
		UID: "uid-gone", APIVersion: "v1", Kind: "Pod", Namespace: "prod", Name: "api-0",
	})

	assert.Equal(t, "uid-gone", obj.UID)
	assert.Empty(t, obj.RawJSON)
}

// The events watch is its own bus, so an event storm does not drive the object watches.
func TestWatchEventsStreamsTheNewestWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newDataFixture(t)
	require.NoError(t, f.store.ApplyChange(ctx, kubestore.Kind{
		APIVersion: "v1", Kind: "Event", Resource: "events",
	}, watch.Added, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata": map[string]any{"uid": "uid-ev", "name": "ev", "namespace": "prod"},
		"reason":   "Pulled", "message": "ok", "type": "Normal",
	}}))

	stream, err := f.data().WatchEvents(ctx, f.clusterID, f.cacheID)
	require.NoError(t, err)

	first := testutil.Recv(t, stream.Frames, "the event")
	require.NotNil(t, first.Event)
	assert.Equal(t, "uid-ev", first.Event.UID)
	assert.Equal(t, "Pulled", first.Event.Reason)
	assert.Equal(t, f.cacheID, first.CacheID)
}

// A stored stamp of zero is absence, not the epoch: the field resolvers map a zero time to
// null, and 1970 would sort to the bottom of every window instead.
func TestMillisToTimeKeepsZeroAsAbsence(t *testing.T) {
	assert.True(t, millisToTime(0).IsZero())
	assert.Equal(t, int64(1500), millisToTime(1500).UnixMilli())
}

// The two watches gate on the same pair the reads do, and a pair that can never resolve
// gets a bookmark over an empty collection rather than a stream that waits forever.
func TestTheObjectAndEventWatchesGateOnThePairToo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newDataFixture(t)

	objects, err := f.data().WatchObjects(ctx, 9999, f.cacheID, "v1", "pods")
	require.NoError(t, err)
	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, objects.Frames, "the bookmark").Type)

	events, err := f.data().WatchEvents(ctx, 9999, f.cacheID)
	require.NoError(t, err)
	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, events.Frames, "the bookmark").Type)
}

// A cache with no file has never synced anything, which is empty rather than a fault: the
// read never creates one, so there is nothing to report.
func TestListKindsAnswersACacheWithNoFileAsEmpty(t *testing.T) {
	f := newDataFixture(t)
	kubestoreFake(f.d).noFile = true

	kinds, err := f.data().ListKinds(context.Background(), f.clusterID, f.cacheID)

	require.NoError(t, err)
	assert.Empty(t, kinds)
}

// A cache whose file goes between the claim and the read is a fault to report — the rows
// were there a moment ago, and answering empty would blank a populated table.
func TestListKindsReportsACatalogItCannotRead(t *testing.T) {
	f := newDataFixture(t)
	store := kubestoreFake(f.d)
	store.afterOpen = func(cacheID int64) { require.NoError(t, store.mgr.Remove(cacheID)) }

	_, err := f.data().ListKinds(context.Background(), f.clusterID, f.cacheID)

	assert.ErrorContains(t, err, "kinds")
}

// A cleared kind that relists the same uids is the case deletes-before-writes exists for:
// ClearKind logs a delete per row and the sync writes the same objects back above it, so a
// watch reading both in one burst must end holding the rows. Final state, not frame count —
// the two land in one burst here and in two on a kind large enough to outrun the debounce.
func TestWatchObjectsKeepsRowsThatWereClearedAndRelisted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newDataFixture(t)
	podKind := kubestore.Kind{APIVersion: "v1", Kind: "Pod", Resource: "pods"}
	require.NoError(t, f.store.SyncKinds(ctx, []kubestore.KindRow{
		{APIVersion: "v1", Kind: "Pod", Resource: "pods", Scope: kubestore.ScopeNamespaced},
	}, true, 7))
	require.NoError(t, f.store.ApplyChange(ctx, podKind, watch.Added, dataPod("uid-1", "api-0", "42")))

	stream, err := f.data().WatchObjects(ctx, f.clusterID, f.cacheID, "v1", "pods")
	require.NoError(t, err)
	testutil.Recv(t, stream.Frames, "the object")
	require.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)

	require.NoError(t, f.store.ClearKind(ctx, podKind))
	require.NoError(t, f.store.ApplyChange(ctx, podKind, watch.Added, dataPod("uid-1", "api-0", "43")))

	held := map[string]bool{}
	for range 2 {
		fr := testutil.Recv(t, stream.Frames, "a frame")
		require.NotNil(t, fr.Object)
		if fr.Type == DeltaFrameDeleted {
			delete(held, fr.Object.UID)
			continue
		}
		held[fr.Object.UID] = len(fr.Object.RawJSON) > 0
	}
	assert.Equal(t, map[string]bool{"uid-1": true}, held, "the relisted row was dropped")
}

// A CRD whose Kind is renamed keeps its plural, and the catalog remaps to the new Kind
// (stmtResolveKindRename). The rows and the deletes the old worker logged are keyed by the
// OLD Kind, so a cursor read under the new one sees neither — the watch would hold the old
// rows for as long as it stayed connected.
func TestWatchObjectsDropsRowsWhenTheKindBehindThePluralIsRenamed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newDataFixture(t)
	was := kubestore.Kind{APIVersion: "v1", Kind: "Widget", Resource: "widgets"}
	require.NoError(t, f.store.SyncKinds(ctx, []kubestore.KindRow{
		{APIVersion: "v1", Kind: "Widget", Resource: "widgets", Scope: kubestore.ScopeNamespaced},
	}, true, 7))
	require.NoError(t, f.store.ApplyChange(ctx, was, watch.Added, dataPod("uid-1", "api-0", "42")))

	stream, err := f.data().WatchObjects(ctx, f.clusterID, f.cacheID, "v1", "widgets")
	require.NoError(t, err)
	testutil.Recv(t, stream.Frames, "the object")
	require.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)

	// The sweep remaps the plural onto the new Kind, and the old kind's worker clears its
	// rows on the way out.
	require.NoError(t, f.store.SyncKinds(ctx, []kubestore.KindRow{
		{APIVersion: "v1", Kind: "Gadget", Resource: "widgets", Scope: kubestore.ScopeNamespaced},
	}, true, 8))
	require.NoError(t, f.store.ClearKind(ctx, was))

	gone := testutil.Recv(t, stream.Frames, "the removal")
	assert.Equal(t, DeltaFrameDeleted, gone.Type)
	assert.Equal(t, "uid-1", gone.Object.UID)
}

// The events watch reads what moved past its cursor like the objects watch does, so a
// re-fired event reaches the client as a Modified rather than waiting for a reconnect.
func TestWatchEventsSendsWhatMovedAfterTheSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newDataFixture(t)
	eventsKind := kubestore.Kind{APIVersion: "v1", Kind: "Event", Resource: "events"}
	require.NoError(t, f.store.ApplyChange(ctx, eventsKind, watch.Added, dataEvent("ev-1", "10", 1)))

	stream, err := f.data().WatchEvents(ctx, f.clusterID, f.cacheID)
	require.NoError(t, err)
	testutil.Recv(t, stream.Frames, "the event")
	require.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)

	require.NoError(t, f.store.ApplyChange(ctx, eventsKind, watch.Modified, dataEvent("ev-1", "11", 2)))

	fr := testutil.Recv(t, stream.Frames, "the re-fired event")
	assert.Equal(t, DeltaFrameModified, fr.Type)
	require.NotNil(t, fr.Event)
	assert.Equal(t, 2, fr.Event.Count)
}
