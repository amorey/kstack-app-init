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
	}, true))
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
	}, true))

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
	}, true))

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

// The objects watch carries the kind as provenance beside the cache, since one client
// switching resources within a cache has to reject the previous subscription's stragglers.
func TestWatchObjectsCarriesItsKindAsProvenance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newDataFixture(t)
	require.NoError(t, f.store.SyncKinds(ctx, []kubestore.KindRow{
		{APIVersion: "v1", Kind: "Pod", Resource: "pods", Scope: kubestore.ScopeNamespaced},
	}, true))
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
	assert.Contains(t, string(first.Object.RawJSON), `"uid":"uid-1"`, "the body was served compressed")
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
