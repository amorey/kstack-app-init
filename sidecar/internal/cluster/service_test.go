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

// White-box (package cluster): the service test seeds beehive objects directly and
// exercises the data/mutation/watch surface in isolation from the (network-touching)
// real controllers, using the shared helpers in testutil_test.go.
package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// noopController satisfies beehive.Controller without reconciling. A test gets a
// ControllerClient to write status directly (the plain Client cannot) from the
// value beehive.Register returns for the kind.
type noopController[Spec, Status any] struct{}

func (c *noopController[Spec, Status]) Reconcile(context.Context, beehive.ControllerClient[Status], *beehive.Object[Spec, Status]) (beehive.Result, error) {
	return beehive.Result{}, nil
}

// fakeCoreController satisfies the coreController seam so the white-box service
// test can assert the out-of-band dispatch (RetryConnection → Reprobe) without a
// real, network-touching ClusterCoreController. StartBackground/StopBackground are
// no-ops; Reprobe records the ids it was handed.
type fakeCoreController struct{ reprobed []ClusterID }

func (f *fakeCoreController) StartBackground()     {}
func (f *fakeCoreController) StopBackground()      {}
func (f *fakeCoreController) Reprobe(id ClusterID) { f.reprobed = append(f.reprobed, id) }

// newServiceTest builds a started beehive with no-op controllers and returns a
// service wired to its clients plus a temp cache manager. The returned
// ControllerClients write Cluster status (core) and ClusterCache status — the
// controller-owned surfaces a white-box test stamps directly.
func newServiceTest(t *testing.T) (*Service, beehive.ControllerClient[ClusterStatus], beehive.ControllerClient[ClusterCacheStatus]) {
	t.Helper()
	st, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	bh, err := beehive.New(st, beehive.WithResyncInterval(0))
	require.NoError(t, err)

	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)

	coreCC, err := beehive.Register(bh, ClusterGroupKind, &noopController[ClusterSpec, ClusterStatus]{})
	require.NoError(t, err)
	cacheCC, err := beehive.Register(bh, ClusterCacheGroupKind, &noopController[ClusterCacheSpec, ClusterCacheStatus]{})
	require.NoError(t, err)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	return &Service{
		coreClient:   coreClient,
		cacheClient:  cacheClient,
		cacheManager: store.NewManager(t.TempDir()),
		connMgr:      NewConnectionManager(),
		coreCtrl:     &fakeCoreController{},
	}, coreCC, cacheCC
}

// seedCluster creates a Cluster (as the importer would, with the kubeconfig
// slug) and returns its ClusterID — the beehive ObjectID beehive assigned.
func seedCluster(t *testing.T, s *Service, ctxName string) ClusterID {
	t.Helper()
	ctx := context.Background()
	name := ctxName
	obj, err := s.coreClient.Create(ctx, ClusterSpec{
		Name:        &name,
		SyncEnabled: true,
		Enabled:     true,
		Source:      ClusterSpecSource{Kubeconfig: &ClusterSpecSourceKubeconfig{Context: ctxName}},
	}, beehive.WithSlug(kubeconfigSlug(ctxName)))
	require.NoError(t, err)
	return ClusterID(obj.ID)
}

// stampActiveUID records uid as a cluster's last-probed kube-system identity by
// writing it to Status.Server.UID (as the ClusterCoreController would after a
// probe). A ClusterCache for the same uid then resolves as the cluster's active
// cache.
func stampActiveUID(t *testing.T, s *Service, coreCC beehive.ControllerClient[ClusterStatus], id ClusterID, uid string) {
	t.Helper()
	ctx := context.Background()
	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	require.NoError(t, coreCC.UpdateStatus(ctx, obj.ID, obj.Generation, ClusterStatus{
		Server: ClusterServer{UID: &uid},
	}))
}

// seedActiveCache creates an active ClusterCache for a cluster: it stamps the
// cluster's connected UID and creates a ClusterCache (owned, UID-keyed slug) for
// that identity. Returns the cache's ObjectID.
func seedActiveCache(t *testing.T, s *Service, coreCC beehive.ControllerClient[ClusterStatus], id ClusterID, uid string) beehive.ObjectID {
	t.Helper()
	ctx := context.Background()
	stampActiveUID(t, s, coreCC, id, uid)
	cacheObj, err := s.cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: uid},
		beehive.WithSlug(ClusterCacheSlug(id, uid)), beehive.WithOwner(beehive.ObjectID(id)))
	require.NoError(t, err)
	return cacheObj.ID
}

func TestServiceListAndGet(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)
	idAlpha := seedCluster(t, s, "alpha")
	seedCluster(t, s, "beta")

	list, err := s.List(ctx)
	require.NoError(t, err)
	assert.Len(t, list, 2)

	c, err := s.Get(ctx, idAlpha)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, idAlpha, c.ID)
	require.NotNil(t, c.Spec.Name)
	assert.Equal(t, "alpha", *c.Spec.Name)

	// Unknown id is (nil, nil), not an error.
	missing, err := s.Get(ctx, ClusterID(999999))
	require.NoError(t, err)
	assert.Nil(t, missing)
}

func TestServiceGetJoinsSyncStatus(t *testing.T) {
	ctx := context.Background()
	s, coreCC, cacheCtl := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	now := time.Now().UTC()
	// Give the ClusterCache a Synced status to join in.
	cacheObj, err := s.cacheClient.Get(ctx, cacheID)
	require.NoError(t, err)
	require.NoError(t, cacheCtl.UpdateStatus(ctx, cacheObj.ID, cacheObj.Generation, ClusterCacheStatus{
		LastSyncedAt: &now,
	}))

	c, err := s.Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, c)
	// The owned cache shows up in Caches and, because its UID matches the cluster's
	// active identity, as ActiveCache with the joined sync status.
	require.Len(t, c.Caches, 1)
	assert.Equal(t, ClusterCacheID(cacheID), c.Caches[0].ID)
	assert.True(t, c.Caches[0].Enabled)
	require.NotNil(t, c.ActiveCache)
	assert.Equal(t, uid, c.ActiveCache.ServerUID)
	require.NotNil(t, c.ActiveCache.Status.LastSyncedAt)
	assert.WithinDuration(t, now, *c.ActiveCache.Status.LastSyncedAt, time.Second)
}

// A cache whose UID does not match the cluster's last-probed identity (left behind
// by a physical migration) is listed in Caches but is not the ActiveCache.
func TestServiceGetInactiveCacheNotActive(t *testing.T) {
	ctx := context.Background()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	// Active identity is "new-uid"; an older cache for "old-uid" lingers.
	seedActiveCache(t, s, coreCC, id, "new-uid")
	_, err := s.cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: "old-uid"},
		beehive.WithSlug(ClusterCacheSlug(id, "old-uid")), beehive.WithOwner(beehive.ObjectID(id)))
	require.NoError(t, err)

	c, err := s.Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Len(t, c.Caches, 2)
	require.NotNil(t, c.ActiveCache)
	assert.Equal(t, "new-uid", c.ActiveCache.ServerUID)
	// Exactly one is enabled (the active identity's cache).
	enabledCount := 0
	for _, cc := range c.Caches {
		if cc.Enabled {
			enabledCount++
		}
	}
	assert.Equal(t, 1, enabledCount)
}

func TestServiceGetDeletionPendingIsNil(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	require.NoError(t, s.coreClient.Delete(ctx, obj.ID))

	c, err := s.Get(ctx, id)
	require.NoError(t, err)
	assert.Nil(t, c)
}

func TestServiceSetEnabled(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	c, err := s.SetEnabled(ctx, id, false)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.False(t, c.Spec.Enabled)

	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	assert.False(t, obj.Spec.Enabled)
}

func TestServiceSetSyncEnabled(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	c, err := s.SetSyncEnabled(ctx, id, false)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.False(t, c.Spec.SyncEnabled)

	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	assert.False(t, obj.Spec.SyncEnabled)
}

// RetryConnection dispatches an out-of-band re-probe to the controller without
// mutating the spec. The harness wires a fakeCoreController, so we pin both that
// the dispatch reaches Reprobe and that the spec is untouched; an unknown id
// errors before any dispatch.
func TestServiceRetryConnectionDoesNotMutateSpec(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	before, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)

	require.NoError(t, s.RetryConnection(ctx, id))

	after, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	assert.Equal(t, before.Generation, after.Generation, "RetryConnection must not write the spec")
	assert.Equal(t, before.Spec, after.Spec)
	assert.Equal(t, []ClusterID{id}, s.coreCtrl.(*fakeCoreController).reprobed, "retry must dispatch a reprobe")

	// An unknown id is ErrNotFound and dispatches nothing further.
	assert.ErrorIs(t, s.RetryConnection(ctx, ClusterID(999999)), ErrNotFound)
	assert.Equal(t, []ClusterID{id}, s.coreCtrl.(*fakeCoreController).reprobed, "unknown id must not reprobe")
}

func TestServiceClearCacheDeletesCacheAndReturnsCluster(t *testing.T) {
	ctx := context.Background()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	// cacheCtrl is nil in this white-box harness, so ClearCache deletes the
	// on-disk cache (a no-op here — none exists on disk) and returns the record
	// without restarting an engine. The engine-restart path is covered in
	// cache_controller_test.go.
	c, err := s.ClearCache(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, id, c.ID)

	stats, err := s.CacheStats(ctx, id, ClusterCacheID(cacheID))
	require.NoError(t, err)
	assert.False(t, stats.Exists)
}

// cacheRef resolves the *active* cache's on-disk locator: the directory id is the
// ClusterID (the parent Cluster's beehive ObjectID), and the file id is the
// ClusterCache for the cluster's currently-connected identity (its UID matches
// Status.Server.UID). A cluster with no active cache resolves to found=false.
func TestServiceCacheRefResolvesActiveCache(t *testing.T) {
	ctx := context.Background()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	ref, found, err := s.cacheRef(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, store.CacheRef{ClusterID: int64(id), CacheID: int64(cacheID)}, ref,
		"ref must be the parent Cluster + active ClusterCache ObjectIDs")

	// A cluster that has never probed (no Server.UID) has no active cache: no error.
	id2 := seedCluster(t, s, "beta")
	_, found2, err := s.cacheRef(ctx, id2)
	require.NoError(t, err)
	assert.False(t, found2)

	// A cluster whose only cache is for a migrated-away identity (UID != active) also
	// has no active cache.
	id3 := seedCluster(t, s, "gamma")
	stampActiveUID(t, s, coreCC, id3, "new-uid")
	_, err = s.cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: "old-uid"},
		beehive.WithSlug(ClusterCacheSlug(id3, "old-uid")), beehive.WithOwner(beehive.ObjectID(id3)))
	require.NoError(t, err)
	_, found3, err := s.cacheRef(ctx, id3)
	require.NoError(t, err)
	assert.False(t, found3)
}

func TestServiceDeleteTombstonesCluster(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)

	// Seed with a finalizer so the soft-delete tombstone is observable without a
	// race: beehive GC is a no-op while an object still holds a finalizer, so the
	// deletion-pending row lingers deterministically. Without it, the kind's (noop)
	// controller collects the finalizer-less, referrer-less row on the reconcile
	// pass that Delete enqueues — and that physical delete races the Get
	// below (passing locally, "object not found" under CI timing).
	name := "alpha"
	obj, err := s.coreClient.Create(ctx, ClusterSpec{
		Name:        &name,
		SyncEnabled: true,
		Enabled:     true,
		Source:      ClusterSpecSource{Kubeconfig: &ClusterSpecSourceKubeconfig{Context: "alpha"}},
	}, beehive.WithSlug(kubeconfigSlug("alpha")), beehive.WithFinalizers("test/hold"))
	require.NoError(t, err)
	id := ClusterID(obj.ID)

	require.NoError(t, s.Delete(ctx, id))

	// Delete tombstones the Cluster (soft delete); beehive GC then cascades to its
	// ClusterCache once the finalizers clear.
	obj, err = s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	assert.NotNil(t, obj.DeletionRequestedAt)
}

func TestServiceGetConnection(t *testing.T) {
	s, _, _ := newServiceTest(t)
	id := ClusterID(1)
	cfg := &rest.Config{Host: "https://127.0.0.1:6443"}

	// Nothing stored yet.
	assert.Nil(t, s.GetConnection(id))

	// After the connection manager is populated it is readable via the service.
	s.connMgr.Set(id, cfg)
	assert.Equal(t, cfg, s.GetConnection(id))
}

func TestServiceWatchEmitsSeedThenReemits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	ch, err := s.Watch(ctx)
	require.NoError(t, err)

	// Seed emission.
	seed := recvList(t, ch)
	assert.Len(t, seed, 1)

	// A spec change re-emits the full list. WatchList replays current state on
	// subscribe, so drain emissions until the change lands (this mirrors the
	// webview, which renders the latest full list).
	_, err = s.SetSyncEnabled(ctx, id, false)
	require.NoError(t, err)

	deadline := time.After(2 * time.Second)
	for {
		list := recvListBy(t, ch, deadline)
		require.Len(t, list, 1)
		if !list[0].Spec.SyncEnabled {
			return
		}
	}
}

func recvList(t *testing.T, ch <-chan []*Cluster) []*Cluster {
	t.Helper()
	return recvListBy(t, ch, time.After(2*time.Second))
}

func recvListBy(t *testing.T, ch <-chan []*Cluster, deadline <-chan time.Time) []*Cluster {
	t.Helper()
	select {
	case list := <-ch:
		return list
	case <-deadline:
		t.Fatal("timed out waiting for cluster list emission")
		return nil
	}
}
