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

// White-box: the service test seeds beehive objects directly and exercises the
// data/mutation/watch surface in isolation from the (network-touching) real
// controllers, so it lives in package cluster (it cannot use the testutil
// helpers, which import this package).
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

// newServiceTest builds a started beehive with no-op controllers and returns a
// service wired to its clients plus a temp cache manager. The returned
// ControllerClient writes ClusterCache status (the controller-owned surface).
func newServiceTest(t *testing.T) (*Service, beehive.ControllerClient[ClusterCacheStatus]) {
	t.Helper()
	st, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	bh, err := beehive.New(st, beehive.WithResyncInterval(0))
	require.NoError(t, err)

	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)

	_, err = beehive.Register(bh, ClusterGroupKind, &noopController[ClusterSpec, ClusterStatus]{})
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
	}, cacheCC
}

// seedCluster creates a Cluster (as the importer would) and returns its
// ClusterID.
func seedCluster(t *testing.T, s *Service, ctxName string, id ClusterID) ClusterID {
	t.Helper()
	ctx := context.Background()
	name := ctxName
	_, err := s.coreClient.Create(ctx, ClusterSpec{
		Name:        &name,
		SyncEnabled: true,
		Enabled:     true,
		Source:      ClusterSource{Kubeconfig: &ClusterSourceKubeconfig{Context: ctxName}},
	}, beehive.WithSlug(ClusterSlug(id)))
	require.NoError(t, err)
	return id
}

func TestServiceListAndGet(t *testing.T) {
	ctx := context.Background()
	s, _ := newServiceTest(t)
	seedCluster(t, s, "alpha", "id-alpha")
	seedCluster(t, s, "beta", "id-beta")

	list, err := s.List(ctx)
	require.NoError(t, err)
	assert.Len(t, list, 2)

	c, err := s.Get(ctx, "id-alpha")
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, ClusterID("id-alpha"), c.ID)
	require.NotNil(t, c.Spec.Name)
	assert.Equal(t, "alpha", *c.Spec.Name)

	// Unknown id is (nil, nil), not an error.
	missing, err := s.Get(ctx, "nope")
	require.NoError(t, err)
	assert.Nil(t, missing)
}

func TestServiceGetJoinsSyncStatus(t *testing.T) {
	ctx := context.Background()
	s, cacheCtl := newServiceTest(t)
	id := seedCluster(t, s, "alpha", "id-alpha")

	now := time.Now().UTC()
	_, err := s.cacheClient.Create(ctx, ClusterCacheSpec{}, beehive.WithSlug(ClusterCacheSlug(id)))
	require.NoError(t, err)
	// Give the ClusterCache a Synced status to join in.
	cacheObj, err := s.cacheClient.GetBySlug(ctx, ClusterCacheSlug(id))
	require.NoError(t, err)
	require.NoError(t, cacheCtl.UpdateStatus(ctx, cacheObj.ID, cacheObj.Generation, ClusterCacheStatus{
		LastSyncedAt: &now,
	}))

	c, err := s.Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.NotNil(t, c.Cache.Status.LastSyncedAt)
	assert.WithinDuration(t, now, *c.Cache.Status.LastSyncedAt, time.Second)
}

func TestServiceGetDeletionPendingIsNil(t *testing.T) {
	ctx := context.Background()
	s, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha", "id-alpha")

	obj, err := s.coreClient.GetBySlug(ctx, ClusterSlug(id))
	require.NoError(t, err)
	require.NoError(t, s.coreClient.Delete(ctx, obj.ID))

	c, err := s.Get(ctx, id)
	require.NoError(t, err)
	assert.Nil(t, c)
}

func TestServiceSetEnabled(t *testing.T) {
	ctx := context.Background()
	s, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha", "id-alpha")

	c, err := s.SetEnabled(ctx, id, false)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.False(t, c.Spec.Enabled)

	obj, err := s.coreClient.GetBySlug(ctx, ClusterSlug(id))
	require.NoError(t, err)
	assert.False(t, obj.Spec.Enabled)
}

func TestServiceSetSyncEnabled(t *testing.T) {
	ctx := context.Background()
	s, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha", "id-alpha")

	c, err := s.SetSyncEnabled(ctx, id, false)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.False(t, c.Spec.SyncEnabled)

	obj, err := s.coreClient.GetBySlug(ctx, ClusterSlug(id))
	require.NoError(t, err)
	assert.False(t, obj.Spec.SyncEnabled)
}

// RetryConnection does not mutate the spec: it dispatches an out-of-band re-probe
// to the controller. The white-box harness has coreCtrl == nil, so the actual
// re-probe is covered in core_controller_test.go; here we pin that the spec is
// untouched and an unknown id still errors.
func TestServiceRetryConnectionDoesNotMutateSpec(t *testing.T) {
	ctx := context.Background()
	s, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha", "id-alpha")

	before, err := s.coreClient.GetBySlug(ctx, ClusterSlug(id))
	require.NoError(t, err)

	require.NoError(t, s.RetryConnection(ctx, id))

	after, err := s.coreClient.GetBySlug(ctx, ClusterSlug(id))
	require.NoError(t, err)
	assert.Equal(t, before.Generation, after.Generation, "RetryConnection must not write the spec")
	assert.Equal(t, before.Spec, after.Spec)

	// An unknown id is still ErrNotFound.
	assert.ErrorIs(t, s.RetryConnection(ctx, "nope"), ErrNotFound)
}

func TestServiceClearCacheDeletesCacheAndReturnsCluster(t *testing.T) {
	ctx := context.Background()
	s, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha", "id-alpha")

	// cacheCtrl is nil in this white-box harness, so ClearCache deletes the
	// on-disk cache (a no-op here — none exists) and returns the record without
	// restarting an engine. The engine-restart path is covered in
	// cache_controller_test.go.
	c, err := s.ClearCache(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, id, c.ID)

	stats, err := s.CacheStats(ctx, id)
	require.NoError(t, err)
	assert.False(t, stats.Exists)
}

func TestServiceDeleteTombstonesCluster(t *testing.T) {
	ctx := context.Background()
	s, _ := newServiceTest(t)

	// Seed with a finalizer so the soft-delete tombstone is observable without a
	// race: beehive GC is a no-op while an object still holds a finalizer, so the
	// deletion-pending row lingers deterministically. Without it, the kind's (noop)
	// controller collects the finalizer-less, referrer-less row on the reconcile
	// pass that Delete enqueues — and that physical delete races the GetBySlug
	// below (passing locally, "object not found" under CI timing).
	id := ClusterID("id-alpha")
	name := "alpha"
	_, err := s.coreClient.Create(ctx, ClusterSpec{
		Name:        &name,
		SyncEnabled: true,
		Enabled:     true,
		Source:      ClusterSource{Kubeconfig: &ClusterSourceKubeconfig{Context: "alpha"}},
	}, beehive.WithSlug(ClusterSlug(id)), beehive.WithFinalizers("test/hold"))
	require.NoError(t, err)

	require.NoError(t, s.Delete(ctx, id))

	// Delete tombstones the Cluster (soft delete); beehive GC then cascades to its
	// ClusterCache once the finalizers clear.
	obj, err := s.coreClient.GetBySlug(ctx, ClusterSlug(id))
	require.NoError(t, err)
	assert.NotNil(t, obj.DeletionRequestedAt)
}

func TestServiceGetConnection(t *testing.T) {
	s, _ := newServiceTest(t)
	id := ClusterID("abc")
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
	s, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha", "id-alpha")

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
