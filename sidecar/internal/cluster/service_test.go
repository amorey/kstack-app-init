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

// noopController satisfies beehive.Controller without reconciling — it just
// captures its ControllerClient so a test can write status directly (the plain
// Client cannot).
type noopController[Spec, Status any] struct {
	client beehive.ControllerClient[Status]
}

func (c *noopController[Spec, Status]) Start(cl beehive.ControllerClient[Status]) error {
	c.client = cl
	return nil
}
func (c *noopController[Spec, Status]) Stop(context.Context) error { return nil }
func (c *noopController[Spec, Status]) Reconcile(context.Context, *beehive.Object[Spec, Status]) (beehive.Result, error) {
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

	clusterClient := beehive.NewClient[ClusterSpec, ClusterConnectionStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)

	cacheNoop := &noopController[ClusterCacheSpec, ClusterCacheStatus]{}
	require.NoError(t, beehive.Register(bh, ClusterGroupKind, &noopController[ClusterSpec, ClusterConnectionStatus]{}))
	require.NoError(t, beehive.Register(bh, ClusterCacheGroupKind, cacheNoop))

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	return &Service{
		clusterClient: clusterClient,
		cacheClient:   cacheClient,
		cacheManager:  store.NewManager(t.TempDir()),
		connMgr:       NewConnectionManager(),
	}, cacheNoop.client
}

// seedCluster creates a Cluster (as the importer would) and returns its
// ClusterID.
func seedCluster(t *testing.T, s *Service, ctxName string, id ClusterID) ClusterID {
	t.Helper()
	ctx := context.Background()
	name := ctxName
	_, err := s.clusterClient.Create(ctx, ClusterSpec{
		Name:          &name,
		IsSyncEnabled: true,
		IsActive:      true,
		Source:        ClusterSource{Kubeconfig: &ClusterSourceKubeconfig{Context: ctxName}},
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
	require.NotNil(t, c.Status.SyncStatus.LastSyncedAt)
	assert.WithinDuration(t, now, *c.Status.SyncStatus.LastSyncedAt, time.Second)
}

func TestServiceGetDeletionPendingIsNil(t *testing.T) {
	ctx := context.Background()
	s, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha", "id-alpha")

	obj, err := s.clusterClient.GetBySlug(ctx, ClusterSlug(id))
	require.NoError(t, err)
	require.NoError(t, s.clusterClient.Delete(ctx, obj.ID))

	c, err := s.Get(ctx, id)
	require.NoError(t, err)
	assert.Nil(t, c)
}

func TestServiceSetSyncEnabled(t *testing.T) {
	ctx := context.Background()
	s, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha", "id-alpha")

	c, err := s.SetSyncEnabled(ctx, id, false)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.False(t, c.Spec.IsSyncEnabled)

	obj, err := s.clusterClient.GetBySlug(ctx, ClusterSlug(id))
	require.NoError(t, err)
	assert.False(t, obj.Spec.IsSyncEnabled)
}

func TestServiceRetryConnectionBumpsGeneration(t *testing.T) {
	ctx := context.Background()
	s, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha", "id-alpha")

	require.NoError(t, s.RetryConnection(ctx, id))

	obj, err := s.clusterClient.GetBySlug(ctx, ClusterSlug(id))
	require.NoError(t, err)
	assert.Equal(t, int64(1), obj.Spec.RetryGeneration)
}

func TestServiceClearCacheBumpsPokeGeneration(t *testing.T) {
	ctx := context.Background()
	s, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha", "id-alpha")

	c, err := s.ClearCache(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, c)

	obj, err := s.clusterClient.GetBySlug(ctx, ClusterSlug(id))
	require.NoError(t, err)
	assert.Equal(t, int64(1), obj.Spec.PokeSyncGeneration)
}

func TestServiceDeleteTombstonesCluster(t *testing.T) {
	ctx := context.Background()
	s, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha", "id-alpha")

	require.NoError(t, s.Delete(ctx, id))

	// Delete tombstones the Cluster (soft delete); beehive GC then cascades to
	// its ClusterCache.
	obj, err := s.clusterClient.GetBySlug(ctx, ClusterSlug(id))
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
		if !list[0].Spec.IsSyncEnabled {
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
