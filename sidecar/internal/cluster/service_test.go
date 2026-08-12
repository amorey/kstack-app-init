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

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// RetryConnection dispatches an out-of-band re-probe without mutating the spec. Via the
// fakeCoreController we pin that the dispatch reaches Reprobe and the spec is untouched;
// an unknown id errors before any dispatch.
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
	assert.Equal(t, []domain.ClusterID{id}, s.coreCtrl.(*fakeCoreController).reprobed, "retry must dispatch a reprobe")

	// An unknown id is ErrNotFound and dispatches nothing further.
	assert.ErrorIs(t, s.RetryConnection(ctx, domain.ClusterID(999999)), domain.ErrNotFound)
	assert.Equal(t, []domain.ClusterID{id}, s.coreCtrl.(*fakeCoreController).reprobed, "unknown id must not reprobe")
}

// cacheRef resolves the active cache's on-disk locator: the directory id is the
// ClusterID, the file id is the ClusterCache for the cluster's currently-connected
// identity (UID matches Status.Server.UID). A cluster with no active cache resolves to
// found=false.
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
	_, err = s.cacheClient.Create(ctx, domain.ClusterCacheName(id3, "old-uid"), domain.ClusterCacheSpec{ServerUID: "old-uid"},
		beehive.WithOwner(beehive.ObjectID(id3)))
	require.NoError(t, err)
	_, found3, err := s.cacheRef(ctx, id3)
	require.NoError(t, err)
	assert.False(t, found3)
}

func TestServiceGetConnection(t *testing.T) {
	s, _, _ := newServiceTest(t)
	id := domain.ClusterID(1)
	cfg := &rest.Config{Host: "https://127.0.0.1:6443"}

	// Nothing stored yet.
	assert.Nil(t, s.GetConnection(id))

	// After the connection manager is populated it is readable via the service.
	s.connMgr.Set(id, cfg, "fp")
	assert.Equal(t, cfg, s.GetConnection(id))
}
