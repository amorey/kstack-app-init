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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// The GVR-discovery watch is a standalone delta stream of
// the cache's other sync child, with the parent CacheID resolved from the owner edge. It
// carries identity, spec and conditions only — the pass's gauges are served out of band
// (GVRDiscoveryStats), so there is no status on the record to assert.
func TestServiceWatchGVRDiscoveriesEmitsChildren(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _, _, discCC := newServiceTestSync(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	child, err := s.gvrDiscoveryClient.Create(ctx, domain.ClusterCacheGVRDiscoveryName(cacheID),
		domain.ClusterCacheGVRDiscoverySpec{Enabled: true}, beehive.WithOwner(cacheID))
	require.NoError(t, err)

	require.NoError(t, discCC.SetConditions(ctx, child.ID, []domain.Condition{
		domain.LiveCondition(domain.ConditionDiscovered, domain.ConditionTrue, domain.ReasonDiscovered, ""),
	}))

	ch, err := s.Discovery().Watch(ctx)
	require.NoError(t, err)

	// WatchList replays current state on subscribe (conflated per object), so drain
	// Added changes until the child's verdict lands.
	deadline := time.After(2 * time.Second)
	for {
		ev := recvBy(t, ch, deadline)
		assert.Equal(t, domain.ChangeAdded, ev.Type)
		require.NotNil(t, ev.Discovery)
		assert.Equal(t, domain.ClusterCacheGVRDiscoveryID(child.ID), ev.Discovery.ID)
		assert.Equal(t, domain.ClusterCacheID(cacheID), ev.Discovery.CacheID) // resolved from the owner edge
		assert.True(t, ev.Discovery.Spec.Enabled)
		cond := domain.FindCondition(ev.Discovery.Conditions, domain.ConditionDiscovered)
		if cond != nil {
			assert.Equal(t, domain.ReasonDiscovered, cond.Reason)
			return
		}
	}
}
