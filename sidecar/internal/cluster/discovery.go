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

package cluster

import (
	"context"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// Watch implements Discovery — the discovery counterpart of Caches().Watch,
// parent CacheID resolved from the owner edge.
func (a discoveryAPI) Watch(ctx context.Context) (<-chan domain.ClusterCacheGVRDiscoveryWatchFrame, error) {
	snap, src, err := a.s.gvrDiscoveryClient.WatchList(ctx, beehive.WithLoads(beehive.LoadOwner()))
	if err != nil {
		return nil, err
	}
	return watchListChan(ctx, "ClusterCacheGVRDiscovery", snap, src,
		func(t domain.FrameType, id beehive.ObjectID, obj *beehive.Object[domain.ClusterCacheGVRDiscoverySpec, domain.ClusterCacheGVRDiscoveryStatus]) domain.ClusterCacheGVRDiscoveryWatchFrame {
			if t == domain.FrameBookmark {
				return domain.ClusterCacheGVRDiscoveryWatchFrame{Type: t}
			}
			if obj == nil {
				return domain.ClusterCacheGVRDiscoveryWatchFrame{Type: t, Discovery: &domain.ClusterCacheGVRDiscovery{ID: domain.ClusterCacheGVRDiscoveryID(id)}}
			}
			d := buildGVRDiscovery(obj)
			return domain.ClusterCacheGVRDiscoveryWatchFrame{Type: t, Discovery: &d}
		}), nil
}

// buildGVRDiscovery assembles a domain ClusterCacheGVRDiscovery; parent CacheID
// comes off the eager-loaded owner edge.
func buildGVRDiscovery(obj *beehive.Object[domain.ClusterCacheGVRDiscoverySpec, domain.ClusterCacheGVRDiscoveryStatus]) domain.ClusterCacheGVRDiscovery {
	return domain.ClusterCacheGVRDiscovery{
		ID:         domain.ClusterCacheGVRDiscoveryID(obj.ID),
		CacheID:    domain.ClusterCacheID(ownerObjectID(obj)),
		Spec:       obj.Spec,
		Conditions: obj.Conditions,
	}
}

// GetStats implements Discovery — the discovery record's live gauges, read
// straight from the controller that measured them. A nil controller (a test
// service with no control plane) reads as "nothing measured", the same as a
// record whose first pass hasn't run.
func (a discoveryAPI) GetStats(_ context.Context, id domain.ClusterCacheGVRDiscoveryID) (*domain.ClusterCacheGVRDiscoveryStats, error) {
	if a.s.gvrDiscoveryCtrl == nil {
		return nil, nil
	}
	st, ok := a.s.gvrDiscoveryCtrl.Stats(beehive.ObjectID(id))
	if !ok {
		return nil, nil
	}
	return &st, nil
}
