// Copyright 2026 The Kubetail Authors
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

// The active cluster's cached objects of one kind as a live table feed.
// `clusterCachedDataObjectsWatch` is keyed by the active cache plus the kind
// (apiVersion + resource); each object carries its typed universal identity plus
// `rawJSON`, the full native body (kind-specific columns derive from it — see
// ObjectsTable). Provenance is the FULL triple cacheID + apiVersion + resource —
// this watch is keyed by kind too, so cacheID alone would let the previous kind's
// stragglers leak across a resource switch within one cache.
// See docs/adr/2026-08-09-delta-watch-protocol.md
import { useMemo } from 'react';

import { graphql } from '@/gql';
import type { ClusterCachedDataObjectsWatchSubscription as ClusterCachedDataObjectsWatchSubscriptionType } from '@/gql/graphql';
import { useActiveCluster } from '@/lib/active-cluster';
import { useCacheDeltaWatch, joinProvenance } from '@/lib/graphql/use-cache-delta-watch';
import type { WatchPhase } from '@/lib/graphql/use-watch-subscription';
import { gvrKey } from '@/lib/gvr';
import type { GVR } from '@/lib/gvr';

const ClusterCachedDataObjectsWatchSubscription = graphql(`
  subscription ClusterCachedDataObjectsWatch(
    $id: ObjectID!
    $cacheID: ObjectID!
    $apiVersion: String!
    $resource: String!
  ) {
    clusterCachedDataObjectsWatch(id: $id, cacheID: $cacheID, apiVersion: $apiVersion, resource: $resource) {
      type
      cacheID
      apiVersion
      resource
      object {
        uid
        apiVersion
        kind
        namespace
        name
        creationTimestamp
        rawJSON
      }
    }
  }
`);

// One cached object (the `object` payload of a change), as the table renders a row from.
export type ClusterCachedDataObject = NonNullable<
  ClusterCachedDataObjectsWatchSubscriptionType['clusterCachedDataObjectsWatch']['object']
>;

// Hoisted: the list re-sorts on every delta frame, and per-call `localeCompare`
// re-derives a collator each time.
const COLLATOR = new Intl.Collator();

// The active context's cached objects of `kind`, live. Paused (empty) without an
// active cache. Provenance must carry the full cacheID + apiVersion + resource key
// (see file header).
export function useClusterCachedDataObjects(kind: GVR): {
  objects: ClusterCachedDataObject[];
  active: boolean;
  phase: WatchPhase;
} {
  const { clusterID, cacheID, active } = useActiveCluster();

  const { items, phase } = useCacheDeltaWatch<ClusterCachedDataObjectsWatchSubscriptionType, ClusterCachedDataObject>(
    {
      query: ClusterCachedDataObjectsWatchSubscription,
      variables: { id: clusterID ?? '', cacheID: cacheID ?? '', apiVersion: kind.apiVersion, resource: kind.resource },
      pause: !active,
    },
    {
      select: (d) => {
        const f = d.clusterCachedDataObjectsWatch;
        return { type: f.type, entity: f.object, provenance: joinProvenance(f.cacheID, gvrKey(f)) };
      },
      keyOf: (o) => o.uid,
      currentProvenance: joinProvenance(cacheID ?? '', gvrKey(kind)),
    },
  );

  // Sort by (namespace, name) so the table order is stable across delta churn.
  const objects = useMemo(
    () => [...items].sort((a, b) => COLLATOR.compare(a.namespace, b.namespace) || COLLATOR.compare(a.name, b.name)),
    [items],
  );

  return { objects, active, phase };
}
