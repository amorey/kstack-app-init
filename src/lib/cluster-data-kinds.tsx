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

// The active cluster's discovered kind catalog as a live `ServerKind[]` — shared by
// the dashboard nav and the object tables (which resolve a curated nav id like "pods"
// back to its full apiVersion/resource/scope here). `clusterDataKindsWatch` is a
// per-cache delta watch; per-kind `count` is live (an object write re-emits as
// `Modified`). Reduced via `useCacheDeltaWatch`, whose cacheID provenance guard keeps
// two caches' kinds from mixing — see docs/adr/2026-08-09-delta-watch-protocol.md
import { graphql } from '@/gql';
import type { ClusterDataKindsWatchSubscription as ClusterDataKindsWatchSubscriptionType } from '@/gql/graphql';
import { useActiveCluster } from '@/lib/active-cluster';
import type { ServerKind } from '@/lib/dashboard-resources';
import { useCacheDeltaWatch, joinProvenance } from '@/lib/graphql/use-cache-delta-watch';
import type { WatchPhase } from '@/lib/graphql/use-watch-subscription';
import { gvrKey } from '@/lib/gvr';

const ClusterDataKindsWatchSubscription = graphql(`
  subscription ClusterDataKindsWatch($id: ObjectID!, $cacheID: ObjectID!) {
    clusterDataKindsWatch(id: $id, cacheID: $cacheID) {
      type
      cacheID
      kind {
        apiVersion
        kind
        resource
        scope
        isCRD
        count
      }
    }
  }
`);

// The `kind` payload of a change — structurally a `ServerKind`.
type KindRow = ClusterDataKindsWatchSubscriptionType['clusterDataKindsWatch']['kind'];

// The active context's discovered kinds, live. Paused (empty) without an active
// cache. Provenance is just the cacheID — the variables carry no kind — so a cache
// swap moves the key and re-subscribes. urql dedupes: `useDashboardNav` and an object
// table share one transport. Insertion order (no sort).
export function useClusterDataKinds(): { kinds: ServerKind[]; active: boolean; phase: WatchPhase } {
  const { clusterID, cacheID, active } = useActiveCluster();

  const { items, phase } = useCacheDeltaWatch<ClusterDataKindsWatchSubscriptionType, KindRow>(
    {
      query: ClusterDataKindsWatchSubscription,
      variables: { id: clusterID ?? '', cacheID: cacheID ?? '' },
      pause: !active,
    },
    {
      select: (d) => {
        const f = d.clusterDataKindsWatch;
        return { type: f.type, entity: f.kind, provenance: joinProvenance(f.cacheID) };
      },
      keyOf: gvrKey,
      currentProvenance: joinProvenance(cacheID ?? ''),
    },
  );

  return { kinds: items, active, phase };
}
