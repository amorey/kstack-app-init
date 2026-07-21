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

// The active cluster's discovered kind catalog as a live list of `ServerKind`s — the
// shared source for both the dashboard nav (`useDashboardNav` builds its tree from these)
// and the per-kind object tables (which resolve a selected nav id back to its full
// `apiVersion`/`resource`/`scope` here — a curated id like "pods" carries no apiVersion on
// its own). The active kube-context resolves to a cluster (via the registry's kubeconfig
// source), and `clusterDataKindsWatch` streams that cache's catalog as a delta watch — an
// `Added` burst on subscribe, then per-kind `Added`/`Modified`/`Deleted` (a per-kind
// `count` is live, so an object write re-emits it as `Modified`).
//
// Reduction runs through the shared `useCacheDeltaWatch` (cache-aware provenance: a straggler
// from a superseded cache is dropped and the active cache's first frame after a swap starts
// fresh, so two caches' kinds never mix).
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

// One kind on the delta stream (the `kind` payload of a change) — structurally a
// `ServerKind` (apiVersion/kind/resource/scope/isCRD/count).
type KindRow = ClusterDataKindsWatchSubscriptionType['clusterDataKindsWatch']['kind'];

// The active context's discovered kinds, updated live. `kinds` is empty while
// clusters/kinds haven't loaded (no active cluster, or an unsynced one — it has no active
// cache, so the subscription is paused). `active` = the subscription is live (a cluster +
// active cache to stream from); `phase` classifies connecting vs. empty-snapshot for a
// spinner, mirroring `useClusterDataEvents`. This watch's variables carry no kind, so its
// provenance is just the cacheID (a cache swap under the same cluster moves the key and
// re-subscribes). urql dedupes the subscription, so `useDashboardNav` and an object table
// both consuming this share one transport. Kinds keep insertion order (no sort).
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
