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
// The reducer is cache-aware in the same way as `useClusterDataEvents`: each frame carries
// its cache id, so a straggler from a superseded subscription is dropped and the active
// cache's own first frame after a swap starts fresh — two caches' kinds never mix. urql
// dedupes the underlying subscription, so `useDashboardNav` and an object table both
// consuming this share one transport.
import { useMemo } from 'react';

import { graphql } from '@/gql';
import type { ClusterDataKindsWatchSubscription as ClusterDataKindsWatchSubscriptionType } from '@/gql/graphql';
import { useActiveKubeContext } from '@/lib/active-kube-context';
import { useClusters, applyChange } from '@/lib/clusters';
import type { Keyed } from '@/lib/clusters';
import type { ServerKind } from '@/lib/dashboard-resources';
import { useWatchSubscription, watchPhase } from '@/lib/graphql/use-watch-subscription';
import type { WatchPhase } from '@/lib/graphql/use-watch-subscription';

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

// A kind's identity within a catalog: apiVersion + resource is unique per cache,
// matching the sidecar's diff key — so a `Modified`/`Deleted` targets the right entry.
function kindKey(k: { apiVersion: string; resource: string }): string {
  return `${k.apiVersion}/${k.resource}`;
}

// The reduced catalog: kinds keyed by identity, tagged with the cache id the frames came
// from (read off each frame, not inferred from render state). The tag lets the reducer and
// readers reject a previous cache's data that urql retains across a swap.
type Catalog = { cacheID: string; kinds: Keyed<KindRow> };

// The active context's discovered kinds, updated live. `kinds` is empty while
// clusters/kinds haven't loaded (no active cluster, or an unsynced one — it has no active
// cache, so the subscription is paused). `active` = the subscription is live (a cluster +
// active cache to stream from); `phase` classifies connecting vs. empty-snapshot for a
// spinner, mirroring `useClusterDataEvents`.
export function useClusterDataKinds(): { kinds: ServerKind[]; active: boolean; phase: WatchPhase } {
  const { context } = useActiveKubeContext();
  const { clusters } = useClusters();

  // The active context's cluster and its active cache. Only kubeconfig-sourced records
  // carry a context, so match on that. The subscription is keyed by (cluster id, cache
  // id), so a cache swap under the same cluster moves the key and re-subscribes.
  const cluster = useMemo(
    () => clusters?.find((c) => c.spec.source.kubeconfig?.context === context),
    [clusters, context],
  );
  const clusterID = cluster?.id;
  const cacheID = cluster?.activeCache?.id;

  // Reduce the delta stream into a cache-tagged, id-keyed catalog. Comparing the reducer's
  // active `cacheID` closure against each frame's provenance (`frameCacheID`) discriminates
  // the two "wrong cache" cases: a late straggler from a superseded subscription is dropped
  // (leaving the active cache's catalog untouched), while the active cache's own first frame
  // after a swap starts a fresh catalog so the two never mix. A transport reconnect (same
  // cacheID, full replay) is handled by useWatchSubscription resetting to `undefined`.
  const [{ data, connected }] = useWatchSubscription(
    {
      query: ClusterDataKindsWatchSubscription,
      variables: { id: clusterID ?? '', cacheID: cacheID ?? '' },
      pause: !clusterID || !cacheID,
    },
    (prev: Catalog | undefined, res) => {
      const { type, kind, cacheID: frameCacheID } = res.clusterDataKindsWatch;
      if (frameCacheID !== cacheID) return prev ?? { cacheID: cacheID ?? '', kinds: new Map() };
      const kinds = prev && prev.cacheID === frameCacheID ? prev.kinds : undefined;
      return { cacheID: frameCacheID, kinds: applyChange(kinds, type, kindKey(kind), kind) };
    },
  );

  // The accumulated catalog, but only when it's tagged for the active cache — urql retains
  // the previous cache's `data` across a swap, so reject anything not tagged for it. This
  // one guard feeds both the returned kinds and the watch phase.
  const activeCatalog = data && cacheID && data.cacheID === cacheID ? data.kinds : undefined;
  const kinds = useMemo(() => (activeCatalog ? [...activeCatalog.values()] : []), [activeCatalog]);

  const active = !!(clusterID && cacheID);
  return { kinds, active, phase: watchPhase(!!activeCatalog, connected) };
}
