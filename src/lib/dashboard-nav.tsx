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

// The live dashboard resource tree: the curated base (`DASHBOARD_NAV`) merged with
// the active cluster's discovered kinds. The active kube-context resolves to a
// cluster (via the registry's kubeconfig source), and `clusterDataKindsWatch` streams
// that cluster's kind catalog as a delta watch — an `Added` burst on subscribe, then
// per-kind `Added`/`Modified`/`Deleted` as the sync engine writes objects. Each kind's
// `count` is live (an object write re-emits it as `Modified`), so kinds and counts
// track the cluster in real time. Both the sidebar nav and the dashboard panel consume
// this and agree on the tree (urql dedupes the shared subscription).
//
// The catalog is filled by the sync engine's discovery pass, which may land after this
// subscribes (an unsynced cluster has no active cache, so the subscription is paused →
// curated-only). The subscription is keyed by the active cache id, so a cache swap
// (repoint / server-UID switch) moves the key and re-subscribes; and every frame
// carries its own cache id, so the reducer/guard tag the catalog by that provenance and
// drop anything not from the active cache — the prior cache's retained kinds, or a late
// straggler from a superseded subscription.
import { useMemo } from 'react';

import { graphql } from '@/gql';
import type { ClusterDataKindsWatchSubscription as ClusterDataKindsWatchSubscriptionType } from '@/gql/graphql';
import { useActiveKubeContext } from '@/lib/active-kube-context';
import { useClusters, applyChange } from '@/lib/clusters';
import type { Keyed } from '@/lib/clusters';
import { buildDashboardNav } from '@/lib/dashboard-resources';
import type { DashboardNavNode } from '@/lib/dashboard-resources';
import { useWatchSubscription } from '@/lib/graphql/use-watch-subscription';

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

// One kind on the delta stream (the `kind` payload of a change) — carries the
// `ServerKind` fields `buildDashboardNav` consumes.
type KindRow = ClusterDataKindsWatchSubscriptionType['clusterDataKindsWatch']['kind'];

// A kind's identity within a catalog: apiVersion + resource is unique per cache,
// matching the sidecar's diff key — so a `Modified`/`Deleted` targets the right entry.
function kindKey(k: { apiVersion: string; resource: string }): string {
  return `${k.apiVersion}/${k.resource}`;
}

// The reduced catalog: kinds keyed by identity, tagged with the cache id the frames
// came from (read off each frame, not inferred from render state). The tag lets the
// reducer and nav-build reject a previous cache's data that urql retains across a swap.
type Catalog = { cacheID: string; kinds: Keyed<KindRow> };

// The rendered dashboard nav for the active context: curated + the cluster's
// discovered kinds, updated live. Falls back to the curated-only tree while
// clusters/kinds haven't loaded (no active cluster, or an unsynced one — it has no
// active cache, so the subscription is paused).
export function useDashboardNav(): { nav: DashboardNavNode[] } {
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

  // Reduce the delta stream into a cache-tagged, id-keyed catalog: the `Added` snapshot
  // builds it, later deltas patch it. urql invokes the latest handler, so the reducer's
  // `cacheID` closure is the active cache. Comparing it against each frame's provenance
  // (`frameCacheID`) discriminates the two "wrong cache" cases: a late straggler from a
  // superseded subscription (urql keeps the old stream alive until cleanup) is dropped,
  // preserving the active cache's catalog; while the active cache's own first frame after
  // a swap (prev still holds the old cache) starts a fresh catalog so the two never mix.
  // A transport reconnect (same cacheID, full replay) is handled one level down:
  // useWatchSubscription resets the catalog to `undefined` before the replay streams.
  const [{ data }] = useWatchSubscription(
    {
      query: ClusterDataKindsWatchSubscription,
      variables: { id: clusterID ?? '', cacheID: cacheID ?? '' },
      pause: !clusterID || !cacheID,
    },
    (prev: Catalog | undefined, res) => {
      const { type, kind, cacheID: frameCacheID } = res.clusterDataKindsWatch;
      // Drop a straggler from the old subscription, leaving the active cache's catalog
      // untouched. With no catalog yet, seed an empty one tagged for the active cache so
      // the reducer never yields `undefined` (the build guard keeps it curated-only).
      if (frameCacheID !== cacheID) return prev ?? { cacheID: cacheID ?? '', kinds: new Map() };
      // From the active cache. If prev is still the old cache (first frame after a swap),
      // start fresh; otherwise patch the accumulated catalog.
      const kinds = prev && prev.cacheID === frameCacheID ? prev.kinds : undefined;
      return { cacheID: frameCacheID, kinds: applyChange(kinds, type, kindKey(kind), kind) };
    },
  );

  // Build only from a catalog tagged for the active cache — urql retains the previous
  // cache's `data` across a swap, so reject anything not tagged for the active cache
  // (curated-only) rather than leaking stale kinds.
  const nav = useMemo(
    () => buildDashboardNav(data && cacheID && data.cacheID === cacheID ? [...data.kinds.values()] : []),
    [cacheID, data],
  );
  return { nav };
}
