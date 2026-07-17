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
// cluster (via the cluster registry's kubeconfig source), and `clusterDataKindsWatch`
// streams that cluster's kind catalog as a Kubernetes-style delta watch — the current
// catalog as an `Added` burst on subscribe, then per-kind `Added`/`Modified`/`Deleted`
// as the sync engine writes objects. Because each kind's `count` is live, an object
// write re-emits the kind as `Modified`, so the sidebar's kinds *and* their counts
// track the cluster in real time. Both the sidebar nav and the dashboard panel consume
// this, so they agree on the tree (urql dedupes the shared subscription).
//
// The catalog is populated by the sync engine's discovery pass, which may land *after*
// this first subscribes (an unsynced cluster has no active cache, so the subscription
// is paused → curated-only). Two things keep the nav accurate: the subscription is
// keyed by the active cache id, so a cache swap (repoint / server-UID switch) moves the
// key and re-subscribes to the new cache; and every frame carries its own cache id, so
// the reducer/guard tag the catalog by that provenance and drop anything not from the
// active cache — the prior cache's retained kinds, or a late frame from a superseded
// subscription — before the new cache's first frame lands.
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

// The reduced catalog: the kinds keyed by identity, tagged with the cache id the frames
// came from (their provenance, read off each frame — not inferred from render state).
// The tag is what lets both the reducer and the nav-build reject a previous cache's
// data that urql retains across a re-subscribe (a cache swap).
type Catalog = { cacheID: string; kinds: Keyed<KindRow> };

// The rendered dashboard nav for the active context: curated + the cluster's
// discovered kinds, updated live. Falls back to the curated-only tree while
// clusters/kinds haven't loaded (no active cluster, or an unsynced one — it has no
// active cache, so the subscription is paused).
export function useDashboardNav(): { nav: DashboardNavNode[] } {
  const { context } = useActiveKubeContext();
  const { clusters } = useClusters();

  // The active context's cluster and its active cache. Only kubeconfig-sourced records
  // carry a context, so match on that. The catalog lives in a specific cache, so the
  // subscription is keyed by (cluster id, cache id) — a cache swap under the same
  // cluster is a different cache id, which moves the key and re-subscribes on its own.
  const cluster = useMemo(
    () => clusters?.find((c) => c.spec.source.kubeconfig?.context === context),
    [clusters, context],
  );
  const clusterID = cluster?.id;
  const cacheID = cluster?.activeCache?.id;

  // Reduce the delta stream into a cache-tagged, id-keyed catalog: the `Added` snapshot
  // builds it, later deltas patch it. urql always invokes the latest handler, so the
  // reducer's `cacheID` closure is the currently-active cache. Each frame carries its
  // own provenance (`frameCacheID`), and the two discriminate the two cases a frame from
  // a "wrong" cache can be: a late straggler from a superseded subscription (urql keeps
  // the old stream alive until effect cleanup) is *dropped* — preserving the active
  // cache's accumulated catalog rather than wiping it — while the active cache's own
  // first frame after a swap (prev still holds the old cache) starts a fresh catalog so
  // the two caches' kinds never mix. A transport reconnect (same cacheID, full snapshot
  // replay) is handled one level down: useWatchSubscription resets the catalog to
  // `undefined` before the replay streams.
  const [{ data }] = useWatchSubscription(
    {
      query: ClusterDataKindsWatchSubscription,
      variables: { id: clusterID ?? '', cacheID: cacheID ?? '' },
      pause: !clusterID || !cacheID,
    },
    (prev: Catalog | undefined, res) => {
      const { type, kind, cacheID: frameCacheID } = res.clusterDataKindsWatch;
      // Drop a frame that isn't from the active cache — a late straggler from the old
      // subscription — leaving the active cache's accumulated catalog untouched. (Once
      // the active cache has streamed, this is what keeps a straggler from resetting the
      // catalog to a singleton and losing the rest of the snapshot for good.) With no
      // catalog yet, seed an empty one tagged for the active cache so the reducer never
      // yields `undefined` (the build guard renders it curated-only until a real frame).
      if (frameCacheID !== cacheID) return prev ?? { cacheID: cacheID ?? '', kinds: new Map() };
      // The frame is from the active cache. If prev is still the old cache (the first
      // frame after a swap), start fresh; otherwise patch the accumulated catalog.
      const kinds = prev && prev.cacheID === frameCacheID ? prev.kinds : undefined;
      return { cacheID: frameCacheID, kinds: applyChange(kinds, type, kindKey(kind), kind) };
    },
  );

  // Build only from a catalog whose provenance tag matches the active cache — urql
  // retains the previous cache's accumulated `data` across a swap (and may deliver a
  // late frame from it), so reject any catalog not tagged for the active cache
  // (curated-only) rather than leaking stale kinds.
  const nav = useMemo(
    () => buildDashboardNav(data && cacheID && data.cacheID === cacheID ? [...data.kinds.values()] : []),
    [cacheID, data],
  );
  return { nav };
}
