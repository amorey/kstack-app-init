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
// cluster (via the cluster registry's kubeconfig source), and `clusterDataKinds` reads
// that cluster's kind catalog on demand — the query re-runs whenever the active
// context (hence the cluster id) changes, so the sidebar's discovered kinds track
// the selected cluster. Both the sidebar nav and the dashboard panel consume this, so
// they agree on the tree (urql dedupes the shared query).
//
// The catalog is populated by the sync engine's discovery pass, which may land
// *after* this query first runs (an unsynced cluster returns an empty list). Two
// things keep the nav from getting stuck on that empty snapshot: the query uses
// `cache-and-network` (so revisiting a cluster revalidates rather than reusing a
// cached empty result), and it re-executes whenever the active cache's `Synced`
// condition transitions — which is when discovery has (re)populated the catalog.
// Since that condition only changes on real sync transitions (not the ~30s
// freshness heartbeat), this reacts without polling. (A future `clusterDataKindsWatch`
// subscription would make this fully live — see the Custom Resources work.)
import { useEffect, useMemo, useRef } from 'react';
import { useQuery } from 'urql';

import { graphql } from '@/gql';
import { useActiveKubeContext } from '@/lib/active-kube-context';
import { useClusters } from '@/lib/clusters';
import { buildDashboardNav } from '@/lib/dashboard-resources';
import type { DashboardNavNode } from '@/lib/dashboard-resources';

const ClusterDataKindsQuery = graphql(`
  query ClusterDataKinds($id: ObjectID!, $cacheID: ObjectID!) {
    clusterDataKinds(id: $id, cacheID: $cacheID) {
      apiVersion
      kind
      resource
      scope
      isCRD
    }
  }
`);

// The rendered dashboard nav for the active context: curated + the cluster's
// discovered kinds. Falls back to the curated-only tree while clusters/kinds haven't
// loaded (no active cluster, or an unsynced one — it has no active cache / catalog).
export function useDashboardNav(): { nav: DashboardNavNode[] } {
  const { context } = useActiveKubeContext();
  const { clusters } = useClusters();

  // The active context's cluster and its active cache. Only kubeconfig-sourced
  // records carry a context, so match on that. The catalog lives in a specific cache
  // (like CacheStats), so the query is keyed by (cluster id, cache id) — a cache
  // swap under the same cluster (a repoint / server-UID switch) is a different cache
  // id, which moves the query key on its own.
  const cluster = useMemo(
    () => clusters?.find((c) => c.spec.source.kubeconfig?.context === context),
    [clusters, context],
  );
  const clusterID = cluster?.id;
  const cacheID = cluster?.activeCache?.id;

  // The active cache's `Synced` condition, whose transition marks when discovery has
  // (re)filled `kind_catalog` for the *same* cache (a cache swap moves the query key
  // instead). Streamed live via `clusterCachesWatch`; it doesn't churn on the ~30s
  // freshness heartbeat, so keying a refetch on it fires only on real transitions.
  const synced = cluster?.activeCache?.status.conditions.find((c) => c.type === 'Synced');
  const syncSignal = synced ? `${synced.status}:${synced.reason}` : null;

  const [{ data, operation }, reexecuteQuery] = useQuery({
    query: ClusterDataKindsQuery,
    variables: { id: clusterID ?? '', cacheID: cacheID ?? '' },
    pause: !clusterID || !cacheID,
    requestPolicy: 'cache-and-network',
  });

  // A cache appearing/swapping moves the query variables, so urql refetches on its
  // own; we only nudge a refetch for an *in-place* repopulation of the same cache (a
  // `Synced` transition). Skipping the first observation of each cache id avoids a
  // redundant refetch on top of the variables-driven fetch (mount or cache change).
  const lastCache = useRef<string | undefined>(undefined);
  useEffect(() => {
    if (!clusterID || !cacheID) {
      lastCache.current = undefined;
      return;
    }
    if (lastCache.current === cacheID) reexecuteQuery({ requestPolicy: 'network-only' });
    else lastCache.current = cacheID;
  }, [clusterID, cacheID, syncSignal, reexecuteQuery]);

  // urql retains the *previous* operation's `data` until the new one resolves (and
  // while paused), so `data` alone can belong to a departed cluster, a
  // switched-away context, or a superseded cache. `operation.variables.cacheID` names
  // the cache the current `data` is for (globally unique, so it also distinguishes
  // clusters); only build from it when it matches the active cache, else fall back to
  // the curated-only tree instead of leaking stale kinds.
  const dataCacheID = operation?.variables.cacheID;
  const nav = useMemo(
    () => buildDashboardNav(cacheID && dataCacheID === cacheID ? (data?.clusterDataKinds ?? []) : []),
    [cacheID, dataCacheID, data],
  );
  return { nav };
}
