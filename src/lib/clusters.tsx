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

// The app's cluster registry surfaced to the renderer. The sidecar publishes
// every known cluster — those in the current kubeconfig plus any orphaned
// records — and their caches as two independent Kubernetes-style delta watches:
// `clustersWatch` (Cluster kind) and `clusterCachesWatch` (ClusterCache kind),
// each replaying the current set as `Added` changes on subscribe, then streaming
// `Added`/`Modified`/`Deleted` per object. This provider reduces each stream into
// a map keyed by object id, then **joins** the caches onto the clusters
// client-side (a cluster's active cache is the one whose `serverUid` matches its
// last-probed `status.server.uid`) — so consumers still see a Kubernetes-shaped
// `Cluster` with `spec`/`status`/`activeCache`, exactly as before the split.
import { createContext, useContext, useMemo } from 'react';
import { useSubscription } from 'urql';

import { graphql } from '@/gql';
import type { ClustersWatchSubscription, ClusterCachesWatchSubscription } from '@/gql/graphql';

// One object on each delta stream (the `cluster` / `cache` payload of a change).
type ClusterRow = ClustersWatchSubscription['clustersWatch']['cluster'];
type CacheRow = ClusterCachesWatchSubscription['clusterCachesWatch']['cache'];

// A cluster joined with its active cache — the cache mirroring the cluster's
// currently-connected identity (its `serverUid` matches `status.server.uid`), or
// null when the cluster has no active cache yet (never probed, or mid-migration).
export type Cluster = ClusterRow & { activeCache: CacheRow | null };

const ClustersWatchSubscription = graphql(`
  subscription ClustersWatch {
    clustersWatch {
      type
      cluster {
        id
        spec {
          name
          syncEnabled
          enabled
          source {
            kubeconfig {
              context
            }
          }
        }
        status {
          source {
            kubeconfig {
              cluster
              user
              isPresent
              isDefault
            }
          }
          server {
            uid
          }
          lastConnectedAt
          conditions {
            type
            status
            reason
            message
            lastTransitionTime
          }
        }
      }
    }
  }
`);

const ClusterCachesWatchSubscription = graphql(`
  subscription ClusterCachesWatch {
    clusterCachesWatch {
      type
      cache {
        id
        clusterID
        serverUid
        status {
          conditions {
            type
            status
            reason
            message
          }
          lastSyncedAt
        }
        stats {
          exists
          bytes
        }
      }
    }
  }
`);

type ClustersContextValue = {
  // null = not reported yet (first frame not landed); [] = no known clusters.
  clusters: Cluster[] | null;
};

const ClustersContext = createContext<ClustersContextValue | null>(null);

// A map keyed by object id, accumulated from a delta stream.
type Keyed<T> = ReadonlyMap<string, T>;

// Apply one delta-watch change to a keyed map, returning a NEW map (fresh
// identity so React re-renders): Added/Modified upsert by id, Deleted removes.
function applyChange<T>(prev: Keyed<T> | undefined, type: string, id: string, entity: T): Keyed<T> {
  const next = new Map(prev);
  if (type === 'Deleted') next.delete(id);
  else next.set(id, entity);
  return next;
}

export function ClustersProvider({ children }: { children: React.ReactNode }) {
  // Each stream is reduced into its own id-keyed map via urql's accumulator: the
  // `Added` snapshot builds the map, later deltas patch it.
  const [{ data: clusterMap }] = useSubscription(
    { query: ClustersWatchSubscription },
    (prev: Keyed<ClusterRow> | undefined, data): Keyed<ClusterRow> => {
      const { type, cluster } = data.clustersWatch;
      return applyChange(prev, type, cluster.id, cluster);
    },
  );
  const [{ data: cacheMap }] = useSubscription(
    { query: ClusterCachesWatchSubscription },
    (prev: Keyed<CacheRow> | undefined, data): Keyed<CacheRow> => {
      const { type, cache } = data.clusterCachesWatch;
      return applyChange(prev, type, cache.id, cache);
    },
  );

  // Join: attach each cluster's active cache. The two streams carry no mutual
  // ordering guarantee, so a cluster can briefly have no matching cache (renders
  // as no active cache) — resolved once the cache frame lands. null (no cluster
  // frame yet) is distinct from [] (no clusters).
  const clusters = useMemo<Cluster[] | null>(() => {
    if (!clusterMap) return null;
    const caches = cacheMap ? [...cacheMap.values()] : [];
    return [...clusterMap.values()].map((c) => ({
      ...c,
      activeCache: caches.find((cache) => cache.clusterID === c.id && cache.serverUid === c.status.server.uid) ?? null,
    }));
  }, [clusterMap, cacheMap]);

  // Mirror SyncStatusProvider: no `error` branch (subscribe-exchange reconnects
  // transport drops internally), and a memo keeps the context value stable
  // between non-push re-renders.
  const value = useMemo<ClustersContextValue>(() => ({ clusters }), [clusters]);
  return <ClustersContext.Provider value={value}>{children}</ClustersContext.Provider>;
}

export function useClusters(): ClustersContextValue {
  const ctx = useContext(ClustersContext);
  if (!ctx) throw new Error('useClusters must be used inside <ClustersProvider>');
  return ctx;
}

const KB = 1024;
const MB = KB * 1024;
const GB = MB * 1024;

/**
 * Human-readable cache size. `bytes` is the on-disk size of a cluster's cache
 * (0 = not cached). Binary units (1 KB = 1024 B); raw bytes show no decimals,
 * KB and up show one. Zero renders as an em dash rather than "0 B" so an
 * uncached row reads as "nothing stored".
 */
export function formatBytes(bytes: number): string {
  if (bytes <= 0) return '—';
  if (bytes < KB) return `${bytes} B`;
  if (bytes < MB) return `${(bytes / KB).toFixed(1)} KB`;
  if (bytes < GB) return `${(bytes / MB).toFixed(1)} MB`;
  return `${(bytes / GB).toFixed(1)} GB`;
}
