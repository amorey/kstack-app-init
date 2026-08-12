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

// The app's cluster registry: reduces the three per-kind delta watches
// (`clustersWatch`, `clusterCachesWatch`, `clusterCacheSyncHealthWatch`) into
// id-keyed maps, then joins client-side — caches onto clusters (active cache =
// `serverUid` matches `status.server.uid`), the folded sync verdict onto that cache
// by `cacheID`. Why per-kind streams + client joins:
// see docs/adr/2026-08-09-delta-watch-protocol.md
//
// The verdict is the sidecar's rollup over every kind the cache syncs — the cache's
// own `Synced` condition is deliberately coarse and NOT what the UI reads. Only
// always-mounted reads live here: per-GVR discovery/sync records are subscribed by
// the sync dialog (`cluster-sync-panel.tsx`), not fleet-wide in every window.
import { createContext, useContext, useMemo } from 'react';

import { graphql } from '@/gql';
import type {
  ClustersWatchSubscription,
  ClusterCachesWatchSubscription,
  ClusterCacheSyncHealthWatchSubscription,
} from '@/gql/graphql';
import { useWatchSubscription } from '@/lib/graphql/use-watch-subscription';

// One object on each delta stream (the `cluster` / `cache` / `sync` payload of a change).
// NonNullable: the entity is null only on a Bookmark, which the reducers fold away,
// so nothing downstream of them ever sees one.
type ClusterRow = NonNullable<ClustersWatchSubscription['clustersWatch']['cluster']>;
type CacheRow = NonNullable<ClusterCachesWatchSubscription['clusterCachesWatch']['cache']>;
export type ClusterCacheSyncHealth = ClusterCacheSyncHealthWatchSubscription['clusterCacheSyncHealthWatch'];

// A cache joined with its sync verdict (null until that frame lands). The selection
// is deliberately thin — omitting the coarse `Synced` condition from the query
// enforces "the UI doesn't read it" at the schema level.
type JoinedCache = CacheRow & { syncHealth: ClusterCacheSyncHealth | null };

// A cluster joined with its active cache (`serverUid` matches `status.server.uid`);
// null when never probed or mid-migration.
export type Cluster = ClusterRow & { activeCache: JoinedCache | null };

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
        }
        conditions {
          type
          status
          reason
          message
          liveness
          unconfirmed
          transitionedAt
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
        # No stats: whether a cache file exists, its size and its object/kind counts all
        # ride clusterCacheStatsWatch, since a settled cache's record stops changing and a
        # field here would freeze at whatever the cache held when the window subscribed.
        # Each stats field is also a resolver call (a filesystem stat plus a kind_counts
        # read) per cache on every frame of this always-mounted stream.
      }
    }
  }
`);

const ClusterCacheSyncHealthWatchSubscription = graphql(`
  subscription ClusterCacheSyncHealthWatch {
    clusterCacheSyncHealthWatch {
      cacheID
      status
      reason
      unhealthyKindRefs {
        apiVersion
        resource
      }
      totalKinds
      unhealthyKinds
      lastUpdateAt
      lastLiveAt
    }
  }
`);

type ClustersContextValue = {
  // null until the cluster snapshot's Bookmark lands; [] = no known clusters. The
  // two are distinct on purpose — [] renders "no clusters", null renders a spinner.
  clusters: Cluster[] | null;
  // Transport up? Sourced from the backbone `clustersWatch` stream; pair with
  // `clusters` via `watchPhase` to tell "reconnecting" from "live".
  connected: boolean;
};

const ClustersContext = createContext<ClustersContextValue | null>(null);

// A map keyed by object id, accumulated from a delta stream.
export type Keyed<T> = ReadonlyMap<string, T>;

// A delta stream's accumulator: the id-keyed map plus whether its snapshot is
// complete. Reading `items` before `synced` shows a partial snapshot as the whole
// collection.
type DeltaState<T> = { items: Keyed<T>; synced: boolean };

// Apply one delta-watch change: Added/Modified upsert, Deleted removes. Returns a
// fresh map (new identity so React re-renders). Shared by every delta-watch reducer.
export function applyChange<T>(prev: Keyed<T> | undefined, type: string, id: string, entity: T): Keyed<T> {
  const next = new Map(prev);
  if (type === 'Deleted') next.delete(id);
  else next.set(id, entity);
  return next;
}

export function ClustersProvider({ children }: { children: React.ReactNode }) {
  // Each stream reduces into its own id-keyed map; useWatchSubscription resets it
  // to `undefined` on a transport reconnect.
  const [{ data: clusterState, connected }] = useWatchSubscription(
    { query: ClustersWatchSubscription },
    (prev: DeltaState<ClusterRow> | undefined, data) => {
      const { type, cluster } = data.clustersWatch;
      const base = prev ?? { items: new Map<string, ClusterRow>(), synced: false };
      // The Bookmark closes the snapshot and is the only frame carrying no cluster.
      if (!cluster) return { ...base, synced: true };
      return { ...base, items: applyChange(base.items, type, cluster.id, cluster) };
    },
  );
  // Held back until the snapshot is complete, so a half-listed fleet never reads as
  // the whole fleet.
  const clusterMap = clusterState?.synced ? clusterState.items : undefined;
  const [{ data: cacheMap }] = useWatchSubscription(
    { query: ClusterCachesWatchSubscription },
    (prev: Keyed<CacheRow> | undefined, data) => {
      const { type, cache } = data.clusterCachesWatch;
      // The Bookmark carries no cache; the caches join in as they arrive (the streams
      // carry no mutual ordering), so it needs no gate of its own here.
      if (!cache) return prev ?? new Map();
      return applyChange(prev, type, cache.id, cache);
    },
  );
  // A latest-value gauge, not a delta stream: each frame replaces that cache's
  // reading outright. Which verdicts are LIVE is decided below, against the cache stream.
  const [{ data: verdictMap }] = useWatchSubscription(
    { query: ClusterCacheSyncHealthWatchSubscription },
    (prev: Keyed<ClusterCacheSyncHealth> | undefined, data) => {
      const h = data.clusterCacheSyncHealthWatch;
      return applyChange(prev, 'Added', h.cacheID, h);
    },
  );
  // A gauge has no Deleted, so a verdict's lifetime is its CACHE's. Derive liveness
  // here (against the cache stream) rather than sweeping in the verdict reducer: a
  // sweep gated on verdict traffic never runs once the fleet goes quiet. And since
  // the streams carry no mutual ordering, an early verdict is only HIDDEN until its
  // cache lands — eviction would lose it for good.
  const healthMap = useMemo<Keyed<ClusterCacheSyncHealth> | undefined>(() => {
    if (!verdictMap) return undefined;
    if (!cacheMap) return new Map();
    const live = new Map<string, ClusterCacheSyncHealth>();
    verdictMap.forEach((h, cacheID) => {
      if (cacheMap.has(cacheID)) live.set(cacheID, h);
    });
    return live;
  }, [verdictMap, cacheMap]);

  // Join. No mutual ordering across streams: a cluster can briefly lack its cache,
  // a cache its verdict — each resolves once its frame lands. null (no cluster
  // frame yet) is distinct from [] (no clusters).
  const clusters = useMemo<Cluster[] | null>(() => {
    if (!clusterMap) return null;
    const caches = cacheMap ? [...cacheMap.values()] : [];

    return [...clusterMap.values()].map((c) => {
      const active = caches.find((cache) => cache.clusterID === c.id && cache.serverUid === c.status.server.uid);
      return {
        ...c,
        activeCache: active ? { ...active, syncHealth: healthMap?.get(active.id) ?? null } : null,
      };
    });
  }, [clusterMap, cacheMap, healthMap]);

  // No `error` branch — subscribe-exchange reconnects transport drops internally.
  const value = useMemo<ClustersContextValue>(() => ({ clusters, connected }), [clusters, connected]);
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
