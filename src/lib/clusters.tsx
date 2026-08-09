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
// records — as three independent Kubernetes-style delta watches, one per object
// kind: `clustersWatch` (Cluster), `clusterCachesWatch` (ClusterCache), and
// `clusterCacheSyncHealthWatch` (each cache's folded sync verdict), each replaying
// the current set as `Added` changes on subscribe, then streaming
// `Added`/`Modified`/`Deleted` per object. This provider reduces each stream into
// a map keyed by object id, then joins them client-side down the chain — caches
// onto clusters (a cluster's active cache is the one whose `serverUid` matches its
// last-probed `status.server.uid`), the sync verdict onto that cache by `cacheID` —
// so consumers see a Kubernetes-shaped `Cluster` with `spec`/`status`/`activeCache`,
// the cache carrying its `syncHealth`.
//
// **The verdict is a rollup, not one child's condition.** A cache syncs one kind per
// served GVR — a hundred or more — and each fails independently, so no single record's
// `Synced` is the cache's: ninety-nine healthy kinds beside one forbidden CRD is not a
// healthy cache. The sidecar folds them (`clusterCacheSyncHealthWatch`), which is also
// what keeps this stream one record per cache instead of a hundred; the per-kind records
// themselves ride a cache-scoped watch opened only by whoever is looking at a cache.
// The cache's own `Synced` condition is deliberately coarse (Syncing/Paused) and is not
// what the UI reads.
//
// **What lives here is what an always-mounted consumer reads.** The cache's
// GVR-discovery record and its per-kind sync records are deliberately NOT joined in:
// nothing outside the sync dialog's expanded row reads them, so they are subscribed
// there (`cluster-sync-panel.tsx`) rather than opening fleet-wide streams in every
// window.
import { createContext, useContext, useMemo } from 'react';

import { graphql } from '@/gql';
import type {
  ClustersWatchSubscription,
  ClusterCachesWatchSubscription,
  ClusterCacheSyncHealthWatchSubscription,
} from '@/gql/graphql';
import { useWatchSubscription } from '@/lib/graphql/use-watch-subscription';

// One object on each delta stream (the `cluster` / `cache` / `sync` payload of a change).
type ClusterRow = ClustersWatchSubscription['clustersWatch']['cluster'];
type CacheRow = ClusterCachesWatchSubscription['clusterCachesWatch']['cache'];
export type ClusterCacheSyncHealth = ClusterCacheSyncHealthWatchSubscription['clusterCacheSyncHealthWatch'];

// A cache joined with its sync verdict — folded from every kind the cache syncs. null
// until that frame lands.
//
// The cache selection is deliberately thin: its coarse `Synced` condition is not what the
// UI reads, and leaving it out of the query keeps that invariant enforced by the schema
// rather than by a comment. Add a field back when a consumer appears.
type JoinedCache = CacheRow & { syncHealth: ClusterCacheSyncHealth | null };

// A cluster joined with its active cache — the cache mirroring the cluster's
// currently-connected identity (its `serverUid` matches `status.server.uid`), or
// null when the cluster has no active cache yet (never probed, or mid-migration).
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
  // null = not reported yet (first frame not landed); [] = no known clusters.
  clusters: Cluster[] | null;
  // Is the registry's transport up right now? Sourced from the `clustersWatch`
  // stream (the backbone `clusters` derives from; the caches stream just joins on),
  // so consumers can pair it with `clusters` to tell "connecting" (down, no data)
  // from "connected, empty snapshot" (up, no clusters) via `watchPhase`.
  connected: boolean;
};

const ClustersContext = createContext<ClustersContextValue | null>(null);

// A map keyed by object id, accumulated from a delta stream.
export type Keyed<T> = ReadonlyMap<string, T>;

// Apply one delta-watch change to a keyed map, returning a fresh map (new identity
// so React re-renders): Added/Modified upsert by id, Deleted removes. Shared by
// every delta-watch reducer (this file's streams and the dashboard nav's).
export function applyChange<T>(prev: Keyed<T> | undefined, type: string, id: string, entity: T): Keyed<T> {
  const next = new Map(prev);
  if (type === 'Deleted') next.delete(id);
  else next.set(id, entity);
  return next;
}

export function ClustersProvider({ children }: { children: React.ReactNode }) {
  // Each stream reduces into its own id-keyed map: the `Added` snapshot builds it,
  // later deltas patch it. useWatchSubscription resets the map to `undefined`
  // ("not reported yet") on a transport reconnect.
  const [{ data: clusterMap, connected }] = useWatchSubscription(
    { query: ClustersWatchSubscription },
    (prev: Keyed<ClusterRow> | undefined, data) => {
      const { type, cluster } = data.clustersWatch;
      return applyChange(prev, type, cluster.id, cluster);
    },
  );
  const [{ data: cacheMap }] = useWatchSubscription(
    { query: ClusterCachesWatchSubscription },
    (prev: Keyed<CacheRow> | undefined, data) => {
      const { type, cache } = data.clusterCachesWatch;
      return applyChange(prev, type, cache.id, cache);
    },
  );
  // A latest-value gauge, not a delta stream: each frame is one cache's whole verdict, so
  // it replaces the previous reading for that cache outright. The reducer only accumulates;
  // which verdicts are LIVE is decided below, against the cache stream.
  const [{ data: verdictMap }] = useWatchSubscription(
    { query: ClusterCacheSyncHealthWatchSubscription },
    (prev: Keyed<ClusterCacheSyncHealth> | undefined, data) => {
      const h = data.clusterCacheSyncHealthWatch;
      return applyChange(prev, 'Added', h.cacheID, h);
    },
  );
  // A gauge has no Deleted of its own, so a verdict's lifetime is its CACHE's — the sidecar
  // drops a cache's verdict from its snapshot the same way. Deriving that here, from the
  // cache stream, rather than sweeping inside the verdict reducer, is what stops it hanging
  // on traffic that has nothing to do with it: a fleet that goes quiet right after a cache
  // is deleted sends no further verdict frames, so a sweep gated on one would never run.
  //
  // Deriving is also what makes "absent from cacheMap" a safe rule at all. The streams carry
  // no mutual ordering, so a verdict can arrive before its cache's frame — but here that
  // only HIDES it until the cache lands (where it would have been unusable anyway), whereas
  // evicting it from the accumulator would have thrown it away for good.
  const healthMap = useMemo<Keyed<ClusterCacheSyncHealth> | undefined>(() => {
    if (!verdictMap) return undefined;
    if (!cacheMap) return new Map();
    const live = new Map<string, ClusterCacheSyncHealth>();
    verdictMap.forEach((h, cacheID) => {
      if (cacheMap.has(cacheID)) live.set(cacheID, h);
    });
    return live;
  }, [verdictMap, cacheMap]);

  // Join: attach each cluster's active cache, and that cache's sync verdict. The three
  // streams carry no mutual ordering guarantee, so a cluster can briefly have no matching
  // cache (renders as no active cache) and a cache no verdict (renders as "not reported
  // yet") — each resolves once its frame lands. null (no cluster frame yet) is distinct
  // from [] (no clusters).
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
  // The memo keeps the context value stable between non-push re-renders.
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
