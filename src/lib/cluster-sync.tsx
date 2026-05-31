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

// Per-cluster sync status surfaced to the renderer. The sidecar's sync
// engine will publish each mirrored cluster's lifecycle + download rate on
// the `clusterSyncStatusWatch` subscription (full snapshot first, then
// updates — same shape as SyncStatusProvider/KubeConfigProvider). This
// provider just adapts that stream into context. Today the backend resolver
// is a stub that emits an empty list; a follow-up wires it to the engine.
import { createContext, useContext, useMemo } from 'react';
import { useSubscription } from 'urql';

import { graphql } from '@/gql';
import type { ClusterSyncStatusWatchSubscription } from '@/gql/graphql';

export type ClusterSyncStatus = ClusterSyncStatusWatchSubscription['clusterSyncStatusWatch'][number];

const ClusterSyncStatusWatchSubscription = graphql(`
  subscription ClusterSyncStatusWatch {
    clusterSyncStatusWatch {
      context
      state
      lastError
      lastSyncedAt
      downloadRateBps
    }
  }
`);

type ClusterSyncContextValue = {
  // null = not reported yet (first frame not landed); [] = no clusters syncing.
  clusters: ClusterSyncStatus[] | null;
};

const ClusterSyncContext = createContext<ClusterSyncContextValue | null>(null);

export function ClusterSyncProvider({ children }: { children: React.ReactNode }) {
  const [{ data }] = useSubscription({ query: ClusterSyncStatusWatchSubscription });
  const clusters = data?.clusterSyncStatusWatch ?? null;
  // Mirror SyncStatusProvider: no `error` branch (subscribe-exchange
  // reconnects transport drops internally), and a memo keeps the context
  // value stable between non-push re-renders.
  const value = useMemo<ClusterSyncContextValue>(() => ({ clusters }), [clusters]);
  return <ClusterSyncContext.Provider value={value}>{children}</ClusterSyncContext.Provider>;
}

export function useClusterSync(): ClusterSyncContextValue {
  const ctx = useContext(ClusterSyncContext);
  if (!ctx) throw new Error('useClusterSync must be used inside <ClusterSyncProvider>');
  return ctx;
}

const KB = 1024;
const MB = KB * 1024;
const GB = MB * 1024;

/**
 * Human-readable per-cluster download rate. `bps` is bytes/sec from the
 * engine (0 = idle/offline). Binary units (1 KB = 1024 B); raw bytes show
 * no decimals, KB/s and up show one. Zero renders as an em dash rather than
 * "0 B/s" so an idle row reads as "nothing flowing" instead of a live zero.
 */
export function formatDownloadRate(bps: number): string {
  if (bps <= 0) return '—';
  if (bps < KB) return `${bps} B/s`;
  if (bps < MB) return `${(bps / KB).toFixed(1)} KB/s`;
  if (bps < GB) return `${(bps / MB).toFixed(1)} MB/s`;
  return `${(bps / GB).toFixed(1)} GB/s`;
}
