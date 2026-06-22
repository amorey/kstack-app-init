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
// records — on the `clustersWatch` subscription (currently the full snapshot
// once on subscribe; live re-emits on registry changes come later). Each row
// is Kubernetes-shaped: `spec` carries the user-owned fields (name, flags,
// kube-context) and `status` the observed state (kubeconfig presence, probed
// server facts, status conditions, and the live cache object) — display
// state derives from the status fields client-side. This provider adapts
// that stream into context.
import { createContext, useContext, useMemo } from 'react';
import { useSubscription } from 'urql';

import { graphql } from '@/gql';
import type { ClustersWatchSubscription } from '@/gql/graphql';

export type Cluster = ClustersWatchSubscription['clustersWatch'][number];

const ClustersWatchSubscription = graphql(`
  subscription ClustersWatch {
    clustersWatch {
      id
      spec {
        name
        isSyncEnabled
        isActive
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
        syncStatus {
          lastSyncedAt
        }
        cache {
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

export function ClustersProvider({ children }: { children: React.ReactNode }) {
  const [{ data }] = useSubscription({ query: ClustersWatchSubscription });
  const clusters = data?.clustersWatch ?? null;
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
