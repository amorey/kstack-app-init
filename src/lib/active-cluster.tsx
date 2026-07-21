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

// Resolves the window's active kube-context down to its cluster record and active cache —
// the "active kube-context → cluster → active cache" join every cluster-data hook needs
// (`useClusterDataKinds`, `useClusterDataEvents`, `useClusterDataObjects`), and future
// consumers like chat context. One definition of "the active cluster" instead of the same
// find + id reads copied per hook.
//
// Only kubeconfig-sourced records carry a context, so the match is on
// `spec.source.kubeconfig.context`. The returned `clusterID`/`cacheID` are primitives
// (stable across renders), so a caller can key a subscription on them without re-subscribing
// each render; `cluster` is memoized. `active` is true only when both ids resolve — a
// never-synced/paused cluster has no active cache, so its data watches stay paused.
import { useMemo } from 'react';

import { useActiveKubeContext } from '@/lib/active-kube-context';
import { useClusters } from '@/lib/clusters';
import type { Cluster } from '@/lib/clusters';

export function useActiveCluster(): {
  cluster: Cluster | undefined;
  clusterID: string | undefined;
  cacheID: string | undefined;
  active: boolean;
} {
  const { context } = useActiveKubeContext();
  const { clusters } = useClusters();

  const cluster = useMemo(
    () => clusters?.find((c) => c.spec.source.kubeconfig?.context === context),
    [clusters, context],
  );
  const clusterID = cluster?.id;
  const cacheID = cluster?.activeCache?.id;
  return { cluster, clusterID, cacheID, active: !!(clusterID && cacheID) };
}
