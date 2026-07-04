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

// The kubeconfig's context list surfaced to the renderer, derived from the
// cluster registry stream (each present cluster record carries its context
// name and the current-context flag) — so this provider must be mounted
// inside <ClustersProvider>. The sidecar exposes no dedicated
// kubeConfigWatch subscription; if a consumer ever needs the raw kubeconfig
// (auth-infos, server URLs), grow the Cluster type instead.
import { createContext, useContext, useMemo } from 'react';

import { useClusters } from '@/lib/clusters';

export type KubeConfig = {
  currentContext: string;
  contexts: { name: string }[];
};

type KubeConfigContextValue = {
  // null = clusters not reported yet (first frame not landed).
  kubeConfig: KubeConfig | null;
};

const KubeConfigCtx = createContext<KubeConfigContextValue | null>(null);

export function KubeConfigProvider({ children }: { children: React.ReactNode }) {
  const { clusters } = useClusters();
  const value = useMemo<KubeConfigContextValue>(() => {
    if (clusters === null) return { kubeConfig: null };
    // The context picker shows only clusters the user has enabled in the app and
    // that are still present in the kubeconfig. Disabled records stay tracked but
    // hidden; orphaned ones (context gone) have nothing to switch to. Only
    // kubeconfig-sourced records carry a context.
    const present = clusters.filter(
      (c) => c.spec.enabled && c.status.source.kubeconfig?.isPresent && c.spec.source.kubeconfig,
    );
    return {
      kubeConfig: {
        currentContext:
          present.find((c) => c.status.source.kubeconfig?.isDefault)?.spec.source.kubeconfig?.context ?? '',
        contexts: present.map((c) => ({ name: c.spec.source.kubeconfig?.context ?? '' })),
      },
    };
  }, [clusters]);
  return <KubeConfigCtx.Provider value={value}>{children}</KubeConfigCtx.Provider>;
}

export function useKubeConfig(): KubeConfigContextValue {
  const ctx = useContext(KubeConfigCtx);
  if (!ctx) throw new Error('useKubeConfig must be used inside <KubeConfigProvider>');
  return ctx;
}
