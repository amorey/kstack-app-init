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

// The kubeconfig's context list, derived from the cluster registry stream — must
// mount inside <ClustersProvider>. There is no dedicated kubeConfigWatch; a
// consumer needing the raw kubeconfig should grow the Cluster type instead.
import { createContext, useContext, useMemo } from 'react';

import { useClusters } from '@/lib/clusters';

// One switchable context: name plus the kubeconfig cluster/user it binds to
// (the context bar surfaces the latter).
export type KubeContextInfo = {
  name: string;
  cluster: string;
  user: string;
};

export type KubeConfig = {
  currentContext: string;
  contexts: KubeContextInfo[];
};

type KubeConfigContextValue = {
  // null until the cluster snapshot is complete; a present value with no contexts
  // means a genuinely empty kubeconfig.
  kubeConfig: KubeConfig | null;
  // Registry transport up? Pair with `kubeConfig === null` via `watchPhase`.
  connected: boolean;
};

const KubeConfigCtx = createContext<KubeConfigContextValue | null>(null);

export function KubeConfigProvider({ children }: { children: React.ReactNode }) {
  const { clusters, connected } = useClusters();
  const value = useMemo<KubeConfigContextValue>(() => {
    if (clusters === null) return { kubeConfig: null, connected };
    // Only enabled, kubeconfig-present, kubeconfig-sourced records are switchable;
    // disabled ones stay tracked but hidden, orphaned ones have nothing to switch to.
    const present = clusters.filter(
      (c) => c.spec.enabled && c.status.source.kubeconfig?.isPresent && c.spec.source.kubeconfig,
    );
    return {
      connected,
      kubeConfig: {
        currentContext:
          present.find((c) => c.status.source.kubeconfig?.isDefault)?.spec.source.kubeconfig?.context ?? '',
        contexts: present.map((c) => ({
          name: c.spec.source.kubeconfig?.context ?? '',
          cluster: c.status.source.kubeconfig?.cluster ?? '',
          user: c.status.source.kubeconfig?.user ?? '',
        })),
      },
    };
  }, [clusters, connected]);
  return <KubeConfigCtx.Provider value={value}>{children}</KubeConfigCtx.Provider>;
}

export function useKubeConfig(): KubeConfigContextValue {
  const ctx = useContext(KubeConfigCtx);
  if (!ctx) throw new Error('useKubeConfig must be used inside <KubeConfigProvider>');
  return ctx;
}
