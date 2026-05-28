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

// KubeConfig surfaced to the renderer. The sidecar's fsnotify-backed
// watcher publishes the current snapshot on the `kubeConfigWatch`
// subscription (ADDED first, then MODIFIED on every reload). This
// provider just adapts that stream into context — same shape as
// SyncStatusProvider.
import { createContext, useContext, useMemo } from 'react';
import { useSubscription } from 'urql';

import { graphql } from '@/gql';
import type { KubeConfigWatchSubscription } from '@/gql/graphql';

export type KubeConfig = NonNullable<KubeConfigWatchSubscription['kubeConfigWatch']>['object'];

const KubeConfigWatchSubscription = graphql(`
  subscription KubeConfigWatch {
    kubeConfigWatch {
      type
      object {
        currentContext
        authInfos {
          name
          locationOfOrigin
        }
        clusters {
          name
          locationOfOrigin
          server
        }
        contexts {
          name
          locationOfOrigin
          cluster
          authInfo
          namespace
        }
      }
    }
  }
`);

type KubeConfigContextValue = {
  kubeConfig: KubeConfig | null;
};

const KubeConfigCtx = createContext<KubeConfigContextValue | null>(null);

export function KubeConfigProvider({ children }: { children: React.ReactNode }) {
  const [{ data }] = useSubscription({ query: KubeConfigWatchSubscription });
  const kubeConfig = data?.kubeConfigWatch?.object ?? null;
  // Treat the first ADDED frame and subsequent MODIFIED frames identically:
  // both carry the current snapshot. Null = not reported yet (watcher
  // disabled or first frame not landed).
  const value = useMemo<KubeConfigContextValue>(() => ({ kubeConfig }), [kubeConfig]);
  return <KubeConfigCtx.Provider value={value}>{children}</KubeConfigCtx.Provider>;
}

export function useKubeConfig(): KubeConfigContextValue {
  const ctx = useContext(KubeConfigCtx);
  if (!ctx) throw new Error('useKubeConfig must be used inside <KubeConfigProvider>');
  return ctx;
}
