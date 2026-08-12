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

// The window's active kubeconfig context. Source of truth is the `kubeContext` URL
// search param on `_app` (per-window, deep-linkable; see
// docs/adr/2026-08-09-url-params-as-window-state.md). Frontend view-scope only —
// never rewrites the kubeconfig's current-context.
import { useNavigate, useSearch } from '@tanstack/react-router';

import { watchPhase } from '@/lib/graphql/use-watch-subscription';
import type { WatchPhase } from '@/lib/graphql/use-watch-subscription';
import { useKubeConfig } from '@/lib/kube-config';
import type { KubeContextInfo } from '@/lib/kube-config';

type ActiveKubeContext = {
  // Resolved name: the URL param when it names a present context, else the
  // kubeconfig's current context, else '' (clusters not reported yet).
  context: string;
  // The resolved context's full record (cluster/user); undefined until resolved.
  active: KubeContextInfo | undefined;
  contexts: KubeContextInfo[];
  // Distinguishes "connecting" from "connected, empty kubeconfig" for the picker.
  phase: WatchPhase;
  // Writes the URL search param.
  setContext: (name: string) => void;
};

export function useActiveKubeContext(): ActiveKubeContext {
  // `strict: false`: read the merged search of whatever `_app` descendant renders this.
  const { kubeContext: param } = useSearch({ strict: false });
  const navigate = useNavigate();
  const { kubeConfig, connected } = useKubeConfig();

  // `kubeConfig === null` is exactly "the registry's snapshot is not complete" — it
  // is derived from `clusters`, which the provider holds back until the Bookmark.
  const phase = watchPhase(kubeConfig !== null, connected);
  const contexts = kubeConfig?.contexts ?? [];
  // A param naming a gone context yields to the default — a stale deep link
  // never points at nothing.
  const valid = param && contexts.some((c) => c.name === param) ? param : undefined;
  const context = valid ?? kubeConfig?.currentContext ?? '';
  const active = contexts.find((c) => c.name === context);

  const setContext = (name: string) => {
    navigate({ to: '.', search: (prev) => ({ ...prev, kubeContext: name }) });
  };

  return { context, active, contexts, phase, setContext };
}
