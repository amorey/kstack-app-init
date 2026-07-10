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

// The window's active kubeconfig context — shared by every mode (chat,
// dashboard) that renders under the `_app` layout route. The source of truth is
// the `kubeContext` URL search param (see `_app.tsx`, which declares it and
// retains it across the chat<->dashboard navigation), so selection is per-window,
// deep-linkable, and survives the mode switch without any provider. Selection is
// a frontend view-scope only; it never rewrites the kubeconfig's current-context.
import { useNavigate, useSearch } from '@tanstack/react-router';

import { useKubeConfig } from '@/lib/kube-config';

type ActiveContext = {
  // The resolved context: the URL param when it names a present context, else
  // the kubeconfig's current context, else '' (clusters not reported yet).
  context: string;
  // Available contexts to switch between (enabled, present kubeconfig clusters).
  contexts: { name: string }[];
  // Persist a choice by writing it to the URL search param.
  setContext: (name: string) => void;
};

export function useActiveContext(): ActiveContext {
  // `strict: false` decouples the hook from the owning route's id — it reads the
  // merged search of whatever `_app` descendant renders it.
  const { kubeContext: param } = useSearch({ strict: false });
  const navigate = useNavigate();
  const { kubeConfig } = useKubeConfig();

  const contexts = kubeConfig?.contexts ?? [];
  // A param naming a context that's since disappeared silently yields to the
  // default, so a stale deep link never points at nothing.
  const valid = param && contexts.some((c) => c.name === param) ? param : undefined;
  const context = valid ?? kubeConfig?.currentContext ?? '';

  const setContext = (name: string) => {
    navigate({ to: '.', search: (prev) => ({ ...prev, kubeContext: name }) });
  };

  return { context, contexts, setContext };
}
