// Copyright 2026 The Kstack Authors
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

// Thin control over `useActiveKubeContext`. Selection is view-scope only — it
// never rewrites the kubeconfig's current-context — and lives in the
// `kubeContext` search param, not provider state;
// see docs/adr/2026-08-09-url-params-as-window-state.md
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@kubetail/ui/elements/select';
import { Spinner } from '@kubetail/ui/elements/spinner';

import { useActiveKubeContext } from '@/lib/active-kube-context';

export function KubeContextPicker() {
  const { context, contexts, phase, setContext } = useActiveKubeContext();

  if (contexts.length === 0) {
    // Split the reason so a stalled initial dial doesn't read as a permanent
    // "No kubeconfig".
    if (phase === 'connecting') {
      return (
        <span className="flex items-center gap-1.5 text-xs text-muted-foreground" data-testid="kube-context-connecting">
          <Spinner size="xs" className="mr-0" />
          Connecting…
        </span>
      );
    }
    return (
      <span className="text-xs text-muted-foreground" data-testid="kube-context-empty">
        No kubeconfig
      </span>
    );
  }

  return (
    <Select value={context} onValueChange={(v) => v !== null && setContext(v)}>
      <SelectTrigger size="sm" className="min-w-[12rem] max-w-[32rem]">
        <SelectValue placeholder="Select context" />
      </SelectTrigger>
      <SelectContent>
        {contexts.map((c) => (
          <SelectItem key={c.name} value={c.name}>
            {c.name}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
