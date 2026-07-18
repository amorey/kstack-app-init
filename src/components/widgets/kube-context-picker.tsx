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

// Picker for the window's active kubeconfig context. A thin control over
// `useActiveKubeContext`: the resolved context drives the value and picking one
// writes it to the `kubeContext` URL param, sharing the choice across chat and
// dashboard. Selection is view-scope only — it doesn't rewrite the kubeconfig's
// current-context. The trigger grows to fit a full FQDN context name up to a cap,
// past which the value line-clamps.
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@kubetail/ui/elements/select';

import { useActiveKubeContext } from '@/lib/active-kube-context';

export function KubeContextPicker() {
  const { context, contexts, setContext } = useActiveKubeContext();

  if (contexts.length === 0) {
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
