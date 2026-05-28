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

// Top-bar picker for the active kubeconfig context. Reads contexts and
// currentContext off the `kubeConfigWatch` snapshot. Local selection
// state for now — no setKubeContext mutation exists yet, so changes are
// renderer-only. Wire to a mutation when the sidecar exposes one.
import { useEffect, useState } from 'react';

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@kubetail/ui/elements/select';

import { useKubeConfig } from '@/lib/kube-config';

export function KubeContextPicker() {
  const { kubeConfig } = useKubeConfig();
  const [selected, setSelected] = useState<string | null>(null);

  // Adopt the watcher's currentContext as the initial selection, and
  // re-sync if it changes on disk while the user hasn't picked anything
  // yet. Once the user picks, their choice sticks until they pick again.
  useEffect(() => {
    if (selected === null && kubeConfig?.currentContext) {
      setSelected(kubeConfig.currentContext);
    }
  }, [kubeConfig?.currentContext, selected]);

  const contexts = kubeConfig?.contexts ?? [];
  const value = selected ?? kubeConfig?.currentContext ?? '';

  if (contexts.length === 0) {
    return (
      <span className="text-xs text-muted-foreground" data-testid="kube-context-empty">
        No kubeconfig
      </span>
    );
  }

  return (
    <Select value={value} onValueChange={(v) => setSelected(v as string)}>
      <SelectTrigger size="sm" className="min-w-[10rem]">
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
