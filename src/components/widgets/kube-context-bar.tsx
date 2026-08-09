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

// Window-wide strip above the `Outlet`, mounted in `AppLayout` so chat and
// dashboard share one picker. Namespace isn't exposed by the sidecar yet.
import { HistoryNav } from '@/components/widgets/history-nav';
import { KubeContextPicker } from '@/components/widgets/kube-context-picker';
import { useActiveKubeContext } from '@/lib/active-kube-context';

function Meta({ label, value }: { label: string; value: string }) {
  return (
    <span className="flex min-w-0 items-baseline gap-1">
      <span className="shrink-0 text-muted-foreground/70">{label}</span>
      <span className="truncate" title={value}>
        {value}
      </span>
    </span>
  );
}

export function KubeContextBar() {
  const { active } = useActiveKubeContext();

  return (
    <div className="flex h-11 shrink-0 items-center gap-4 border-b border-border bg-background px-4">
      <HistoryNav />
      <KubeContextPicker />
      {active && (active.cluster || active.user) && (
        <div className="flex min-w-0 items-center gap-4 text-xs text-muted-foreground">
          {active.cluster && <Meta label="cluster" value={active.cluster} />}
          {active.user && <Meta label="user" value={active.user} />}
        </div>
      )}
    </div>
  );
}
