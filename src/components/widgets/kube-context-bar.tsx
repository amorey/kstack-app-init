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

// Window-wide strip above the `Outlet`, mounted in `AppLayout` so chat and
// dashboard share one picker. Namespace isn't exposed by the sidecar yet.
import { HistoryNav } from '@/components/widgets/history-nav';
import { KubeContextPicker } from '@/components/widgets/kube-context-picker';
import { useActiveKubeContext } from '@/lib/active-kube-context';
import { tlsUnverifiedReason } from '@/lib/kube-config';

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

const UNVERIFIED_TLS_TITLE: Record<'skip-verify' | 'plain-http', string> = {
  'skip-verify': "The server's certificate is not checked: this context's cluster sets insecure-skip-tls-verify.",
  'plain-http': "The server's certificate is not checked: this context's server URL is plain http.",
};

function UnverifiedTLSBadge({ reason }: { reason: 'skip-verify' | 'plain-http' }) {
  return (
    <span
      className="inline-block shrink-0 rounded bg-amber-100 px-1.5 py-0.5 text-xs font-medium text-amber-800 dark:bg-amber-950 dark:text-amber-300"
      title={UNVERIFIED_TLS_TITLE[reason]}
    >
      Unverified TLS
    </span>
  );
}

export function KubeContextBar() {
  const { active } = useActiveKubeContext();
  const unverified = tlsUnverifiedReason(active?.clusterEntry ?? null);

  return (
    <div className="flex h-11 shrink-0 items-center gap-4 border-b border-border bg-background px-4">
      <HistoryNav />
      <KubeContextPicker />
      {unverified && <UnverifiedTLSBadge reason={unverified} />}
      {active && (active.cluster || active.user) && (
        <div className="flex min-w-0 items-center gap-4 text-xs text-muted-foreground">
          {active.cluster && <Meta label="cluster" value={active.cluster} />}
          {active.user && <Meta label="user" value={active.user} />}
        </div>
      )}
    </div>
  );
}
