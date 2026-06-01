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

// Slide-out panel listing every cluster in the app's registry, split into two
// groups: Active (still in the current kubeconfig) and Orphaned (a leftover
// cache whose context has left the kubeconfig). Each row shows its sync state
// and cache size; the toggle enables or disables syncing, and cached rows can
// have their cache deleted.
// The caller positions the trigger (today: the top-right toolbar). Data + state
// come from the sidecar via useClusters() — the toggle and delete write through
// to it, and the resulting clustersWatch push updates the row.
import { Database, Trash2 } from 'lucide-react';
import { useMutation } from 'urql';

import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from '@kubetail/ui/elements/sheet';
import { Switch } from '@kubetail/ui/elements/switch';

import { graphql } from '@/gql';
import { type Cluster, formatBytes, useClusters } from '@/lib/clusters';
import { errorMessage, reportError } from '@/lib/error-bus';
import { formatSyncFreshness } from '@/lib/sync-status';

const SetClusterEnabledMutation = graphql(`
  mutation SetClusterEnabled($uuid: String!, $enabled: Boolean!) {
    setClusterEnabled(uuid: $uuid, enabled: $enabled) {
      uuid
      enabled
    }
  }
`);

const DeleteClusterCacheMutation = graphql(`
  mutation DeleteClusterCache($uuid: String!) {
    deleteClusterCache(uuid: $uuid)
  }
`);

type Tone = 'ok' | 'warn' | 'muted';

const DOT_CLASS: Record<Tone, string> = {
  ok: 'bg-emerald-500',
  warn: 'bg-amber-500',
  muted: 'bg-muted-foreground/50',
};

type Group = 'active' | 'orphaned';

// A pending cluster is a kubeconfig context the sidecar hasn't identified yet
// (its kube-system UID probe hasn't succeeded — e.g. a stopped minikube): it has
// no stable UUID, so it can't be synced, cached, or toggled until it's reachable.
function isPending(c: Cluster): boolean {
  return !c.uuid;
}

// A row's sync state is derived from its group and flags. Orphaned rows need no
// status word — the section header already says "Orphaned" — so they just carry
// an amber dot. A pending (unidentified) context reads as "Not synced"; an
// identified active row is Syncing (enabled) or Paused (off).
function statusOf(c: Cluster, group: Group): { label: string | null; tone: Tone } {
  if (group === 'orphaned') return { label: null, tone: 'warn' };
  if (isPending(c)) return { label: 'Not synced', tone: 'muted' };
  return c.enabled ? { label: 'Syncing', tone: 'ok' } : { label: 'Paused', tone: 'muted' };
}

function ClusterRow({
  cluster,
  group,
  onToggle,
  onDelete,
}: {
  cluster: Cluster;
  group: Group;
  onToggle: (enabled: boolean) => void;
  onDelete: () => void;
}) {
  const status = statusOf(cluster, group);
  const detail = [
    cluster.cached ? formatBytes(cluster.cacheBytes) : null,
    cluster.lastSyncedAt ? formatSyncFreshness(cluster.lastSyncedAt) : null,
  ]
    .filter(Boolean)
    .join(' · ');
  return (
    <li className="flex items-center gap-3 py-2">
      <span aria-hidden className={`size-2 shrink-0 rounded-full ${DOT_CLASS[status.tone]}`} />
      <div className="min-w-0 flex-1">
        <div className="truncate text-sm font-medium">{cluster.name || cluster.uuid}</div>
        <div className="text-xs text-muted-foreground">
          {/* Status in its own span so it stays individually addressable. */}
          {status.label ? <span>{status.label}</span> : null}
          {detail ? <span>{status.label ? ` · ${detail}` : detail}</span> : null}
        </div>
      </div>
      {cluster.cached ? (
        <button
          type="button"
          aria-label={`Delete cache for ${cluster.name || cluster.uuid}`}
          onClick={onDelete}
          className="shrink-0 rounded p-1 text-muted-foreground outline-none hover:text-destructive focus-visible:ring-2 focus-visible:ring-ring"
        >
          <Trash2 className="size-3.5" aria-hidden />
        </button>
      ) : null}
      {/* No sync toggle for a pending context — there's no UUID to enable yet. */}
      {isPending(cluster) ? null : (
        <Switch
          size="sm"
          aria-label={`Sync ${cluster.name || cluster.uuid}`}
          checked={cluster.enabled}
          onCheckedChange={onToggle}
          className="shrink-0"
        />
      )}
    </li>
  );
}

// Active = still in the current kubeconfig; Orphaned = a leftover cache whose
// context is gone (so !present). Anything not present is shown as orphaned so
// no known cluster is silently dropped. Empty groups are omitted by the caller.
const GROUPS: { key: Group; label: string; match: (c: Cluster) => boolean }[] = [
  { key: 'active', label: 'Active', match: (c) => c.present },
  { key: 'orphaned', label: 'Orphaned', match: (c) => !c.present },
];

export function ClusterSyncPanel() {
  const { clusters } = useClusters();
  const rows = clusters ?? [];
  const groups = GROUPS.map((g) => ({ ...g, clusters: rows.filter(g.match) })).filter((g) => g.clusters.length > 0);

  // Toggle/delete write through to the sidecar; the resulting clustersWatch
  // push is the source of truth for each row's state (no local mirror). urql's
  // execute resolves (never rejects) with an OperationResult, so a failed
  // mutation would otherwise be swallowed — surface its error on the bus.
  const [, setClusterEnabledMut] = useMutation(SetClusterEnabledMutation);
  const [, deleteClusterCacheMut] = useMutation(DeleteClusterCacheMutation);

  const setClusterEnabled = (uuid: string, enabled: boolean) => {
    setClusterEnabledMut({ uuid, enabled }).then((result) => {
      if (result.error) {
        reportError({ source: 'graphql', message: errorMessage(result.error), cause: result.error });
      }
    });
  };

  const deleteClusterCache = (uuid: string) => {
    deleteClusterCacheMut({ uuid }).then((result) => {
      if (result.error) {
        reportError({ source: 'graphql', message: errorMessage(result.error), cause: result.error });
      }
    });
  };

  return (
    <Sheet>
      <SheetTrigger className="inline-flex items-center gap-1.5 rounded-full bg-muted px-2.5 py-1 text-xs font-medium text-muted-foreground outline-none hover:bg-muted/80 focus-visible:ring-2 focus-visible:ring-ring">
        <Database className="size-3.5" aria-hidden />
        Clusters
      </SheetTrigger>
      <SheetContent side="right" className="w-80">
        <SheetHeader>
          <SheetTitle>Clusters</SheetTitle>
          <SheetDescription>Clusters in your kubeconfig and any leftover local caches.</SheetDescription>
        </SheetHeader>
        {rows.length === 0 ? (
          <p className="px-4 py-6 text-sm text-muted-foreground">No clusters yet.</p>
        ) : (
          <div className="flex flex-col gap-5 px-4">
            {groups.map((g) => (
              <section key={g.key} aria-label={`${g.label} clusters`}>
                <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-muted-foreground">{g.label}</h3>
                <ul className="divide-y">
                  {g.clusters.map((c) => (
                    <ClusterRow
                      key={c.uuid || c.context}
                      cluster={c}
                      group={g.key}
                      onToggle={(enabled) => setClusterEnabled(c.uuid, enabled)}
                      onDelete={() => deleteClusterCache(c.uuid)}
                    />
                  ))}
                </ul>
              </section>
            ))}
          </div>
        )}
      </SheetContent>
    </Sheet>
  );
}
