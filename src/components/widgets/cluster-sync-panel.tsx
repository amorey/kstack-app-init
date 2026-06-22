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

// Slide-out panel showing the app's cluster registry as a table, with rows
// grouped into Active (still in the current kubeconfig) and Orphaned (a leftover
// cache whose context has left the kubeconfig). Columns: Cluster & connection,
// Sync status, Cache, Actions. Each row's actions are a play/pause toggle for
// syncing and a clear-cache button; orphaned rows disable play/pause and add a
// trash button that forgets the cluster entirely (removes it from app.db).
// Data + state come from the sidecar via useClusters() — the mutations write
// through to it, and the resulting clustersWatch push updates the table.
import { Database, Pause, Play, Slash, Trash2 } from 'lucide-react';
import { useMutation } from 'urql';

import { Button } from '@kubetail/ui/elements/button';
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from '@kubetail/ui/elements/sheet';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@kubetail/ui/elements/table';

import { graphql } from '@/gql';
import { type Cluster, formatBytes, useClusters } from '@/lib/clusters';
import { errorMessage, reportError } from '@/lib/error-bus';
import { formatSyncFreshness } from '@/lib/sync-status';

const ClusterSyncEnabledSetMutation = graphql(`
  mutation ClusterSyncEnabledSet($id: ID!, $syncEnabled: Boolean!) {
    clusterSyncEnabledSet(id: $id, syncEnabled: $syncEnabled) {
      id
      spec {
        isSyncEnabled
      }
    }
  }
`);

const ClusterCacheClearMutation = graphql(`
  mutation ClusterCacheClear($id: ID!) {
    clusterCacheClear(id: $id) {
      id
    }
  }
`);

const ClusterDeleteMutation = graphql(`
  mutation ClusterDelete($id: ID!) {
    clusterDelete(id: $id)
  }
`);

type Tone = 'ok' | 'error' | 'muted';
type Group = 'active' | 'orphaned';

const DOT_CLASS: Record<Tone, string> = {
  ok: 'bg-emerald-500',
  error: 'bg-red-500',
  muted: 'bg-muted-foreground/50',
};

// A pending cluster is a kubeconfig context the sidecar hasn't identified yet
// (its kube-system UID probe hasn't succeeded — e.g. a stopped minikube): until
// it's reachable it can't be synced or cached.
function isPending(c: Cluster): boolean {
  return !c.status.server.uid;
}

function displayName(c: Cluster): string {
  return c.spec.name || c.spec.source.kubeconfig?.context || c.id;
}

// The Connection column: can we currently reach the cluster's API? A reachable,
// identified context is Active (green); one we can't reach (its UID probe failed)
// is an Error (red); an orphan has no live cluster to connect to (Unavailable).
function connectionStatus(c: Cluster, group: Group): { label: string; tone: Tone } {
  if (group === 'orphaned') return { label: 'Unavailable', tone: 'muted' };
  if (isPending(c)) return { label: 'Error', tone: 'error' };
  return { label: 'Active', tone: 'ok' };
}

// Sync status: an orphan isn't syncing (its cluster is gone — "Stopped", not
// repeating the group's "Orphaned" header); a pending context can't sync yet;
// otherwise it's Syncing (enabled) or Paused (off).
function statusOf(c: Cluster, group: Group): { label: string; tone: Tone } {
  if (group === 'orphaned') return { label: 'Stopped', tone: 'muted' };
  if (isPending(c)) return { label: 'Not synced', tone: 'muted' };
  return c.spec.isSyncEnabled ? { label: 'Syncing', tone: 'ok' } : { label: 'Paused', tone: 'muted' };
}

// A database icon with a diagonal line through it — "clear the cache".
function ClearCacheIcon() {
  return (
    <span className="relative inline-flex size-4" aria-hidden>
      <Database className="size-full" />
      <Slash className="absolute inset-0 size-full" />
    </span>
  );
}

function ClusterRow({
  cluster,
  group,
  onToggle,
  onClearCache,
  onRemove,
}: {
  cluster: Cluster;
  group: Group;
  onToggle: (enabled: boolean) => void;
  onClearCache: () => void;
  onRemove: () => void;
}) {
  const name = displayName(cluster);
  const connection = connectionStatus(cluster, group);
  const status = statusOf(cluster, group);
  const orphaned = group === 'orphaned';
  const pending = isPending(cluster);
  // Sync can only start/stop for an identified cluster still in the kubeconfig.
  const syncing = cluster.spec.isSyncEnabled && !orphaned && !pending;
  const canToggle = !orphaned && !pending;

  return (
    <TableRow>
      <TableCell className="font-medium">{name}</TableCell>
      <TableCell>
        <span className="inline-flex items-center gap-1.5">
          <span aria-hidden className={`size-2 shrink-0 rounded-full ${DOT_CLASS[connection.tone]}`} />
          {connection.label}
        </span>
      </TableCell>
      <TableCell>
        <span className="inline-flex items-center gap-1.5">
          <span aria-hidden className={`size-2 shrink-0 rounded-full ${DOT_CLASS[status.tone]}`} />
          {status.label}
        </span>
        {cluster.status.syncStatus.lastSyncedAt ? (
          <div className="text-xs text-muted-foreground">
            {formatSyncFreshness(Date.parse(cluster.status.syncStatus.lastSyncedAt))}
          </div>
        ) : null}
      </TableCell>
      <TableCell className="tabular-nums">
        {cluster.status.cache.exists ? formatBytes(cluster.status.cache.bytes) : '—'}
      </TableCell>
      <TableCell>
        <div className="flex items-center justify-end gap-0.5">
          <Button
            type="button"
            variant="ghost"
            size="icon-sm"
            aria-label={`${syncing ? 'Pause' : 'Resume'} sync for ${name}`}
            disabled={!canToggle}
            onClick={() => onToggle(!cluster.spec.isSyncEnabled)}
          >
            {syncing ? <Pause className="size-4" aria-hidden /> : <Play className="size-4" aria-hidden />}
          </Button>
          <Button
            type="button"
            variant="ghost"
            size="icon-sm"
            aria-label={`Clear cache for ${name}`}
            disabled={!cluster.status.cache.exists}
            onClick={onClearCache}
          >
            <ClearCacheIcon />
          </Button>
          {/* Always rendered so both groups have the same action count (columns
              stay aligned); removing only applies to an orphan, so it's disabled
              for an active cluster (a present context would just be re-discovered). */}
          <Button
            type="button"
            variant="ghost"
            size="icon-sm"
            aria-label={`Remove ${name}`}
            className="hover:text-destructive"
            disabled={!orphaned}
            onClick={onRemove}
          >
            <Trash2 className="size-4" aria-hidden />
          </Button>
        </div>
      </TableCell>
    </TableRow>
  );
}

// Active = still in the current kubeconfig; Orphaned = a kubeconfig-sourced
// record whose context is gone (its observation block stays, with
// isPresent=false). Anything not active is shown as orphaned so no known
// cluster is silently dropped. Empty groups are omitted by the caller.
const GROUPS: { key: Group; label: string; suffix: string; match: (c: Cluster) => boolean }[] = [
  {
    key: 'active',
    label: 'Active',
    suffix: 'in kubeconfig',
    match: (c) => c.status.source.kubeconfig?.isPresent ?? false,
  },
  {
    key: 'orphaned',
    label: 'Orphaned',
    suffix: 'cache on disk, no longer in kubeconfig',
    match: (c) => !c.status.source.kubeconfig?.isPresent,
  },
];

export function ClusterSyncPanel() {
  const { clusters } = useClusters();
  const rows = clusters ?? [];
  const groups = GROUPS.map((g) => ({ ...g, clusters: rows.filter(g.match) })).filter((g) => g.clusters.length > 0);

  // Each action writes through to the sidecar; the resulting clustersWatch push
  // is the source of truth for each row's state (no local mirror). urql's execute
  // resolves (never rejects) with an OperationResult, so a failed mutation would
  // otherwise be swallowed — surface its error on the bus.
  const [, clusterSyncEnabledSetMut] = useMutation(ClusterSyncEnabledSetMutation);
  const [, clusterCacheClearMut] = useMutation(ClusterCacheClearMutation);
  const [, clusterDeleteMut] = useMutation(ClusterDeleteMutation);

  const run = (p: Promise<{ error?: unknown }>) => {
    p.then((result) => {
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
      {/* Match the sheet's own `data-[side=right]:` width utilities so tailwind-merge
          replaces them (a plain `w-…` is a different key, so it'd be kept *alongside*
          the built-in and lose on specificity) — widen the panel to fit the table. */}
      <SheetContent side="right" className="data-[side=right]:w-[56rem] data-[side=right]:sm:max-w-[95vw]">
        <SheetHeader>
          <SheetTitle>Clusters</SheetTitle>
          <SheetDescription>Clusters in your kubeconfig and any leftover local caches.</SheetDescription>
        </SheetHeader>
        {rows.length === 0 ? (
          <p className="px-4 py-6 text-sm text-muted-foreground">No clusters yet.</p>
        ) : (
          <div className="min-h-0 flex-1 overflow-y-auto px-4 pb-4">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Cluster</TableHead>
                  <TableHead>Connection</TableHead>
                  <TableHead>Sync status</TableHead>
                  <TableHead>Cache</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              {groups.map((g) => (
                <TableBody key={g.key} aria-label={`${g.label} clusters`}>
                  <TableRow className="hover:bg-transparent">
                    <TableHead colSpan={5} className="h-auto py-1 text-xs tracking-wide text-muted-foreground">
                      <span className="font-semibold uppercase">{g.label}</span>
                      <span className="font-normal"> · {g.suffix}</span>
                    </TableHead>
                  </TableRow>
                  {g.clusters.map((c) => (
                    <ClusterRow
                      key={c.id}
                      cluster={c}
                      group={g.key}
                      onToggle={(syncEnabled) => run(clusterSyncEnabledSetMut({ id: c.id, syncEnabled }))}
                      onClearCache={() => run(clusterCacheClearMut({ id: c.id }))}
                      onRemove={() => run(clusterDeleteMut({ id: c.id }))}
                    />
                  ))}
                </TableBody>
              ))}
            </Table>
          </div>
        )}
      </SheetContent>
    </Sheet>
  );
}
