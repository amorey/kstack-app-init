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
import { ChevronDown, Database, Pause, Play, Power, PowerOff, RotateCw, Slash, Trash2 } from 'lucide-react';
import { type ReactNode, useEffect, useState } from 'react';
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

const ClusterEnabledSetMutation = graphql(`
  mutation ClusterEnabledSet($id: ObjectID!, $enabled: Boolean!) {
    clusterEnabledSet(id: $id, enabled: $enabled) {
      id
      spec {
        enabled
      }
    }
  }
`);

const ClusterSyncEnabledSetMutation = graphql(`
  mutation ClusterSyncEnabledSet($id: ObjectID!, $syncEnabled: Boolean!) {
    clusterSyncEnabledSet(id: $id, syncEnabled: $syncEnabled) {
      id
      spec {
        syncEnabled
      }
    }
  }
`);

const ClusterCacheClearMutation = graphql(`
  mutation ClusterCacheClear($id: ObjectID!) {
    clusterCacheClear(id: $id) {
      id
    }
  }
`);

const ClusterDeleteMutation = graphql(`
  mutation ClusterDelete($id: ObjectID!) {
    clusterDelete(id: $id)
  }
`);

const ClusterConnectionRetryMutation = graphql(`
  mutation ClusterConnectionRetry($id: ObjectID!) {
    clusterConnectionRetry(id: $id)
  }
`);

type Tone = 'ok' | 'attention' | 'error' | 'muted';
type Group = 'active' | 'orphaned';

// One source of truth per tone, keeping its three facets locked together:
//   severity — the overall circle shows the *worst* of the two axes, so a single
//              glance answers "does this row need me?" (error > attention > ok > muted).
//   dot      — solid fill for the leading status circle (the at-a-glance anchor).
//   text     — text-tuned shade (darker in light mode) for the tinted column
//              label, so the colored word explains *why* the circle is that
//              colour. `muted` stays neutral so healthy/idle rows read calm.
const TONE: Record<Tone, { severity: number; dot: string; text: string }> = {
  error: { severity: 3, dot: 'bg-red-500', text: 'text-red-600 dark:text-red-400' },
  attention: { severity: 2, dot: 'bg-amber-500', text: 'text-amber-600 dark:text-amber-400' },
  ok: { severity: 1, dot: 'bg-emerald-500', text: 'text-emerald-600 dark:text-emerald-400' },
  muted: { severity: 0, dot: 'bg-muted-foreground/50', text: 'text-muted-foreground' },
};

// The table's column count, the single source for the colSpan magic numbers
// below — adding a column means touching only this.
const COLUMN_COUNT = 6;

// The leading status-circle column is sized to its dot; shared so the header,
// each row, and the detail row's placeholder stay aligned.
const STATUS_CELL_CLASS = 'w-0 pr-0';

// Combine the connection and sync tones into the single overall tone. Exported
// for unit testing.
export function overallTone(connection: Tone, sync: Tone): Tone {
  return TONE[connection].severity >= TONE[sync].severity ? connection : sync;
}

// A tinted status label (the connection/sync column word). The colour is the
// only thing that varies, so it lives here once.
function ToneText({ tone, children }: { tone: Tone; children: ReactNode }) {
  return (
    <span data-tone={tone} className={TONE[tone].text}>
      {children}
    </span>
  );
}

// A pending cluster is a kubeconfig context the sidecar hasn't identified yet
// (its kube-system UID probe hasn't succeeded — e.g. a stopped minikube): until
// it's reachable it can't be synced or cached.
function isPending(c: Cluster): boolean {
  return !c.status.server.uid;
}

function displayName(c: Cluster): string {
  return c.spec.name || c.spec.source.kubeconfig?.context || c.id;
}

// Look up a Kubernetes-style status condition by its type; undefined if absent.
function findCondition<T extends { type: string }>(conditions: T[], type: string): T | undefined {
  return conditions.find((cond) => cond.type === type);
}

// The Connection column: can we currently reach the cluster's API? This derives
// from the live `Connected` condition the sidecar publishes (not the sticky
// `server.uid`, which records the last *successful* probe and never clears —
// so a dropped connection would otherwise keep reading "Active"). True → Active
// (green); False → Disconnected (red, e.g. internet off / probe failed);
// Unknown or not-yet-probed → Connecting (muted); an orphan has no live cluster
// to connect to (Unavailable).
function connectionStatus(c: Cluster, group: Group): { label: string; tone: Tone } {
  if (group === 'orphaned') return { label: 'Unavailable', tone: 'muted' };
  const connected = findCondition(c.status.conditions, 'Connected');
  if (connected?.status === 'True') return { label: 'Active', tone: 'ok' };
  if (connected?.status === 'False') return { label: 'Disconnected', tone: 'error' };
  return { label: 'Connecting', tone: 'attention' };
}

// Sync status. An orphan isn't syncing (its cluster is gone — "Stopped", not
// repeating the group's "Orphaned" header); a pending context can't sync yet;
// sync turned off is "Paused". For an enabled, identified cluster we reflect
// the actual sync state, but the engine's own signal isn't enough on its own:
// per-driver watch failures are retried in the background, so a dropped
// connection leaves the engine reporting a stale "Watching" rather than an
// error. So gate on the live `Connected` condition first — disconnected sync
// is "Stalled" (engine retrying, no data flowing) — then surface an
// engine-level "SyncFailed" as an Error, otherwise it's Syncing.
function statusOf(c: Cluster, group: Group): { label: string; tone: Tone } {
  if (group === 'orphaned') return { label: 'Stopped', tone: 'muted' };
  if (isPending(c)) return { label: 'Not synced', tone: 'muted' };
  if (!c.spec.syncEnabled) return { label: 'Paused', tone: 'muted' };
  const connected = findCondition(c.status.conditions, 'Connected');
  // Gated value: the fault is in the connection axis, not here — so this is
  // muted (gray), not amber. The connection column carries the actual error
  // colour; graying the gated sync value keeps it from reading as an
  // independent source of trouble rather than a downstream symptom.
  if (connected?.status === 'False') return { label: 'Stalled', tone: 'muted' };
  const synced = findCondition(c.activeCache?.status.conditions ?? [], 'Synced');
  if (synced?.reason === 'SyncFailed') return { label: 'Error', tone: 'error' };
  return { label: 'Syncing', tone: 'ok' };
}

// The leading status circle: a rollup of both axes (tone = worst of the two),
// with a tooltip summarising each so the single dot stays informative on hover.
// Takes the already-derived column statuses so the row computes each once.
type Status = { label: string; tone: Tone };
function overallStatus(connection: Status, sync: Status): { tone: Tone; summary: string } {
  return { tone: overallTone(connection.tone, sync.tone), summary: `${connection.label} · ${sync.label}` };
}

// Parse an ISO timestamp to epoch-ms, or null if absent/unparseable.
function parseTimeOrNull(iso: string | null | undefined): number | null {
  const ms = iso ? Date.parse(iso) : NaN;
  return Number.isNaN(ms) ? null : ms;
}

// Diagnostics behind a Disconnected cluster: the probe error and the relevant
// timestamps. `message` is the live `Connected` condition's message (the
// underlying error); `lostAt` is when it last flipped to disconnected;
// `lastConnectedAt` is the last *successful* connect (null = never reached).
function connectionDetail(c: Cluster): { message: string; lostAtMs: number | null; lastConnectedAtMs: number | null } {
  const cond = findCondition(c.status.conditions, 'Connected');
  return {
    message: cond?.message ?? '',
    lostAtMs: parseTimeOrNull(cond?.lastTransitionTime),
    lastConnectedAtMs: parseTimeOrNull(c.status.lastConnectedAt),
  };
}

// The expanded connection diagnostics for a Disconnected cluster: the probe
// error, how long it's been down, when it last connected, and a Retry-now action
// (force an immediate reconnect, resetting backoff). Rendered inline in an
// expandable row rather than a floating popover: the panel lives inside the modal
// Sheet (a base-ui Dialog), which inerts everything outside its own subtree — so
// a body-portaled popover would be unclickable.
function ConnectionDetail({ cluster, onRetry }: { cluster: Cluster; onRetry: () => void }) {
  const detail = connectionDetail(cluster);
  // The re-probe runs out-of-band: the mutation resolves the instant it's
  // *scheduled*, and the actual outcome arrives later via clustersWatch (a
  // successful reconnect flips the row to Active and unmounts this panel). So
  // there's no completion to await — show a brief "Retrying…" acknowledgement so
  // the click isn't silent, then revert if we're still disconnected.
  const [retrying, setRetrying] = useState(false);
  useEffect(() => {
    if (!retrying) return undefined;
    const timer = setTimeout(() => setRetrying(false), 4_000);
    return () => clearTimeout(timer);
  }, [retrying]);

  return (
    <div className="space-y-2 rounded-md border bg-muted/30 p-3">
      <p className="text-sm font-medium">Connection failed</p>
      {detail.message ? (
        <p className="break-words font-mono text-xs leading-snug text-muted-foreground">{detail.message}</p>
      ) : null}
      <dl className="space-y-0.5 text-xs text-muted-foreground">
        <div className="flex gap-2">
          <dt className="w-28 shrink-0">Last connected</dt>
          <dd className="tabular-nums">
            {detail.lastConnectedAtMs !== null ? formatSyncFreshness(detail.lastConnectedAtMs) : 'never'}
          </dd>
        </div>
        {detail.lostAtMs !== null ? (
          <div className="flex gap-2">
            <dt className="w-28 shrink-0">Lost connection</dt>
            <dd className="tabular-nums">{formatSyncFreshness(detail.lostAtMs)}</dd>
          </div>
        ) : null}
      </dl>
      <Button
        type="button"
        variant="outline"
        size="sm"
        disabled={retrying}
        onClick={() => {
          setRetrying(true);
          onRetry();
        }}
      >
        <RotateCw className={`size-3.5 ${retrying ? 'animate-spin' : ''}`} aria-hidden />
        {retrying ? 'Retrying…' : 'Retry now'}
      </Button>
    </div>
  );
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
  onSetEnabled,
  onToggle,
  onClearCache,
  onRemove,
  onRetry,
}: {
  cluster: Cluster;
  group: Group;
  onSetEnabled: (enabled: boolean) => void;
  onToggle: (enabled: boolean) => void;
  onClearCache: () => void;
  onRemove: () => void;
  onRetry: () => void;
}) {
  const name = displayName(cluster);
  const connection = connectionStatus(cluster, group);
  const status = statusOf(cluster, group);
  const overall = overallStatus(connection, status);
  const orphaned = group === 'orphaned';
  // Only a Disconnected cluster has diagnostics worth expanding; its label
  // becomes a disclosure toggle for an inline detail row.
  const connFailed = connection.tone === 'error';
  const [showDetail, setShowDetail] = useState(false);
  const pending = isPending(cluster);
  const { enabled } = cluster.spec;
  // Sync can only start/stop for an enabled, identified cluster still in the
  // kubeconfig.
  const syncing = cluster.spec.syncEnabled && enabled && !orphaned && !pending;
  const canToggle = enabled && !orphaned && !pending;

  return (
    <>
      <TableRow>
        <TableCell className={`${STATUS_CELL_CLASS} align-top`}>
          <span
            role="img"
            aria-label={overall.summary}
            title={overall.summary}
            data-tone={overall.tone}
            className={`mt-1 block size-2.5 shrink-0 rounded-full ${TONE[overall.tone].dot}`}
          />
        </TableCell>
        <TableCell className="font-medium align-top">{name}</TableCell>
        <TableCell className="align-top">
          {connFailed ? (
            <button
              type="button"
              aria-expanded={showDetail}
              onClick={() => setShowDetail((v) => !v)}
              data-tone={connection.tone}
              className={`${TONE[connection.tone].text} inline-flex cursor-pointer items-center gap-1 rounded-sm underline decoration-dotted underline-offset-2 outline-none focus-visible:ring-2 focus-visible:ring-ring`}
            >
              {connection.label}
              <ChevronDown className={`size-3 transition-transform ${showDetail ? 'rotate-180' : ''}`} aria-hidden />
            </button>
          ) : (
            <ToneText tone={connection.tone}>{connection.label}</ToneText>
          )}
        </TableCell>
        <TableCell>
          <ToneText tone={status.tone}>{status.label}</ToneText>
          {cluster.activeCache?.status.lastSyncedAt ? (
            <div className="text-xs text-muted-foreground">
              {formatSyncFreshness(Date.parse(cluster.activeCache.status.lastSyncedAt))}
            </div>
          ) : null}
        </TableCell>
        <TableCell className="tabular-nums">
          {cluster.activeCache?.stats.exists ? formatBytes(cluster.activeCache.stats.bytes) : '—'}
        </TableCell>
        <TableCell>
          <div className="flex items-center justify-end gap-0.5">
            {/* Enable/disable the cluster in the app. A disabled cluster stays
              tracked but dormant (no connection, hidden from the context picker).
              Only meaningful while the context is present, so it's disabled for
              an orphan. */}
            <Button
              type="button"
              variant="ghost"
              size="icon-sm"
              aria-label={`${enabled ? 'Disable' : 'Enable'} ${name}`}
              disabled={orphaned}
              onClick={() => onSetEnabled(!enabled)}
            >
              {enabled ? <Power className="size-4" aria-hidden /> : <PowerOff className="size-4" aria-hidden />}
            </Button>
            <Button
              type="button"
              variant="ghost"
              size="icon-sm"
              aria-label={`${syncing ? 'Pause' : 'Resume'} sync for ${name}`}
              disabled={!canToggle}
              onClick={() => onToggle(!cluster.spec.syncEnabled)}
            >
              {syncing ? <Pause className="size-4" aria-hidden /> : <Play className="size-4" aria-hidden />}
            </Button>
            <Button
              type="button"
              variant="ghost"
              size="icon-sm"
              aria-label={`Clear cache for ${name}`}
              disabled={!cluster.activeCache?.stats.exists}
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
      {connFailed && showDetail ? (
        <TableRow className="hover:bg-transparent">
          <TableCell className={STATUS_CELL_CLASS} />
          <TableCell colSpan={COLUMN_COUNT - 1} className="pt-0">
            <ConnectionDetail cluster={cluster} onRetry={onRetry} />
          </TableCell>
        </TableRow>
      ) : null}
    </>
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
  const [, clusterEnabledSetMut] = useMutation(ClusterEnabledSetMutation);
  const [, clusterSyncEnabledSetMut] = useMutation(ClusterSyncEnabledSetMutation);
  const [, clusterCacheClearMut] = useMutation(ClusterCacheClearMutation);
  const [, clusterDeleteMut] = useMutation(ClusterDeleteMutation);
  const [, clusterConnectionRetryMut] = useMutation(ClusterConnectionRetryMutation);

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
                  <TableHead className={STATUS_CELL_CLASS}>
                    <span className="sr-only">Status</span>
                  </TableHead>
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
                    <TableHead
                      colSpan={COLUMN_COUNT}
                      className="h-auto py-1 text-xs tracking-wide text-muted-foreground"
                    >
                      <span className="font-semibold uppercase">{g.label}</span>
                      <span className="font-normal"> · {g.suffix}</span>
                    </TableHead>
                  </TableRow>
                  {g.clusters.map((c) => (
                    <ClusterRow
                      key={c.id}
                      cluster={c}
                      group={g.key}
                      onSetEnabled={(enabled) => run(clusterEnabledSetMut({ id: c.id, enabled }))}
                      onToggle={(syncEnabled) => run(clusterSyncEnabledSetMut({ id: c.id, syncEnabled }))}
                      onClearCache={() => run(clusterCacheClearMut({ id: c.id }))}
                      onRemove={() => run(clusterDeleteMut({ id: c.id }))}
                      onRetry={() => run(clusterConnectionRetryMut({ id: c.id }))}
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
