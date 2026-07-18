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

// Dialog showing the app's cluster registry as a table, grouped into Active (in
// the current kubeconfig) and Orphaned (a leftover cache whose context is gone).
// Data and state come from the sidecar via useClusters(); mutations write through
// and the resulting clustersWatch push updates the table.
import { ChevronDown, Database, Pause, Play, Power, PowerOff, RotateCw, Slash, Trash2 } from 'lucide-react';
import { type ReactNode, useEffect, useState } from 'react';
import ReactTimeAgo, { type Formatter } from 'react-timeago';
import { useMutation } from 'urql';

import { Button } from '@kubetail/ui/elements/button';
import { Spinner } from '@kubetail/ui/elements/spinner';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@kubetail/ui/elements/table';

import { Dialog } from '@/components/widgets/dialog';
import { graphql } from '@/gql';
import { type Cluster, formatBytes, useClusters } from '@/lib/clusters';
import { type AppDialogProps } from '@/lib/dialog';
import { errorMessage, reportError } from '@/lib/error-bus';
import { useWatchSubscription } from '@/lib/graphql/use-watch-subscription';

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

// Connection-probe history from the control plane's event log (category
// "connection"), decoupled from clustersWatch so probe chatter never re-emits the
// whole registry. Subscribed only while a row's diagnostics are open.
const ClusterConnectionEventsSubscription = graphql(`
  subscription ClusterConnectionEvents($id: ObjectID!) {
    clusterEventsWatch(id: $id, category: "connection") {
      id
      type
      reason
      message
      count
      firstAt
      lastAt
    }
  }
`);

// One aggregated event run, projected from a generic Event (`ok` = Normal type).
// Shared by the connection-probe and cache-sync histories.
type EventRun = {
  id: string;
  ok: boolean;
  reason: string;
  message: string;
  count: number;
  firstAt: string;
  lastAt: string;
};

// A raw Event frame off a *EventsWatch subscription, before `ok` is derived.
type RawEvent = Omit<EventRun, 'ok'> & { type: string };

// Fold one raw Event frame into a newest-first run list: upsert by run id, then
// re-sort by lastAt descending.
function foldRun(prev: EventRun[] | undefined, ev: RawEvent): EventRun[] {
  const run: EventRun = {
    id: ev.id,
    ok: ev.type === 'Normal',
    reason: ev.reason,
    message: ev.message,
    count: ev.count,
    firstAt: ev.firstAt,
    lastAt: ev.lastAt,
  };
  const next = (prev ?? []).filter((a) => a.id !== run.id);
  next.push(run);
  next.sort((a, b) => (a.lastAt < b.lastAt ? 1 : -1));
  return next;
}

function useConnectionAttempts(clusterId: string): EventRun[] {
  const [{ data }] = useWatchSubscription<{ clusterEventsWatch: RawEvent }, EventRun[]>(
    { query: ClusterConnectionEventsSubscription, variables: { id: clusterId } },
    (prev, resp) => foldRun(prev, resp.clusterEventsWatch),
  );
  return data ?? [];
}

// Per-cache sync-event history: the cache controller records each engine state
// (Watching / Syncing / SyncFailed) under category "sync" on the ClusterCache's
// timeline. Keyed by the active cache's id; subscribed only while sync detail is open.
const ClusterSyncEventsSubscription = graphql(`
  subscription ClusterSyncEvents($id: ObjectID!) {
    clusterCacheEventsWatch(id: $id, category: "sync") {
      id
      type
      reason
      message
      count
      firstAt
      lastAt
    }
  }
`);

function useSyncEvents(cacheId: string): EventRun[] {
  const [{ data }] = useWatchSubscription<{ clusterCacheEventsWatch: RawEvent }, EventRun[]>(
    { query: ClusterSyncEventsSubscription, variables: { id: cacheId } },
    (prev, resp) => foldRun(prev, resp.clusterCacheEventsWatch),
  );
  return data ?? [];
}

// The next-reconcile time, streamed per-cluster (a scheduling change fires no list
// watch, so this is the only way the "Next check" countdown stays live for an idle
// disconnected cluster). Subscribed only while a row's diagnostics are open.
const ClusterScheduleSubscription = graphql(`
  subscription ClusterSchedule($id: ObjectID!) {
    clusterScheduleWatch(id: $id) {
      nextRequeueAt
      probing
    }
  }
`);

type NextCheck = { atMs: number | null; probing: boolean };

function useNextCheck(clusterId: string): NextCheck {
  // `nextRequeueAt` is null in two cases: a reconcile is in flight (so `probing` is
  // true), or nothing is scheduled at all — disabled/orphaned/ineligible (`probing`
  // false). Hold the last scheduled time only across the in-flight window; once
  // `probing` is false a null must clear the countdown, not freeze a stale one.
  const [{ data }] = useWatchSubscription<
    { clusterScheduleWatch: { nextRequeueAt: string | null; probing: boolean } },
    { nextRequeueAt: string | null; probing: boolean }
  >({ query: ClusterScheduleSubscription, variables: { id: clusterId } }, (prev, resp) => {
    const { nextRequeueAt, probing } = resp.clusterScheduleWatch;
    return {
      nextRequeueAt: nextRequeueAt ?? (probing ? (prev?.nextRequeueAt ?? null) : null),
      probing,
    };
  });
  return { atMs: parseTimeOrNull(data?.nextRequeueAt ?? null), probing: data?.probing ?? false };
}

type Tone = 'ok' | 'attention' | 'error' | 'muted';
type Group = 'active' | 'orphaned';

// One source of truth per tone:
//   severity — the overall circle shows the worst of the two axes (error > attention > ok > muted).
//   dot      — solid fill for the leading status circle.
//   text     — text-tuned shade (darker in light mode) for the tinted column label.
const TONE: Record<Tone, { severity: number; dot: string; text: string }> = {
  error: { severity: 3, dot: 'bg-red-500', text: 'text-red-600 dark:text-red-400' },
  attention: { severity: 2, dot: 'bg-amber-500', text: 'text-amber-600 dark:text-amber-400' },
  ok: { severity: 1, dot: 'bg-emerald-500', text: 'text-emerald-600 dark:text-emerald-400' },
  muted: { severity: 0, dot: 'bg-muted-foreground/50', text: 'text-muted-foreground' },
};

// The table's column count — single source for the colSpan numbers below.
const COLUMN_COUNT = 6;

// The leading status-circle column, sized to its dot and shared so header, rows,
// and the detail placeholder align. Non-zero width so table-fixed doesn't collapse it.
const STATUS_CELL_CLASS = 'w-5 pr-0';

// Exported for unit testing.
export function overallTone(connection: Tone, sync: Tone): Tone {
  return TONE[connection].severity >= TONE[sync].severity ? connection : sync;
}

// A tinted status label (the connection/sync column word).
function ToneText({ tone, children }: { tone: Tone; children: ReactNode }) {
  return (
    <span data-tone={tone} className={TONE[tone].text}>
      {children}
    </span>
  );
}

// A pending cluster is a context the sidecar hasn't identified yet (its kube-system
// UID probe hasn't succeeded — e.g. a stopped minikube): it can't sync or cache yet.
function isPending(c: Cluster): boolean {
  return !c.status.server.uid;
}

function displayName(c: Cluster): string {
  return c.spec.name || c.spec.source.kubeconfig?.context || c.id;
}

function findCondition<T extends { type: string }>(conditions: T[], type: string): T | undefined {
  return conditions.find((cond) => cond.type === type);
}

// The Connection column: can we currently reach the cluster's API? Derives from the
// live `Connected` condition, not the sticky `server.uid` (which records the last
// successful probe and never clears, so it would keep reading "Active" after a drop).
function connectionStatus(c: Cluster, group: Group): { label: string; tone: Tone } {
  if (group === 'orphaned') return { label: 'Unavailable', tone: 'muted' };
  const connected = findCondition(c.status.conditions, 'Connected');
  if (connected?.status === 'True') return { label: 'Active', tone: 'ok' };
  if (connected?.status === 'False') return { label: 'Disconnected', tone: 'error' };
  return { label: 'Connecting', tone: 'attention' };
}

// Sync status. The engine's own signal isn't enough: per-driver watch failures are
// retried in the background, so a dropped connection leaves the engine reporting a
// stale "Watching". So gate on the live `Connected` condition first (disconnected
// sync is "Stalled"), then surface an engine-level "SyncFailed" as Error.
function statusOf(c: Cluster, group: Group): { label: string; tone: Tone } {
  if (group === 'orphaned') return { label: 'Stopped', tone: 'muted' };
  if (isPending(c)) return { label: 'Not synced', tone: 'muted' };
  if (!c.spec.syncEnabled) return { label: 'Paused', tone: 'muted' };
  const connected = findCondition(c.status.conditions, 'Connected');
  // Muted, not amber: the fault is in the connection axis, which carries the error
  // colour — graying the gated sync value keeps it reading as a downstream symptom.
  if (connected?.status === 'False') return { label: 'Stalled', tone: 'muted' };
  const synced = findCondition(c.activeCache?.status.conditions ?? [], 'Synced');
  if (synced?.reason === 'SyncFailed') return { label: 'Error', tone: 'error' };
  // Connected but the watch went quiet past the freshness threshold: cache may be
  // behind. Amber, not the hard error a SyncFailed is.
  if (synced?.reason === 'Stale') return { label: 'Stale', tone: 'attention' };
  return { label: 'Syncing', tone: 'ok' };
}

// The leading status circle: a rollup of both axes (tone = worst of the two) plus a
// tooltip summarising each. Takes the already-derived column statuses.
type Status = { label: string; tone: Tone };
function overallStatus(connection: Status, sync: Status): { tone: Tone; summary: string } {
  return { tone: overallTone(connection.tone, sync.tone), summary: `${connection.label} · ${sync.label}` };
}

// Parse an ISO timestamp to epoch-ms, or null if absent/unparseable.
function parseTimeOrNull(iso: string | null | undefined): number | null {
  const ms = iso ? Date.parse(iso) : NaN;
  return Number.isNaN(ms) ? null : ms;
}

// Diagnostics behind a cluster's connection: whether it's up, and `stateSinceMs`
// (the `Connected` condition's last transition — how long it's held that state).
function connectionDetail(c: Cluster): {
  connected: boolean;
  stateSinceMs: number | null;
} {
  const cond = findCondition(c.status.conditions, 'Connected');
  return {
    connected: cond?.status === 'True',
    stateSinceMs: parseTimeOrNull(cond?.lastTransitionTime),
  };
}

const SECOND_MS = 1_000;
const MINUTE_MS = 60 * SECOND_MS;
const HOUR_MS = 60 * MINUTE_MS;

// Live "Xs ago" counter rolling up to minutes/hours; no "just now" bucket, so the
// diagnostics read as a live counter (react-timeago ticks every second sub-minute).
const relativeFormatter: Formatter = (_value, _unit, _suffix, epochMs) => {
  const diff = Math.max(0, Date.now() - epochMs);
  if (diff < MINUTE_MS) return `${Math.floor(diff / SECOND_MS)}s ago`;
  if (diff < HOUR_MS) return `${Math.floor(diff / MINUTE_MS)}m ago`;
  return `${Math.floor(diff / HOUR_MS)}h ago`;
};

// A coarse, minute-grained elapsed counter ("<1m" / "5m" / "3h") for how long the
// connection has held its state — so it doesn't tick per-second against the "Next
// check" countdown (two fast counters running opposite ways read as noise).
const elapsedCoarseFormatter: Formatter = (_value, _unit, _suffix, epochMs) => {
  const diff = Math.max(0, Date.now() - epochMs);
  if (diff < MINUTE_MS) return '<1m';
  if (diff < HOUR_MS) return `${Math.floor(diff / MINUTE_MS)}m`;
  return `${Math.floor(diff / HOUR_MS)}h`;
};

// Span of an aggregated run, largest unit only ("45s" / "3m" / "2h"); "Once" for a
// single probe (no meaningful window).
function formatRunDuration(count: number, firstMs: number | null, lastMs: number | null): string {
  if (count <= 1 || firstMs === null || lastMs === null) return 'Once';
  const diff = Math.max(0, lastMs - firstMs);
  if (diff < MINUTE_MS) return `${Math.floor(diff / SECOND_MS)}s`;
  if (diff < HOUR_MS) return `${Math.floor(diff / MINUTE_MS)}m`;
  return `${Math.floor(diff / HOUR_MS)}h`;
}

// Counts down to the next scheduled retry — "in 1m 30s", "in 14s". Shows minutes and
// seconds so it visibly ticks all the way down; "now" once it's due.
const countdownFormatter: Formatter = (_value, _unit, _suffix, epochMs) => {
  const totalSec = Math.floor(Math.max(0, epochMs - Date.now()) / SECOND_MS);
  if (totalSec < 1) return 'now';
  if (totalSec < 60) return `in ${totalSec}s`;
  return `in ${Math.floor(totalSec / 60)}m ${totalSec % 60}s`;
};

// A live-ticking relative time. maxPeriod={1} forces a re-render every second;
// react-timeago otherwise throttles to once-a-minute above 60s, freezing a countdown.
function RelativeTime({ ms, formatter = relativeFormatter }: { ms: number; formatter?: Formatter }) {
  return <ReactTimeAgo date={ms} component="span" formatter={formatter} maxPeriod={1} />;
}

// One label/value line of the connection diagnostics. A non-null `ms` ticks live; a
// null `ms` shows `fallback`, or the row is omitted when there's none.
function DetailRow({
  label,
  ms,
  fallback,
  formatter,
}: {
  label: string;
  ms: number | null;
  fallback?: ReactNode;
  formatter?: Formatter;
}) {
  if (ms === null && fallback === undefined) return null;
  return (
    <div className="flex gap-2">
      <dt className="w-28 shrink-0">{label}</dt>
      <dd className="tabular-nums">{ms !== null ? <RelativeTime ms={ms} formatter={formatter} /> : fallback}</dd>
    </div>
  );
}

// A newest-first list of aggregated event runs — the shared body of the connection
// and sync detail panes. Each entry is a run of same-outcome occurrences (×count over
// its [firstAt, lastAt] window). `labelOf` names a run for its category; `showDuration`
// adds a per-run duration column (the sync pane omits it — its runs are one-shot).
function EventRunList({
  title,
  runs,
  labelOf,
  showDuration = true,
}: {
  title: string;
  runs: EventRun[];
  labelOf: (r: EventRun) => string;
  showDuration?: boolean;
}) {
  if (runs.length === 0) return null;
  return (
    <div className="space-y-1">
      <p className="text-xs font-medium text-muted-foreground">{title}</p>
      {/* Scrolls vertically so a long history doesn't blow out the panel. The
          timestamp/reason/duration columns size to content; the message column takes
          the rest and wraps. Subgrid keeps a row's cells aligned. */}
      <ul
        className={`grid max-h-40 ${
          showDuration ? 'grid-cols-[auto_auto_auto_1fr]' : 'grid-cols-[auto_auto_1fr]'
        } divide-y overflow-x-auto overflow-y-auto rounded-md border text-xs`}
      >
        {runs.map((a) => {
          const firstMs = parseTimeOrNull(a.firstAt);
          const lastMs = parseTimeOrNull(a.lastAt);
          return (
            <li
              key={a.id}
              className={`${
                showDuration ? 'col-span-4' : 'col-span-3'
              } grid grid-cols-subgrid items-baseline gap-x-2 px-2 py-1`}
            >
              {/* The run's end time; full [firstAt, lastAt] window on hover. */}
              <span
                className="whitespace-nowrap font-mono text-muted-foreground tabular-nums"
                title={a.count > 1 ? `${a.firstAt} – ${a.lastAt}` : a.lastAt}
              >
                {a.lastAt}
              </span>
              <span className={`whitespace-nowrap ${a.ok ? TONE.ok.text : TONE.error.text}`}>
                {a.count > 1 ? `${labelOf(a)} ×${a.count}` : labelOf(a)}
              </span>
              {showDuration ? (
                <span className="whitespace-nowrap font-mono text-muted-foreground tabular-nums">
                  {`(${formatRunDuration(a.count, firstMs, lastMs)})`}
                </span>
              ) : null}
              <span className="wrap-break-word text-muted-foreground">{a.message}</span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

// The expanded connection diagnostics: probe message, connection timestamps, and the
// recent-attempt history, plus a Retry-now action. When down (`connFailed`) it titles
// itself "Connection failed". Rendered inline rather than as a popover because the
// modal dialog inerts everything outside its subtree, so a body-portaled popover
// would be unclickable.
function ConnectionDetail({
  cluster,
  connFailed,
  onRetry,
}: {
  cluster: Cluster;
  connFailed: boolean;
  onRetry: () => void;
}) {
  const detail = connectionDetail(cluster);
  const attempts = useConnectionAttempts(cluster.id);
  const { atMs: nextAttemptAtMs, probing } = useNextCheck(cluster.id);
  // The re-probe runs out-of-band: the mutation resolves the instant it's scheduled,
  // and the outcome arrives later via clustersWatch. There's no completion to await,
  // so show a brief "Retrying…" acknowledgement, then revert if still disconnected.
  const [retrying, setRetrying] = useState(false);
  useEffect(() => {
    if (!retrying) return undefined;
    const timer = setTimeout(() => setRetrying(false), 4_000);
    return () => clearTimeout(timer);
  }, [retrying]);

  return (
    <div className="space-y-2 rounded-md border bg-muted/30 p-3">
      <p className="text-sm font-medium">{connFailed ? 'Connection failed' : 'Connection'}</p>
      <dl className="space-y-0.5 text-xs text-muted-foreground">
        {/* How long the connection has been up (coarse, so it doesn't tick per-second
            against the "Next check" countdown); "0m" while down. */}
        <DetailRow
          label="Uptime"
          ms={detail.connected ? detail.stateSinceMs : null}
          fallback="0m"
          formatter={elapsedCoarseFormatter}
        />
        {/* While a probe is in flight (`probing`) show the "checking…" spinner in
            place of the countdown. When nothing is scheduled and no probe is running
            (disabled/orphaned/ineligible, or the pre-first-schedule window) show a
            neutral placeholder, not the spinner. The row stays mounted either way so
            the panel never reflows. */}
        <DetailRow
          label="Next check"
          ms={probing ? null : nextAttemptAtMs}
          fallback={
            probing ? (
              <span className="inline-flex items-center gap-1">
                <Spinner size="xs" className="mr-0" />
                checking…
              </span>
            ) : (
              '—'
            )
          }
          formatter={countdownFormatter}
        />
      </dl>
      <EventRunList title="Recent attempts" runs={attempts} labelOf={(a) => (a.ok ? 'Success' : a.reason)} />
      {/* Force an immediate re-probe (reset backoff). */}
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

// "2,203 objects across 120 kinds" — thousands-grouped and pluralised. Includes
// cached events, matching the engine's own object/kind rollup.
function cacheSummary(objectCount: number, kindCount: number): string {
  const objects = `${objectCount.toLocaleString()} ${objectCount === 1 ? 'object' : 'objects'}`;
  const kinds = `${kindCount.toLocaleString()} ${kindCount === 1 ? 'kind' : 'kinds'}`;
  return `${objects} across ${kinds}`;
}

// The expanded sync diagnostics: the recent cache-sync event history for a cluster's
// active cache. Inline (not a popover) for the same modal-dialog inert reason as
// ConnectionDetail, keyed by the active cache's id (the stream lives on ClusterCache).
function SyncDetail({
  cacheId,
  lastSyncedAt,
  objectCount,
  kindCount,
  syncReason,
  syncMessage,
}: {
  cacheId: string;
  lastSyncedAt: string | null;
  objectCount: number;
  kindCount: number;
  syncReason?: string;
  syncMessage?: string;
}) {
  const events = useSyncEvents(cacheId);
  const lastSyncedMs = parseTimeOrNull(lastSyncedAt);
  // Staleness is engine-derived (the Stale condition reason), never inferred from
  // lastSyncedAt's age — a quiet-but-healthy cache legitimately has an old one.
  const stale = syncReason === 'Stale';
  return (
    <div className="space-y-2 rounded-md border bg-muted/30 p-3">
      <p className="text-sm font-medium">Sync status</p>
      {stale ? (
        <p className={`text-xs ${TONE.attention.text}`}>
          Possibly stale — {syncMessage || 'the watch may have stopped delivering updates.'}
        </p>
      ) : null}
      {/* Cache-content summary — shown only when the cache has objects (an empty one
          is already covered by the freshness line's "No updates received yet."). */}
      {objectCount > 0 ? (
        <div className="flex gap-2 text-xs text-muted-foreground">
          <span>Cached:</span>
          <span className="tabular-nums">{cacheSummary(objectCount, kindCount)}</span>
        </div>
      ) : null}
      {/* Freshness — when the cache last received data, as a live counter. Separate
          from the sync-event history below (a transition log): "current now?" vs
          "what happened?". */}
      {lastSyncedMs !== null ? (
        <div className="flex gap-2 text-xs text-muted-foreground">
          <span>Last update received:</span>
          <span className="tabular-nums">
            <RelativeTime ms={lastSyncedMs} />
          </span>
        </div>
      ) : (
        <p className="text-xs text-muted-foreground">No updates received yet.</p>
      )}
      {events.length > 0 ? (
        <EventRunList title="Recent sync events" runs={events} labelOf={(e) => e.reason} showDuration={false} />
      ) : (
        <p className="text-xs text-muted-foreground">No sync events yet.</p>
      )}
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
  const connFailed = connection.tone === 'error';
  // Only one detail row can be open at a time.
  const [openDetail, setOpenDetail] = useState<'connection' | 'sync' | null>(null);
  const showDetail = openDetail === 'connection';
  const setShowDetail = (open: boolean) => setOpenDetail(open ? 'connection' : null);
  // The sync label is a disclosure only when there's an active cache to stream sync
  // events for (a pending/never-cached row has no timeline).
  const cacheId = cluster.activeCache?.id;
  // Drives the sync detail's stale banner.
  const syncCond = findCondition(cluster.activeCache?.status.conditions ?? [], 'Synced');
  const showSyncDetail = openDetail === 'sync';
  const setShowSyncDetail = (open: boolean) => setOpenDetail(open ? 'sync' : null);
  const pending = isPending(cluster);
  const { enabled } = cluster.spec;
  // Sync toggles only for an enabled, identified cluster still in the kubeconfig.
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
        <TableCell className="max-w-0 truncate font-medium align-top">{name}</TableCell>
        <TableCell className="align-top">
          <button
            type="button"
            aria-expanded={showDetail}
            onClick={() => setShowDetail(!showDetail)}
            data-tone={connection.tone}
            className={`${TONE[connection.tone].text} inline-flex cursor-pointer items-center gap-1 rounded-sm underline decoration-dotted underline-offset-2 outline-none focus-visible:ring-2 focus-visible:ring-ring`}
          >
            {connection.label}
            <ChevronDown className={`size-3 transition-transform ${showDetail ? 'rotate-180' : ''}`} aria-hidden />
          </button>
        </TableCell>
        <TableCell className="align-top">
          {/* A disclosure (sync-event history) when there's an active cache, else
              plain text. Mirrors the connection column's toggle. */}
          {cacheId ? (
            <button
              type="button"
              aria-expanded={showSyncDetail}
              onClick={() => setShowSyncDetail(!showSyncDetail)}
              data-tone={status.tone}
              className={`${TONE[status.tone].text} inline-flex cursor-pointer items-center gap-1 rounded-sm underline decoration-dotted underline-offset-2 outline-none focus-visible:ring-2 focus-visible:ring-ring`}
            >
              {status.label}
              <ChevronDown
                className={`size-3 transition-transform ${showSyncDetail ? 'rotate-180' : ''}`}
                aria-hidden
              />
            </button>
          ) : (
            <ToneText tone={status.tone}>{status.label}</ToneText>
          )}
        </TableCell>
        <TableCell className="tabular-nums">
          {cluster.activeCache?.stats.exists ? formatBytes(cluster.activeCache.stats.bytes) : '—'}
        </TableCell>
        <TableCell>
          <div className="flex items-center justify-end gap-0.5">
            {/* Enable/disable the cluster: a disabled one stays tracked but dormant
              (no connection, hidden from the context picker). Disabled for an orphan. */}
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
            {/* Always rendered so both groups keep the same action count (columns
              align); removing applies only to an orphan (a present context would just
              be re-discovered), so it's disabled otherwise. */}
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
      {showDetail ? (
        <TableRow className="hover:bg-transparent">
          <TableCell className={STATUS_CELL_CLASS} />
          <TableCell colSpan={COLUMN_COUNT - 1} className="pt-0">
            <ConnectionDetail cluster={cluster} connFailed={connFailed} onRetry={onRetry} />
          </TableCell>
        </TableRow>
      ) : null}
      {showSyncDetail && cacheId ? (
        <TableRow className="hover:bg-transparent">
          <TableCell className={STATUS_CELL_CLASS} />
          <TableCell colSpan={COLUMN_COUNT - 1} className="pt-0">
            <SyncDetail
              cacheId={cacheId}
              lastSyncedAt={cluster.activeCache?.status.lastSyncedAt ?? null}
              objectCount={cluster.activeCache?.stats.objectCount ?? 0}
              kindCount={cluster.activeCache?.stats.kindCount ?? 0}
              syncReason={syncCond?.reason}
              syncMessage={syncCond?.message}
            />
          </TableCell>
        </TableRow>
      ) : null}
    </>
  );
}

// Active = still in the current kubeconfig; Orphaned = a record whose context is gone
// (isPresent=false). Anything not active shows as orphaned so no known cluster is
// silently dropped. Empty groups are omitted by the caller.
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

// Open state is caller-controlled (`AppDialogs`, via the account menu); this renders
// just the dialog content and mounts only while open. The registry is tracked
// regardless by `ClustersProvider`'s root `clustersWatch`; per-row event/schedule
// streams mount only when a row's diagnostics open.
export function ClusterSyncPanel({ open, onOpenChange }: AppDialogProps) {
  const { clusters } = useClusters();
  const rows = clusters ?? [];
  const groups = GROUPS.map((g) => ({ ...g, clusters: rows.filter(g.match) })).filter((g) => g.clusters.length > 0);

  // Each action writes through to the sidecar; the resulting clustersWatch push is
  // the source of truth (no local mirror). urql's execute resolves (never rejects)
  // with an OperationResult, so surface a failed mutation's error on the bus.
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
    <Dialog
      open={open}
      onOpenChange={onOpenChange}
      title="Clusters"
      description="Clusters in your kubeconfig and any leftover local caches."
      // Widen past the dialog's default to fit the table, keeping a margin off the
      // window edges until it hits its max.
      className="w-[calc(100%-4rem)] max-w-4xl sm:max-w-4xl"
    >
      {rows.length === 0 ? (
        <p className="py-6 text-sm text-muted-foreground">No clusters yet.</p>
      ) : (
        <Table className="table-fixed">
          <TableHeader>
            <TableRow>
              <TableHead className={STATUS_CELL_CLASS}>
                <span className="sr-only">Status</span>
              </TableHead>
              <TableHead>Cluster</TableHead>
              <TableHead className="w-28">Connection</TableHead>
              <TableHead className="w-32">Sync status</TableHead>
              <TableHead className="w-20">Cache</TableHead>
              <TableHead className="w-36 text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          {groups.map((g) => (
            <TableBody key={g.key} aria-label={`${g.label} clusters`}>
              <TableRow className="hover:bg-transparent">
                <TableHead colSpan={COLUMN_COUNT} className="h-auto py-1 text-xs tracking-wide text-muted-foreground">
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
      )}
    </Dialog>
  );
}
