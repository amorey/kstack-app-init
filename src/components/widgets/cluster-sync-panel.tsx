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
import ReactTimeAgo, { type Formatter } from 'react-timeago';
import { useMutation, useSubscription } from 'urql';

import { Button } from '@kubetail/ui/elements/button';
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from '@kubetail/ui/elements/sheet';
import { Spinner } from '@kubetail/ui/elements/spinner';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@kubetail/ui/elements/table';

import { graphql } from '@/gql';
import { type Cluster, formatBytes, useClusters } from '@/lib/clusters';
import { errorMessage, reportError } from '@/lib/error-bus';

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

// The connection-probe history is served generically from the control plane's
// event log (category "connection"), decoupled from the clustersWatch list so
// probe chatter never re-emits the whole registry. It streams bare runs — the
// timeline as a snapshot on subscribe, then live runs conflated per id — so this
// is only subscribed while a row's diagnostics are open.
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

// One aggregated event run, projected from a generic Event: `ok` derives from the
// event type (Normal = a healthy/successful run). Shared by the connection-probe
// and cache-sync histories — both are the same generic Event timeline, keyed and
// rendered identically.
type EventRun = {
  id: string;
  ok: boolean;
  reason: string;
  message: string;
  count: number;
  firstAt: string;
  lastAt: string;
};

// A raw generic Event frame off a *EventsWatch subscription, before `ok` is
// derived from its type.
type RawEvent = Omit<EventRun, 'ok'> & { type: string };

// Fold one raw Event frame into a newest-first run list: upsert by run id (a
// re-delivered id is an updated run with a bumped count; a new id is a new run),
// then re-sort by lastAt descending.
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

// The per-cluster connection-probe history (event category "connection"),
// subscribed only while a row's diagnostics are open.
function useConnectionAttempts(clusterId: string): EventRun[] {
  const [{ data }] = useSubscription<{ clusterEventsWatch: RawEvent }, EventRun[]>(
    { query: ClusterConnectionEventsSubscription, variables: { id: clusterId } },
    (prev, resp) => foldRun(prev, resp.clusterEventsWatch),
  );
  return data ?? [];
}

// The per-cache sync-event history: the cache controller records each engine
// state (Watching / Syncing / SyncFailed) under category "sync" on the
// ClusterCache's own timeline, streamed generically like the connection history
// but keyed by the active cache's id. Subscribed only while a row's sync detail
// is open, so an idle cluster costs nothing.
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
  const [{ data }] = useSubscription<{ clusterCacheEventsWatch: RawEvent }, EventRun[]>(
    { query: ClusterSyncEventsSubscription, variables: { id: cacheId } },
    (prev, resp) => foldRun(prev, resp.clusterCacheEventsWatch),
  );
  return data ?? [];
}

// The next-reconcile time is a gauge streamed per-cluster (current-on-subscribe,
// then a fresh value on every reschedule), decoupled from the clustersWatch list.
// A scheduling change fires no list watch, so this is the only way the "Next
// check" countdown stays live for an otherwise-idle disconnected cluster —
// subscribed only while a row's diagnostics are open.
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
  // Two facts ride this one gauge: `nextRequeueAt` (when the next probe is
  // scheduled) and `probing` (a probe is running *now*, asserted by the
  // controller). `nextRequeueAt` is null in two very different situations: the
  // scheduler reports the zero time *while a reconcile is in flight* (so `probing`
  // is true), and separately when nothing is scheduled at all — a disabled,
  // orphaned, or otherwise ineligible cluster (so `probing` is false). Hold the
  // last scheduled time only across the in-flight window; once `probing` is false a
  // null is authoritative and must *clear* the countdown, not freeze a stale one.
  // `probing` is always taken from the latest frame (no holding).
  const [{ data }] = useSubscription<
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
// each row, and the detail row's placeholder stay aligned. Must be a non-zero
// width so table-fixed doesn't collapse the column to 0px.
const STATUS_CELL_CLASS = 'w-5 pr-0';

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
  // Connected but the watch went quiet past the freshness threshold: the cache
  // may be behind. Amber (attention) — a real concern, but not the hard error a
  // SyncFailed is, and the connection axis is fine.
  if (synced?.reason === 'Stale') return { label: 'Stale', tone: 'attention' };
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

// Diagnostics behind a cluster's connection: whether it's currently up and since
// when. `stateSinceMs` is the `Connected` condition's last transition — how long
// it's held its current up/down state. The next-reconcile time and the
// recent-attempt history are fetched separately (per open row) via
// useNextCheck / useConnectionAttempts, not carried on the cluster object.
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

// Always surface the elapsed seconds (no "just now" bucket) so the connection
// diagnostics read as a live counter — "1s ago", "2s ago", … — rolling up to
// minutes/hours past those thresholds. react-timeago ticks every second while
// sub-minute (lib/index.js), so this increments in realtime; only the 4th
// formatter arg (the timestamp's epoch ms) is needed.
const relativeFormatter: Formatter = (_value, _unit, _suffix, epochMs) => {
  const diff = Math.max(0, Date.now() - epochMs);
  if (diff < MINUTE_MS) return `${Math.floor(diff / SECOND_MS)}s ago`;
  if (diff < HOUR_MS) return `${Math.floor(diff / MINUTE_MS)}m ago`;
  return `${Math.floor(diff / HOUR_MS)}h ago`;
};

// A coarse, slow-ticking elapsed counter ("<1m" / "5m" / "3h") — largest unit,
// minute-grained. Used for how long the connection has held its current state
// (up or down), so it doesn't tick by the second alongside the "Next check"
// countdown (two fast counters running opposite directions read as noise).
const elapsedCoarseFormatter: Formatter = (_value, _unit, _suffix, epochMs) => {
  const diff = Math.max(0, Date.now() - epochMs);
  if (diff < MINUTE_MS) return '<1m';
  if (diff < HOUR_MS) return `${Math.floor(diff / MINUTE_MS)}m`;
  return `${Math.floor(diff / HOUR_MS)}h`;
};

// Human-readable span of an aggregated run, largest unit only ("45s" / "3m" /
// "2h"). "Once" when the run is a single probe (count === 1) — there's no
// meaningful window to describe.
function formatRunDuration(count: number, firstMs: number | null, lastMs: number | null): string {
  if (count <= 1 || firstMs === null || lastMs === null) return 'Once';
  const diff = Math.max(0, lastMs - firstMs);
  if (diff < MINUTE_MS) return `${Math.floor(diff / SECOND_MS)}s`;
  if (diff < HOUR_MS) return `${Math.floor(diff / MINUTE_MS)}m`;
  return `${Math.floor(diff / HOUR_MS)}h`;
}

// The mirror image, counting *down* to a future time — "in 1m 30s", "in 14s" —
// for the next scheduled retry. Shows minutes *and* seconds so it visibly ticks
// the whole way down (not a static "1m"); "now" once it's due (a probe is
// imminent; the next push restamps it into the future).
const countdownFormatter: Formatter = (_value, _unit, _suffix, epochMs) => {
  const totalSec = Math.floor(Math.max(0, epochMs - Date.now()) / SECOND_MS);
  if (totalSec < 1) return 'now';
  if (totalSec < 60) return `in ${totalSec}s`;
  return `in ${Math.floor(totalSec / 60)}m ${totalSec % 60}s`;
};

// A live-ticking relative time. maxPeriod={1} forces a re-render every second:
// react-timeago otherwise throttles to once-a-minute above 60s (its "X ago"
// assumption), which freezes a countdown (e.g. stuck at "59s") instead of ticking.
function RelativeTime({ ms, formatter = relativeFormatter }: { ms: number; formatter?: Formatter }) {
  return <ReactTimeAgo date={ms} component="span" formatter={formatter} maxPeriod={1} />;
}

// One label/value line of the connection diagnostics. A non-null `ms` ticks live;
// a null `ms` shows `fallback`, or the row is omitted entirely when there's none.
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

// A newest-first list of aggregated event runs (a probe/sync history), under a
// small heading — the shared body of the connection and sync detail panes. Each
// entry is a run of consecutive same-outcome occurrences (×count over its
// [firstAt, lastAt] window). Renders nothing until at least one run has streamed
// in. `labelOf` names a run for its category (a connection success has no reason
// code, so it's labelled "Success"; a sync run just shows its reason).
// `showDuration` adds a per-run duration column ("Once" / "45s"); the sync pane
// omits it since its runs are one-shot transitions.
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
      {/* The stream returns newest-run-first. Scrolls vertically so a long history
          doesn't blow out the panel. The timestamp/reason/duration columns size to
          their content (shown in full, never truncated); the message column takes
          the remaining space and wraps. Subgrid keeps a row's cells aligned. */}
      <ul
        className={`grid max-h-40 ${
          showDuration ? 'grid-cols-[auto_auto_auto_1fr]' : 'grid-cols-[auto_auto_1fr]'
        } divide-y overflow-x-auto overflow-y-auto rounded-md border text-xs`}
      >
        {/* The run id is stable across re-deliveries (an extended run keeps it). */}
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
              {/* {T2} — the run's end time. The start is implied (T1 = T2 −
                  Duration); the full [firstAt, lastAt] window is on hover. */}
              <span
                className="whitespace-nowrap font-mono text-muted-foreground tabular-nums"
                title={a.count > 1 ? `${a.firstAt} – ${a.lastAt}` : a.lastAt}
              >
                {a.lastAt}
              </span>
              {/* {Label} ×{Count} — coloured by the run's own tone. */}
              <span className={`whitespace-nowrap ${a.ok ? TONE.ok.text : TONE.error.text}`}>
                {a.count > 1 ? `${labelOf(a)} ×${a.count}` : labelOf(a)}
              </span>
              {/* {Duration} — "Once" for a single occurrence, else the run's span. */}
              {showDuration ? (
                <span className="whitespace-nowrap font-mono text-muted-foreground tabular-nums">
                  {`(${formatRunDuration(a.count, firstMs, lastMs)})`}
                </span>
              ) : null}
              {/* {Message} — fills the remaining space and wraps so it's shown in
                  full rather than truncated. */}
              <span className="wrap-break-word text-muted-foreground">{a.message}</span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

// The expanded connection diagnostics, available in every connection state: the
// probe message, the connection timestamps, and the recent-attempt history. When
// the connection is down (`connFailed`) it also titles itself "Connection failed"
// and offers a Retry-now action (force an immediate reconnect, resetting backoff);
// otherwise it stays a neutral read-only view. Rendered inline in an expandable
// row rather than a floating popover: the panel lives inside the modal Sheet (a
// base-ui Dialog), which inerts everything outside its own subtree — so a
// body-portaled popover would be unclickable.
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
      <p className="text-sm font-medium">{connFailed ? 'Connection failed' : 'Connection'}</p>
      <dl className="space-y-0.5 text-xs text-muted-foreground">
        {/* How long the connection has been up (coarse, so it doesn't tick by
            the second against the "Next check" countdown) — "0m" while it's
            down — then the countdown to the scheduled re-probe. */}
        <DetailRow
          label="Uptime"
          ms={detail.connected ? detail.stateSinceMs : null}
          fallback="0m"
          formatter={elapsedCoarseFormatter}
        />
        {/* While a probe is actually in flight (`probing`, asserted by the
            controller) show the "checking…" spinner in place of the countdown —
            the `probing` flag is the signal for the spinner. When nothing is
            scheduled and no probe is running
            (a disabled/orphaned/ineligible cluster, or the brief pre-first-schedule
            window) the countdown is genuinely absent — show a neutral placeholder,
            never the spinner, so an idle cluster doesn't look like it's probing.
            The row stays mounted either way so the panel never reflows/flickers. */}
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
      {/* Force an immediate re-probe (reset backoff) — available in any state. */}
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

// "2,203 objects across 120 kinds" — the cache-content summary, thousands-grouped
// and pluralised. Includes cached events (one of the kinds), matching the
// engine's own object/kind rollup.
function cacheSummary(objectCount: number, kindCount: number): string {
  const objects = `${objectCount.toLocaleString()} ${objectCount === 1 ? 'object' : 'objects'}`;
  const kinds = `${kindCount.toLocaleString()} ${kindCount === 1 ? 'kind' : 'kinds'}`;
  return `${objects} across ${kinds}`;
}

// The expanded sync diagnostics: the recent cache-sync event history for a
// cluster's active cache. Mirrors ConnectionDetail (an inline expandable region,
// not a popover, for the same modal-Sheet inert reason), keyed by the active
// cache's id — the sync-event stream lives on the ClusterCache, not the Cluster.
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
  // lastSyncedAt's age — a quiet-but-healthy cache legitimately has an old
  // lastSyncedAt, so only the engine's watch-liveness signal can flag it.
  const stale = syncReason === 'Stale';
  return (
    <div className="space-y-2 rounded-md border bg-muted/30 p-3">
      <p className="text-sm font-medium">Sync status</p>
      {stale ? (
        <p className={`text-xs ${TONE.attention.text}`}>
          Possibly stale — {syncMessage || 'the watch may have stopped delivering updates.'}
        </p>
      ) : null}
      {/* Cache-content summary — "how much do I hold?", a static counterpart to the
          freshness line below. Shown only when the cache has objects (an empty or
          not-yet-populated cache is already covered by the freshness line's
          "No updates received yet."). */}
      {objectCount > 0 ? (
        <div className="flex gap-2 text-xs text-muted-foreground">
          <span>Cached:</span>
          <span className="tabular-nums">{cacheSummary(objectCount, kindCount)}</span>
        </div>
      ) : null}
      {/* Freshness — "is my local copy current?" — answered by when the cache last
          received data, as a live relative counter. Deliberately separate from the
          sync-event history below (which is a transition log): the two answer
          different questions ("is it current now?" vs "what happened?"). */}
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
  // The connection label is a disclosure toggle in every state — expanding it
  // shows the diagnostics and the recent-attempt history (failed or not). The
  // detail panel adapts its header/actions to whether the connection is down.
  const connFailed = connection.tone === 'error';
  // Only one detail row can be open at a time — opening one collapses the other.
  const [openDetail, setOpenDetail] = useState<'connection' | 'sync' | null>(null);
  const showDetail = openDetail === 'connection';
  const setShowDetail = (open: boolean) => setOpenDetail(open ? 'connection' : null);
  // The sync label is a disclosure too — but only when there's an active cache to
  // stream sync events for (a pending/never-cached row has no timeline). Expanding
  // it shows the recent cache-sync event history.
  const cacheId = cluster.activeCache?.id;
  // The active cache's Synced condition — drives the sync detail's stale banner.
  const syncCond = findCondition(cluster.activeCache?.status.conditions ?? [], 'Synced');
  const showSyncDetail = openDetail === 'sync';
  const setShowSyncDetail = (open: boolean) => setOpenDetail(open ? 'sync' : null);
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
          {/* The sync label is a disclosure (recent sync-event history) when the
              cluster has an active cache; otherwise it's plain text (nothing to
              show). Mirrors the connection column's toggle. */}
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
      <SheetContent side="right" className="data-[side=right]:w-4xl data-[side=right]:sm:max-w-[95vw]">
        <SheetHeader>
          <SheetTitle>Clusters</SheetTitle>
          <SheetDescription>Clusters in your kubeconfig and any leftover local caches.</SheetDescription>
        </SheetHeader>
        {rows.length === 0 ? (
          <p className="px-4 py-6 text-sm text-muted-foreground">No clusters yet.</p>
        ) : (
          <div className="min-h-0 flex-1 overflow-y-auto px-4 pb-4">
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
