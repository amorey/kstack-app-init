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
import { type ReactNode, useEffect, useMemo, useState } from 'react';
import ReactTimeAgo, { type Formatter } from 'react-timeago';
import { useMutation } from 'urql';

import { Button } from '@kubetail/ui/elements/button';
import { Spinner } from '@kubetail/ui/elements/spinner';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@kubetail/ui/elements/table';

import { Dialog } from '@/components/widgets/dialog';
import { graphql } from '@/gql';
import type { ClusterCacheGvrDiscoveriesSubscription, ClusterCacheGvrSyncsSubscription } from '@/gql/graphql';
import {
  type Cluster,
  type ClusterCacheSyncHealth,
  type Keyed,
  applyChange,
  formatBytes,
  useClusters,
} from '@/lib/clusters';
import { type AppDialogProps } from '@/lib/dialog';
import { errorMessage, reportError } from '@/lib/error-bus';
import { EVENTS_GVR, gvrKey } from '@/lib/gvr';
import { useWatchSubscription, watchPhase } from '@/lib/graphql/use-watch-subscription';

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

// Sync-event history: each kind's worker records its transitions (SyncStart /
// SyncComplete / SyncDegraded / SyncStale …) under category "sync" on its own
// ClusterCacheGVRSync record's timeline — so this is keyed by that record's id, not the
// cache's. Subscribed only while sync detail is open, and only for one kind at a time.
const ClusterSyncEventsSubscription = graphql(`
  subscription ClusterSyncEvents($id: ObjectID!) {
    clusterCacheGVRSyncEventsWatch(id: $id, category: "sync") {
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

// One kind's sync-transition log. Paused until there is a kind to ask about — before the
// per-kind stream delivers anything there is no id, and subscribing with a placeholder
// would open a stream guaranteed to carry nothing.
function useSyncEvents(syncId: string | undefined): EventRun[] {
  const [{ data }] = useWatchSubscription<{ clusterCacheGVRSyncEventsWatch: RawEvent }, EventRun[]>(
    { query: ClusterSyncEventsSubscription, variables: { id: syncId ?? '' }, pause: !syncId },
    (prev, resp) => foldRun(prev, resp.clusterCacheGVRSyncEventsWatch),
  );
  return data ?? [];
}

// The cache's GVR-discovery record: which kinds the cluster serves and how current that
// answer is. It rides the fleet-wide stream (one record per cache — small enough to be
// unscoped), but is subscribed HERE rather than in ClustersProvider because this pane is
// its only reader: mounting it app-wide would open a stream in every window, and rebuild
// every joined Cluster identity on each discovery pass, for a row nobody has expanded.
const ClusterCacheGVRDiscoveriesSubscription = graphql(`
  subscription ClusterCacheGVRDiscoveries {
    clusterCacheGVRDiscoveriesWatch {
      type
      discovery {
        id
        cacheID
        stats {
          lastDiscoveryAt
          resourceCount
        }
        conditions {
          type
          reason
          message
          unconfirmed
        }
      }
    }
  }
`);

type GVRDiscovery = ClusterCacheGvrDiscoveriesSubscription['clusterCacheGVRDiscoveriesWatch']['discovery'];

// The discovery record for one cache, or null until its frame lands. The stream carries
// every cache's record, so the reducer keeps only the one asked for — the same id-keyed
// fold the registry does, narrowed to a single key.
function useGVRDiscoveries(): Keyed<GVRDiscovery> {
  const [{ data }] = useWatchSubscription<
    { clusterCacheGVRDiscoveriesWatch: { type: string; discovery: GVRDiscovery } },
    Keyed<GVRDiscovery>
  >({ query: ClusterCacheGVRDiscoveriesSubscription }, (prev, resp) => {
    const { type, discovery } = resp.clusterCacheGVRDiscoveriesWatch;
    // A Deleted is matched on the record's own id, not on cacheID. A hard delete carries
    // only the id — the row is already collected, so there is no owner edge left to read a
    // cacheID from, and it arrives as "0". Filtering those by cacheID dropped every one of
    // them, and this anchor carries no finalizer, so a cascade can collect it with no
    // deletion-pending frame ahead of it: the pane would keep showing a record that is gone.
    if (type !== 'Deleted') return applyChange(prev, type, discovery.cacheID, discovery);
    const gone = [...(prev ?? new Map())].find(([, d]) => d.id === discovery.id);
    return gone ? applyChange(prev, 'Deleted', gone[0], discovery) : (prev ?? new Map());
  });
  return data ?? new Map();
}

// The cache's contents, streamed as a live gauge. NOT read off the ClusterCache record:
// that object stops changing once its sync settles, so a field there froze at whatever
// the cache held when the window subscribed — an early, tiny slice of a cold sync.
const ClusterCacheStatsSubscription = graphql(`
  subscription ClusterCacheStats($id: ObjectID!, $cacheID: ObjectID!) {
    clusterCacheStatsWatch(id: $id, cacheID: $cacheID) {
      exists
      bytes
      objectCount
      kindCount
    }
  }
`);

type CacheContents = { exists: boolean; bytes: number; objectCount: number; kindCount: number };

// The cache's current contents, or null until the first frame. A gauge, so each frame
// replaces the last outright — there is nothing to accumulate.
function useCacheContents(clusterId: string, cacheId: string, pause: boolean): CacheContents | null {
  const [{ data }] = useWatchSubscription<{ clusterCacheStatsWatch: CacheContents }, CacheContents>(
    { query: ClusterCacheStatsSubscription, variables: { id: clusterId, cacheID: cacheId }, pause },
    (_prev, resp) => resp.clusterCacheStatsWatch,
  );
  return data ?? null;
}

// One cache's per-kind sync records. Cache-scoped on purpose: there is one per synced
// kind, so a cluster-wide stream would be a hundred-plus records per cache.
//
// Identity only, no conditions: the row's verdict comes from the sidecar's rollup
// (clusterCacheSyncHealthWatch), which is the one place health is decided — see statusOf.
// All this stream is asked for is which record owns the timeline worth showing, so
// selecting per-kind conditions would ship five more fields per record, per frame, for a
// reading nothing here may make.
const ClusterCacheGVRSyncsSubscription = graphql(`
  subscription ClusterCacheGVRSyncs($cacheID: ObjectID!) {
    clusterCacheGVRSyncsWatch(cacheID: $cacheID) {
      type
      sync {
        id
        spec {
          apiVersion
          resource
        }
      }
    }
  }
`);

type GVRSync = ClusterCacheGvrSyncsSubscription['clusterCacheGVRSyncsWatch']['sync'];

// The cache's kind syncs, id-keyed through the registry's shared delta fold.
function useGVRSyncs(cacheId: string): GVRSync[] {
  const [{ data }] = useWatchSubscription<
    { clusterCacheGVRSyncsWatch: { type: string; sync: GVRSync } },
    Keyed<GVRSync>
  >({ query: ClusterCacheGVRSyncsSubscription, variables: { cacheID: cacheId } }, (prev, resp) => {
    const { type, sync } = resp.clusterCacheGVRSyncsWatch;
    return applyChange(prev, type, sync.id, sync);
  });
  return useMemo(() => (data ? [...data.values()] : []), [data]);
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
  const connected = findCondition(c.conditions, 'Connected');
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
  const connected = findCondition(c.conditions, 'Connected');
  // Muted, not amber: the fault is in the connection axis, which carries the error
  // colour — graying the gated sync value keeps it reading as a downstream symptom.
  if (connected?.status === 'False') return { label: 'Stalled', tone: 'muted' };
  // The verdict, read off the cache's sync rollup — folded from every kind it syncs, and
  // dominated by the worst of them. Neither the cache's own Synced condition (coarse:
  // Syncing/Paused) nor any single kind's would do: a cache with ninety-nine healthy
  // kinds and one forbidden CRD is not healthy, and one child's verdict picks a side at
  // random. The sidecar's fold already ignores conditions a previous process wrote and
  // this one hasn't re-confirmed, so nothing is asserted that nobody observed.
  //
  // The cache's `Discovered` axis deliberately does not participate: a partial or failed
  // kind list doesn't stop the kinds already known from syncing, so it reads as a note in
  // SyncDetail (discoveryWarning) rather than a verdict on this column.
  const health = c.activeCache?.syncHealth;
  // No rollup at all yet — nothing has been observed, so there is nothing to report but
  // the work in progress.
  if (!health) return { label: 'Syncing', tone: 'ok' };
  switch (health.reason) {
    case 'SyncFailed':
      return { label: 'Error', tone: 'error' };
    // Connected but a watch went quiet past the freshness threshold: cache may be behind.
    // Amber, not the hard error a SyncFailed is.
    case 'Stale':
      return { label: 'Stale', tone: 'attention' };
    // Every kind caught up and streaming deltas — the steady state, distinct from the
    // catch-up work "Syncing" names.
    case 'Watching':
      return { label: 'Synced', tone: 'ok' };
    // Waiting on credentials the connection hasn't produced yet. Muted for the same reason
    // as Stalled: nothing is wrong with the sync itself.
    case 'NoConnection':
      return { label: 'Connecting', tone: 'muted' };
    // Every kind is paused. Distinct from the spec.syncEnabled gate above, which flips the
    // instant the user resumes: the kinds stay paused until their reconciles land, and
    // painting that gap as healthy green claims a sync that is doing nothing.
    case 'Paused':
      return { label: 'Paused', tone: 'muted' };
    // Catching up, or (with no kinds yet) the discovery pass hasn't landed — not a fault.
    case 'Syncing':
    case 'Unknown':
      return { label: 'Syncing', tone: 'ok' };
    // A reason this build doesn't know is DEGRADED, not healthy — the schema says to read
    // it that way, and the sidecar's own fold does. Falling through to green here would
    // silently paint every future verdict as healthy.
    default:
      return { label: 'Degraded', tone: 'attention' };
  }
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
//
// `unconfirmed` means the status is a downgrade of a previous sidecar process's write,
// so neither half is knowable yet: the stamp predates this process (rendering it as
// "up for 3h" would be a claim about a connection nobody has re-established), and the
// not-True status isn't a real "down" either. Report both as unknown and let the caller
// show its pending state.
function connectionDetail(c: Cluster): {
  connected: boolean;
  unconfirmed: boolean;
  stateSinceMs: number | null;
} {
  const cond = findCondition(c.conditions, 'Connected');
  const unconfirmed = cond?.unconfirmed ?? false;
  return {
    connected: cond?.status === 'True',
    unconfirmed,
    stateSinceMs: unconfirmed ? null : parseTimeOrNull(cond?.transitionedAt),
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
// null `ms` shows `fallback`, or the row is omitted when there's none. `prefix` is static
// text rendered ahead of the ticking value, for a reading that is a fact plus its age
// ("137 kinds · 45s ago").
function DetailRow({
  label,
  ms,
  prefix,
  fallback,
  formatter,
}: {
  label: string;
  ms: number | null;
  prefix?: string;
  fallback?: ReactNode;
  formatter?: Formatter;
}) {
  if (ms === null && fallback === undefined) return null;
  return (
    <div className="flex gap-2">
      <dt className="w-28 shrink-0">{label}</dt>
      <dd className="tabular-nums">
        {ms !== null ? (
          <>
            {prefix}
            <RelativeTime ms={ms} formatter={formatter} />
          </>
        ) : (
          fallback
        )}
      </dd>
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
          fallback={detail.unconfirmed ? '—' : '0m'}
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

// "120 kinds" / "1 kind" — thousands-grouped and pluralised by suffixing an "s", which
// covers every noun this panel counts.
function countLabel(n: number, noun: string): string {
  return `${n.toLocaleString()} ${n === 1 ? noun : `${noun}s`}`;
}

// The cache's on-disk size. Streamed rather than read off the cache record for the same
// reason as the object counts: the size is a file stat that grows with every write, and
// the record it used to hang off stops changing once the sync settles. Costs one
// subscription per row, and this whole panel is a dialog — nothing is mounted while it
// is closed.
function CacheSizeCell({ contents }: { contents: CacheContents | null }) {
  return <>{contents?.exists ? formatBytes(contents.bytes) : '—'}</>;
}

// "118 of 120 kinds — widgets, gateways not syncing" when some are struggling, plain
// "120 kinds" when all are healthy. Read straight off the rollup: the sidecar already
// decided which kinds count as unhealthy (and skipped any verdict a previous process
// wrote), so re-folding the per-kind stream here would be a second definition of health
// that can disagree with the badge above it mid-frame.
function kindsSyncingLabel(health: ClusterCacheSyncHealth): string {
  if (health.unhealthyKinds === 0) return countLabel(health.totalKinds, 'kind');
  const syncing = `${health.totalKinds - health.unhealthyKinds} of ${countLabel(health.totalKinds, 'kind')}`;
  // The two fields answer different questions: unhealthyKinds counts every kind that is
  // not currently Watching, while unhealthyKindRefs names only the ones the fold ranked
  // as offenders. A cache mid-pause has the count with no names, so there is nobody to put
  // in the "… not syncing" clause — say how many are syncing and stop.
  const offenders = offenderList(health);
  if (!offenders) return `${syncing} syncing`;
  return `${syncing} — ${offenders} not syncing`;
}

// The offending kinds as one phrase, capped so a cluster-wide outage doesn't produce an
// unreadable line. The cap lives here, not in the sidecar: how many names fit is a layout
// question, and the wire carries the full sorted list.
const OFFENDER_CAP = 3;

function offenderList(health: ClusterCacheSyncHealth): string {
  // Rendered by plural alone: the api group each ref carries is for keying, not display.
  const names = health.unhealthyKindRefs.map((k) => k.resource);
  if (names.length <= OFFENDER_CAP) return names.join(', ');
  return `${names.slice(0, OFFENDER_CAP).join(', ')} +${names.length - OFFENDER_CAP} more`;
}

// The per-kind record whose transition log to show. Deterministic and sticky: the rollup
// names its offenders in sorted order, so following the first keeps the choice stable
// while a hundred kinds stream in — picking "whichever unhealthy record arrived first"
// would re-key the subscription on every frame and re-dial the stream each time.
// Falls back to Events, the one kind always present.
function timelineSyncFor(all: GVRSync[], health: ClusterCacheSyncHealth): GVRSync | null {
  const firstOffender = health.unhealthyKindRefs[0];
  if (firstOffender) {
    // Matched on the whole kind, not the plural: a CRD may reuse a built-in's plural under
    // its own api group (example.com/v1 gateways beside gateway.networking.k8s.io/v1
    // gateways), and matching loosely would open the healthy kind's timeline under the
    // failing kind's heading.
    const match = all.find((s) => gvrKey(s.spec) === gvrKey(firstOffender));
    if (match) return match;
  }
  return all.find((s) => gvrKey(s.spec) === gvrKey(EVENTS_GVR)) ?? null;
}

// "2,203 objects across 120 kinds". Includes cached events, matching the engine's own
// object/kind rollup.
function cacheSummary(objectCount: number, kindCount: number): string {
  return `${countLabel(objectCount, 'object')} across ${countLabel(kindCount, 'kind')}`;
}

// The kind-discovery reading: how many kinds the cluster serves, and when that list was
// last confirmed. Discovery cannot be watched (its document carries no resourceVersion),
// so the list is refreshed by a poll and its currency is a real question — hence the
// live timestamp beside the count rather than a bare number.
//
// null whenever a pass has yet to land — no record, or `stats` null because the sidecar
// has run no pass since it started — so the caller omits the row rather than claiming a
// count nobody has observed. The gauges are sampled per frame from the sidecar rather
// than stored on the record, so a frame always carries the current reading.
function discoveredKinds(discovery: GVRDiscovery | null): { prefix: string; ms: number } | null {
  const lastMs = parseTimeOrNull(discovery?.stats?.lastDiscoveryAt ?? null);
  if (!discovery?.stats || lastMs === null) return null;
  return { prefix: `${countLabel(discovery.stats.resourceCount, 'kind')} · `, ms: lastMs };
}

// A warning about the kind list itself, distinct from whether the kinds are syncing: the
// last pass either got a partial answer (some api group didn't respond, so the list is
// known-incomplete — nothing was pruned from it) or failed outright (the list is simply
// as old as `lastDiscoveryAt` says). Why this is a note and not a status — see statusOf.
//
// Unconfirmed conditions are skipped for the same reason as in statusOf: the reason
// survives beehive's post-restart downgrade and describes a state this process has not
// re-observed.
function discoveryWarning(discovery: GVRDiscovery | null): string | null {
  const cond = findCondition(discovery?.conditions ?? [], 'Discovered');
  if (!cond || cond.unconfirmed) return null;
  if (cond.reason === 'DiscoveryPartial') {
    return cond.message || 'Some api groups did not respond — the kind list may be incomplete.';
  }
  if (cond.reason === 'DiscoveryFailed') {
    return `Kind discovery is failing — ${cond.message || 'the cluster could not be asked which kinds it serves.'}`;
  }
  // The kind list is right but a kind the cluster still serves has no live child yet: an
  // earlier prune's child is still draining and holds the name. Transient, but it means one
  // kind is not being synced at all, so it must not render as nothing.
  if (cond.reason === 'DiscoveryDraining') {
    return cond.message || 'Waiting for replaced kinds to finish draining.';
  }
  return null;
}

// The expanded sync diagnostics: the cache's rolled-up freshness, its live contents,
// per-kind sync health, and one kind's recent transition history. Inline (not a popover) for the
// same modal-dialog inert reason as ConnectionDetail.
//
// Everything here is subscribed only while the row is expanded, which is what makes the
// per-kind stream affordable — it is a hundred-plus records for the one cache being
// looked at, not for every cache at once.
function SyncDetail({
  health,
  cacheId,
  contents,
  discovery,
}: {
  health: ClusterCacheSyncHealth;
  cacheId: string;
  contents: CacheContents | null;
  discovery: GVRDiscovery | null;
}) {
  const kindSyncs = useGVRSyncs(cacheId);
  const timelineKind = timelineSyncFor(kindSyncs, health);
  const events = useSyncEvents(timelineKind?.id);
  // Rolled up across kinds: the newest write anywhere, beside the OLDEST proof — a cache
  // is only as verified as its least-recently proven watch.
  const lastUpdateMs = parseTimeOrNull(health.lastUpdateAt ?? null);
  const lastLiveMs = parseTimeOrNull(health.lastLiveAt ?? null);
  // Staleness is engine-derived (the Stale reason), never inferred from either stamp's
  // age — a quiet-but-healthy cache legitimately has an old lastUpdateAt.
  const stale = health.reason === 'Stale';
  const discoveryNote = discoveryWarning(discovery);
  return (
    <div className="space-y-2 rounded-md border bg-muted/30 p-3">
      <p className="text-sm font-medium">Sync status</p>
      {stale ? (
        <p className={`text-xs ${TONE.attention.text}`}>
          Possibly stale —{' '}
          {health.unhealthyKindRefs.length
            ? `${offenderList(health)} not receiving updates.`
            : 'the watch may have stopped delivering updates.'}
        </p>
      ) : null}
      {discoveryNote ? <p className={`text-xs ${TONE.attention.text}`}>{discoveryNote}</p> : null}
      <dl className="space-y-0.5 text-xs text-muted-foreground">
        {/* Cache-content summary — shown only when the cache has objects (an empty one
            is already covered by the freshness line's "No updates received yet."). */}
        {contents && contents.objectCount > 0 ? (
          <DetailRow label="Cached" ms={null} fallback={cacheSummary(contents.objectCount, contents.kindCount)} />
        ) : null}
        {/* Freshness, as two live counters. Separate from the sync-event history below
            (a transition log): "current now?" vs "what happened?".

            The two stamps answer different questions and must not be conflated. An update
            is data actually written to the cache; on a quiet cluster none arrives for
            hours, which is normal and not a fault. "Sync verified" is when the watch last
            proved it's alive (a delta or an api-server bookmark), so it stays recent
            throughout that quiet — it's what says the old update time is nothing to worry
            about. Showing only the first would read as a stall; only the second would
            claim updates that never came. */}
        {/* What the cluster says it serves, and how current that answer is — the input
            to the sync, as distinct from the "Cached" line above (what was mirrored)
            and the freshness lines below (whether the mirroring is live). Omitted
            entirely until the discovery record streams in. */}
        <DetailRow label="Kinds discovered" {...(discoveredKinds(discovery) ?? { ms: null })} />
        {/* Per-kind sync health — the axis neither the cache's coarse Synced condition
            nor discovery's Discovered can report: a cache's kinds each sync on their own
            worker and fail independently, so one forbidden CRD is invisible in every
            other reading. Omitted until the stream has delivered something. */}
        {health.totalKinds > 0 ? (
          <DetailRow label="Kinds syncing" ms={null} fallback={kindsSyncingLabel(health)} />
        ) : null}
        <DetailRow label="Last update received" ms={lastUpdateMs} fallback="No updates received yet." />
        {/* No fallback: before the first proof there is nothing honest to say, so the
            row is omitted rather than asserting a verification that never happened. */}
        <DetailRow label="Sync verified" ms={lastLiveMs} />
      </dl>
      {events.length > 0 ? (
        <EventRunList
          title={timelineKind ? `Recent sync events — ${timelineKind.spec.resource}` : 'Recent sync events'}
          runs={events}
          labelOf={(e) => e.reason}
          showDuration={false}
        />
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
// DetailPane is which of a row's two disclosures is open; only one may be at a time.
type DetailPane = 'connection' | 'sync' | null;

// DisclosureLabel is a status label that opens its detail row — the connection column's
// toggle and the sync column's are the same control over the same piece of state, so they
// are one component rather than two identical buttons whose styling has to be kept in step.
function DisclosureLabel({
  tone,
  label,
  expanded,
  onToggle,
}: {
  tone: Tone;
  label: string;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <button
      type="button"
      aria-expanded={expanded}
      onClick={onToggle}
      data-tone={tone}
      className={`${TONE[tone].text} inline-flex cursor-pointer items-center gap-1 rounded-sm underline decoration-dotted underline-offset-2 outline-none focus-visible:ring-2 focus-visible:ring-ring`}
    >
      {label}
      <ChevronDown className={`size-3 transition-transform ${expanded ? 'rotate-180' : ''}`} aria-hidden />
    </button>
  );
}

function ClusterRow({
  cluster,
  group,
  discovery,
  onSetEnabled,
  onToggle,
  onClearCache,
  onRemove,
  onRetry,
}: {
  cluster: Cluster;
  group: Group;
  discovery: GVRDiscovery | null;
  onSetEnabled: (enabled: boolean) => void;
  onToggle: (enabled: boolean) => void;
  onClearCache: () => void;
  onRemove: () => void;
  onRetry: () => void;
}) {
  const name = displayName(cluster);
  // ONE subscription per row for the cache's contents, shared by the always-visible size
  // cell and the expanded sync detail. They previously subscribed separately with identical
  // variables, which urql resolves to the same operation — and a subscription's second
  // subscriber joins mid-stream with no replay, so whichever mounted later (the detail)
  // sat on null until the next frame and never rendered its "N objects across M kinds".
  const { activeCache } = cluster;
  const contents = useCacheContents(cluster.id, activeCache?.id ?? '', !activeCache);
  const connection = connectionStatus(cluster, group);
  const status = statusOf(cluster, group);
  const overall = overallStatus(connection, status);
  const orphaned = group === 'orphaned';
  const connFailed = connection.tone === 'error';
  // Only one detail row can be open at a time.
  const [openDetail, setOpenDetail] = useState<DetailPane>(null);
  const toggleDetail = (pane: Exclude<DetailPane, null>) => setOpenDetail(openDetail === pane ? null : pane);
  // The sync label is a disclosure only once the cache's rollup has streamed in — a
  // pending/never-synced row has no verdict and so nothing to open.
  const sync = cluster.activeCache?.syncHealth;
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
          <DisclosureLabel
            tone={connection.tone}
            label={connection.label}
            expanded={openDetail === 'connection'}
            onToggle={() => toggleDetail('connection')}
          />
        </TableCell>
        <TableCell className="align-top">
          {/* A disclosure (sync-event history) once the sync child has streamed in,
              else plain text. Mirrors the connection column's toggle. */}
          {sync ? (
            <DisclosureLabel
              tone={status.tone}
              label={status.label}
              expanded={openDetail === 'sync'}
              onToggle={() => toggleDetail('sync')}
            />
          ) : (
            <ToneText tone={status.tone}>{status.label}</ToneText>
          )}
        </TableCell>
        <TableCell className="tabular-nums">
          <CacheSizeCell contents={contents} />
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
              disabled={!contents?.exists}
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
      {openDetail === 'connection' ? (
        <TableRow className="hover:bg-transparent">
          <TableCell className={STATUS_CELL_CLASS} />
          <TableCell colSpan={COLUMN_COUNT - 1} className="pt-0">
            <ConnectionDetail cluster={cluster} connFailed={connFailed} onRetry={onRetry} />
          </TableCell>
        </TableRow>
      ) : null}
      {openDetail === 'sync' && sync ? (
        <TableRow className="hover:bg-transparent">
          <TableCell className={STATUS_CELL_CLASS} />
          <TableCell colSpan={COLUMN_COUNT - 1} className="pt-0">
            <SyncDetail health={sync} cacheId={sync.cacheID} contents={contents} discovery={discovery} />
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
  const { clusters, connected } = useClusters();
  // ONE fleet-wide discovery subscription for the whole dialog, folded into a map by
  // cacheID. Every expanded row wants this stream and it takes no variables, so per-row
  // subscriptions all resolved to the same urql operation — and a subscription's second
  // subscriber joins mid-stream with no replay, so the second row expanded saw nothing
  // until the next 5-minute pass. It stays dialog-scoped (nothing subscribes while the
  // panel is closed), which is the property that mattered; it is now open for the dialog's
  // life rather than only while a row is expanded, which costs one record per cache.
  const discoveries = useGVRDiscoveries();
  const rows = clusters ?? [];
  const groups = GROUPS.map((g) => ({ ...g, clusters: rows.filter(g.match) })).filter((g) => g.clusters.length > 0);

  // The registry's transport state, split so the empty-state copy reads correctly:
  // "connecting" (spinner) when the stream hasn't reported yet and the transport is
  // down, vs "empty" ("No clusters yet.") when it's up but carries no clusters. A
  // drop with the registry already loaded is "reconnecting" — keep the table, flag it.
  const phase = watchPhase(clusters !== null, connected);

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
      {/* The transport dropped with the registry already loaded: the table below is
          last-known state, so flag it as reconnecting rather than silently going stale. */}
      {phase === 'reconnecting' ? (
        <p className={`mb-2 flex items-center gap-1.5 text-xs ${TONE.attention.text}`}>
          <Spinner size="xs" className="mr-0" />
          Reconnecting…
        </p>
      ) : null}
      {/* Connecting (no snapshot, transport down) vs empty (up, no clusters) vs the
          table. Split into guarded branches rather than a nested ternary; `rows` is
          only non-empty once loaded, so the table and connecting states never overlap. */}
      {phase === 'connecting' && (
        <p className="flex items-center gap-2 py-6 text-sm text-muted-foreground">
          <Spinner size="sm" className="mr-0" />
          Connecting…
        </p>
      )}
      {phase !== 'connecting' && rows.length === 0 && (
        <p className="py-6 text-sm text-muted-foreground">No clusters yet.</p>
      )}
      {rows.length > 0 && (
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
                  discovery={discoveries.get(c.activeCache?.id ?? '') ?? null}
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
