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

// Cluster-registry dialog, grouped into Active (in the current kubeconfig) and
// Orphaned (leftover cache, context gone). Mutations write through; the resulting
// clustersWatch push updates the table.
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

// Connection-probe history (event-log category "connection") — decoupled from
// clustersWatch so probe chatter never re-emits the registry. Subscribed only
// while a row's diagnostics are open.
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

// One aggregated event run (`ok` = Normal type); shared by both histories.
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

// Upsert by run id, newest-first by lastAt.
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

// Sync-event history, keyed by a ClusterCacheGVRSync record's id (each kind's
// worker logs to its own record), not the cache's. Subscribed only while sync
// detail is open, one kind at a time.
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

// One kind's sync-transition log; paused until there's an id (a placeholder
// subscription would carry nothing).
function useSyncEvents(syncId: string | undefined): EventRun[] {
  const [{ data }] = useWatchSubscription<{ clusterCacheGVRSyncEventsWatch: RawEvent }, EventRun[]>(
    { query: ClusterSyncEventsSubscription, variables: { id: syncId ?? '' }, pause: !syncId },
    (prev, resp) => foldRun(prev, resp.clusterCacheGVRSyncEventsWatch),
  );
  return data ?? [];
}

// The cache's GVR-discovery record (which kinds the cluster serves, and how current
// that answer is). Fleet-wide stream, but subscribed HERE, not in ClustersProvider:
// this pane is its only reader, and an app-wide mount would stream + rebuild joined
// identities in every window for a row nobody expanded.
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

// Every cache's discovery record, folded by cacheID.
function useGVRDiscoveries(): Keyed<GVRDiscovery> {
  const [{ data }] = useWatchSubscription<
    { clusterCacheGVRDiscoveriesWatch: { type: string; discovery: GVRDiscovery } },
    Keyed<GVRDiscovery>
  >({ query: ClusterCacheGVRDiscoveriesSubscription }, (prev, resp) => {
    const { type, discovery } = resp.clusterCacheGVRDiscoveriesWatch;
    // A Deleted must match on the record's own id, not cacheID: a hard delete's
    // frame carries cacheID "0" (the owner edge is already collected), so keying it
    // by cacheID would drop every delete and the pane would show gone records.
    if (type !== 'Deleted') return applyChange(prev, type, discovery.cacheID, discovery);
    const gone = [...(prev ?? new Map())].find(([, d]) => d.id === discovery.id);
    return gone ? applyChange(prev, 'Deleted', gone[0], discovery) : (prev ?? new Map());
  });
  return data ?? new Map();
}

// The cache's contents as a live gauge. NOT read off the ClusterCache record: that
// object stops changing once its sync settles, so a field there would freeze at
// subscribe time.
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

// A gauge: each frame replaces the last outright. null until the first frame.
function useCacheContents(clusterId: string, cacheId: string, pause: boolean): CacheContents | null {
  const [{ data }] = useWatchSubscription<{ clusterCacheStatsWatch: CacheContents }, CacheContents>(
    { query: ClusterCacheStatsSubscription, variables: { id: clusterId, cacheID: cacheId }, pause },
    (_prev, resp) => resp.clusterCacheStatsWatch,
  );
  return data ?? null;
}

// One cache's per-kind sync records — cache-scoped (one per synced kind; a
// cluster-wide stream would be a hundred-plus records per cache). Identity only,
// no conditions: the verdict comes from the sidecar's rollup (see statusOf), and
// this stream is only asked which record owns the timeline worth showing.
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

// Next-reconcile time, streamed per-cluster (a scheduling change fires no list
// watch — the only way the countdown stays live for an idle disconnected cluster).
// Subscribed only while a row's diagnostics are open.
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
  // Null `nextRequeueAt` = reconcile in flight (`probing`) or nothing scheduled.
  // Hold the last time only across the in-flight window; with `probing` false a
  // null must clear the countdown, not freeze a stale one.
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

// Per-tone: severity (overall circle = worst of the two axes), dot fill, and a
// text-tuned shade for the tinted column label.
const TONE: Record<Tone, { severity: number; dot: string; text: string }> = {
  error: { severity: 3, dot: 'bg-red-500', text: 'text-red-600 dark:text-red-400' },
  attention: { severity: 2, dot: 'bg-amber-500', text: 'text-amber-600 dark:text-amber-400' },
  ok: { severity: 1, dot: 'bg-emerald-500', text: 'text-emerald-600 dark:text-emerald-400' },
  muted: { severity: 0, dot: 'bg-muted-foreground/50', text: 'text-muted-foreground' },
};

// Single source for the colSpan numbers below.
const COLUMN_COUNT = 6;

// Status-circle column, shared so header/rows/detail align. Non-zero width so
// table-fixed doesn't collapse it.
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

// Pending = not yet identified (kube-system UID probe hasn't succeeded, e.g. a
// stopped minikube): can't sync or cache yet.
function isPending(c: Cluster): boolean {
  return !c.status.server.uid;
}

function displayName(c: Cluster): string {
  return c.spec.name || c.spec.source.kubeconfig?.context || c.id;
}

function findCondition<T extends { type: string }>(conditions: T[], type: string): T | undefined {
  return conditions.find((cond) => cond.type === type);
}

// Connection column: derives from the live `Connected` condition, not the sticky
// `server.uid` (never clears, so it would read "Active" after a drop).
function connectionStatus(c: Cluster, group: Group): { label: string; tone: Tone } {
  if (group === 'orphaned') return { label: 'Unavailable', tone: 'muted' };
  const connected = findCondition(c.conditions, 'Connected');
  if (connected?.status === 'True') return { label: 'Active', tone: 'ok' };
  if (connected?.status === 'False') return { label: 'Disconnected', tone: 'error' };
  return { label: 'Connecting', tone: 'attention' };
}

// Sync status. Gate on the live `Connected` condition first (a dropped connection
// leaves the engine reporting a stale "Watching"), then read the rollup.
function statusOf(c: Cluster, group: Group): { label: string; tone: Tone } {
  if (group === 'orphaned') return { label: 'Stopped', tone: 'muted' };
  if (isPending(c)) return { label: 'Not synced', tone: 'muted' };
  if (!c.spec.syncEnabled) return { label: 'Paused', tone: 'muted' };
  const connected = findCondition(c.conditions, 'Connected');
  // Muted, not amber: the fault is on the connection axis — graying the gated sync
  // value keeps it reading as a downstream symptom.
  if (connected?.status === 'False') return { label: 'Stalled', tone: 'muted' };
  // Verdict = the sidecar's per-kind rollup, dominated by the worst kind — neither
  // the cache's coarse Synced condition nor any single kind's would do. The
  // `Discovered` axis deliberately doesn't participate (a partial kind list doesn't
  // stop known kinds from syncing) — it's a note in SyncDetail instead.
  const health = c.activeCache?.syncHealth;
  // No rollup yet — nothing observed, only work in progress.
  if (!health) return { label: 'Syncing', tone: 'ok' };
  switch (health.reason) {
    case 'SyncFailed':
      return { label: 'Error', tone: 'error' };
    // A watch went quiet past the freshness threshold: amber, not a hard error.
    case 'Stale':
      return { label: 'Stale', tone: 'attention' };
    // Steady state — all kinds caught up and streaming deltas.
    case 'Watching':
      return { label: 'Synced', tone: 'ok' };
    // Waiting on credentials; muted like Stalled — nothing wrong with the sync itself.
    case 'NoConnection':
      return { label: 'Connecting', tone: 'muted' };
    // Every kind paused. Distinct from the spec.syncEnabled gate above (which flips
    // instantly on resume, before the kinds' reconciles land).
    case 'Paused':
      return { label: 'Paused', tone: 'muted' };
    // Catching up, or discovery hasn't landed — not a fault.
    case 'Syncing':
    case 'Unknown':
      return { label: 'Syncing', tone: 'ok' };
    // An unknown reason is DEGRADED, not healthy (the schema and the sidecar's fold
    // both read it that way) — green here would paint every future verdict healthy.
    default:
      return { label: 'Degraded', tone: 'attention' };
  }
}

// Leading status circle: worst of the two axes, plus a summary tooltip.
type Status = { label: string; tone: Tone };
function overallStatus(connection: Status, sync: Status): { tone: Tone; summary: string } {
  return { tone: overallTone(connection.tone, sync.tone), summary: `${connection.label} · ${sync.label}` };
}

function parseTimeOrNull(iso: string | null | undefined): number | null {
  const ms = iso ? Date.parse(iso) : NaN;
  return Number.isNaN(ms) ? null : ms;
}

// Connection diagnostics: up?, and `stateSinceMs` (the `Connected` condition's last
// transition). `unconfirmed` = the status is a downgrade of a previous sidecar
// process's write — neither half is knowable yet, so report both as unknown.
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

// Live "Xs ago" counter rolling up to minutes/hours; no "just now" bucket.
const relativeFormatter: Formatter = (_value, _unit, _suffix, epochMs) => {
  const diff = Math.max(0, Date.now() - epochMs);
  if (diff < MINUTE_MS) return `${Math.floor(diff / SECOND_MS)}s ago`;
  if (diff < HOUR_MS) return `${Math.floor(diff / MINUTE_MS)}m ago`;
  return `${Math.floor(diff / HOUR_MS)}h ago`;
};

// Coarse elapsed counter ("<1m" / "5m" / "3h") — minute-grained so it doesn't tick
// per-second against the "Next check" countdown.
const elapsedCoarseFormatter: Formatter = (_value, _unit, _suffix, epochMs) => {
  const diff = Math.max(0, Date.now() - epochMs);
  if (diff < MINUTE_MS) return '<1m';
  if (diff < HOUR_MS) return `${Math.floor(diff / MINUTE_MS)}m`;
  return `${Math.floor(diff / HOUR_MS)}h`;
};

// Run span, largest unit only; "Once" for a single probe.
function formatRunDuration(count: number, firstMs: number | null, lastMs: number | null): string {
  if (count <= 1 || firstMs === null || lastMs === null) return 'Once';
  const diff = Math.max(0, lastMs - firstMs);
  if (diff < MINUTE_MS) return `${Math.floor(diff / SECOND_MS)}s`;
  if (diff < HOUR_MS) return `${Math.floor(diff / MINUTE_MS)}m`;
  return `${Math.floor(diff / HOUR_MS)}h`;
}

// Countdown to the next retry ("in 1m 30s"); "now" once due.
const countdownFormatter: Formatter = (_value, _unit, _suffix, epochMs) => {
  const totalSec = Math.floor(Math.max(0, epochMs - Date.now()) / SECOND_MS);
  if (totalSec < 1) return 'now';
  if (totalSec < 60) return `in ${totalSec}s`;
  return `in ${Math.floor(totalSec / 60)}m ${totalSec % 60}s`;
};

// maxPeriod={1} forces a per-second re-render; react-timeago otherwise throttles
// to once-a-minute above 60s, freezing a countdown.
function RelativeTime({ ms, formatter = relativeFormatter }: { ms: number; formatter?: Formatter }) {
  return <ReactTimeAgo date={ms} component="span" formatter={formatter} maxPeriod={1} />;
}

// One diagnostics line. Non-null `ms` ticks live; null shows `fallback` or omits
// the row. `prefix` renders ahead of the ticking value ("137 kinds · 45s ago").
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

// Newest-first aggregated event runs — shared body of both detail panes.
// `showDuration` adds a per-run duration column (the sync pane's runs are one-shot).
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
      {/* Scrolls so a long history doesn't blow out the panel; subgrid aligns cells. */}
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
              {/* End time; full window on hover. */}
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

// Expanded connection diagnostics + Retry-now. Inline, not a popover: the modal
// dialog inerts everything outside its subtree, so a body-portaled popover would
// be unclickable.
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
  // The re-probe runs out-of-band (mutation resolves on schedule; outcome arrives
  // via clustersWatch) — show a brief "Retrying…" acknowledgement, then revert.
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
        {/* Coarse uptime; "0m" while down. */}
        <DetailRow
          label="Uptime"
          ms={detail.connected ? detail.stateSinceMs : null}
          fallback={detail.unconfirmed ? '—' : '0m'}
          formatter={elapsedCoarseFormatter}
        />
        {/* Probing → "checking…" spinner; nothing scheduled → neutral placeholder.
            The row stays mounted either way so the panel never reflows. */}
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

// "120 kinds" / "1 kind" — suffix-"s" pluralisation covers every noun counted here.
function countLabel(n: number, noun: string): string {
  return `${n.toLocaleString()} ${n === 1 ? noun : `${noun}s`}`;
}

// On-disk size, streamed for the same reason as the object counts: the cache
// record stops changing once its sync settles. One subscription per row, and the
// panel is a dialog — nothing mounts while closed.
function CacheSizeCell({ contents }: { contents: CacheContents | null }) {
  return <>{contents?.exists ? formatBytes(contents.bytes) : '—'}</>;
}

// "118 of 120 kinds — widgets, gateways not syncing", or plain "120 kinds". Read
// straight off the rollup — re-folding the per-kind stream here would be a second
// definition of health that can disagree with the badge above it mid-frame.
function kindsSyncingLabel(health: ClusterCacheSyncHealth): string {
  if (health.unhealthyKinds === 0) return countLabel(health.totalKinds, 'kind');
  const syncing = `${health.totalKinds - health.unhealthyKinds} of ${countLabel(health.totalKinds, 'kind')}`;
  // unhealthyKinds counts every non-Watching kind; unhealthyKindRefs names only the
  // ranked offenders. Mid-pause there's a count with no names — say how many are
  // syncing and stop.
  const offenders = offenderList(health);
  if (!offenders) return `${syncing} syncing`;
  return `${syncing} — ${offenders} not syncing`;
}

// Cap lives here, not in the sidecar: how many names fit is a layout question; the
// wire carries the full sorted list.
const OFFENDER_CAP = 3;

function offenderList(health: ClusterCacheSyncHealth): string {
  // Plural alone; the ref's api group is for keying, not display.
  const names = health.unhealthyKindRefs.map((k) => k.resource);
  if (names.length <= OFFENDER_CAP) return names.join(', ');
  return `${names.slice(0, OFFENDER_CAP).join(', ')} +${names.length - OFFENDER_CAP} more`;
}

// Which per-kind record's transition log to show. Deterministic and sticky (first
// sorted offender) — picking "whichever unhealthy record arrived first" would
// re-key the subscription per frame. Falls back to Events, always present.
function timelineSyncFor(all: GVRSync[], health: ClusterCacheSyncHealth): GVRSync | null {
  const firstOffender = health.unhealthyKindRefs[0];
  if (firstOffender) {
    // Match the whole kind, not the plural: a CRD may reuse a built-in's plural
    // under its own api group, and a loose match would open the wrong timeline.
    const match = all.find((s) => gvrKey(s.spec) === gvrKey(firstOffender));
    if (match) return match;
  }
  return all.find((s) => gvrKey(s.spec) === gvrKey(EVENTS_GVR)) ?? null;
}

// "2,203 objects across 120 kinds" (includes cached events, matching the engine's rollup).
function cacheSummary(objectCount: number, kindCount: number): string {
  return `${countLabel(objectCount, 'object')} across ${countLabel(kindCount, 'kind')}`;
}

// Kind-discovery reading: count + when last confirmed. Discovery is poll-refreshed
// (no resourceVersion to watch), so its currency is a real question — hence the
// live timestamp beside the count. null until a pass lands, so the caller omits
// the row rather than claiming a count nobody observed.
function discoveredKinds(discovery: GVRDiscovery | null): { prefix: string; ms: number } | null {
  const lastMs = parseTimeOrNull(discovery?.stats?.lastDiscoveryAt ?? null);
  if (!discovery?.stats || lastMs === null) return null;
  return { prefix: `${countLabel(discovery.stats.resourceCount, 'kind')} · `, ms: lastMs };
}

// Warning about the kind list itself, distinct from whether kinds are syncing (a
// note, not a status — see statusOf). Unconfirmed conditions are skipped: they
// describe a state this process hasn't re-observed.
function discoveryWarning(discovery: GVRDiscovery | null): string | null {
  const cond = findCondition(discovery?.conditions ?? [], 'Discovered');
  if (!cond || cond.unconfirmed) return null;
  if (cond.reason === 'DiscoveryPartial') {
    return cond.message || 'Some api groups did not respond — the kind list may be incomplete.';
  }
  if (cond.reason === 'DiscoveryFailed') {
    return `Kind discovery is failing — ${cond.message || 'the cluster could not be asked which kinds it serves.'}`;
  }
  // A served kind has no live child yet (a prune's child still draining holds the
  // name). Transient, but that kind isn't syncing at all — must not render as nothing.
  if (cond.reason === 'DiscoveryDraining') {
    return cond.message || 'Waiting for replaced kinds to finish draining.';
  }
  return null;
}

// Expanded sync diagnostics. Inline for the same modal-inert reason as
// ConnectionDetail. Everything subscribes only while expanded — that's what makes
// the hundred-plus-record per-kind stream affordable.
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
  // Newest write anywhere, beside the OLDEST proof — a cache is only as verified
  // as its least-recently proven watch.
  const lastUpdateMs = parseTimeOrNull(health.lastUpdateAt ?? null);
  const lastLiveMs = parseTimeOrNull(health.lastLiveAt ?? null);
  // Staleness is engine-derived, never inferred from a stamp's age — a
  // quiet-but-healthy cache legitimately has an old lastUpdateAt.
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
        {/* Only when the cache has objects (empty is covered by the freshness line). */}
        {contents && contents.objectCount > 0 ? (
          <DetailRow label="Cached" ms={null} fallback={cacheSummary(contents.objectCount, contents.kindCount)} />
        ) : null}
        {/* The two freshness stamps must not be conflated: "Last update" is data
            written (a quiet cluster legitimately goes hours without), "Sync verified"
            is the watch's last proof of life (delta or bookmark) — only the pair says
            an old update time is fine. */}
        {/* What the cluster serves, and how current that answer is; omitted until
            the discovery record streams in. */}
        <DetailRow label="Kinds discovered" {...(discoveredKinds(discovery) ?? { ms: null })} />
        {/* Per-kind sync health — kinds fail independently, so one forbidden CRD is
            invisible in every other reading. Omitted until something has streamed in. */}
        {health.totalKinds > 0 ? (
          <DetailRow label="Kinds syncing" ms={null} fallback={kindsSyncingLabel(health)} />
        ) : null}
        <DetailRow label="Last update received" ms={lastUpdateMs} fallback="No updates received yet." />
        {/* No fallback: omit rather than assert a verification that never happened. */}
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
// Which of a row's two disclosures is open; at most one.
type DetailPane = 'connection' | 'sync' | null;

// Status label that opens its detail row — one component for both columns so their
// styling can't drift.
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
  // ONE cache-contents subscription per row, shared by the size cell and the sync
  // detail. Separate hooks with identical variables resolve to one urql operation,
  // and a second subscriber joins mid-stream with no replay — the later mount would
  // sit on null until the next frame.
  const { activeCache } = cluster;
  const contents = useCacheContents(cluster.id, activeCache?.id ?? '', !activeCache);
  const connection = connectionStatus(cluster, group);
  const status = statusOf(cluster, group);
  const overall = overallStatus(connection, status);
  const orphaned = group === 'orphaned';
  const connFailed = connection.tone === 'error';
  const [openDetail, setOpenDetail] = useState<DetailPane>(null);
  const toggleDetail = (pane: Exclude<DetailPane, null>) => setOpenDetail(openDetail === pane ? null : pane);
  // Sync label is a disclosure only once the rollup has streamed in — a
  // pending/never-synced row has nothing to open.
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
          {/* Disclosure once the sync child has streamed in, else plain text. */}
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
            {/* Enable/disable: a disabled cluster stays tracked but dormant. */}
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
            {/* Always rendered so columns align; remove applies only to an orphan
              (a present context would just be re-discovered). */}
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

// Active = in the current kubeconfig; anything else shows as Orphaned so no known
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

// Open state is caller-controlled (`AppDialogs`); mounts only while open. Per-row
// event/schedule streams mount only when a row's diagnostics open.
export function ClusterSyncPanel({ open, onOpenChange }: AppDialogProps) {
  const { clusters, connected } = useClusters();
  // ONE fleet-wide discovery subscription for the whole dialog, folded by cacheID.
  // Per-row hooks would resolve to the same urql operation, and a second subscriber
  // joins mid-stream with no replay — a second expanded row would see nothing until
  // the next 5-minute pass. Dialog-scoped (nothing subscribes while closed), open
  // for the dialog's life at a cost of one record per cache.
  const discoveries = useGVRDiscoveries();
  const rows = clusters ?? [];
  const groups = GROUPS.map((g) => ({ ...g, clusters: rows.filter(g.match) })).filter((g) => g.clusters.length > 0);

  // Transport phase: "connecting" (no report, transport down) vs "empty" (up, no
  // clusters) vs "reconnecting" (drop with the registry loaded — keep the table, flag it).
  const phase = watchPhase(clusters !== null, connected);

  // Actions write through; the clustersWatch push is the source of truth. urql's
  // execute resolves (never rejects), so surface a failed mutation's error on the bus.
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
      // Widened past the dialog default to fit the table.
      className="w-[calc(100%-4rem)] max-w-4xl sm:max-w-4xl"
    >
      {/* Transport dropped with the registry loaded: the table is last-known state. */}
      {phase === 'reconnecting' ? (
        <p className={`mb-2 flex items-center gap-1.5 text-xs ${TONE.attention.text}`}>
          <Spinner size="xs" className="mr-0" />
          Reconnecting…
        </p>
      ) : null}
      {/* Guarded branches, not a nested ternary; `rows` is only non-empty once
          loaded, so table and connecting states never overlap. */}
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
