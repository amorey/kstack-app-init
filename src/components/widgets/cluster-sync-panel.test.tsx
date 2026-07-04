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

import { cleanup, render, screen, act, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Provider as UrqlProvider } from 'urql';
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { mockTauriCore } from '@/test-utils';

// Mocks ---------------------------------------------------------------

const { invokeMock, channels, factory } = mockTauriCore();
vi.mock('@tauri-apps/api/core', () => factory());

const { createGraphqlClient } = await import('@/lib/graphql/client');
const { ClustersProvider } = await import('@/lib/clusters');
const { ClusterSyncPanel, overallTone } = await import('./cluster-sync-panel');

// Helpers -------------------------------------------------------------

const flush = () => act(async () => {});

type Row = {
  uuid: string; // '' = pending (the UID probe never succeeded → server.uid null)
  name: string;
  enabled: boolean;
  present: boolean;
  cached: boolean;
  isCurrent?: boolean;
  cacheBytes?: number;
  objectCount?: number;
  kindCount?: number;
  lastSyncedAt?: string;
  // The live `Connected` condition the connection column reads. Defaults to
  // 'True' (reachable); set 'False' to model a dropped connection (internet
  // off / probe failed), 'Unknown' for not-yet-probed.
  connected?: 'True' | 'False' | 'Unknown';
  // Model an engine-level sync failure (Synced=False/SyncFailed). Defaults to a
  // healthy Synced=True/Watching condition.
  syncFailed?: boolean;
  // Model a stalled-but-connected watch (Synced=False/Stale) — the cache may be
  // behind even though the connection is up.
  stale?: boolean;
  // Connection diagnostics surfaced in the Disconnected popover.
  connMessage?: string; // Connected condition's `message` (the probe error)
  disconnectedSince?: string; // Connected condition's `lastTransitionTime` (ISO)
  lastConnectedAt?: string; // status.lastConnectedAt (ISO; null = never)
};

// The subscribe-exchange news a fake Channel per graphql_subscribe in call order,
// so the Nth subscribe (matched by query text) maps to channels[N]. The panel runs
// several subscriptions at once (clustersWatch, and per open row clusterEventsWatch
// + clusterScheduleWatch), so target by query rather than liveChannel().
function channelFor(queryPart: string) {
  const subs = invokeMock.mock.calls.filter(([cmd]) => cmd === 'graphql_subscribe');
  const idx = subs.findIndex(([, arg]) => (arg as { query: string }).query.includes(queryPart));
  if (idx < 0) throw new Error(`no subscription for ${queryPart}`);
  return channels[idx];
}

// The `Synced` condition a probed row's ClusterCache carries, keyed off the
// row's modelled sync state (failed / stale / healthy).
function syncedCondition(r: Row) {
  if (r.syncFailed) return { type: 'Synced', status: 'False', reason: 'SyncFailed' };
  if (r.stale) {
    return {
      type: 'Synced',
      status: 'False',
      reason: 'Stale',
      message: 'No watch heartbeat for Pod — cache may be behind',
    };
  }
  return { type: 'Synced', status: 'True', reason: 'Watching' };
}

// Push each row as an Added change on the two delta streams: a Cluster change on
// clustersWatch, plus (for a probed row) its ClusterCache change on
// clusterCachesWatch. The provider joins them into the row's activeCache by
// matching cache.serverUid to cluster.status.server.uid.
function pushClusters(rows: Row[]) {
  const clusterCh = channelFor('clustersWatch');
  const cacheCh = channelFor('clusterCachesWatch');
  rows.forEach((r, i) => {
    const id = r.uuid || `pending-${i}`;
    clusterCh.onmessage!(
      JSON.stringify({
        type: 'next',
        payload: {
          data: {
            clustersWatch: {
              type: 'Added',
              cluster: {
                id,
                spec: {
                  name: r.name,
                  syncEnabled: r.enabled,
                  enabled: true,
                  source: { kubeconfig: { context: r.name } },
                },
                status: {
                  source: {
                    kubeconfig: {
                      cluster: `${r.name}-cluster`,
                      user: `${r.name}-user`,
                      isPresent: r.present,
                      isDefault: r.isCurrent ?? false,
                    },
                  },
                  server: { uid: r.uuid || null },
                  lastConnectedAt: r.lastConnectedAt ?? null,
                  conditions: [
                    {
                      type: 'Connected',
                      status: r.connected ?? 'True',
                      reason: '',
                      message: r.connMessage ?? '',
                      lastTransitionTime: r.disconnectedSince ?? null,
                    },
                  ],
                },
              },
            },
          },
        },
      }),
    );
    if (!r.uuid) return;
    cacheCh.onmessage!(
      JSON.stringify({
        type: 'next',
        payload: {
          data: {
            clusterCachesWatch: {
              type: 'Added',
              cache: {
                id: `cache-${r.uuid}`,
                clusterID: id,
                serverUid: r.uuid,
                status: {
                  conditions: [syncedCondition(r)],
                  lastSyncedAt: r.lastSyncedAt ?? null,
                },
                stats: {
                  exists: r.cached,
                  bytes: r.cacheBytes ?? 0,
                  objectCount: r.objectCount ?? 0,
                  kindCount: r.kindCount ?? 0,
                },
              },
            },
          },
        },
      }),
    );
  });
}

// Push one frame on the per-cluster clusterEventsWatch stream (a bare Event).
// Call after a row's diagnostics are open (mounts the events subscription).
function pushConnectionEvent(ev: {
  id: string;
  type: 'Normal' | 'Warning';
  reason: string;
  message: string;
  count: number;
  firstAt: string;
  lastAt: string;
}) {
  channelFor('clusterEventsWatch').onmessage!(
    JSON.stringify({ type: 'next', payload: { data: { clusterEventsWatch: ev } } }),
  );
}

// Push one frame on the per-cache clusterCacheEventsWatch stream (a bare Event).
// Call after a row's sync diagnostics are open (mounts the sync-events subscription).
function pushSyncEvent(ev: {
  id: string;
  type: 'Normal' | 'Warning';
  reason: string;
  message: string;
  count: number;
  firstAt: string;
  lastAt: string;
}) {
  channelFor('clusterCacheEventsWatch').onmessage!(
    JSON.stringify({ type: 'next', payload: { data: { clusterCacheEventsWatch: ev } } }),
  );
}

// Push one frame on the per-cluster clusterScheduleWatch gauge (the next-attempt
// time + the in-flight `probing` flag). Call after a row's diagnostics are open
// (mounts the schedule subscription).
function pushSchedule(nextRequeueAt: string | null, probing = false) {
  channelFor('clusterScheduleWatch').onmessage!(
    JSON.stringify({ type: 'next', payload: { data: { clusterScheduleWatch: { nextRequeueAt, probing } } } }),
  );
}

function renderPanel() {
  return render(
    <UrqlProvider value={createGraphqlClient()}>
      <ClustersProvider>
        <ClusterSyncPanel />
      </ClustersProvider>
    </UrqlProvider>,
  );
}

// Render the panel, push rows, and open the sheet.
async function openWith(rows: Row[]) {
  const user = userEvent.setup();
  renderPanel();
  await flush();
  await act(async () => {
    pushClusters(rows);
  });
  await user.click(screen.getByRole('button', { name: /clusters/i }));
  return user;
}

describe('overallTone', () => {
  it('rolls up to the most severe of the two sub-system tones', () => {
    expect(overallTone('ok', 'ok')).toBe('ok');
    expect(overallTone('ok', 'muted')).toBe('ok');
    expect(overallTone('muted', 'muted')).toBe('muted');
    expect(overallTone('attention', 'muted')).toBe('attention');
    expect(overallTone('ok', 'attention')).toBe('attention');
    expect(overallTone('error', 'attention')).toBe('error');
    expect(overallTone('ok', 'error')).toBe('error');
  });
});

describe('ClusterSyncPanel', () => {
  // base-ui's dialog relies on pointer-capture / scroll APIs jsdom lacks.
  beforeAll(() => {
    Element.prototype.hasPointerCapture = vi.fn();
    Element.prototype.releasePointerCapture = vi.fn();
    Element.prototype.scrollIntoView = vi.fn();
  });

  beforeEach(() => {
    invokeMock.mockReset();
    channels.length = 0;
    let id = 0;
    invokeMock.mockImplementation(async (cmd: string) => {
      if (cmd === 'graphql_subscribe') {
        id += 1;
        return id;
      }
      if (cmd === 'graphql_unsubscribe') return undefined;
      if (cmd === 'graphql_query') {
        // Any mutation succeeds; extra fields are ignored by urql.
        return {
          status: 200,
          body: JSON.stringify({
            data: {
              clusterEnabledSet: { __typename: 'Cluster', id: 'u', spec: { enabled: false } },
              clusterSyncEnabledSet: { __typename: 'Cluster', id: 'u', spec: { syncEnabled: false } },
              clusterCacheClear: { __typename: 'Cluster', id: 'u' },
              clusterDelete: true,
              clusterConnectionRetry: true,
            },
          }),
        };
      }
      throw new Error(`unexpected ${cmd}`);
    });
  });

  afterEach(cleanup);

  it('subscribes to clustersWatch', async () => {
    renderPanel();
    await flush();
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_subscribe',
      expect.objectContaining({ query: expect.stringContaining('clustersWatch') }),
    );
  });

  it('shows an empty state when there are no clusters', async () => {
    const user = await openWith([]);
    expect(user).toBeDefined();
    expect(await screen.findByText(/no clusters yet/i)).toBeInTheDocument();
  });

  it('renders the table columns', async () => {
    await openWith([{ uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true }]);
    expect(await screen.findByRole('columnheader', { name: /^cluster$/i })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: /^connection$/i })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: /sync status/i })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: /^cache$/i })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: /actions/i })).toBeInTheDocument();
  });

  it('splits clusters into active and orphaned row groups by kubeconfig presence', async () => {
    await openWith([
      { uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true, cacheBytes: 1_300_000 },
      { uuid: 'u-stg', name: 'staging', enabled: false, present: true, cached: true, cacheBytes: 524_288 },
      { uuid: 'u-old', name: 'old-cluster', enabled: true, present: false, cached: true, cacheBytes: 1024 },
    ]);

    const active = await screen.findByRole('rowgroup', { name: /active/i });
    const orphaned = screen.getByRole('rowgroup', { name: /orphaned/i });

    expect(within(active).getByText('prod-us')).toBeInTheDocument();
    expect(within(active).getByText('staging')).toBeInTheDocument();
    expect(within(active).queryByText('old-cluster')).not.toBeInTheDocument();

    expect(within(orphaned).getByText('old-cluster')).toBeInTheDocument();
    expect(within(orphaned).queryByText('prod-us')).not.toBeInTheDocument();

    // Status + formatted cache sizes (binary units).
    expect(within(active).getByText(/^syncing$/i)).toBeInTheDocument();
    expect(within(active).getByText(/^paused$/i)).toBeInTheDocument();
    expect(within(orphaned).getByText(/^stopped$/i)).toBeInTheDocument();
    expect(screen.getByText(/1\.2 MB/)).toBeInTheDocument();
    expect(screen.getByText(/512\.0 KB/)).toBeInTheDocument();
  });

  it('shows a short connection status for each cluster', async () => {
    await openWith([
      {
        uuid: 'u-prod',
        name: 'prod-us',
        enabled: true,
        present: true,
        cached: true,
        isCurrent: true,
        connected: 'True',
      },
      // Previously reachable but now unreachable (e.g. internet off): the
      // Connected condition flips to False even though server.uid lingers.
      { uuid: 'u-remote', name: 'remote', enabled: true, present: true, cached: true, connected: 'False' },
      { uuid: '', name: 'minikube', enabled: false, present: true, cached: false, connected: 'Unknown' },
      { uuid: 'u-old', name: 'old-cluster', enabled: true, present: false, cached: true },
    ]);

    // Scoped to cells so the group-header "Active" label doesn't collide.
    const active = await screen.findByRole('rowgroup', { name: /active/i });
    const orphaned = screen.getByRole('rowgroup', { name: /orphaned/i });

    expect(within(active).getByRole('cell', { name: 'Active' })).toBeInTheDocument(); // reachable
    expect(within(active).getByRole('cell', { name: 'Disconnected' })).toBeInTheDocument(); // connection dropped
    expect(within(active).getByRole('cell', { name: 'Connecting' })).toBeInTheDocument(); // not yet probed
    expect(within(orphaned).getByRole('cell', { name: 'Unavailable' })).toBeInTheDocument(); // gone
  });

  it('reflects the live sync state, not just the enabled toggle', async () => {
    await openWith([
      // Enabled + connected + watching → Syncing.
      { uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true, connected: 'True' },
      // Enabled but disconnected: the engine keeps a stale Watching state, so
      // gate on the connection — this is Stalled, not Syncing.
      { uuid: 'u-remote', name: 'remote', enabled: true, present: true, cached: true, connected: 'False' },
      // Enabled + connected but the engine reported an engine-level failure.
      {
        uuid: 'u-broke',
        name: 'broke',
        enabled: true,
        present: true,
        cached: true,
        connected: 'True',
        syncFailed: true,
      },
    ]);

    const active = await screen.findByRole('rowgroup', { name: /active/i });
    expect(within(active).getByRole('cell', { name: 'Syncing' })).toBeInTheDocument();
    expect(within(active).getByRole('cell', { name: 'Stalled' })).toBeInTheDocument();
    expect(within(active).getByRole('cell', { name: 'Error' })).toBeInTheDocument();
  });

  it('shows one overall status indicator per row, rolled up to the most severe axis', async () => {
    await openWith([
      { uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true, connected: 'True' },
      { uuid: 'u-remote', name: 'remote', enabled: true, present: true, cached: true, connected: 'False' },
      { uuid: '', name: 'minikube', enabled: false, present: true, cached: false, connected: 'Unknown' },
      { uuid: 'u-old', name: 'old-cluster', enabled: true, present: false, cached: true },
    ]);

    // One indicator per row, named by both axes (the tooltip summary), tinted by
    // the most severe of the two.
    expect(await screen.findByRole('img', { name: 'Active · Syncing' })).toHaveAttribute('data-tone', 'ok');
    expect(screen.getByRole('img', { name: 'Disconnected · Stalled' })).toHaveAttribute('data-tone', 'error');
    expect(screen.getByRole('img', { name: 'Connecting · Not synced' })).toHaveAttribute('data-tone', 'attention');
    expect(screen.getByRole('img', { name: 'Unavailable · Stopped' })).toHaveAttribute('data-tone', 'muted');
  });

  it('color-codes the connection and sync text by their own tone, to explain the overall color', async () => {
    await openWith([
      { uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true, connected: 'True' },
      { uuid: 'u-remote', name: 'remote', enabled: true, present: true, cached: true, connected: 'False' },
      { uuid: '', name: 'minikube', enabled: false, present: true, cached: false, connected: 'Unknown' },
      { uuid: 'u-old', name: 'old-cluster', enabled: true, present: false, cached: true },
    ]);

    await screen.findByRole('rowgroup', { name: /active/i });
    // Connection axis — the `[data-tone]` selector disambiguates the cell label
    // from the group-header text (which shares words like "Active").
    expect(screen.getByText('Active', { selector: '[data-tone]' })).toHaveAttribute('data-tone', 'ok');
    expect(screen.getByText('Disconnected', { selector: '[data-tone]' })).toHaveAttribute('data-tone', 'error');
    expect(screen.getByText('Connecting', { selector: '[data-tone]' })).toHaveAttribute('data-tone', 'attention');
    expect(screen.getByText('Unavailable', { selector: '[data-tone]' })).toHaveAttribute('data-tone', 'muted');
    // Sync axis.
    expect(screen.getByText('Syncing', { selector: '[data-tone]' })).toHaveAttribute('data-tone', 'ok');
    // Stalled is a *gated* value (its fault is the connection, not sync), so it
    // grays out rather than going amber — the red "Disconnected" carries the cause.
    expect(screen.getByText('Stalled', { selector: '[data-tone]' })).toHaveAttribute('data-tone', 'muted');
    expect(screen.getByText('Not synced', { selector: '[data-tone]' })).toHaveAttribute('data-tone', 'muted');
    expect(screen.getByText('Stopped', { selector: '[data-tone]' })).toHaveAttribute('data-tone', 'muted');
  });

  it('requests connection diagnostics in the clustersWatch subscription', async () => {
    renderPanel();
    await flush();
    const sub = invokeMock.mock.calls.find(
      ([cmd, arg]) => cmd === 'graphql_subscribe' && (arg as { query: string }).query.includes('clustersWatch'),
    )?.[1] as { query: string };
    expect(sub.query).toContain('lastConnectedAt');
    expect(sub.query).toContain('message');
    expect(sub.query).toContain('lastTransitionTime');
    // The probe history and the next-attempt countdown are not inlined on
    // the list — they stream per-row via clusterEventsWatch / clusterScheduleWatch.
    expect(sub.query).not.toContain('connectionAttempts');
    expect(sub.query).not.toContain('nextAttemptAt');
  });

  it('reveals the underlying error and a retry action when a disconnected cluster is opened', async () => {
    const user = await openWith([
      {
        uuid: 'u-remote',
        name: 'remote',
        enabled: true,
        present: true,
        cached: true,
        connected: 'False',
        connMessage: 'dial tcp 10.0.0.1:6443: connect: connection refused',
        disconnectedSince: new Date(Date.now() - 3 * 60_000).toISOString(),
        lastConnectedAt: new Date(Date.now() - 12 * 60_000).toISOString(),
      },
    ]);

    // The Disconnected label is an interactive trigger (only the error state is).
    // Opening it mounts the per-row clusterEventsWatch + clusterScheduleWatch subs.
    await user.click(await screen.findByRole('button', { name: /disconnected/i }));

    // The next-attempt countdown streams in on the schedule gauge (decoupled from
    // the list), and the probe history on the events stream (bare runs, newest
    // first by lastAt), with `ok` derived from the generic event type.
    await act(async () => {
      pushSchedule(new Date(Date.now() + 15_000).toISOString());
      pushConnectionEvent({
        id: '2',
        type: 'Warning',
        reason: 'ProbeFailed',
        message: 'TLS handshake timeout',
        count: 2,
        firstAt: new Date(Date.now() - 30_000).toISOString(),
        lastAt: new Date(Date.now() - 20_000).toISOString(),
      });
      pushConnectionEvent({
        id: '1',
        type: 'Warning',
        reason: 'ProbeFailed',
        message: 'i/o timeout',
        count: 5,
        firstAt: new Date(Date.now() - 90_000).toISOString(),
        lastAt: new Date(Date.now() - 40_000).toISOString(),
      });
    });

    // The popover surfaces the connection timestamps, the countdown to the next
    // scheduled retry (streamed from the schedule gauge), and the recent-attempt
    // log (which carries the probe errors).
    expect(await screen.findByText(/uptime/i)).toBeInTheDocument();
    expect(screen.getByText(/next check/i)).toBeInTheDocument();
    // The ~15s-out schedule renders a live countdown, not the "now" fallback.
    expect(screen.getByText(/in \d+s/i)).toBeInTheDocument();
    // The attempt history lists each recorded outcome with its own probe message.
    expect(screen.getByText(/recent attempts/i)).toBeInTheDocument();
    expect(await screen.findByText(/i\/o timeout/i)).toBeInTheDocument();
    expect(screen.getByText(/tls handshake timeout/i)).toBeInTheDocument();

    // Retry fires clusterConnectionRetry.
    await user.click(screen.getByRole('button', { name: /retry/i }));
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_query',
      expect.objectContaining({ body: expect.stringContaining('clusterConnectionRetry') }),
    );
  });

  it('reveals recent sync events when a cached cluster’s sync status is opened', async () => {
    const user = await openWith([{ uuid: 'u-sync', name: 'prod', enabled: true, present: true, cached: true }]);

    // The sync-status label is a disclosure trigger for a row with an active cache
    // (there's a cache to stream sync events for). A healthy cached row reads
    // "Syncing". Opening it mounts the per-cache clusterCacheEventsWatch sub, keyed
    // by the active cache's id — decoupled from clusterCachesWatch.
    await user.click(await screen.findByRole('button', { name: /syncing/i }));

    // The sync history streams in as bare runs (newest first by lastAt), with `ok`
    // derived from the generic event type (Normal = a healthy run).
    await act(async () => {
      pushSyncEvent({
        id: '2',
        type: 'Warning',
        reason: 'SyncDegraded',
        message: 'discovery returned no syncable resources',
        count: 3,
        firstAt: new Date(Date.now() - 60_000).toISOString(),
        lastAt: new Date(Date.now() - 20_000).toISOString(),
      });
      pushSyncEvent({
        id: '1',
        type: 'Normal',
        reason: 'SyncComplete',
        message: 'Initial sync complete — cached 5 objects across 3 kinds in 2s',
        count: 1,
        firstAt: new Date(Date.now() - 90_000).toISOString(),
        lastAt: new Date(Date.now() - 80_000).toISOString(),
      });
    });

    // The sync detail lists each recorded run by its raw reason code (the message
    // field already carries the human-readable detail, so a friendly label would
    // just repeat it) and its message.
    expect(await screen.findByText(/recent sync events/i)).toBeInTheDocument();
    expect(await screen.findByText(/discovery returned no syncable resources/i)).toBeInTheDocument();
    // Raw reason codes, with the run's ×count multiplier when > 1.
    expect(screen.getByText(/SyncDegraded ×3/i)).toBeInTheDocument();
    expect(screen.getByText('SyncComplete')).toBeInTheDocument();
  });

  it('shows a last-update freshness line in the sync detail, driven by lastSyncedAt', async () => {
    const syncedAt = new Date(Date.now() - 30_000).toISOString();
    const user = await openWith([
      { uuid: 'u-fresh', name: 'prod', enabled: true, present: true, cached: true, lastSyncedAt: syncedAt },
    ]);

    // Open the sync-status disclosure (the row has an active cache).
    await user.click(await screen.findByRole('button', { name: /syncing/i }));

    // The freshness line answers "is my cache current?" directly from lastSyncedAt
    // — a live relative counter, independent of the (separate) sync-event history.
    expect(await screen.findByText(/last update received/i)).toBeInTheDocument();
    expect(await screen.findByText(/\d+s ago/)).toBeInTheDocument();
  });

  it('summarises cache contents (objects across kinds) in the sync detail, thousands-grouped', async () => {
    const user = await openWith([
      {
        uuid: 'u-big',
        name: 'prod',
        enabled: true,
        present: true,
        cached: true,
        objectCount: 2203,
        kindCount: 120,
      },
    ]);

    await user.click(await screen.findByRole('button', { name: /syncing/i }));

    // The summary is the static "how much do I hold?" counterpart to the
    // freshness line — locale-grouped and pluralised.
    expect(await screen.findByText('2,203 objects across 120 kinds')).toBeInTheDocument();
  });

  it('omits the cache summary when the cache holds no objects', async () => {
    const user = await openWith([
      { uuid: 'u-empty', name: 'prod', enabled: true, present: true, cached: true, objectCount: 0 },
    ]);

    await user.click(await screen.findByRole('button', { name: /syncing/i }));

    // An empty cache is already covered by the freshness line; no "0 objects" noise.
    expect(await screen.findByText(/no updates received yet/i)).toBeInTheDocument();
    expect(screen.queryByText(/objects across/i)).not.toBeInTheDocument();
  });

  it('surfaces a stalled watch: the sync column reads Stale and the detail explains why', async () => {
    const user = await openWith([
      {
        uuid: 'u-stale',
        name: 'prod',
        enabled: true,
        present: true,
        cached: true,
        stale: true,
        lastSyncedAt: new Date(Date.now() - 6 * 60_000).toISOString(),
      },
    ]);

    // The sync column reflects the stalled state (not a healthy "Syncing").
    const staleBtn = await screen.findByRole('button', { name: /stale/i });
    await user.click(staleBtn);

    // The detail flags it and carries the condition's explanation.
    expect(await screen.findByText(/possibly stale/i)).toBeInTheDocument();
    expect(screen.getByText(/no watch heartbeat for pod/i)).toBeInTheDocument();

    // A SyncStale event renders by its raw reason code alongside its message.
    await act(async () => {
      pushSyncEvent({
        id: '9',
        type: 'Warning',
        reason: 'SyncStale',
        message: 'Pod watch went quiet',
        count: 1,
        firstAt: new Date(Date.now() - 60_000).toISOString(),
        lastAt: new Date(Date.now() - 10_000).toISOString(),
      });
    });
    expect(await screen.findByText('SyncStale')).toBeInTheDocument();
    expect(screen.getByText(/pod watch went quiet/i)).toBeInTheDocument();
  });

  it('holds the countdown across an in-flight probe, then clears it once the schedule is authoritatively empty', async () => {
    const user = await openWith([
      { uuid: 'u-remote', name: 'remote', enabled: true, present: true, cached: true, connected: 'False' },
    ]);
    await user.click(await screen.findByRole('button', { name: /disconnected/i }));

    // A scheduled next attempt → a live countdown.
    await act(async () => pushSchedule(new Date(Date.now() + 15_000).toISOString()));
    expect(await screen.findByText(/in \d+s/i)).toBeInTheDocument();

    // The gauge reports the zero time (null) while the scheduled reconcile is
    // dispatched — but the controller asserts the probe is running (probing:
    // true). That is the in-flight case: show "checking…", not a blank countdown.
    await act(async () => pushSchedule(null, true));
    expect(await screen.findByText(/checking…/i)).toBeInTheDocument();

    // The probe ends and the cluster is now ineligible (e.g. disabled): nothing
    // scheduled and no probe running ({ null, false }). That is authoritative —
    // the stale countdown must clear, and nothing should imply an active probe.
    await act(async () => pushSchedule(null, false));
    expect(screen.queryByText(/in \d+s/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/checking…/i)).not.toBeInTheDocument();
  });

  it('shows the "checking…" spinner while a probe is in flight, even with a scheduled next attempt', async () => {
    const user = await openWith([
      { uuid: 'u-remote', name: 'remote', enabled: true, present: true, cached: true, connected: 'False' },
    ]);
    await user.click(await screen.findByRole('button', { name: /disconnected/i }));

    // A scheduled next attempt → a live countdown.
    await act(async () => pushSchedule(new Date(Date.now() + 15_000).toISOString()));
    expect(await screen.findByText(/in \d+s/i)).toBeInTheDocument();

    // The controller asserts a probe is now running (probing: true) while the
    // schedule still carries the last time → "checking…" overrides the countdown.
    await act(async () => pushSchedule(new Date(Date.now() + 15_000).toISOString(), true));
    expect(await screen.findByText(/checking…/i)).toBeInTheDocument();
    expect(screen.getByRole('status')).toBeInTheDocument();
    expect(screen.queryByText(/in \d+s/i)).not.toBeInTheDocument();

    // The probe finishes and re-arms a schedule (probing: false) → back to a countdown.
    await act(async () => pushSchedule(new Date(Date.now() + 30_000).toISOString(), false));
    expect(await screen.findByText(/in \d+s/i)).toBeInTheDocument();
    expect(screen.queryByText(/checking…/i)).not.toBeInTheDocument();
  });

  it('shows a "checking…" spinner only while a probe is actually in flight, not merely when nothing is scheduled', async () => {
    const user = await openWith([
      { uuid: 'u-remote', name: 'remote', enabled: true, present: true, cached: true, connected: 'False' },
    ]);
    await user.click(await screen.findByRole('button', { name: /disconnected/i }));

    // Nothing scheduled and no probe running (the pre-first-schedule window, or an
    // ineligible cluster): a neutral placeholder, never the spinner — an idle
    // cluster must not look like it is actively probing.
    await act(async () => pushSchedule(null, false));
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
    expect(screen.queryByText(/checking…/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/in \d+s/i)).not.toBeInTheDocument();

    // The controller now asserts a probe is in flight → the spinner appears.
    await act(async () => pushSchedule(null, true));
    expect(await screen.findByText(/checking…/i)).toBeInTheDocument();
    expect(screen.getByRole('status')).toBeInTheDocument();
  });

  it('expands connection diagnostics for a reachable cluster with the neutral (non-failed) header', async () => {
    const user = await openWith([
      { uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true, connected: 'True' },
    ]);
    await screen.findByRole('rowgroup', { name: /active/i });
    // The connection label is a disclosure toggle in every state, including Active.
    await user.click(screen.getByRole('button', { name: /^active$/i }));
    // A healthy cluster gets the neutral "Connection" panel — not "Connection
    // failed" — but the Retry action is still available in every state.
    expect(await screen.findByText(/^connection$/i, { selector: 'p' })).toBeInTheDocument();
    expect(screen.queryByText(/connection failed/i)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument();
  });

  it('omits a row group that has no members', async () => {
    await openWith([{ uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true }]);
    expect(await screen.findByRole('rowgroup', { name: /active/i })).toBeInTheDocument();
    expect(screen.queryByRole('rowgroup', { name: /orphaned/i })).not.toBeInTheDocument();
  });

  it('toggling sync fires the play/pause action via clusterSyncEnabledSet', async () => {
    const user = await openWith([{ uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true }]);

    // Enabled + active → a Pause button.
    await user.click(await screen.findByRole('button', { name: /pause sync for prod-us/i }));
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_query',
      expect.objectContaining({ body: expect.stringContaining('clusterSyncEnabledSet') }),
    );
  });

  it('toggling enable fires clusterEnabledSet', async () => {
    const user = await openWith([{ uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: false }]);

    // The row's spec.enabled defaults to true (see pushClusters) → a Disable button.
    await user.click(await screen.findByRole('button', { name: /disable prod-us/i }));
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_query',
      expect.objectContaining({ body: expect.stringContaining('clusterEnabledSet') }),
    );
  });

  it('clearing a cache fires clusterCacheClear, and is disabled when uncached', async () => {
    const user = await openWith([
      { uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true },
      { uuid: 'u-stg', name: 'staging', enabled: false, present: true, cached: false },
    ]);

    expect(await screen.findByRole('button', { name: /clear cache for staging/i })).toBeDisabled();

    await user.click(screen.getByRole('button', { name: /clear cache for prod-us/i }));
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_query',
      expect.objectContaining({ body: expect.stringContaining('clusterCacheClear') }),
    );
  });

  it('shows an unreachable kubeconfig context as a pending active row with disabled actions', async () => {
    await openWith([{ uuid: '', name: 'minikube', enabled: false, present: true, cached: false }]);

    const active = await screen.findByRole('rowgroup', { name: /active/i });
    expect(within(active).getByText('minikube')).toBeInTheDocument();
    expect(within(active).getByText(/not synced/i)).toBeInTheDocument();
    // Can't sync or clear a context with no identity / no cache yet.
    expect(screen.getByRole('button', { name: /sync for minikube/i })).toBeDisabled();
    expect(screen.getByRole('button', { name: /clear cache for minikube/i })).toBeDisabled();
    // Remove is rendered (so the Actions column aligns) but disabled in Active.
    expect(screen.getByRole('button', { name: /^remove minikube/i })).toBeDisabled();
  });

  it('disables play/pause for orphaned rows and removes them via clusterDelete', async () => {
    const user = await openWith([{ uuid: 'u-old', name: 'old-cluster', enabled: true, present: false, cached: true }]);

    expect(await screen.findByRole('button', { name: /sync for old-cluster/i })).toBeDisabled();

    const remove = screen.getByRole('button', { name: /^remove old-cluster/i });
    expect(remove).toBeEnabled();
    await user.click(remove);
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_query',
      expect.objectContaining({ body: expect.stringContaining('clusterDelete') }),
    );
  });
});
