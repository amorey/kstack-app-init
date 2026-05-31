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

import { cleanup, render, screen, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Provider as UrqlProvider } from 'urql';
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { mockTauriCore } from '@/test-utils';

// Mocks ---------------------------------------------------------------

const { invokeMock, channels, liveChannel, factory } = mockTauriCore();
vi.mock('@tauri-apps/api/core', () => factory());

const { createGraphqlClient } = await import('@/lib/graphql/client');
const { ClusterSyncProvider } = await import('@/lib/cluster-sync');
const { ClusterSyncPanel } = await import('./cluster-sync-panel');

// Helpers -------------------------------------------------------------

const flush = () => act(async () => {});

type Row = {
  context: string;
  state: 'PENDING' | 'SYNCING' | 'LIVE' | 'BACKOFF' | 'OFFLINE';
  lastError?: string;
  lastSyncedAt?: number;
  downloadRateBps?: number;
};

function pushClusters(rows: Row[]) {
  liveChannel().onmessage!(
    JSON.stringify({
      type: 'next',
      payload: {
        data: {
          clusterSyncStatusWatch: rows.map((r) => ({
            lastError: '',
            lastSyncedAt: 0,
            downloadRateBps: 0,
            ...r,
          })),
        },
      },
    }),
  );
}

function renderPanel() {
  return render(
    <UrqlProvider value={createGraphqlClient()}>
      <ClusterSyncProvider>
        <ClusterSyncPanel />
      </ClusterSyncProvider>
    </UrqlProvider>,
  );
}

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
      throw new Error(`unexpected ${cmd}`);
    });
  });

  afterEach(cleanup);

  it('renders a toolbar trigger button', async () => {
    renderPanel();
    await flush();
    expect(screen.getByRole('button', { name: /clusters/i })).toBeInTheDocument();
  });

  it('subscribes to clusterSyncStatusWatch', async () => {
    renderPanel();
    await flush();
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_subscribe',
      expect.objectContaining({ query: expect.stringContaining('clusterSyncStatusWatch') }),
    );
  });

  it('shows an empty state when no clusters are syncing', async () => {
    const user = userEvent.setup();
    renderPanel();
    await flush();
    await act(async () => {
      pushClusters([]);
    });

    await user.click(screen.getByRole('button', { name: /clusters/i }));
    expect(await screen.findByText(/no clusters syncing/i)).toBeInTheDocument();
  });

  it('lists a row per cluster with state and download rate', async () => {
    const user = userEvent.setup();
    renderPanel();
    await flush();
    await act(async () => {
      pushClusters([
        { context: 'prod-us', state: 'LIVE', lastSyncedAt: 1_700_000_000_000, downloadRateBps: 1_300_000 },
        { context: 'staging', state: 'SYNCING', downloadRateBps: 348_160 },
        { context: 'dev-local', state: 'OFFLINE', lastError: 'unreachable' },
      ]);
    });

    await user.click(screen.getByRole('button', { name: /clusters/i }));

    // Each cluster name renders.
    expect(await screen.findByText('prod-us')).toBeInTheDocument();
    expect(screen.getByText('staging')).toBeInTheDocument();
    expect(screen.getByText('dev-local')).toBeInTheDocument();

    // States surface as labels.
    expect(screen.getByText(/^live$/i)).toBeInTheDocument();
    expect(screen.getByText(/^syncing$/i)).toBeInTheDocument();
    expect(screen.getByText(/^offline$/i)).toBeInTheDocument();

    // Download rate: formatted for active clusters, em dash for the idle one.
    expect(screen.getByText('1.2 MB/s')).toBeInTheDocument();
    expect(screen.getByText('340.0 KB/s')).toBeInTheDocument();
    expect(screen.getByText('—')).toBeInTheDocument();
  });

  it('renders a per-cluster sync toggle, on by default', async () => {
    const user = userEvent.setup();
    renderPanel();
    await flush();
    await act(async () => {
      pushClusters([
        { context: 'prod-us', state: 'LIVE' },
        { context: 'staging', state: 'SYNCING' },
      ]);
    });

    await user.click(screen.getByRole('button', { name: /clusters/i }));

    const prod = await screen.findByRole('switch', { name: /prod-us/i });
    const staging = screen.getByRole('switch', { name: /staging/i });
    expect(prod).toBeChecked();
    expect(staging).toBeChecked();
  });

  it('toggling a cluster off flips its switch without any backend call (no-op for now)', async () => {
    const user = userEvent.setup();
    renderPanel();
    await flush();
    await act(async () => {
      pushClusters([{ context: 'prod-us', state: 'LIVE' }]);
    });

    await user.click(screen.getByRole('button', { name: /clusters/i }));
    const prod = await screen.findByRole('switch', { name: /prod-us/i });

    await user.click(prod);
    expect(prod).not.toBeChecked();

    // No-op: nothing is sent to the sidecar. The only IPC is the provider's
    // own subscription; no query/mutation is fired by the toggle.
    expect(invokeMock).not.toHaveBeenCalledWith('graphql_query', expect.anything());
  });
});
