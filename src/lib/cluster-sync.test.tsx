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

import { render, screen, act } from '@testing-library/react';
import { Provider as UrqlProvider } from 'urql';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { mockTauriCore } from '@/test-utils';

// Mocks ---------------------------------------------------------------

const { invokeMock, channels, liveChannel, factory } = mockTauriCore();
vi.mock('@tauri-apps/api/core', () => factory());

const { createGraphqlClient } = await import('@/lib/graphql/client');
const { formatDownloadRate, ClusterSyncProvider, useClusterSync } = await import('./cluster-sync');

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

// A probe that renders the hook's value so tests can assert on it.
function Probe() {
  const { clusters } = useClusterSync();
  return <div data-testid="probe">{clusters === null ? 'null' : JSON.stringify(clusters.map((c) => c.context))}</div>;
}

function renderProvider() {
  return render(
    <UrqlProvider value={createGraphqlClient()}>
      <ClusterSyncProvider>
        <Probe />
      </ClusterSyncProvider>
    </UrqlProvider>,
  );
}

// formatDownloadRate --------------------------------------------------

describe('formatDownloadRate', () => {
  it('renders an em dash for an idle/zero rate', () => {
    expect(formatDownloadRate(0)).toBe('—');
  });

  it('reports raw bytes/sec below 1 KiB (no decimals)', () => {
    expect(formatDownloadRate(512)).toBe('512 B/s');
    expect(formatDownloadRate(1)).toBe('1 B/s');
  });

  it('scales to KB/s, MB/s, GB/s with one decimal (binary base)', () => {
    expect(formatDownloadRate(1536)).toBe('1.5 KB/s'); // 1.5 * 1024
    expect(formatDownloadRate(5_000_000)).toBe('4.8 MB/s'); // 5e6 / 1024^2 ≈ 4.768
    expect(formatDownloadRate(3 * 1024 ** 3)).toBe('3.0 GB/s');
  });

  it('crosses unit boundaries at 1024, not 1000', () => {
    expect(formatDownloadRate(1023)).toBe('1023 B/s');
    expect(formatDownloadRate(1024)).toBe('1.0 KB/s');
  });
});

// ClusterSyncProvider / useClusterSync --------------------------------

describe('useClusterSync', () => {
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

  it('throws outside the provider', () => {
    function Bare() {
      useClusterSync();
      return null;
    }
    expect(() => render(<Bare />)).toThrow(/ClusterSyncProvider/);
  });

  it('reports null before the first push', async () => {
    renderProvider();
    await flush();
    expect(screen.getByTestId('probe')).toHaveTextContent('null');
  });

  it('subscribes to clusterSyncStatusWatch', async () => {
    renderProvider();
    await flush();
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_subscribe',
      expect.objectContaining({ query: expect.stringContaining('clusterSyncStatusWatch') }),
    );
  });

  it('reflects the pushed cluster list', async () => {
    renderProvider();
    await flush();

    await act(async () => {
      pushClusters([
        { context: 'prod-us', state: 'LIVE' },
        { context: 'staging', state: 'SYNCING', downloadRateBps: 1536 },
      ]);
    });
    expect(screen.getByTestId('probe')).toHaveTextContent('["prod-us","staging"]');
  });
});
