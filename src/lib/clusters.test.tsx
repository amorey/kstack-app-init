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
const { formatBytes, ClustersProvider, useClusters } = await import('./clusters');

// Helpers -------------------------------------------------------------

const flush = () => act(async () => {});

type Row = { uuid: string; name: string; enabled?: boolean; present?: boolean; cached?: boolean };

function pushClusters(rows: Row[]) {
  liveChannel().onmessage!(
    JSON.stringify({
      type: 'next',
      payload: {
        data: {
          clustersWatch: rows.map((r) => ({
            context: r.name,
            isCurrent: false,
            enabled: true,
            present: true,
            cached: false,
            cacheBytes: 0,
            lastSyncedAt: 0,
            lastSeenInKubeconfigAt: 0,
            ...r,
          })),
        },
      },
    }),
  );
}

// A probe that renders the hook's value so tests can assert on it.
function Probe() {
  const { clusters } = useClusters();
  return <div data-testid="probe">{clusters === null ? 'null' : JSON.stringify(clusters.map((c) => c.name))}</div>;
}

function renderProvider() {
  return render(
    <UrqlProvider value={createGraphqlClient()}>
      <ClustersProvider>
        <Probe />
      </ClustersProvider>
    </UrqlProvider>,
  );
}

// formatBytes ---------------------------------------------------------

describe('formatBytes', () => {
  it('renders an em dash for an uncached/zero size', () => {
    expect(formatBytes(0)).toBe('—');
  });

  it('reports raw bytes below 1 KiB (no decimals)', () => {
    expect(formatBytes(512)).toBe('512 B');
    expect(formatBytes(1)).toBe('1 B');
  });

  it('scales to KB, MB, GB with one decimal (binary base)', () => {
    expect(formatBytes(1536)).toBe('1.5 KB'); // 1.5 * 1024
    expect(formatBytes(5_000_000)).toBe('4.8 MB'); // 5e6 / 1024^2 ≈ 4.768
    expect(formatBytes(3 * 1024 ** 3)).toBe('3.0 GB');
  });

  it('crosses unit boundaries at 1024, not 1000', () => {
    expect(formatBytes(1023)).toBe('1023 B');
    expect(formatBytes(1024)).toBe('1.0 KB');
  });
});

// ClustersProvider / useClusters --------------------------------------

describe('useClusters', () => {
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
      useClusters();
      return null;
    }
    expect(() => render(<Bare />)).toThrow(/ClustersProvider/);
  });

  it('reports null before the first push', async () => {
    renderProvider();
    await flush();
    expect(screen.getByTestId('probe')).toHaveTextContent('null');
  });

  it('subscribes to clustersWatch', async () => {
    renderProvider();
    await flush();
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_subscribe',
      expect.objectContaining({ query: expect.stringContaining('clustersWatch') }),
    );
  });

  it('reflects the pushed cluster list', async () => {
    renderProvider();
    await flush();

    await act(async () => {
      pushClusters([
        { uuid: 'u-a', name: 'prod-us' },
        { uuid: 'u-b', name: 'staging', enabled: false },
      ]);
    });
    expect(screen.getByTestId('probe')).toHaveTextContent('["prod-us","staging"]');
  });
});
