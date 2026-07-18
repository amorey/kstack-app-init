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
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { clusterOf, mockTauriCore, pushClusters } from '@/test-utils';
import type { ClusterRow } from '@/test-utils';

// Mocks ---------------------------------------------------------------

const { invokeMock, channels, channelFor, factory } = mockTauriCore();
vi.mock('@tauri-apps/api/core', () => factory());

const { createGraphqlClient } = await import('@/lib/graphql/client');
const { formatBytes, ClustersProvider, useClusters } = await import('./clusters');

// Helpers -------------------------------------------------------------

const flush = () => act(async () => {});

// Drop both transports (the host's SSE streams died), advance past the backoff
// so subscribe-exchange re-establishes each subscription, then deliver the
// `open` frame the host sends on each re-established connection (which resets
// the accumulators before the snapshot replays).
async function reconnectBothStreams() {
  await act(async () => {
    channelFor('clustersWatch').onmessage!(JSON.stringify({ type: 'complete' }));
    channelFor('clusterCachesWatch').onmessage!(JSON.stringify({ type: 'complete' }));
  });
  await act(async () => {
    await vi.advanceTimersByTimeAsync(1_000);
  });
  await act(async () => {
    channelFor('clustersWatch').onmessage!(JSON.stringify({ type: 'open' }));
    channelFor('clusterCachesWatch').onmessage!(JSON.stringify({ type: 'open' }));
  });
}

// Push one Cluster change on the clustersWatch delta stream.
function pushClusterChange(type: 'Added' | 'Modified' | 'Deleted', cluster: object) {
  channelFor('clustersWatch').onmessage!(
    JSON.stringify({ type: 'next', payload: { data: { clustersWatch: { type, cluster } } } }),
  );
}

// Push one ClusterCache change on the clusterCachesWatch delta stream.
function pushCacheChange(type: 'Added' | 'Modified' | 'Deleted', cache: object) {
  channelFor('clusterCachesWatch').onmessage!(
    JSON.stringify({ type: 'next', payload: { data: { clusterCachesWatch: { type, cache } } } }),
  );
}

// A cache mirroring a row's active identity (serverUid matches the cluster's uid),
// so the provider joins it as that cluster's activeCache.
function cacheOf(r: ClusterRow) {
  return {
    id: `cache-${r.id}`,
    clusterID: r.id,
    serverUid: `uid-${r.id}`,
    status: { conditions: [], lastSyncedAt: null },
    stats: { exists: false, bytes: 0 },
  };
}

// A probe that renders the hook's value so tests can assert on it.
function Probe() {
  const { clusters } = useClusters();
  return <div data-testid="probe">{clusters === null ? 'null' : JSON.stringify(clusters.map((c) => c.spec.name))}</div>;
}

// A probe that also reveals each cluster's joined activeCache (id or '-').
function JoinProbe() {
  const { clusters } = useClusters();
  return (
    <div data-testid="probe">
      {clusters === null ? 'null' : JSON.stringify(clusters.map((c) => `${c.spec.name}:${c.activeCache?.id ?? '-'}`))}
    </div>
  );
}

function renderProvider(probe: React.ReactNode = <Probe />) {
  return render(
    <UrqlProvider value={createGraphqlClient()}>
      <ClustersProvider>{probe}</ClustersProvider>
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

  afterEach(() => {
    vi.useRealTimers();
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

  it('subscribes to both the cluster and cache delta watches', async () => {
    renderProvider();
    await flush();
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_subscribe',
      expect.objectContaining({ query: expect.stringContaining('clustersWatch') }),
    );
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_subscribe',
      expect.objectContaining({ query: expect.stringContaining('clusterCachesWatch') }),
    );
  });

  it('accumulates Added changes into the cluster list', async () => {
    renderProvider();
    await flush();

    await act(async () => {
      pushClusters(channelFor, [
        { id: 'u-a', name: 'prod-us' },
        { id: 'u-b', name: 'staging', syncEnabled: false },
      ]);
    });
    expect(screen.getByTestId('probe')).toHaveTextContent('["prod-us","staging"]');
  });

  it('removes a cluster on a Deleted change', async () => {
    renderProvider();
    await flush();

    await act(async () => {
      pushClusters(channelFor, [
        { id: 'u-a', name: 'prod-us' },
        { id: 'u-b', name: 'staging' },
      ]);
    });
    expect(screen.getByTestId('probe')).toHaveTextContent('["prod-us","staging"]');

    await act(async () => {
      pushClusterChange('Deleted', clusterOf({ id: 'u-a', name: 'prod-us' }));
    });
    expect(screen.getByTestId('probe')).toHaveTextContent('["staging"]');
  });

  it('joins a cache onto its cluster by matching serverUid', async () => {
    renderProvider(<JoinProbe />);
    await flush();

    // Cluster arrives first, with no cache yet.
    await act(async () => {
      pushClusters(channelFor, [{ id: 'u-a', name: 'prod-us' }]);
    });
    expect(screen.getByTestId('probe')).toHaveTextContent('["prod-us:-"]');

    // Its cache lands on the other stream and is joined as the active cache.
    await act(async () => {
      pushCacheChange('Added', cacheOf({ id: 'u-a', name: 'prod-us' }));
    });
    expect(screen.getByTestId('probe')).toHaveTextContent('["prod-us:cache-u-a"]');
  });

  it('drops clusters and caches deleted during an outage once the transport reconnects', async () => {
    vi.useFakeTimers();
    renderProvider(<JoinProbe />);
    await flush();

    await act(async () => {
      pushClusters(channelFor, [
        { id: 'u-a', name: 'prod-us' },
        { id: 'u-b', name: 'staging' },
      ]);
      pushCacheChange('Added', cacheOf({ id: 'u-a', name: 'prod-us' }));
    });
    expect(screen.getByTestId('probe')).toHaveTextContent('["prod-us:cache-u-a","staging:-"]');

    // While the transport is down, prod-us and its cache are deleted
    // server-side. The reconnect replays only what still exists (staging) —
    // no Deleted frame is ever sent for the ones that vanished.
    await reconnectBothStreams();
    await act(async () => {
      pushClusterChange('Added', clusterOf({ id: 'u-b', name: 'staging' }));
    });
    expect(screen.getByTestId('probe')).toHaveTextContent('["staging:-"]');
  });

  it('resets to the not-reported state when the reconnect snapshot is empty', async () => {
    vi.useFakeTimers();
    renderProvider();
    await flush();

    await act(async () => {
      pushClusters(channelFor, [{ id: 'u-a', name: 'prod-us' }]);
    });
    expect(screen.getByTestId('probe')).toHaveTextContent('["prod-us"]');

    // Everything was deleted during the outage: the reconnected stream
    // replays nothing, so no frame ever arrives to displace the stale map —
    // the reset alone must clear it.
    await reconnectBothStreams();
    expect(screen.getByTestId('probe')).toHaveTextContent('null');
  });
});
