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
const { ClustersProvider } = await import('@/lib/clusters');
const { ClusterSyncPanel } = await import('./cluster-sync-panel');

// Helpers -------------------------------------------------------------

const flush = () => act(async () => {});

type Row = {
  uuid: string;
  name: string;
  enabled: boolean;
  present: boolean;
  cached: boolean;
  cacheBytes?: number;
  lastSyncedAt?: number;
};

function pushClusters(rows: Row[]) {
  liveChannel().onmessage!(
    JSON.stringify({
      type: 'next',
      payload: {
        data: {
          clustersWatch: rows.map((r) => ({
            context: r.name,
            isCurrent: false,
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

function renderPanel() {
  return render(
    <UrqlProvider value={createGraphqlClient()}>
      <ClustersProvider>
        <ClusterSyncPanel />
      </ClustersProvider>
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
      if (cmd === 'graphql_query') {
        // Either mutation succeeds; extra fields are ignored by urql.
        return {
          status: 200,
          body: JSON.stringify({
            data: {
              setClusterEnabled: { __typename: 'Cluster', uuid: 'u', enabled: false },
              deleteClusterCache: true,
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
    const user = userEvent.setup();
    renderPanel();
    await flush();
    await act(async () => {
      pushClusters([]);
    });

    await user.click(screen.getByRole('button', { name: /clusters/i }));
    expect(await screen.findByText(/no clusters yet/i)).toBeInTheDocument();
  });

  it('lists a row per cluster with derived status and cache size', async () => {
    const user = userEvent.setup();
    renderPanel();
    await flush();
    await act(async () => {
      pushClusters([
        { uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true, cacheBytes: 1_300_000 },
        { uuid: 'u-stg', name: 'staging', enabled: false, present: true, cached: true, cacheBytes: 524_288 },
        { uuid: 'u-old', name: 'old-cluster', enabled: true, present: false, cached: true, cacheBytes: 1024 },
      ]);
    });

    await user.click(screen.getByRole('button', { name: /clusters/i }));

    expect(await screen.findByText('prod-us')).toBeInTheDocument();
    expect(screen.getByText('staging')).toBeInTheDocument();
    expect(screen.getByText('old-cluster')).toBeInTheDocument();

    // Status derived from the enabled/present flags.
    expect(screen.getByText(/^syncing$/i)).toBeInTheDocument(); // prod: enabled + present
    expect(screen.getByText(/^paused$/i)).toBeInTheDocument(); // staging: disabled
    expect(screen.getByText(/^orphaned$/i)).toBeInTheDocument(); // old: enabled but gone

    // Cache sizes formatted (binary units).
    expect(screen.getByText(/1\.2 MB/)).toBeInTheDocument();
    expect(screen.getByText(/512\.0 KB/)).toBeInTheDocument();
  });

  it('reflects the backend enabled flag on each switch', async () => {
    const user = userEvent.setup();
    renderPanel();
    await flush();
    await act(async () => {
      pushClusters([
        { uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true },
        { uuid: 'u-stg', name: 'staging', enabled: false, present: true, cached: true },
      ]);
    });

    await user.click(screen.getByRole('button', { name: /clusters/i }));
    expect(await screen.findByRole('switch', { name: /prod-us/i })).toBeChecked();
    expect(screen.getByRole('switch', { name: /staging/i })).not.toBeChecked();
  });

  it('toggling a cluster fires the setClusterEnabled mutation', async () => {
    const user = userEvent.setup();
    renderPanel();
    await flush();
    await act(async () => {
      pushClusters([{ uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true }]);
    });

    await user.click(screen.getByRole('button', { name: /clusters/i }));
    await user.click(await screen.findByRole('switch', { name: /prod-us/i }));

    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_query',
      expect.objectContaining({ body: expect.stringContaining('setClusterEnabled') }),
    );
  });

  it('deleting a cached cluster fires the deleteClusterCache mutation', async () => {
    const user = userEvent.setup();
    renderPanel();
    await flush();
    await act(async () => {
      pushClusters([{ uuid: 'u-old', name: 'old-cluster', enabled: true, present: false, cached: true }]);
    });

    await user.click(screen.getByRole('button', { name: /clusters/i }));
    await user.click(await screen.findByRole('button', { name: /delete cache for old-cluster/i }));

    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_query',
      expect.objectContaining({ body: expect.stringContaining('deleteClusterCache') }),
    );
  });
});
