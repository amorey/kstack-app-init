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
  isCurrent?: boolean;
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
              setClusterEnabled: { __typename: 'Cluster', uuid: 'u', enabled: false },
              deleteClusterCache: true,
              removeCluster: true,
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
      { uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true, isCurrent: true },
      { uuid: '', name: 'minikube', enabled: false, present: true, cached: false },
      { uuid: 'u-old', name: 'old-cluster', enabled: true, present: false, cached: true },
    ]);

    // Scoped to cells so the group-header "Active" label doesn't collide.
    const active = await screen.findByRole('rowgroup', { name: /active/i });
    const orphaned = screen.getByRole('rowgroup', { name: /orphaned/i });

    expect(within(active).getByRole('cell', { name: 'Active' })).toBeInTheDocument(); // reachable
    expect(within(active).getByRole('cell', { name: 'Error' })).toBeInTheDocument(); // probe failed
    expect(within(orphaned).getByRole('cell', { name: 'Unavailable' })).toBeInTheDocument(); // gone
  });

  it('omits a row group that has no members', async () => {
    await openWith([{ uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true }]);
    expect(await screen.findByRole('rowgroup', { name: /active/i })).toBeInTheDocument();
    expect(screen.queryByRole('rowgroup', { name: /orphaned/i })).not.toBeInTheDocument();
  });

  it('toggling sync fires the play/pause action via setClusterEnabled', async () => {
    const user = await openWith([{ uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true }]);

    // Enabled + active → a Pause button.
    await user.click(await screen.findByRole('button', { name: /pause sync for prod-us/i }));
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_query',
      expect.objectContaining({ body: expect.stringContaining('setClusterEnabled') }),
    );
  });

  it('clearing a cache fires deleteClusterCache, and is disabled when uncached', async () => {
    const user = await openWith([
      { uuid: 'u-prod', name: 'prod-us', enabled: true, present: true, cached: true },
      { uuid: 'u-stg', name: 'staging', enabled: false, present: true, cached: false },
    ]);

    expect(await screen.findByRole('button', { name: /clear cache for staging/i })).toBeDisabled();

    await user.click(screen.getByRole('button', { name: /clear cache for prod-us/i }));
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_query',
      expect.objectContaining({ body: expect.stringContaining('deleteClusterCache') }),
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

  it('disables play/pause for orphaned rows and removes them via removeCluster', async () => {
    const user = await openWith([{ uuid: 'u-old', name: 'old-cluster', enabled: true, present: false, cached: true }]);

    expect(await screen.findByRole('button', { name: /sync for old-cluster/i })).toBeDisabled();

    const remove = screen.getByRole('button', { name: /^remove old-cluster/i });
    expect(remove).toBeEnabled();
    await user.click(remove);
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_query',
      expect.objectContaining({ body: expect.stringContaining('removeCluster') }),
    );
  });
});
