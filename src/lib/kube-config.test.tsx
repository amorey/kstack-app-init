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
const { ClustersProvider } = await import('./clusters');
const { KubeConfigProvider, useKubeConfig } = await import('./kube-config');

// Helpers -------------------------------------------------------------

const flush = () => act(async () => {});

type Row = {
  id: string;
  name: string;
  present?: boolean;
  enabled?: boolean;
  isDefault?: boolean;
};

function pushClusters(rows: Row[]) {
  liveChannel().onmessage!(
    JSON.stringify({
      type: 'next',
      payload: {
        data: {
          clustersWatch: rows.map((r) => ({
            id: r.id,
            spec: {
              name: r.name,
              syncEnabled: true,
              enabled: r.enabled ?? true,
              source: { kubeconfig: { context: r.name } },
            },
            status: {
              source: {
                kubeconfig: {
                  cluster: `${r.name}-cluster`,
                  user: `${r.name}-user`,
                  isPresent: r.present ?? true,
                  isDefault: r.isDefault ?? false,
                },
              },
              server: { uid: `uid-${r.id}` },
              conditions: [],
            },
            cache: {
              status: { conditions: [], lastSyncedAt: null },
              stats: { exists: false, bytes: 0 },
            },
          })),
        },
      },
    }),
  );
}

// A probe that renders the derived kubeconfig so tests can assert on it.
function Probe() {
  const { kubeConfig } = useKubeConfig();
  return <div data-testid="probe">{kubeConfig === null ? 'null' : JSON.stringify(kubeConfig)}</div>;
}

function renderProvider() {
  return render(
    <UrqlProvider value={createGraphqlClient()}>
      <ClustersProvider>
        <KubeConfigProvider>
          <Probe />
        </KubeConfigProvider>
      </ClustersProvider>
    </UrqlProvider>,
  );
}

describe('useKubeConfig', () => {
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

  it('excludes disabled and orphaned clusters from the context list', async () => {
    renderProvider();
    await flush();

    await act(async () => {
      pushClusters([
        { id: 'a', name: 'prod', enabled: true, present: true, isDefault: true },
        { id: 'b', name: 'staging', enabled: false, present: true },
        { id: 'c', name: 'gone', enabled: true, present: false },
      ]);
    });

    const probe = screen.getByTestId('probe');
    const kubeConfig = JSON.parse(probe.textContent ?? '');
    expect(kubeConfig.contexts).toEqual([{ name: 'prod' }]);
    expect(kubeConfig.currentContext).toBe('prod');
  });
});
