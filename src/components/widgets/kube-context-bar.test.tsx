// Copyright 2026 The Kstack Authors
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

import { act, screen } from '@testing-library/react';
import { createRootRoute, createRoute, Outlet, retainSearchParams } from '@tanstack/react-router';
import { Provider as UrqlProvider } from 'urql';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { mockTauriCore, pushClusters, renderWithRouter } from '@/test-utils';

// Mocks ---------------------------------------------------------------

const { invokeMock, channels, channelFor, factory } = mockTauriCore();
vi.mock('@tauri-apps/api/core', () => factory());

const { createGraphqlClient } = await import('@/lib/graphql/client');
const { ClustersProvider } = await import('@/lib/clusters');
const { KubeConfigProvider } = await import('@/lib/kube-config');
const { KubeContextBar } = await import('./kube-context-bar');

// Helpers -------------------------------------------------------------

// Mirrors the `_app` layout's env: the router (for the `kubeContext` search
// param) plus the kubeconfig providers the bar reads through.
function buildTree() {
  const root = createRootRoute({
    component: () => (
      <UrqlProvider value={createGraphqlClient()}>
        <ClustersProvider>
          <KubeConfigProvider>
            <Outlet />
          </KubeConfigProvider>
        </ClustersProvider>
      </UrqlProvider>
    ),
  });
  const app = createRoute({
    getParentRoute: () => root,
    id: 'app',
    validateSearch: (s: Record<string, unknown>): { kubeContext?: string } =>
      typeof s.kubeContext === 'string' ? { kubeContext: s.kubeContext } : {},
    search: { middlewares: [retainSearchParams(['kubeContext'])] },
    component: () => <Outlet />,
  });
  const chat = createRoute({
    getParentRoute: () => app,
    path: '/chat',
    component: () => <KubeContextBar />,
  });
  return root.addChildren([app.addChildren([chat])]);
}

describe('KubeContextBar', () => {
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

  it('shows the active context plus its cluster and user metadata', async () => {
    await renderWithRouter(buildTree(), '/chat?kubeContext=staging');
    await act(async () => {
      pushClusters(channelFor, [
        { id: 'a', name: 'prod', isDefault: true },
        { id: 'b', name: 'staging' },
      ]);
    });

    // Picker reflects the resolved context...
    expect(screen.getByRole('combobox')).toHaveTextContent('staging');
    // ...and the bar surfaces that context's cluster/user.
    expect(screen.getByText('staging-cluster')).toBeInTheDocument();
    expect(screen.getByText('staging-user')).toBeInTheDocument();
  });

  it('renders no metadata when there is no kubeconfig', async () => {
    await renderWithRouter(buildTree(), '/chat');
    await act(async () => {
      // A disabled cluster is tracked but not a switchable context, so nothing
      // resolves and the bar shows only the picker's empty state.
      pushClusters(channelFor, [{ id: 'a', name: 'prod', enabled: false }]);
    });
    expect(screen.getByTestId('kube-context-empty')).toBeInTheDocument();
    expect(screen.queryByText(/cluster/)).not.toBeInTheDocument();
  });
});
