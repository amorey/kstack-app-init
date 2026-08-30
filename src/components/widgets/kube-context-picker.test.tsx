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

import { act, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createRootRoute, createRoute, Outlet, retainSearchParams } from '@tanstack/react-router';
import { Provider as UrqlProvider } from 'urql';
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  flushWatchesSynchronously,
  mockTauriCore,
  pushClusters,
  pushWatchBookmark,
  renderWithRouter,
} from '@/test-utils';

// Mocks ---------------------------------------------------------------

// Frames pushed here are asserted on immediately.
flushWatchesSynchronously();

const { invokeMock, channels, channelFor, factory } = mockTauriCore();
vi.mock('@tauri-apps/api/core', () => factory());

const { createGraphqlClient } = await import('@/lib/graphql/client');
const { ClustersProvider } = await import('@/lib/clusters');
const { KubeConfigProvider } = await import('@/lib/kube-config');
const { KubeContextPicker } = await import('./kube-context-picker');

// Helpers -------------------------------------------------------------

// The `open` frame the host sends on each established connection (ahead of the
// snapshot). It marks the clustersWatch stream connected, so the picker reads
// "connected, empty" rather than "still connecting".
function openStream() {
  channelFor('clustersWatch').onmessage!(JSON.stringify({ type: 'open' }));
}

// The picker reads/writes the active context, so it needs both the router (for
// the `kubeContext` search param) and the kubeconfig providers — mirrors the
// `_app` layout's env.
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
    component: () => <KubeContextPicker />,
  });
  return root.addChildren([app.addChildren([chat])]);
}

describe('KubeContextPicker', () => {
  // base-ui's select relies on pointer-capture / scroll APIs jsdom lacks.
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

  it('shows the resolved active context as the trigger value', async () => {
    await renderWithRouter(buildTree(), '/chat');
    await act(async () => {
      pushClusters(channelFor, [
        { id: 'a', name: 'prod', isDefault: true },
        { id: 'b', name: 'staging' },
      ]);
    });
    expect(screen.getByRole('combobox')).toHaveTextContent('prod');
  });

  it('reflects the kubeContext URL param over the default', async () => {
    await renderWithRouter(buildTree(), '/chat?kubeContext=staging');
    await act(async () => {
      pushClusters(channelFor, [
        { id: 'a', name: 'prod', isDefault: true },
        { id: 'b', name: 'staging' },
      ]);
    });
    expect(screen.getByRole('combobox')).toHaveTextContent('staging');
  });

  it('writes the picked context to the URL search param', async () => {
    const user = userEvent.setup();
    const { router } = await renderWithRouter(buildTree(), '/chat');
    await act(async () => {
      pushClusters(channelFor, [
        { id: 'a', name: 'prod', isDefault: true },
        { id: 'b', name: 'staging' },
      ]);
    });

    await user.click(screen.getByRole('combobox'));
    await user.click(await screen.findByRole('option', { name: 'staging' }));

    await waitFor(() => {
      expect((router.state.location.search as { kubeContext?: string }).kubeContext).toBe('staging');
    });
    expect(screen.getByRole('combobox')).toHaveTextContent('staging');
  });

  it('renders "No kubeconfig" when there are no present contexts', async () => {
    await renderWithRouter(buildTree(), '/chat');
    await act(async () => {
      // A disabled cluster is tracked but not a switchable context, so the list
      // is empty even though the kubeconfig has reported.
      pushClusters(channelFor, [{ id: 'a', name: 'prod', enabled: false }]);
    });
    expect(screen.getByTestId('kube-context-empty')).toBeInTheDocument();
    expect(screen.queryByRole('combobox')).not.toBeInTheDocument();
  });

  it('renders "No kubeconfig" on a connected but empty registry snapshot', async () => {
    await renderWithRouter(buildTree(), '/chat');
    await act(async () => {
      openStream();
      // The Bookmark over an empty snapshot: what makes "no clusters" a fact rather
      // than a not-yet. Without it the picker must keep showing Connecting….
      pushWatchBookmark(channelFor, 'clustersWatch', 'cluster');
    });
    expect(screen.getByTestId('kube-context-empty')).toBeInTheDocument();
    expect(screen.queryByTestId('kube-context-connecting')).not.toBeInTheDocument();
  });

  it('shows a connecting state while the registry stream has not reported', async () => {
    await renderWithRouter(buildTree(), '/chat');
    // No `open`/`next` on clustersWatch: the transport is still dialing, so nothing
    // has been reported. This must read as connecting, not as an empty kubeconfig.
    expect(screen.getByTestId('kube-context-connecting')).toBeInTheDocument();
    expect(screen.queryByTestId('kube-context-empty')).not.toBeInTheDocument();
    expect(screen.queryByRole('combobox')).not.toBeInTheDocument();
  });
});
