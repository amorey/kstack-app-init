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

import { act, screen, waitFor } from '@testing-library/react';
import { createRootRoute, createRoute, Outlet } from '@tanstack/react-router';
import { describe, expect, it } from 'vitest';

import { isDashboardResource } from '@/lib/dashboard-resources';
import { renderWithRouter } from '@/test-utils';
import { DashboardResourceNav } from './dashboard-resource-nav';

// A minimal tree with a `/dashboard` route that owns the `resource` search param
// (mirroring the real route) so the nav's `Link`s resolve and can write it. The
// nav is told which kind is active by the resolved search — that's what a page
// does — so the test reads the current param back off the router.
function buildTree() {
  const root = createRootRoute({ component: () => <Outlet /> });
  const dashboard = createRoute({
    getParentRoute: () => root,
    path: '/dashboard',
    // Mirror the real search shape, and keep `kubeContext` (which the app owns
    // on `_app`) so the nav's param-preserving spread has something to preserve.
    validateSearch: (search: Record<string, unknown>) => ({
      ...(isDashboardResource(search.resource) ? { resource: search.resource } : {}),
      ...(typeof search.kubeContext === 'string' ? { kubeContext: search.kubeContext } : {}),
    }),
    component: DashboardResourceNav,
  });
  return root.addChildren([dashboard]);
}

describe('DashboardResourceNav', () => {
  it('links each resource to the dashboard with its own search param', async () => {
    await renderWithRouter(buildTree(), '/dashboard');
    expect(screen.getByRole('link', { name: 'Nodes' })).toHaveAttribute('href', '/dashboard?resource=nodes');
    expect(screen.getByRole('link', { name: 'Namespaces' })).toHaveAttribute('href', '/dashboard?resource=namespaces');
  });

  it('renders group nodes and their children as links, children indented', async () => {
    await renderWithRouter(buildTree(), '/dashboard');
    // A group is selectable in its own right...
    expect(screen.getByRole('link', { name: 'Workloads' })).toHaveAttribute('href', '/dashboard?resource=workloads');
    // ...and its children render as deeper-indented links.
    const workloads = screen.getByRole('link', { name: 'Workloads' });
    const pods = screen.getByRole('link', { name: 'Pods' });
    expect(pods).toHaveAttribute('href', '/dashboard?resource=pods');
    expect(screen.getByRole('link', { name: 'DaemonSets' })).toBeInTheDocument();
    const inset = (el: HTMLElement) => parseFloat(el.style.paddingLeft);
    expect(inset(pods)).toBeGreaterThan(inset(workloads));
  });

  it('selects a nested child, writing its id to the param', async () => {
    const { router } = await renderWithRouter(buildTree(), '/dashboard');
    act(() => {
      screen.getByRole('link', { name: 'Pods' }).click();
    });
    await waitFor(() => expect(router.state.location.search).toEqual({ resource: 'pods' }));
    expect(screen.getByRole('link', { name: 'Pods' })).toHaveAttribute('aria-current', 'page');
  });

  it('marks the default resource active when the param is absent', async () => {
    await renderWithRouter(buildTree(), '/dashboard');
    expect(screen.getByRole('link', { name: 'Nodes' })).toHaveAttribute('aria-current', 'page');
    expect(screen.getByRole('link', { name: 'Namespaces' })).not.toHaveAttribute('aria-current');
  });

  it('navigates to a resource and pushes a history entry on selection', async () => {
    const { router } = await renderWithRouter(buildTree(), '/dashboard');

    act(() => {
      screen.getByRole('link', { name: 'Namespaces' }).click();
    });

    await waitFor(() => expect(router.state.location.search).toEqual({ resource: 'namespaces' }));
    expect(screen.getByRole('link', { name: 'Namespaces' })).toHaveAttribute('aria-current', 'page');
    expect(screen.getByRole('link', { name: 'Nodes' })).not.toHaveAttribute('aria-current');

    // The selection is a push (not a replace), so a future back/forward has a
    // step to walk: history grew from the initial entry.
    expect(router.history.length).toBeGreaterThan(1);
  });

  it('preserves other search params (e.g. the active kube-context) when switching', async () => {
    const { router } = await renderWithRouter(buildTree(), '/dashboard?kubeContext=prod');

    act(() => {
      screen.getByRole('link', { name: 'Namespaces' }).click();
    });

    await waitFor(() => expect(router.state.location.search).toEqual({ kubeContext: 'prod', resource: 'namespaces' }));
  });
});
