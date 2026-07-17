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
import { describe, expect, it, vi } from 'vitest';

import { renderWithRouter } from '@/test-utils';

// The nav builds its tree from `useDashboardNav`, which reaches into urql +
// providers for the active cluster's kinds. This test is about the tree's
// rendering (links, indentation, the "Show more…" reveal), so stub the hook with a
// fixed tree — a curated base plus one workloads group carrying a discovered kind
// on `moreChildren`, and a childless System group whose kinds are also on
// `moreChildren` (empty curated `children`) — mirroring what `buildDashboardNav`
// produces; the renderer derives System's parent-level chevron from that shape.
const { NAV } = vi.hoisted(() => ({
  NAV: [
    { id: 'overview', label: 'Overview' },
    { id: 'nodes', label: 'Nodes' },
    { id: 'namespaces', label: 'Namespaces' },
    {
      id: 'workloads',
      label: 'Workloads',
      children: [
        { id: 'pods', label: 'Pods' },
        { id: 'daemonsets', label: 'DaemonSets' },
      ],
      moreChildren: [{ id: 'apps/replicasets', label: 'ReplicaSet' }],
    },
    {
      id: 'system',
      label: 'System',
      children: [],
      moreChildren: [{ id: 'coordination.k8s.io/leases', label: 'Lease' }],
    },
  ],
}));

vi.mock('@/lib/dashboard-nav', () => ({ useDashboardNav: () => ({ nav: NAV }) }));

const { DashboardResourceNav } = await import('./dashboard-resource-nav');

// A minimal tree with a `/dashboard` route that owns the `resource` search param
// (mirroring the real route) so the nav's `Link`s resolve and can write it. The
// nav is told which kind is active by the resolved search — that's what a page
// does — so the test reads the current param back off the router.
function buildTree() {
  const root = createRootRoute({ component: () => <Outlet /> });
  const dashboard = createRoute({
    getParentRoute: () => root,
    path: '/dashboard',
    // Mirror the real (lenient) search shape, and keep `kubeContext` (which the
    // app owns on `_app`) so the nav's param-preserving spread has something to
    // preserve.
    validateSearch: (search: Record<string, unknown>) => ({
      ...(typeof search.resource === 'string' && search.resource ? { resource: search.resource } : {}),
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
    expect(screen.getByRole('link', { name: 'Overview' })).toHaveAttribute('aria-current', 'page');
    expect(screen.getByRole('link', { name: 'Nodes' })).not.toHaveAttribute('aria-current');
  });

  it('navigates to a resource and pushes a history entry on selection', async () => {
    const { router } = await renderWithRouter(buildTree(), '/dashboard');

    act(() => {
      screen.getByRole('link', { name: 'Namespaces' }).click();
    });

    await waitFor(() => expect(router.state.location.search).toEqual({ resource: 'namespaces' }));
    expect(screen.getByRole('link', { name: 'Namespaces' })).toHaveAttribute('aria-current', 'page');
    expect(screen.getByRole('link', { name: 'Overview' })).not.toHaveAttribute('aria-current');

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

  it('hides discovered kinds behind a "Show more…" toggle, revealing them on click', async () => {
    await renderWithRouter(buildTree(), '/dashboard');

    // Collapsed: the toggle reads "Show more…" and the kind isn't rendered.
    const toggle = screen.getByRole('button', { name: /Show more/ });
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByRole('link', { name: 'ReplicaSet' })).not.toBeInTheDocument();

    act(() => toggle.click());

    // Revealed: the toggle flips to "Show less" and the kind is now a link, at the
    // same depth as the curated children (aligned with Pods, not nested deeper).
    const showLess = screen.getByRole('button', { name: /Show less/ });
    expect(showLess).toBeInTheDocument();
    const replicaset = screen.getByRole('link', { name: 'ReplicaSet' });
    expect(replicaset).toBeInTheDocument();
    const inset = (el: HTMLElement) => parseFloat(el.style.paddingLeft);
    expect(inset(replicaset)).toBeCloseTo(inset(screen.getByRole('link', { name: 'Pods' })));
    // The toggle's own label aligns with the resource labels too.
    expect(inset(showLess)).toBeCloseTo(inset(replicaset));
  });

  it('selects a discovered kind revealed by "Show more…", writing its group/resource id', async () => {
    const { router } = await renderWithRouter(buildTree(), '/dashboard');
    act(() => screen.getByRole('button', { name: /Show more/ }).click());
    act(() => {
      screen.getByRole('link', { name: 'ReplicaSet' }).click();
    });
    await waitFor(() => expect(router.state.location.search).toEqual({ resource: 'apps/replicasets' }));
  });

  it('auto-reveals the group holding the active (deep-linked) discovered kind', async () => {
    await renderWithRouter(buildTree(), '/dashboard?resource=apps/replicasets');
    // No click needed: "Show more…" is already open because the group holds the
    // active kind, so the deep-linked resource is visible and marked current.
    expect(screen.getByRole('button', { name: /Show less/ })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'ReplicaSet' })).toHaveAttribute('aria-current', 'page');
  });

  it('renders a childless group (System) as a navigable link with a chevron toggle', async () => {
    await renderWithRouter(buildTree(), '/dashboard');
    // The group label is a link (navigable, like every other group)...
    expect(screen.getByRole('link', { name: 'System' })).toHaveAttribute('href', '/dashboard?resource=system');
    // ...and its kinds hide behind a collapsed chevron, separate from the link.
    const chevron = screen.getByRole('button', { name: 'Expand System' });
    expect(chevron).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByRole('link', { name: 'Lease' })).not.toBeInTheDocument();

    act(() => chevron.click());
    expect(screen.getByRole('button', { name: 'Collapse System' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Lease' })).toBeInTheDocument();
  });

  it('navigates to a childless group from its label without expanding it', async () => {
    const { router } = await renderWithRouter(buildTree(), '/dashboard');
    act(() => {
      screen.getByRole('link', { name: 'System' }).click();
    });
    await waitFor(() => expect(router.state.location.search).toEqual({ resource: 'system' }));
    expect(screen.getByRole('link', { name: 'System' })).toHaveAttribute('aria-current', 'page');
    // Selecting the group doesn't force its kinds open.
    expect(screen.queryByRole('link', { name: 'Lease' })).not.toBeInTheDocument();
  });

  it('auto-expands a childless group holding the active (deep-linked) kind', async () => {
    await renderWithRouter(buildTree(), '/dashboard?resource=coordination.k8s.io/leases');
    expect(screen.getByRole('button', { name: 'Collapse System' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Lease' })).toHaveAttribute('aria-current', 'page');
  });
});
