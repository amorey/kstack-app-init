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

import { createRootRoute, createRoute } from '@tanstack/react-router';
import { screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { mockTauriWindow, renderWithRouter } from '@/test-utils';

// Mocks ---------------------------------------------------------------

// `WindowControls` (in the sidebar's custom title bar off macOS) drives the
// native window via `getCurrentWindow`.
const { factory } = mockTauriWindow();
vi.mock('@tauri-apps/api/window', () => factory());

// Replace each chrome widget with an identifiable stub so the test asserts
// *placement* (title bar vs sidebar vs inset) without dragging in their
// providers.
vi.mock('@/components/widgets/app-menu', () => ({
  AppMenu: () => <div data-testid="app-menu" />,
}));
vi.mock('@/components/widgets/account-menu', () => ({
  AccountMenu: () => <div data-testid="account-menu" />,
}));
vi.mock('@/components/widgets/mode-nav', () => ({
  ModeNav: () => <div data-testid="mode-nav" />,
}));
vi.mock('@/lib/connection-status', () => ({
  ConnectionStatus: () => <div data-testid="connection-status" />,
}));

const { AppLayout } = await import('./app-layout');

// Helpers -------------------------------------------------------------

// AppLayout renders an <Outlet/>, so it can only be exercised inside a router.
// Mount it as the root layout with one child page that drops a marker in the
// inset.
function buildTree() {
  const root = createRootRoute({ component: AppLayout });
  const index = createRoute({
    getParentRoute: () => root,
    path: '/',
    component: () => <div data-testid="page-content" />,
  });
  return root.addChildren([index]);
}

function sidebarOf(container: HTMLElement) {
  const el = container.querySelector('[data-testid="app-sidebar"]');
  if (!el) throw new Error('sidebar not found');
  return el;
}

function insetOf(container: HTMLElement) {
  const el = container.querySelector('[data-testid="sidebar-inset"]');
  if (!el) throw new Error('sidebar inset not found');
  return el;
}

// Tests ---------------------------------------------------------------

describe('AppLayout', () => {
  it('places the mode nav inside the sidebar', async () => {
    const { container } = await renderWithRouter(buildTree(), '/');
    expect(sidebarOf(container).contains(screen.getByTestId('mode-nav'))).toBe(true);
  });

  it('places the account chrome inside the sidebar', async () => {
    const { container } = await renderWithRouter(buildTree(), '/');
    expect(sidebarOf(container).contains(screen.getByTestId('account-menu'))).toBe(true);
  });

  it('places the app menu in the title bar, not the sidebar', async () => {
    const { container } = await renderWithRouter(buildTree(), '/');
    expect(sidebarOf(container).contains(screen.getByTestId('app-menu'))).toBe(false);
  });

  it('renders the routed page in the inset, not the sidebar', async () => {
    const { container } = await renderWithRouter(buildTree(), '/');
    const page = screen.getByTestId('page-content');
    expect(insetOf(container).contains(page)).toBe(true);
    expect(sidebarOf(container).contains(page)).toBe(false);
  });

  it('renders the connection-status banner', async () => {
    await renderWithRouter(buildTree(), '/');
    expect(screen.getByTestId('connection-status')).toBeInTheDocument();
  });
});
