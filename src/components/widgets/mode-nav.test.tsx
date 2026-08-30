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
import { createRootRoute, createRoute, Outlet } from '@tanstack/react-router';
import { describe, expect, it } from 'vitest';

import { renderWithRouter } from '@/test-utils';
import { ModeNav } from './mode-nav';

// A minimal tree: the nav plus two destination pages, so navigation can be
// observed by which page renders.
function buildTree() {
  const root = createRootRoute({
    component: () => (
      <>
        <ModeNav />
        <Outlet />
      </>
    ),
  });
  const chat = createRoute({
    getParentRoute: () => root,
    path: '/chat',
    component: () => <div>chat-page</div>,
  });
  const dashboard = createRoute({
    getParentRoute: () => root,
    path: '/dashboard',
    component: () => <div>dashboard-page</div>,
  });
  return root.addChildren([chat, dashboard]);
}

describe('ModeNav', () => {
  it('links to the chat and dashboard routes', async () => {
    await renderWithRouter(buildTree(), '/chat');
    expect(screen.getByRole('link', { name: 'Chat' })).toHaveAttribute('href', '/chat');
    expect(screen.getByRole('link', { name: 'Dashboard' })).toHaveAttribute('href', '/dashboard');
  });

  it('marks only the current route active', async () => {
    await renderWithRouter(buildTree(), '/chat');
    expect(screen.getByRole('link', { name: 'Chat' })).toHaveAttribute('aria-current', 'page');
    expect(screen.getByRole('link', { name: 'Dashboard' })).not.toHaveAttribute('aria-current');
  });

  it('navigates to the dashboard when its link is clicked', async () => {
    await renderWithRouter(buildTree(), '/chat');
    expect(screen.getByText('chat-page')).toBeInTheDocument();

    act(() => {
      screen.getByRole('link', { name: 'Dashboard' }).click();
    });

    await waitFor(() => expect(screen.getByText('dashboard-page')).toBeInTheDocument());
    expect(screen.getByRole('link', { name: 'Dashboard' })).toHaveAttribute('aria-current', 'page');
    expect(screen.getByRole('link', { name: 'Chat' })).not.toHaveAttribute('aria-current');
  });
});
