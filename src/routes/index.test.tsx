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

import { createRootRoute, createRoute, redirect } from '@tanstack/react-router';
import { screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { renderWithRouter } from '@/test-utils';
import { DEFAULT_ROUTE } from '.';

// `/` carries no view of its own; it redirects to whichever route `DEFAULT_ROUTE`
// names. This pins that default to chat (so flipping the constant to make
// something else the landing view is a deliberate, test-visible change) without
// dragging in the real chat page's provider stack.
function buildTree() {
  const root = createRootRoute();
  const index = createRoute({
    getParentRoute: () => root,
    path: '/',
    beforeLoad: () => {
      // TanStack signals a redirect by throwing the `redirect()` result.
      // eslint-disable-next-line @typescript-eslint/only-throw-error
      throw redirect({ to: DEFAULT_ROUTE });
    },
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
  return root.addChildren([index, chat, dashboard]);
}

describe('default route', () => {
  it('sends / to the chat route', async () => {
    await renderWithRouter(buildTree(), '/');
    expect(screen.getByText('chat-page')).toBeInTheDocument();
    expect(screen.queryByText('dashboard-page')).not.toBeInTheDocument();
  });
});
