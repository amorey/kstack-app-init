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

import { act, render } from '@testing-library/react';
import { createMemoryHistory, createRouter, RouterProvider } from '@tanstack/react-router';
import type { AnyRoute } from '@tanstack/react-router';
import { vi } from 'vitest';

export type FakeChannel = { onmessage?: (raw: string) => void };

// Shared fake for '@tauri-apps/api/core' — the webview never reaches the
// real Tauri bridge under test. Usage:
//
//   const { invokeMock, channels, liveChannel, factory } = mockTauriCore();
//   vi.mock('@tauri-apps/api/core', () => factory());
//
// `vi.mock` is hoisted, but its factory runs lazily (when the unit under
// test is dynamically `await import`-ed), by which point this helper has
// initialised — same contract the call sites already relied on.
export function mockTauriCore() {
  const invokeMock = vi.fn();
  const channels: FakeChannel[] = [];
  return {
    invokeMock,
    channels,
    liveChannel: () => channels.at(-1)!,
    factory: () => ({
      invoke: (...args: unknown[]) => invokeMock(...args),
      Channel: function FakeChannelCtor(this: FakeChannel) {
        channels.push(this);
      },
    }),
  };
}

export async function renderWithRouter(routeTree: AnyRoute, path: string) {
  const router = createRouter({
    routeTree,
    history: createMemoryHistory({ initialEntries: [path] }),
  });

  await router.load();

  let result!: ReturnType<typeof render>;
  await act(async () => {
    result = render(<RouterProvider router={router} />);
  });

  return {
    ...result,
    router,
  };
}
