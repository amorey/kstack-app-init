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

// Shared fake for '@tauri-apps/api/window' — components that drive the native
// window (e.g. `WindowControls`, `AppSidebar`) reach it through `getCurrentWindow`.
// Usage mirrors `mockTauriCore`:
//
//   const { windowMock, factory } = mockTauriWindow();
//   vi.mock('@tauri-apps/api/window', () => factory());
//
// `windowMock` exposes the spied methods for asserting calls; callers that only
// need the module to resolve can ignore it.
export function mockTauriWindow() {
  const windowMock = {
    minimize: vi.fn(() => Promise.resolve()),
    toggleMaximize: vi.fn(() => Promise.resolve()),
    close: vi.fn(() => Promise.resolve()),
    isMaximized: vi.fn(() => Promise.resolve(false)),
    // Matches Tauri's `onResized`: resolves to an unlisten fn.
    onResized: vi.fn(() => Promise.resolve(() => {})),
  };
  return {
    windowMock,
    factory: () => ({ getCurrentWindow: () => windowMock }),
  };
}

// User-agent strings and overrides for exercising `isMacOS()`-branched UI. The
// WebView's UA is fixed per-OS, so tests flip it to pick a platform.
export const MAC_USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15';
export const NON_MAC_USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36';

const originalUserAgent = window.navigator.userAgent;

export function setUserAgent(value: string) {
  Object.defineProperty(window.navigator, 'userAgent', { value, configurable: true });
}

export function restoreUserAgent() {
  setUserAgent(originalUserAgent);
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
