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

// Shared fake for '@tauri-apps/api/core'. Usage:
//
//   const { invokeMock, channels, liveChannel, factory } = mockTauriCore();
//   vi.mock('@tauri-apps/api/core', () => factory());
//
// `vi.mock` is hoisted, but its factory runs lazily (at the dynamic `await
// import` of the unit under test), by which point this helper has initialised.
export function mockTauriCore() {
  const invokeMock = vi.fn();
  const channels: FakeChannel[] = [];
  // The live fake Channel for the subscription whose query contains `queryPart`.
  // subscribe-exchange news one Channel per `graphql_subscribe` in call order,
  // so the Nth matching subscribe maps to `channels[N]`; the *last* match is the
  // live one, since a reconnect opens a fresh subscription for the same query.
  // `findLastIndex` is ES2023, outside the app's TS lib — hence `lastIndexOf`.
  const channelFor = (queryPart: string): FakeChannel => {
    const subs = invokeMock.mock.calls.filter(([cmd]) => cmd === 'graphql_subscribe');
    const idx = subs.map(([, arg]) => (arg as { query: string }).query.includes(queryPart)).lastIndexOf(true);
    if (idx < 0) throw new Error(`no subscription for ${queryPart}`);
    return channels[idx];
  };
  return {
    invokeMock,
    channels,
    channelFor,
    liveChannel: () => channels.at(-1)!,
    factory: () => ({
      invoke: (...args: unknown[]) => invokeMock(...args),
      Channel: function FakeChannelCtor(this: FakeChannel) {
        channels.push(this);
      },
    }),
  };
}

// Cluster-delta test helpers ------------------------------------------
//
// Seed the ClustersProvider by pushing `Added` frames onto the `clustersWatch`
// delta stream.

// Cluster/user metadata derive from `name` so a resolved context has predictable
// fields to assert on.
export type ClusterRow = {
  id: string;
  name: string;
  syncEnabled?: boolean;
  enabled?: boolean;
  present?: boolean;
  isDefault?: boolean;
  // Set to mark the record as being torn down; the boundary streams these through.
  deleting?: boolean;
};

export function clusterOf(r: ClusterRow) {
  return {
    id: r.id,
    deletionRequestedAt: r.deleting ? '2026-08-16T00:00:00Z' : null,
    spec: {
      name: r.name,
      syncEnabled: r.syncEnabled ?? true,
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
      lastConnectedAt: null,
      conditions: [],
    },
  };
}

// Close a delta watch's snapshot the way the server does: one Bookmark carrying no
// entity. A consumer treats the stream as still loading until it lands, so a test
// asserting a loaded state — an empty one especially — has to send it.
//
// entityField is the frame's own entity selection (`cluster` on clustersWatch, `cache`
// on clusterCachesWatch): it is null on this frame alone, and each watch names it
// differently, so the caller says which.
export function pushWatchBookmark(
  channelFor: (queryPart: string) => FakeChannel,
  watchField: string,
  entityField: string,
) {
  channelFor(watchField).onmessage!(
    JSON.stringify({ type: 'next', payload: { data: { [watchField]: { type: 'Bookmark', [entityField]: null } } } }),
  );
}

// Pass the `channelFor` from `mockTauriCore()`. Sends the rows as the snapshot and
// closes it, so the registry reads as loaded.
export function pushClusters(channelFor: (queryPart: string) => FakeChannel, rows: ClusterRow[]) {
  const ch = channelFor('clustersWatch');
  rows.forEach((r) => {
    ch.onmessage!(
      JSON.stringify({ type: 'next', payload: { data: { clustersWatch: { type: 'Added', cluster: clusterOf(r) } } } }),
    );
  });
  pushWatchBookmark(channelFor, 'clustersWatch', 'cluster');
}

// Shared fake for '@tauri-apps/api/window', reached via `getCurrentWindow`.
// Usage mirrors `mockTauriCore`:
//
//   const { windowMock, factory } = mockTauriWindow();
//   vi.mock('@tauri-apps/api/window', () => factory());
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

// Platform-branched UI (`isMacOS`/`isLinux`) keys off the WebView's per-OS UA,
// so tests flip it to pick a platform. `NON_MAC_USER_AGENT` is Linux
// (frameless + transparent); `WINDOWS_USER_AGENT` is frameless but opaque.
// See docs/adr/2026-08-09-per-platform-window-chrome.md
export const MAC_USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15';
export const NON_MAC_USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36';
export const WINDOWS_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36';

const originalUserAgent = window.navigator.userAgent;

export function setUserAgent(value: string) {
  Object.defineProperty(window.navigator, 'userAgent', { value, configurable: true });
}

export function restoreUserAgent() {
  setUserAgent(originalUserAgent);
}

// Shared fake for '@tauri-apps/api/event': captures `listen` registrations and
// fires events to them wrapped in `act`. Usage mirrors `mockTauriCore`:
//
//   const { emitEvent, factory } = mockTauriEvent();
//   vi.mock('@tauri-apps/api/event', () => factory());
//   ...
//   emitEvent('host-file-updated', { colorSchemePreference: 'dark' });
export function mockTauriEvent() {
  const listeners = new Map<string, Set<(event: { payload: unknown }) => void>>();
  return {
    emitEvent: (name: string, payload: unknown) => {
      act(() => {
        listeners.get(name)?.forEach((cb) => cb({ payload }));
      });
    },
    factory: () => ({
      listen: (name: string, cb: (event: { payload: unknown }) => void) => {
        let set = listeners.get(name);
        if (!set) {
          set = new Set();
          listeners.set(name, set);
        }
        set.add(cb);
        return Promise.resolve(() => listeners.get(name)?.delete(cb));
      },
    }),
  };
}

// Controllable `window.matchMedia`, returning a `setMatches(next)` that flips
// `matches` and fires `change` (in `act`) like the browser — for the
// color-scheme query and the sidebar breakpoint. `matches` is a live getter so a
// captured `MediaQueryList` reflects later flips. jsdom's default,
// non-controllable stub lives in `vitest.setup.ts`.
export function mockMatchMedia(initialMatches = false) {
  let matches = initialMatches;
  const listeners = new Set<(e: MediaQueryListEvent) => void>();
  window.matchMedia = ((query: string) => ({
    get matches() {
      return matches;
    },
    media: query,
    onchange: null,
    addEventListener: (_type: string, cb: (e: MediaQueryListEvent) => void) => listeners.add(cb),
    removeEventListener: (_type: string, cb: (e: MediaQueryListEvent) => void) => listeners.delete(cb),
    addListener: () => {},
    removeListener: () => {},
    dispatchEvent: () => false,
  })) as unknown as typeof window.matchMedia;
  return (next: boolean) => {
    matches = next;
    act(() => {
      listeners.forEach((cb) => cb({ matches } as MediaQueryListEvent));
    });
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
