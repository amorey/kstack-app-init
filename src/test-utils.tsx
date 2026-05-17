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
