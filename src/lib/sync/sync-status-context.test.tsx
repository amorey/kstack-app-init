import { render, screen, act } from '@testing-library/react';
import { Provider as UrqlProvider } from 'urql';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { mockTauriCore } from '@/test-utils';

// Mocks ---------------------------------------------------------------

const { invokeMock, channels, liveChannel, factory } = mockTauriCore();
vi.mock('@tauri-apps/api/core', () => factory());

const { createGraphqlClient } = await import('@/lib/graphql/client');
const { SyncStatusProvider, useSyncStatus, formatSyncFreshness } = await import('./sync-status-context');
const { SyncHealthBadge } = await import('./sync-health-badge');

// Helpers -------------------------------------------------------------

const flush = () => act(async () => {});

function pushStatus(s: {
  state: 'CONNECTING' | 'LIVE' | 'BACKOFF' | 'OFFLINE';
  lastError?: string;
  lastSyncedAt?: number;
  retryAt?: number;
}) {
  liveChannel().onmessage!(
    JSON.stringify({
      type: 'next',
      payload: {
        data: {
          syncStatusWatch: { lastError: '', lastSyncedAt: 0, retryAt: 0, ...s },
        },
      },
    }),
  );
}

function renderWithProvider(ui: React.ReactNode) {
  return render(
    <UrqlProvider value={createGraphqlClient()}>
      <SyncStatusProvider>{ui}</SyncStatusProvider>
    </UrqlProvider>,
  );
}

describe('formatSyncFreshness', () => {
  const base = 1_000_000_000_000;

  it('reports never-synced for 0', () => {
    expect(formatSyncFreshness(0, base)).toMatch(/never synced/i);
  });

  it('buckets recent syncs into a relative label', () => {
    expect(formatSyncFreshness(base - 2_000, base)).toMatch(/just now/i);
    expect(formatSyncFreshness(base - 42_000, base)).toMatch(/42s/);
    expect(formatSyncFreshness(base - 5 * 60_000, base)).toMatch(/5m/);
    expect(formatSyncFreshness(base - 3 * 3_600_000, base)).toMatch(/3h/);
  });

  it('handles the exact bucket boundaries (strict <)', () => {
    expect(formatSyncFreshness(base - 5_000, base)).toMatch(/5s/); // not "just now"
    expect(formatSyncFreshness(base - 60_000, base)).toMatch(/1m/); // not "60s"
    expect(formatSyncFreshness(base - 3_600_000, base)).toMatch(/1h/); // not "60m"
  });

  it('clamps a future lastSyncedAt to "just now" (clock skew)', () => {
    expect(formatSyncFreshness(base + 10_000, base)).toMatch(/just now/i);
  });
});

describe('SyncStatusProvider / SyncHealthBadge', () => {
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

  it('useSyncStatus throws outside the provider', () => {
    function Bare() {
      useSyncStatus();
      return null;
    }
    expect(() => render(<Bare />)).toThrow(/SyncStatusProvider/);
  });

  it('renders a muted connecting state before the first push', async () => {
    renderWithProvider(<SyncHealthBadge />);
    await flush();
    expect(screen.getByRole('status')).toHaveTextContent(/connecting/i);
  });

  it('subscribes to syncStatusWatch and reflects the engine state', async () => {
    renderWithProvider(<SyncHealthBadge />);
    await flush();

    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_subscribe',
      expect.objectContaining({ query: expect.stringContaining('syncStatusWatch') }),
    );

    await act(async () => {
      pushStatus({ state: 'LIVE', lastSyncedAt: 1_700_000_000_000 });
    });
    expect(screen.getByRole('status')).toHaveTextContent(/synced/i);
  });

  it('updates the badge when a new status is pushed (wake/backoff)', async () => {
    renderWithProvider(<SyncHealthBadge />);
    await flush();

    await act(async () => {
      pushStatus({ state: 'LIVE', lastSyncedAt: 1_700_000_000_000 });
    });
    expect(screen.getByRole('status')).toHaveTextContent(/synced/i);

    await act(async () => {
      pushStatus({ state: 'BACKOFF', lastError: 'dial tcp: refused', retryAt: 1_700_000_005_000 });
    });
    expect(screen.getByRole('status')).toHaveTextContent(/reconnect/i);
  });

  it('surfaces the last error when offline', async () => {
    renderWithProvider(<SyncHealthBadge />);
    await flush();

    await act(async () => {
      pushStatus({ state: 'OFFLINE', lastError: 'no credentials' });
    });
    const badge = screen.getByRole('status');
    expect(badge).toHaveTextContent(/offline/i);
    expect(badge).toHaveTextContent(/no credentials/);
  });
});
