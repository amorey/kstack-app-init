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

import { render, screen, act } from '@testing-library/react';
import { Provider as UrqlProvider } from 'urql';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { mockTauriCore } from '@/test-utils';

// Mocks ---------------------------------------------------------------

const { invokeMock, channels, liveChannel, factory } = mockTauriCore();
vi.mock('@tauri-apps/api/core', () => factory());

const { createGraphqlClient } = await import('@/lib/graphql/client');
const { SyncStatusProvider } = await import('@/lib/sync-status');
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

function renderBadge() {
  return render(
    <UrqlProvider value={createGraphqlClient()}>
      <SyncStatusProvider>
        <SyncHealthBadge />
      </SyncStatusProvider>
    </UrqlProvider>,
  );
}

describe('SyncHealthBadge', () => {
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

  it('renders a muted connecting state before the first push', async () => {
    renderBadge();
    await flush();
    expect(screen.getByRole('status')).toHaveTextContent(/connecting/i);
  });

  it('subscribes to syncStatusWatch and reflects the engine state', async () => {
    renderBadge();
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
    renderBadge();
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
    renderBadge();
    await flush();

    await act(async () => {
      pushStatus({ state: 'OFFLINE', lastError: 'no credentials' });
    });
    const badge = screen.getByRole('status');
    expect(badge).toHaveTextContent(/offline/i);
    expect(badge).toHaveTextContent(/no credentials/);
  });

  it('shows a bare Offline label when no error is attached', async () => {
    renderBadge();
    await flush();

    await act(async () => {
      pushStatus({ state: 'OFFLINE' });
    });
    expect(screen.getByRole('status')).toHaveTextContent(/^offline$/i);
  });
});
