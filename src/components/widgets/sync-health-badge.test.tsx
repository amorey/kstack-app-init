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

import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { SyncStatus } from '@/lib/sync-status';

// The `syncStatusWatch` GraphQL surface does not exist yet (see the TODO in
// sync-status.tsx), so we drive the badge by mocking useSyncStatus directly. This
// keeps the badge's label/tone logic covered independent of the (currently
// absent) status source; formatSyncFreshness stays real via importOriginal.
let mockStatus: SyncStatus | null = null;
vi.mock('@/lib/sync-status', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/sync-status')>();
  return { ...actual, useSyncStatus: () => ({ status: mockStatus }) };
});

const { SyncHealthBadge } = await import('./sync-health-badge');

describe('SyncHealthBadge', () => {
  beforeEach(() => {
    mockStatus = null;
  });

  it('renders a muted connecting state when status is not reported', () => {
    render(<SyncHealthBadge />);
    expect(screen.getByRole('status')).toHaveTextContent(/connecting/i);
  });

  it('shows a synced label when live', () => {
    mockStatus = { state: 'LIVE', lastError: null, lastSyncedAt: 1_700_000_000_000, retryAt: 0 };
    render(<SyncHealthBadge />);
    expect(screen.getByRole('status')).toHaveTextContent(/synced/i);
  });

  it('shows reconnecting on backoff', () => {
    mockStatus = { state: 'BACKOFF', lastError: 'dial tcp: refused', lastSyncedAt: 0, retryAt: 1_700_000_005_000 };
    render(<SyncHealthBadge />);
    expect(screen.getByRole('status')).toHaveTextContent(/reconnect/i);
  });

  it('surfaces the last error when offline', () => {
    mockStatus = { state: 'OFFLINE', lastError: 'no credentials', lastSyncedAt: 0, retryAt: 0 };
    render(<SyncHealthBadge />);
    const badge = screen.getByRole('status');
    expect(badge).toHaveTextContent(/offline/i);
    expect(badge).toHaveTextContent(/no credentials/);
  });

  it('shows a bare Offline label when no error is attached', () => {
    mockStatus = { state: 'OFFLINE', lastError: null, lastSyncedAt: 0, retryAt: 0 };
    render(<SyncHealthBadge />);
    expect(screen.getByRole('status')).toHaveTextContent(/^offline$/i);
  });
});
