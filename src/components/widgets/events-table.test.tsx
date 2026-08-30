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

import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { stubLayout } from '@/test-utils';

// EventsTable is pure presentation over useClusterCachedDataEvents — mock the hook to drive each
// of the four states (no active cache, connecting, empty, live rows) directly.
const { useClusterCachedDataEventsMock } = vi.hoisted(() => ({
  useClusterCachedDataEventsMock: vi.fn<typeof import('@/lib/cluster-cached-data-events').useClusterCachedDataEvents>(),
}));
vi.mock('@/lib/cluster-cached-data-events', () => ({ useClusterCachedDataEvents: useClusterCachedDataEventsMock }));

const { EventsTable } = await import('./events-table');

function evt(over: Record<string, unknown> = {}) {
  return {
    uid: 'e1',
    type: 'Warning',
    reason: 'BackOff',
    message: 'Back-off restarting failed container',
    count: 3,
    firstSeen: '2026-07-20T10:00:00Z',
    lastSeen: '2026-07-20T10:00:00Z',
    involvedKind: 'Pod',
    involvedNamespace: 'default',
    involvedName: 'my-pod',
    ...over,
  };
}

// Rows render only inside a laid-out viewport (see VirtualTable).
stubLayout(400);

beforeEach(() => vi.clearAllMocks());

describe('EventsTable', () => {
  it('explains the unsynced state when there is no active cache', () => {
    useClusterCachedDataEventsMock.mockReturnValue({ events: [], active: false, phase: 'connecting' });
    render(<EventsTable />);
    expect(screen.getByText(/No synced cache/i)).toBeInTheDocument();
  });

  it('shows a spinner while connecting', () => {
    useClusterCachedDataEventsMock.mockReturnValue({ events: [], active: true, phase: 'connecting' });
    render(<EventsTable />);
    expect(screen.getByText(/Loading events/i)).toBeInTheDocument();
  });

  it('shows an empty state on a connected empty snapshot', () => {
    useClusterCachedDataEventsMock.mockReturnValue({ events: [], active: true, phase: 'live' });
    render(<EventsTable />);
    expect(screen.getByText('No events.')).toBeInTheDocument();
  });

  it('renders a row with the event fields and a kubectl-style involved reference', () => {
    useClusterCachedDataEventsMock.mockReturnValue({ events: [evt()], active: true, phase: 'live' });
    render(<EventsTable />);
    expect(screen.getByText('Warning')).toBeInTheDocument();
    expect(screen.getByText('BackOff')).toBeInTheDocument();
    expect(screen.getByText('Pod default/my-pod')).toBeInTheDocument();
    expect(screen.getByText('Back-off restarting failed container')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();
  });

  it('omits the namespace from the involved reference for a name-only object', () => {
    useClusterCachedDataEventsMock.mockReturnValue({
      events: [evt({ involvedKind: 'Node', involvedNamespace: '', involvedName: 'node-1' })],
      active: true,
      phase: 'live',
    });
    render(<EventsTable />);
    expect(screen.getByText('Node node-1')).toBeInTheDocument();
  });

  it('labels an event with an empty type (open Kubernetes field) as Unknown', () => {
    useClusterCachedDataEventsMock.mockReturnValue({ events: [evt({ type: '' })], active: true, phase: 'live' });
    render(<EventsTable />);
    expect(screen.getByText('Unknown')).toBeInTheDocument();
  });

  it('renders "—" for a null lastSeen instead of an ancient date', () => {
    useClusterCachedDataEventsMock.mockReturnValue({ events: [evt({ lastSeen: null })], active: true, phase: 'live' });
    render(<EventsTable />);
    // The Last Seen cell falls back to the em dash; no year-0001 date leaks through.
    expect(screen.getByText('—')).toBeInTheDocument();
    expect(screen.queryByText(/0001/)).not.toBeInTheDocument();
  });

  it('renders the live rows through the virtualized, fixed-layout table', () => {
    useClusterCachedDataEventsMock.mockReturnValue({ events: [evt()], active: true, phase: 'live' });
    render(<EventsTable />);
    expect(screen.getByRole('table')).toHaveClass('table-fixed');
  });
});
