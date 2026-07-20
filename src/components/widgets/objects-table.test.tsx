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

import { render, screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

// ObjectsTable is pure presentation over useClusterDataObjects — mock the hook to drive
// each of the four states and the column/cell alignment directly.
const { useClusterDataObjectsMock } = vi.hoisted(() => ({ useClusterDataObjectsMock: vi.fn() }));
vi.mock('@/lib/cluster-data-objects', () => ({ useClusterDataObjects: useClusterDataObjectsMock }));

const { ObjectsTable } = await import('./objects-table');

const PODS = { apiVersion: 'v1', resource: 'pods', kind: 'Pod', namespaced: true };

function obj(over: Record<string, unknown> = {}) {
  return {
    uid: 'u1',
    apiVersion: 'v1',
    kind: 'Pod',
    namespace: 'default',
    name: 'my-pod',
    creationTimestamp: '2026-07-20T10:00:00Z',
    ...over,
  };
}

beforeEach(() => vi.clearAllMocks());

describe('ObjectsTable', () => {
  it('explains the unsynced state when there is no active cache', () => {
    useClusterDataObjectsMock.mockReturnValue({ objects: [], active: false, phase: 'connecting' });
    render(<ObjectsTable {...PODS} />);
    expect(screen.getByText(/No synced cache/i)).toBeInTheDocument();
  });

  it('shows a spinner while connecting', () => {
    useClusterDataObjectsMock.mockReturnValue({ objects: [], active: true, phase: 'connecting' });
    render(<ObjectsTable {...PODS} />);
    expect(screen.getByText(/Loading pod/i)).toBeInTheDocument();
  });

  it('shows an empty state on a connected empty snapshot', () => {
    useClusterDataObjectsMock.mockReturnValue({ objects: [], active: true, phase: 'empty' });
    render(<ObjectsTable {...PODS} />);
    expect(screen.getByText('No pod objects.')).toBeInTheDocument();
  });

  it('renders the universal Name/Namespace/Age columns', () => {
    useClusterDataObjectsMock.mockReturnValue({ objects: [obj()], active: true, phase: 'live' });
    render(<ObjectsTable {...PODS} />);
    const headers = screen.getAllByRole('columnheader').map((h) => h.textContent);
    expect(headers).toEqual(['Namespace', 'Name', 'Age']);
    expect(screen.getByText('my-pod')).toBeInTheDocument();
    expect(screen.getByText('default')).toBeInTheDocument();
  });

  it('omits the Namespace column for a cluster-scoped kind', () => {
    useClusterDataObjectsMock.mockReturnValue({
      objects: [obj({ namespace: '', name: 'node-1' })],
      active: true,
      phase: 'live',
    });
    render(<ObjectsTable apiVersion="v1" resource="nodes" kind="Node" namespaced={false} />);
    const headers = screen.getAllByRole('columnheader').map((h) => h.textContent);
    expect(headers).toEqual(['Name', 'Age']);
  });

  it('renders "—" for a null creationTimestamp instead of an ancient date', () => {
    useClusterDataObjectsMock.mockReturnValue({
      objects: [obj({ creationTimestamp: null })],
      active: true,
      phase: 'live',
    });
    render(<ObjectsTable {...PODS} />);
    const row = screen.getByText('my-pod').closest('tr')!;
    expect(within(row).getByText('—')).toBeInTheDocument();
    expect(screen.queryByText(/0001/)).not.toBeInTheDocument();
  });
});
