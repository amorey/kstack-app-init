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

import { render, screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

// ObjectsTable is pure presentation over useClusterCachedDataObjects — mock the hook to drive
// each of the four states and the column/cell alignment directly.
const { useClusterCachedDataObjectsMock } = vi.hoisted(() => ({
  useClusterCachedDataObjectsMock:
    vi.fn<typeof import('@/lib/cluster-cached-data-objects').useClusterCachedDataObjects>(),
}));
vi.mock('@/lib/cluster-cached-data-objects', () => ({ useClusterCachedDataObjects: useClusterCachedDataObjectsMock }));

const { ObjectsTable } = await import('./objects-table');

const PODS = { apiVersion: 'v1', resource: 'pods', kind: 'Pod', namespaced: true };
// A CRD kind with no registered columns — exercises the universal-columns fallback.
const WIDGETS = { apiVersion: 'example.com/v1', resource: 'widgets', kind: 'Widget', namespaced: true };

function obj(over: Record<string, unknown> = {}) {
  return {
    uid: 'u1',
    apiVersion: 'v1',
    kind: 'Pod',
    namespace: 'default',
    name: 'my-pod',
    creationTimestamp: '2026-07-20T10:00:00Z',
    rawJSON: undefined as unknown,
    ...over,
  };
}

beforeEach(() => vi.clearAllMocks());

describe('ObjectsTable', () => {
  it('explains the unsynced state when there is no active cache', () => {
    useClusterCachedDataObjectsMock.mockReturnValue({ objects: [], active: false, phase: 'connecting' });
    render(<ObjectsTable {...PODS} />);
    expect(screen.getByText(/No synced cache/i)).toBeInTheDocument();
  });

  it('shows a spinner while connecting', () => {
    useClusterCachedDataObjectsMock.mockReturnValue({ objects: [], active: true, phase: 'connecting' });
    render(<ObjectsTable {...PODS} />);
    expect(screen.getByText(/Loading pod/i)).toBeInTheDocument();
  });

  it('shows an empty state on a connected empty snapshot', () => {
    useClusterCachedDataObjectsMock.mockReturnValue({ objects: [], active: true, phase: 'live' });
    render(<ObjectsTable {...PODS} />);
    expect(screen.getByText('No pod objects.')).toBeInTheDocument();
  });

  it('falls back to the universal Name/Namespace/Age columns for a kind with no registry entry', () => {
    useClusterCachedDataObjectsMock.mockReturnValue({
      objects: [obj({ kind: 'Widget' })],
      active: true,
      phase: 'live',
    });
    render(<ObjectsTable {...WIDGETS} />);
    const headers = screen.getAllByRole('columnheader').map((h) => h.textContent);
    expect(headers).toEqual(['Namespace', 'Name', 'Age']);
    expect(screen.getByText('my-pod')).toBeInTheDocument();
    expect(screen.getByText('default')).toBeInTheDocument();
  });

  it("renders a CRD's declared columns between Name and Age", () => {
    useClusterCachedDataObjectsMock.mockReturnValue({
      objects: [obj({ kind: 'Widget', rawJSON: { spec: { replicas: 3 } } })],
      active: true,
      phase: 'live',
    });
    render(
      <ObjectsTable
        {...WIDGETS}
        printerColumns={[{ name: 'Replicas', type: 'integer', jsonPath: '.spec.replicas', priority: 0 }]}
      />,
    );

    expect(screen.getAllByRole('columnheader').map((h) => h.textContent)).toEqual([
      'Namespace',
      'Name',
      'Replicas',
      'Age',
    ]);
    expect(screen.getByText('3')).toBeInTheDocument();
  });

  it('omits the Namespace column for a cluster-scoped kind', () => {
    useClusterCachedDataObjectsMock.mockReturnValue({
      objects: [obj({ namespace: '', name: 'node-1' })],
      active: true,
      phase: 'live',
    });
    render(<ObjectsTable apiVersion="v1" resource="nodes" kind="Node" namespaced={false} />);
    const headers = screen.getAllByRole('columnheader').map((h) => h.textContent);
    expect(headers).toEqual(['Name', 'Age']);
  });

  it('renders Pod-specific columns (Ready/Status/Restarts) computed from the body', () => {
    const pod = obj({
      rawJSON: {
        spec: { containers: [{}, {}] },
        status: {
          phase: 'Running',
          containerStatuses: [
            { ready: true, restartCount: 1 },
            { ready: false, restartCount: 2 },
          ],
        },
      },
    });
    useClusterCachedDataObjectsMock.mockReturnValue({ objects: [pod], active: true, phase: 'live' });
    render(<ObjectsTable {...PODS} />);
    const headers = screen.getAllByRole('columnheader').map((h) => h.textContent);
    expect(headers).toEqual(['Namespace', 'Name', 'Ready', 'Status', 'Restarts', 'Age']);
    const row = screen.getByText('my-pod').closest('tr')!;
    expect(within(row).getByText('1/2')).toBeInTheDocument(); // ready / total containers
    expect(within(row).getByText('Running')).toBeInTheDocument();
    expect(within(row).getByText('3')).toBeInTheDocument(); // summed restarts
  });

  // kubectl derives a pod's Status from container state, not `status.phase` — a crash-looping
  // pod is phase=Running, so trusting the phase would report "Running" for a broken pod.
  it('reports a waiting container reason over the coarse phase', () => {
    const pod = obj({
      rawJSON: {
        spec: { containers: [{}] },
        status: {
          phase: 'Running',
          containerStatuses: [{ ready: false, restartCount: 5, state: { waiting: { reason: 'CrashLoopBackOff' } } }],
        },
      },
    });
    useClusterCachedDataObjectsMock.mockReturnValue({ objects: [pod], active: true, phase: 'live' });
    render(<ObjectsTable {...PODS} />);
    const row = screen.getByText('my-pod').closest('tr')!;
    expect(within(row).getByText('CrashLoopBackOff')).toBeInTheDocument();
    expect(within(row).queryByText('Running')).not.toBeInTheDocument();
  });

  it('reports init-container progress while a pod is initializing', () => {
    const pod = obj({
      rawJSON: {
        spec: { containers: [{}], initContainers: [{ name: 'setup' }, { name: 'migrate' }] },
        status: {
          phase: 'Pending',
          initContainerStatuses: [{ name: 'setup', state: { waiting: { reason: 'PodInitializing' } } }],
        },
      },
    });
    useClusterCachedDataObjectsMock.mockReturnValue({ objects: [pod], active: true, phase: 'live' });
    render(<ObjectsTable {...PODS} />);
    const row = screen.getByText('my-pod').closest('tr')!;
    expect(within(row).getByText('Init:0/2')).toBeInTheDocument();
  });

  it('reports Terminating for a pod with a deletion timestamp', () => {
    const pod = obj({
      rawJSON: {
        metadata: { deletionTimestamp: '2026-07-21T10:00:00Z' },
        spec: { containers: [{}] },
        status: { phase: 'Running', containerStatuses: [{ ready: true, state: { running: {} } }] },
      },
    });
    useClusterCachedDataObjectsMock.mockReturnValue({ objects: [pod], active: true, phase: 'live' });
    render(<ObjectsTable {...PODS} />);
    const row = screen.getByText('my-pod').closest('tr')!;
    expect(within(row).getByText('Terminating')).toBeInTheDocument();
  });

  // Retries during init land in initContainerStatuses while containerStatuses is still empty —
  // reading only the latter would show "—" despite a nonzero restart count.
  it('counts init-container restarts in the Restarts total', () => {
    const pod = obj({
      rawJSON: {
        spec: { containers: [{}], initContainers: [{ name: 'setup' }] },
        status: {
          phase: 'Pending',
          initContainerStatuses: [
            { name: 'setup', restartCount: 3, state: { waiting: { reason: 'CrashLoopBackOff' } } },
          ],
        },
      },
    });
    useClusterCachedDataObjectsMock.mockReturnValue({ objects: [pod], active: true, phase: 'live' });
    render(<ObjectsTable {...PODS} />);
    const row = screen.getByText('my-pod').closest('tr')!;
    expect(within(row).getByText('3')).toBeInTheDocument();
  });

  it('sums init and regular container restarts', () => {
    const pod = obj({
      rawJSON: {
        spec: { containers: [{}], initContainers: [{ name: 'setup' }] },
        status: {
          phase: 'Running',
          initContainerStatuses: [{ name: 'setup', restartCount: 2, state: { terminated: { exitCode: 0 } } }],
          containerStatuses: [{ ready: true, restartCount: 4, state: { running: {} } }],
        },
      },
    });
    useClusterCachedDataObjectsMock.mockReturnValue({ objects: [pod], active: true, phase: 'live' });
    render(<ObjectsTable {...PODS} />);
    const row = screen.getByText('my-pod').closest('tr')!;
    expect(within(row).getByText('6')).toBeInTheDocument();
  });

  it('degrades kind-specific cells to "—" when the body is missing', () => {
    // A registered kind (Pod) still shows its columns, but with no body every cell is "—".
    useClusterCachedDataObjectsMock.mockReturnValue({ objects: [obj()], active: true, phase: 'live' });
    render(<ObjectsTable {...PODS} />);
    const row = screen.getByText('my-pod').closest('tr')!;
    // Ready, Status, Restarts all fall back to "—" (Age has a real timestamp here).
    expect(within(row).getAllByText('—')).toHaveLength(3);
  });

  it('renders "—" for a null creationTimestamp instead of an ancient date', () => {
    useClusterCachedDataObjectsMock.mockReturnValue({
      objects: [obj({ kind: 'Widget', creationTimestamp: null })],
      active: true,
      phase: 'live',
    });
    render(<ObjectsTable {...WIDGETS} />);
    const row = screen.getByText('my-pod').closest('tr')!;
    expect(within(row).getByText('—')).toBeInTheDocument();
    expect(screen.queryByText(/0001/)).not.toBeInTheDocument();
  });
});
