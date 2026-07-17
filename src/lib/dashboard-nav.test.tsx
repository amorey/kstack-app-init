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

import { renderHook } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

// The hook composes urql's `useQuery` with the active-context → cluster/cache join,
// a sync-driven re-execute, and a cache-aware stale-data guard. Mock those seams so
// the test drives the catalog + cache state directly and asserts the reactivity the
// reviewer asked for, without standing up a real GraphQL client.
const { useQueryMock, reexecuteQuery } = vi.hoisted(() => ({ useQueryMock: vi.fn(), reexecuteQuery: vi.fn() }));
const { useClustersMock, useActiveKubeContextMock } = vi.hoisted(() => ({
  useClustersMock: vi.fn(),
  useActiveKubeContextMock: vi.fn(),
}));

vi.mock('urql', () => ({ useQuery: useQueryMock }));
vi.mock('@/lib/clusters', () => ({ useClusters: useClustersMock }));
vi.mock('@/lib/active-kube-context', () => ({ useActiveKubeContext: useActiveKubeContextMock }));

const { useDashboardNav } = await import('./dashboard-nav');

const REPLICASET = {
  apiVersion: 'apps/v1',
  kind: 'ReplicaSet',
  resource: 'replicasets',
  scope: 'Namespaced',
  isCRD: false,
};

// A cluster fixture for context "prod" whose active cache has the given id/serverUid
// and Synced condition — or no active cache when `synced` is null.
function clusterFixture(synced: { status: string; reason: string } | null, cacheId = 'c1', serverUid = 'uid-1') {
  return {
    id: '1',
    spec: { source: { kubeconfig: { context: 'prod' } } },
    activeCache: synced ? { id: cacheId, serverUid, status: { conditions: [{ type: 'Synced', ...synced }] } } : null,
  };
}

// Mock a query result: `clusterDataKinds` plus the cache id the data belongs to (urql
// exposes it as `operation.variables.cacheID`; the hook rejects data whose cache id
// doesn't match the active cache). Defaults to the fixture's cache "c1".
function setQueryResult(clusterDataKinds: unknown[], forCacheID = 'c1') {
  useQueryMock.mockReturnValue([
    { data: { clusterDataKinds }, operation: { variables: { id: '1', cacheID: forCacheID } } },
    reexecuteQuery,
  ]);
}

const hasDiscovered = (nav: { moreChildren?: unknown }[]) => nav.some((n) => n.moreChildren);

beforeEach(() => {
  vi.clearAllMocks();
  useActiveKubeContextMock.mockReturnValue({ context: 'prod' });
  setQueryResult([]);
});

describe('useDashboardNav', () => {
  it('re-executes in place on a Synced transition of the same cache, skipping the first observation', () => {
    // First observation of cache "c1": the variables-driven query fetches it, so no
    // manual refetch on top.
    useClustersMock.mockReturnValue({ clusters: [clusterFixture({ status: 'True', reason: 'Watching' })] });
    const { rerender } = renderHook(() => useDashboardNav());
    expect(reexecuteQuery).not.toHaveBeenCalled();

    // Same cache, Synced transitions (discovery repopulated kind_catalog): refetch once.
    useClustersMock.mockReturnValue({ clusters: [clusterFixture({ status: 'False', reason: 'Syncing' })] });
    rerender();
    expect(reexecuteQuery).toHaveBeenCalledTimes(1);
    expect(reexecuteQuery).toHaveBeenCalledWith({ requestPolicy: 'network-only' });

    // No change → no refetch (no loop).
    rerender();
    expect(reexecuteQuery).toHaveBeenCalledTimes(1);
  });

  it('moves the query key when the active cache changes, without a manual refetch', () => {
    useClustersMock.mockReturnValue({
      clusters: [clusterFixture({ status: 'True', reason: 'Watching' }, 'c1', 'uid-1')],
    });
    const { rerender } = renderHook(() => useDashboardNav());
    expect(useQueryMock.mock.lastCall?.[0].variables).toEqual({ id: '1', cacheID: 'c1' });

    // A cache swap (repoint / UID switch) — a different cache id moves the query key,
    // so urql refetches on its own; no manual reexecute needed.
    useClustersMock.mockReturnValue({
      clusters: [clusterFixture({ status: 'True', reason: 'Watching' }, 'c2', 'uid-2')],
    });
    rerender();
    expect(useQueryMock.mock.lastCall?.[0].variables).toEqual({ id: '1', cacheID: 'c2' });
    expect(reexecuteQuery).not.toHaveBeenCalled();
  });

  it('rejects data whose cache id doesn’t match the active cache, then shows it once it catches up', () => {
    // Active cache is "c2", but urql still holds the previous cache's result ("c1").
    useClustersMock.mockReturnValue({
      clusters: [clusterFixture({ status: 'True', reason: 'Watching' }, 'c2', 'uid-2')],
    });
    setQueryResult([REPLICASET], 'c1');
    const { result, rerender } = renderHook(() => useDashboardNav());
    expect(hasDiscovered(result.current.nav)).toBe(false);

    // The query resolves for the active cache "c2": its kinds appear.
    setQueryResult([REPLICASET], 'c2');
    rerender();
    expect(result.current.nav.find((n) => n.id === 'workloads')?.moreChildren?.map((c) => c.id)).toEqual([
      'apps/replicasets',
    ]);
  });

  it('falls back to the curated-only tree when there is no active cluster or cache', () => {
    setQueryResult([REPLICASET], 'c1'); // stale data urql kept around
    // No cluster matches "prod" (departed/disabled); also covers a cluster with no
    // active cache (cacheID undefined ⇒ query paused).
    useClustersMock.mockReturnValue({ clusters: [] });
    const { result } = renderHook(() => useDashboardNav());
    expect(hasDiscovered(result.current.nav)).toBe(false);
    // The query is paused when there's no cache id.
    expect(useQueryMock.mock.lastCall?.[0].pause).toBe(true);
  });
});
