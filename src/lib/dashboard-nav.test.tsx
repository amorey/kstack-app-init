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

// The hook composes urql's `useSubscription` (a delta watch reduced into a keyed
// catalog) with the active-context → cluster/cache join and a cache-aware guard. Mock
// those seams so the test drives the delta stream + cache state directly and asserts
// the nav updates live, without standing up a real GraphQL client. The mock stands in
// for urql's accumulator: it captures the reducer and returns the accumulated data, so
// `pushFrame` folds a delta through the real reducer just as urql would.
const { useSubscriptionMock } = vi.hoisted(() => ({ useSubscriptionMock: vi.fn() }));
const { useClustersMock, useActiveKubeContextMock } = vi.hoisted(() => ({
  useClustersMock: vi.fn(),
  useActiveKubeContextMock: vi.fn(),
}));

vi.mock('urql', () => ({ useSubscription: useSubscriptionMock }));
// Mock the provider hook but keep the real `applyChange` reducer helper (a pure map
// patch the hook reuses) so the delta accumulation under test runs unaltered.
vi.mock('@/lib/clusters', () => ({
  useClusters: useClustersMock,
  applyChange: <T,>(prev: ReadonlyMap<string, T> | undefined, type: string, id: string, entity: T) => {
    const next = new Map(prev);
    if (type === 'Deleted') next.delete(id);
    else next.set(id, entity);
    return next;
  },
}));
vi.mock('@/lib/active-kube-context', () => ({ useActiveKubeContext: useActiveKubeContextMock }));

const { useDashboardNav } = await import('./dashboard-nav');

const REPLICASET = {
  apiVersion: 'apps/v1',
  kind: 'ReplicaSet',
  resource: 'replicasets',
  scope: 'Namespaced',
  isCRD: false,
  count: 7,
};

// A non-curated workloads kind (so it lands in `moreChildren`, unlike the curated
// `jobs`/`deployments`), used to exercise Added/Deleted of a second discovered kind.
const CONTROLLER_REVISION = {
  apiVersion: 'apps/v1',
  kind: 'ControllerRevision',
  resource: 'controllerrevisions',
  scope: 'Namespaced',
  isCRD: false,
  count: 2,
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

const hasDiscovered = (nav: { moreChildren?: unknown }[]) => nav.some((n) => n.moreChildren);
const workloadsExtra = (nav: { id: string; moreChildren?: readonly { id: string; count?: number }[] }[]) =>
  nav.find((n) => n.id === 'workloads')?.moreChildren ?? [];

// urql accumulator stand-in. `acc` is the reduced data the mock returns; `pushFrame`
// folds a delta through the reducer captured on the last render, exactly as urql's live
// handler would. Each frame carries its own cache id (its provenance) — defaulting to
// the currently-subscribed cache, but overridable to model a late frame from a
// superseded subscription.
let acc: unknown;
let lastArgs: { variables?: { cacheID?: string }; pause?: boolean } | undefined;
let lastReducer: ((prev: unknown, res: unknown) => unknown) | undefined;

function pushFrame(type: string, kind: unknown, cacheID = lastArgs?.variables?.cacheID) {
  acc = lastReducer!(acc, { clusterDataKindsWatch: { type, cacheID, kind } });
}

beforeEach(() => {
  vi.clearAllMocks();
  acc = undefined;
  lastArgs = undefined;
  lastReducer = undefined;
  useActiveKubeContextMock.mockReturnValue({ context: 'prod' });
  useSubscriptionMock.mockImplementation((args: typeof lastArgs, reducer: typeof lastReducer) => {
    lastArgs = args;
    lastReducer = reducer;
    return [{ data: acc }];
  });
});

describe('useDashboardNav', () => {
  it('builds discovered kinds with live counts from the snapshot delta burst', () => {
    useClustersMock.mockReturnValue({ clusters: [clusterFixture({ status: 'True', reason: 'Watching' })] });
    const { result, rerender } = renderHook(() => useDashboardNav());

    // Before any frame: curated-only.
    expect(hasDiscovered(result.current.nav)).toBe(false);

    // The on-subscribe snapshot arrives as an Added change; the kind + its count appear.
    pushFrame('Added', REPLICASET);
    rerender();
    const extra = workloadsExtra(result.current.nav);
    expect(extra.map((c) => c.id)).toEqual(['apps/replicasets']);
    expect(extra[0].count).toBe(7);
  });

  it('updates a kind’s count live on a Modified frame', () => {
    useClustersMock.mockReturnValue({ clusters: [clusterFixture({ status: 'True', reason: 'Watching' })] });
    const { result, rerender } = renderHook(() => useDashboardNav());

    pushFrame('Added', REPLICASET);
    rerender();
    expect(workloadsExtra(result.current.nav)[0].count).toBe(7);

    // A later object write bumps the count → Modified re-emits the same kind.
    pushFrame('Modified', { ...REPLICASET, count: 12 });
    rerender();
    expect(workloadsExtra(result.current.nav)[0].count).toBe(12);
  });

  it('reveals a newly-discovered kind on an Added frame and removes one on Deleted', () => {
    useClustersMock.mockReturnValue({ clusters: [clusterFixture({ status: 'True', reason: 'Watching' })] });
    const { result, rerender } = renderHook(() => useDashboardNav());

    pushFrame('Added', REPLICASET);
    pushFrame('Added', CONTROLLER_REVISION);
    rerender();
    expect(
      workloadsExtra(result.current.nav)
        .map((c) => c.id)
        .sort(),
    ).toEqual(['apps/controllerrevisions', 'apps/replicasets']);

    // A kind leaving the catalog is dropped.
    pushFrame('Deleted', REPLICASET);
    rerender();
    expect(workloadsExtra(result.current.nav).map((c) => c.id)).toEqual(['apps/controllerrevisions']);
  });

  it('moves the subscription key on a cache swap and drops the old cache’s kinds until the new cache streams', () => {
    useClustersMock.mockReturnValue({
      clusters: [clusterFixture({ status: 'True', reason: 'Watching' }, 'c1', 'uid-1')],
    });
    const { result, rerender } = renderHook(() => useDashboardNav());
    expect(lastArgs?.variables?.cacheID).toBe('c1');

    pushFrame('Added', REPLICASET);
    rerender();
    expect(hasDiscovered(result.current.nav)).toBe(true);

    // A cache swap (repoint / UID switch) moves the subscription key. urql retains the
    // prior cache's accumulated data until the new cache's first frame, so the
    // cache-aware guard must reject it → curated-only in the meantime.
    useClustersMock.mockReturnValue({
      clusters: [clusterFixture({ status: 'True', reason: 'Watching' }, 'c2', 'uid-2')],
    });
    rerender();
    expect(lastArgs?.variables?.cacheID).toBe('c2');
    expect(hasDiscovered(result.current.nav)).toBe(false);

    // Once the new cache streams, its kinds appear (and the old cache's are gone).
    pushFrame('Added', CONTROLLER_REVISION);
    rerender();
    expect(workloadsExtra(result.current.nav).map((c) => c.id)).toEqual(['apps/controllerrevisions']);
  });

  it('rejects a late frame from a superseded cache instead of mis-tagging it as the active one', () => {
    useClustersMock.mockReturnValue({
      clusters: [clusterFixture({ status: 'True', reason: 'Watching' }, 'c1', 'uid-1')],
    });
    const { result, rerender } = renderHook(() => useDashboardNav());
    expect(lastArgs?.variables?.cacheID).toBe('c1');

    pushFrame('Added', REPLICASET);
    rerender();
    expect(hasDiscovered(result.current.nav)).toBe(true);

    // Swap to c2. urql keeps the c1 subscription alive until effect cleanup, so a frame
    // from c1 can still arrive while the render already targets c2. It carries its own
    // (c1) provenance, so it must NOT be attributed to c2 — even though c2 has streamed
    // nothing yet, the nav stays curated-only rather than showing c1's kind.
    useClustersMock.mockReturnValue({
      clusters: [clusterFixture({ status: 'True', reason: 'Watching' }, 'c2', 'uid-2')],
    });
    rerender();
    expect(lastArgs?.variables?.cacheID).toBe('c2');

    pushFrame('Added', CONTROLLER_REVISION, 'c1'); // a straggler from the old subscription
    rerender();
    expect(hasDiscovered(result.current.nav)).toBe(false);
  });

  it('preserves the active cache’s catalog when a late old-cache straggler arrives after the new cache has streamed', () => {
    useClustersMock.mockReturnValue({
      clusters: [clusterFixture({ status: 'True', reason: 'Watching' }, 'c1', 'uid-1')],
    });
    const { result, rerender } = renderHook(() => useDashboardNav());
    pushFrame('Added', REPLICASET); // c1's snapshot
    rerender();

    // Swap to c2 and let its snapshot stream two kinds.
    useClustersMock.mockReturnValue({
      clusters: [clusterFixture({ status: 'True', reason: 'Watching' }, 'c2', 'uid-2')],
    });
    rerender();
    pushFrame('Added', REPLICASET, 'c2');
    pushFrame('Added', CONTROLLER_REVISION, 'c2');
    rerender();
    expect(
      workloadsExtra(result.current.nav)
        .map((c) => c.id)
        .sort(),
    ).toEqual(['apps/controllerrevisions', 'apps/replicasets']);

    // A late straggler from the superseded c1 subscription must NOT wipe c2's catalog:
    // it's dropped, and c2's fully-accumulated kinds stay put.
    pushFrame('Added', { ...REPLICASET, resource: 'stragglers', apiVersion: 'apps/v1' }, 'c1');
    rerender();
    expect(
      workloadsExtra(result.current.nav)
        .map((c) => c.id)
        .sort(),
    ).toEqual(['apps/controllerrevisions', 'apps/replicasets']);

    // A subsequent legitimate c2 delta still patches the intact catalog (it isn't reset
    // to a singleton by the straggler), so the count updates live.
    pushFrame('Modified', { ...REPLICASET, count: 99 }, 'c2');
    rerender();
    expect(workloadsExtra(result.current.nav).find((c) => c.id === 'apps/replicasets')?.count).toBe(99);
  });

  it('pauses the subscription and falls back to curated-only when there is no active cluster or cache', () => {
    // No cluster matches "prod" (departed/disabled); also covers a cluster with no
    // active cache (cacheID undefined ⇒ subscription paused).
    useClustersMock.mockReturnValue({ clusters: [] });
    const { result } = renderHook(() => useDashboardNav());
    expect(hasDiscovered(result.current.nav)).toBe(false);
    expect(lastArgs?.pause).toBe(true);
  });
});
