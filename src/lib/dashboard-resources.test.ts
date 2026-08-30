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

import { describe, expect, it } from 'vitest';

import {
  apiGroupOf,
  buildDashboardNav,
  DASHBOARD_NAV,
  dashboardResourceLabel,
  DEFAULT_DASHBOARD_RESOURCE,
  flattenNav,
  resolveDashboardResource,
} from './dashboard-resources';
import type { DashboardNavNode, ServerKind } from './dashboard-resources';

// A stand-in tree so the traversal is exercised independently of the live
// catalog's current contents.
const SAMPLE: readonly DashboardNavNode[] = [
  { id: 'nodes', label: 'Nodes' },
  {
    id: 'workloads',
    label: 'Workloads',
    children: [
      { id: 'pods', label: 'Pods' },
      { id: 'daemonsets', label: 'DaemonSets' },
    ],
  },
];

const kind = (apiVersion: string, k: string, resource: string, count = 0): ServerKind => ({
  apiVersion,
  kind: k,
  resource,
  scope: 'Namespaced',
  isCRD: false,
  count,
  printerColumns: [],
});

// A group node's discovered ("Show more…") kinds in a built tree.
function moreOf(nav: DashboardNavNode[], groupId: string): readonly DashboardNavNode[] | undefined {
  return nav.find((n) => n.id === groupId)?.moreChildren;
}

describe('flattenNav', () => {
  it('walks depth-first, parent before children, preserving order', () => {
    expect(flattenNav(SAMPLE).map((n) => n.id)).toEqual(['nodes', 'workloads', 'pods', 'daemonsets']);
  });
});

describe('apiGroupOf', () => {
  it('drops the version, and the core group is empty', () => {
    expect(apiGroupOf('apps/v1')).toBe('apps');
    expect(apiGroupOf('networking.k8s.io/v1')).toBe('networking.k8s.io');
    expect(apiGroupOf('v1')).toBe('');
  });
});

describe('DEFAULT_DASHBOARD_RESOURCE', () => {
  it('is the first node in reading order', () => {
    expect(DEFAULT_DASHBOARD_RESOURCE).toBe(flattenNav(DASHBOARD_NAV)[0].id);
  });
});

describe('resolveDashboardResource', () => {
  it('falls back to the default when absent or empty', () => {
    expect(resolveDashboardResource(undefined)).toBe(DEFAULT_DASHBOARD_RESOURCE);
    expect(resolveDashboardResource('')).toBe(DEFAULT_DASHBOARD_RESOURCE);
  });

  it('passes a present value through, including dynamic ids', () => {
    expect(resolveDashboardResource('pods')).toBe('pods');
    expect(resolveDashboardResource('apps/replicasets')).toBe('apps/replicasets');
  });
});

describe('dashboardResourceLabel', () => {
  it('resolves a curated label, and falls back to the id for unknown', () => {
    expect(dashboardResourceLabel(DASHBOARD_NAV, 'pods')).toBe('Pods');
    expect(dashboardResourceLabel(DASHBOARD_NAV, 'apps/replicasets')).toBe('apps/replicasets');
  });
});

describe('buildDashboardNav', () => {
  it('hangs a non-curated kind off its group as a moreChild, not nested deeper', () => {
    const nav = buildDashboardNav([kind('apps/v1', 'ReplicaSet', 'replicasets')]);
    const workloads = nav.find((n) => n.id === 'workloads');
    // The kind is a peer of the curated children (in moreChildren), not a nested
    // Other node, and the curated children are left intact.
    expect(workloads?.children?.map((c) => c.id)).toContain('pods');
    expect(workloads?.moreChildren?.map((c) => c.id)).toEqual(['apps/replicasets']);
    expect(workloads?.moreChildren?.[0].label).toBe('ReplicaSet');
  });

  it('routes api groups with curated children to their group moreChildren', () => {
    const nav = buildDashboardNav([
      kind('policy/v1', 'PodDisruptionBudget', 'poddisruptionbudgets'), // → workloads
      kind('networking.k8s.io/v1', 'IngressClass', 'ingressclasses'), // → network
      kind('storage.k8s.io/v1', 'StorageClass', 'storageclasses'), // → config-and-storage
      kind('rbac.authorization.k8s.io/v1', 'ClusterRole', 'somerbac'), // → security-and-access
    ]);
    expect(moreOf(nav, 'workloads')?.map((c) => c.id)).toEqual(['policy/poddisruptionbudgets']);
    expect(moreOf(nav, 'network')?.map((c) => c.id)).toEqual(['networking.k8s.io/ingressclasses']);
    expect(moreOf(nav, 'config-and-storage')?.map((c) => c.id)).toEqual(['storage.k8s.io/storageclasses']);
    expect(moreOf(nav, 'security-and-access')?.map((c) => c.id)).toEqual(['rbac.authorization.k8s.io/somerbac']);
  });

  it('hangs a childless group (System)’s kinds off moreChildren, leaving its curated children empty', () => {
    const nav = buildDashboardNav([kind('coordination.k8s.io/v1', 'Lease', 'leases')]);
    const system = nav.find((n) => n.id === 'system');
    // Discovered kinds have one home (moreChildren) whether or not the group has
    // curated children; System's stay empty. The renderer derives the parent-level
    // chevron style from that shape (no stored flag).
    expect(system?.children).toEqual([]);
    expect(system?.moreChildren?.map((c) => c.id)).toEqual(['coordination.k8s.io/leases']);
  });

  it('buckets DRA (resource.k8s.io) kinds into System', () => {
    const nav = buildDashboardNav([kind('resource.k8s.io/v1', 'DeviceClass', 'deviceclasses')]);
    const system = nav.find((n) => n.id === 'system');
    expect(system?.moreChildren?.map((c) => c.id)).toEqual(['resource.k8s.io/deviceclasses']);
  });

  it('drops kinds already in the curated list', () => {
    const nav = buildDashboardNav([kind('apps/v1', 'Deployment', 'deployments')]);
    expect(moreOf(nav, 'workloads')).toBeUndefined();
  });

  it('ignores unmapped api groups, including the core group', () => {
    const nav = buildDashboardNav([
      kind('v1', 'ResourceQuota', 'resourcequotas'), // core → deferred
      kind('example.com/v1', 'Widget', 'widgets'), // unmapped → deferred (custom resources)
    ]);
    // No group gains discovered kinds.
    ['workloads', 'network', 'config-and-storage', 'security-and-access', 'system'].forEach((group) => {
      expect(moreOf(nav, group)).toBeUndefined();
    });
  });

  it('joins object counts onto both curated and discovered leaf kinds', () => {
    const nav = buildDashboardNav([
      kind('v1', 'Pod', 'pods', 42), // curated leaf, core group
      kind('apps/v1', 'ReplicaSet', 'replicasets', 7), // discovered kind
    ]);
    const workloads = nav.find((n) => n.id === 'workloads');
    expect(workloads?.children?.find((c) => c.id === 'pods')?.count).toBe(42);
    expect(workloads?.moreChildren?.find((c) => c.id === 'apps/replicasets')?.count).toBe(7);
    // Group and overview rows map to no kind, so they carry no count.
    expect(workloads?.count).toBeUndefined();
    expect(nav.find((n) => n.id === 'overview')?.count).toBeUndefined();
  });

  it('does not let a CRD reusing a built-in plural in another group steal its count', () => {
    const nav = buildDashboardNav([
      kind('v1', 'Pod', 'pods', 42), // the real built-in Pods
      kind('example.com/v1', 'Pod', 'pods', 999), // a CRD that happens to be plural "pods"
    ]);
    const workloads = nav.find((n) => n.id === 'workloads');
    // The core built-in keeps its own count; the CRD (unmapped group) neither
    // overwrites it nor is treated as already-curated.
    expect(workloads?.children?.find((c) => c.id === 'pods')?.count).toBe(42);
  });

  it('does not join a discovered kind whose plural equals a group row id to that group', () => {
    const nav = buildDashboardNav([kind('apps/v1', 'Workload', 'workloads', 5)]);
    const workloads = nav.find((n) => n.id === 'workloads');
    // The group row never takes a count, and the kind is bucketed under its own id.
    expect(workloads?.count).toBeUndefined();
    expect(workloads?.moreChildren?.map((c) => c.id)).toEqual(['apps/workloads']);
    expect(workloads?.moreChildren?.find((c) => c.id === 'apps/workloads')?.count).toBe(5);
  });

  it('sorts moreChildren by label and leaves a curated-only build untouched', () => {
    const nav = buildDashboardNav([
      kind('batch/v1', 'Job', 'zzz-not-curated'), // label "Job"
      kind('autoscaling/v2', 'HorizontalPodAutoscaler', 'horizontalpodautoscalers'),
    ]);
    // Labels: "HorizontalPodAutoscaler" < "Job" alphabetically.
    expect(moreOf(nav, 'workloads')?.map((c) => c.label)).toEqual(['HorizontalPodAutoscaler', 'Job']);
    // A curated-only build adds no discovered kinds anywhere.
    const plain = buildDashboardNav([]);
    expect(plain.some((n) => n.moreChildren)).toBe(false);
  });
});
