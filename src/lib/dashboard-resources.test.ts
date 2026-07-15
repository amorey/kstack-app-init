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
  DASHBOARD_NAV,
  dashboardResourceLabel,
  DEFAULT_DASHBOARD_RESOURCE,
  flattenNav,
  isDashboardResource,
} from './dashboard-resources';
import type { DashboardNavNode } from './dashboard-resources';

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

describe('flattenNav', () => {
  it('walks depth-first, parent before children, preserving order', () => {
    expect(flattenNav(SAMPLE).map((n) => n.id)).toEqual(['nodes', 'workloads', 'pods', 'daemonsets']);
  });
});

describe('isDashboardResource', () => {
  it('accepts every id in the tree, group and leaf alike', () => {
    flattenNav(DASHBOARD_NAV).forEach((node) => {
      expect(isDashboardResource(node.id)).toBe(true);
    });
  });

  it('rejects unknown values and non-strings', () => {
    expect(isDashboardResource('services')).toBe(false);
    expect(isDashboardResource(undefined)).toBe(false);
    expect(isDashboardResource(42)).toBe(false);
  });
});

describe('DEFAULT_DASHBOARD_RESOURCE', () => {
  it('is the first node in reading order', () => {
    expect(DEFAULT_DASHBOARD_RESOURCE).toBe(flattenNav(DASHBOARD_NAV)[0].id);
  });
});

describe('dashboardResourceLabel', () => {
  it('resolves the label for a nested child id', () => {
    expect(dashboardResourceLabel('pods')).toBe('Pods');
  });
});
