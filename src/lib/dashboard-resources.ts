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

// The dashboard's resource catalog — the tree the sidebar renders and the
// authority for what the `resource` URL search param may hold (see
// `routes/dashboard.tsx`). `DASHBOARD_NAV` is the **single source of truth**: it
// is `as const`, and the selectable-id union, the default, the id→node lookup,
// and the membership guard are all *derived* from it, so adding a resource kind
// is a one-line data edit with nothing to keep in sync.
//
// Every node is selectable (it has its own view / `resource` value) and may
// carry `children`, so the catalog is an ordered, uniformly-selectable tree —
// e.g. a top-level `workloads` node with `pods`/`daemonsets` beneath it, sitting
// alongside leaf nodes like `nodes` and `namespaces`. Keep it ordered as it
// should read top-to-bottom, and keep every `id` unique across the whole tree:
// `resource` is one flat id namespace, so a duplicate id would make two nodes
// indistinguishable in the URL.
export const DASHBOARD_NAV = [
  { id: 'overview', label: 'Overview' },
  { id: 'nodes', label: 'Nodes' },
  { id: 'namespaces', label: 'Namespaces' },
  { id: 'events', label: 'Events' },
  {
    id: 'workloads',
    label: 'Workloads',
    children: [
      { id: 'pods', label: 'Pods' },
      { id: 'deployments', label: 'Deployments' },
      { id: 'daemonsets', label: 'DaemonSets' },
      { id: 'statefulsets', label: 'StatefulSets' },
      { id: 'jobs', label: 'Jobs' },
      { id: 'cronjobs', label: 'CronJobs' },
    ],
  },
  {
    id: 'config-and-storage',
    label: 'Config & Storage',
    children: [
      { id: 'configmaps', label: 'ConfigMaps' },
      { id: 'secrets', label: 'Secrets' },
      { id: 'persistentvolumeclaims', label: 'PersistentVolumeClaims' },
    ],
  },
  {
    id: 'network',
    label: 'Network',
    children: [
      { id: 'services', label: 'Services' },
      { id: 'ingresses', label: 'Ingresses' },
      { id: 'networkpolicies', label: 'NetworkPolicies' },
    ],
  },
  {
    id: 'security-and-access',
    label: 'Security & Access',
    children: [
      { id: 'serviceaccounts', label: 'ServiceAccounts' },
      { id: 'roles', label: 'Roles' },
      { id: 'rolebindings', label: 'RoleBindings' },
      { id: 'clusterroles', label: 'ClusterRoles' },
      { id: 'clusterrolebindings', label: 'ClusterRoleBindings' },
    ],
  },
  {
    id: 'custom-resources',
    label: 'Custom Resources',
    children: [],
  },
  {
    id: 'system',
    label: 'System',
    children: [],
  },
] as const;

// The union of selectable ids, pulled out of the tree: each node contributes its
// own id and — for groups — the ids of its descendants. Deriving it means the
// union can never drift from the data.
type NodeId<T> =
  | (T extends { id: infer I extends string } ? I : never)
  | (T extends { children: readonly (infer C)[] } ? NodeId<C> : never);
export type DashboardResource = NodeId<(typeof DASHBOARD_NAV)[number]>;

// A node in the tree, general over the derived id union — the shape the sidebar
// renders and `flattenNav` walks. `DASHBOARD_NAV` (its narrower `as const` type)
// is assignable to a `readonly DashboardNavNode[]`.
export type DashboardNavNode = {
  readonly id: DashboardResource;
  readonly label: string;
  readonly children?: readonly DashboardNavNode[];
};

// Depth-first, parent-before-children, preserving display order — the flat view
// the identity-based helpers below index. Exported so the traversal is testable
// against an arbitrary tree, not just the live catalog.
export function flattenNav(nodes: readonly DashboardNavNode[]): DashboardNavNode[] {
  return nodes.flatMap((node) => [node, ...(node.children ? flattenNav(node.children) : [])]);
}

// Flatten once and derive both the id→node index and the default from it.
const FLAT_NAV = flattenNav(DASHBOARD_NAV);
const NODES_BY_ID = new Map<string, DashboardNavNode>(FLAT_NAV.map((node) => [node.id, node]));

// The kind shown when the URL names none (or an unknown one): the first node in
// reading order, so the sidebar's default highlight and the panel's default view
// agree.
export const DEFAULT_DASHBOARD_RESOURCE: DashboardResource = FLAT_NAV[0].id;

export function isDashboardResource(value: unknown): value is DashboardResource {
  return typeof value === 'string' && NODES_BY_ID.has(value);
}

// Fold the optional `resource` URL param into a concrete kind: `validateSearch`
// leaves it absent when the URL names none, and this is the one place that rule
// (absent ⇒ default) lives, so the sidebar highlight and the panel can't drift.
export function resolveDashboardResource(resource: DashboardResource | undefined): DashboardResource {
  return resource ?? DEFAULT_DASHBOARD_RESOURCE;
}

// The human label for a resource id (group or leaf), for the panel heading.
export function dashboardResourceLabel(resource: DashboardResource): string {
  return NODES_BY_ID.get(resource)?.label ?? resource;
}
