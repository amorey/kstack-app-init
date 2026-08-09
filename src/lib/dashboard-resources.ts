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

// The dashboard's resource catalog: curated base (`DASHBOARD_NAV`) plus the runtime
// merge of discovered kinds (`buildDashboardNav`). Unmapped api groups (including
// core) are left for Custom Resources. See docs/adr/2026-08-09-dashboard-nav-merge.md
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

// A curated node id or a dynamic kind's id (`<group>/<resource>`). Open string —
// dynamic kinds are data-driven.
export type DashboardResource = string;

// Discovered kinds always live in `moreChildren` (same depth as `children`); the
// disclosure style is derived from the shape, not stored — curated `children`
// present ⇒ "Show more…" toggle, none ⇒ parent-level chevron. `count` is set only
// on leaf kind nodes, so the renderer badges only where a real count exists.
export type DashboardNavNode = {
  readonly id: DashboardResource;
  readonly label: string;
  readonly count?: number;
  readonly children?: readonly DashboardNavNode[];
  readonly moreChildren?: readonly DashboardNavNode[];
};

// A `clusterDataKinds` row. Declared here, not imported from `@/gql`, so this
// module stays pure and testable.
export type ServerKind = {
  apiVersion: string;
  kind: string;
  resource: string;
  scope: string;
  isCRD: boolean;
  count: number;
};

// Groups that can receive discovered kinds. `Extract<…>` ties them to
// `DASHBOARD_NAV`: renaming a group there fails the `API_GROUP_TO_GROUP` value
// check at build time instead of silently dropping kinds.
type TopLevelId = (typeof DASHBOARD_NAV)[number]['id'];
export type DashboardGroupId = Extract<
  TopLevelId,
  'workloads' | 'network' | 'config-and-storage' | 'security-and-access' | 'system'
>;

// api group → curated group for its non-curated kinds. Unlisted groups (including
// core "") are deferred to Custom Resources.
const API_GROUP_TO_GROUP: Record<string, DashboardGroupId> = {
  apps: 'workloads',
  batch: 'workloads',
  autoscaling: 'workloads',
  policy: 'workloads',
  'networking.k8s.io': 'network',
  'discovery.k8s.io': 'network',
  'gateway.networking.k8s.io': 'network',
  'storage.k8s.io': 'config-and-storage',
  'rbac.authorization.k8s.io': 'security-and-access',
  'certificates.k8s.io': 'security-and-access',
  'admissionregistration.k8s.io': 'security-and-access',
  'coordination.k8s.io': 'system',
  'scheduling.k8s.io': 'system',
  'node.k8s.io': 'system',
  'flowcontrol.apiserver.k8s.io': 'system',
  'resource.k8s.io': 'system',
};

// The api group of a "group/version" (or "" for the core group's bare "v1").
export function apiGroupOf(apiVersion: string): string {
  const slash = apiVersion.indexOf('/');
  return slash === -1 ? '' : apiVersion.slice(0, slash);
}

// Depth-first, parent-before-children. Walks `moreChildren` too, so a kind behind
// "Show more…" still resolves.
export function flattenNav(nodes: readonly DashboardNavNode[]): DashboardNavNode[] {
  return nodes.flatMap((node) => [
    node,
    ...(node.children ? flattenNav(node.children) : []),
    ...(node.moreChildren ? flattenNav(node.moreChildren) : []),
  ]);
}

// Depth-first search by id (walks `moreChildren` too), short-circuiting at the first match.
export function findNode(nodes: readonly DashboardNavNode[], id: DashboardResource): DashboardNavNode | undefined {
  let match: DashboardNavNode | undefined;
  nodes.some((node) => {
    match = node.id === id ? node : (findNode(node.children ?? [], id) ?? findNode(node.moreChildren ?? [], id));
    return match !== undefined;
  });
  return match;
}

// Each curated leaf's api group, keyed by resource plural (node id). A discovered
// kind joins only when BOTH api group and plural match, so a CRD reusing a
// built-in's plural in another group can't hijack its row or count. Core group is "".
const CURATED_LEAF_API_GROUP: Record<string, string> = {
  nodes: '',
  namespaces: '',
  events: '',
  pods: '',
  deployments: 'apps',
  daemonsets: 'apps',
  statefulsets: 'apps',
  jobs: 'batch',
  cronjobs: 'batch',
  configmaps: '',
  secrets: '',
  persistentvolumeclaims: '',
  services: '',
  ingresses: 'networking.k8s.io',
  networkpolicies: 'networking.k8s.io',
  serviceaccounts: '',
  roles: 'rbac.authorization.k8s.io',
  rolebindings: 'rbac.authorization.k8s.io',
  clusterroles: 'rbac.authorization.k8s.io',
  clusterrolebindings: 'rbac.authorization.k8s.io',
};

// Shown when the URL names no resource.
export const DEFAULT_DASHBOARD_RESOURCE: DashboardResource = DASHBOARD_NAV[0].id;

// Absent ⇒ default; present passes through untouched (may be a dynamic kind not
// yet in the tree).
export function resolveDashboardResource(resource: DashboardResource | undefined): DashboardResource {
  return resource && resource.length > 0 ? resource : DEFAULT_DASHBOARD_RESOURCE;
}

// The curated leaf id a discovered kind is, or undefined. Matches on api group AND
// plural (see CURATED_LEAF_API_GROUP).
function curatedLeafIdForKind(k: ServerKind): string | undefined {
  const group = CURATED_LEAF_API_GROUP[k.resource];
  return group !== undefined && group === apiGroupOf(k.apiVersion) ? k.resource : undefined;
}

// A kind's nav node id: curated built-ins by bare plural ("pods"), others by
// `group/resource`. The single home for this rule — bucketing and the count join
// both key by it.
function navIdForKind(k: ServerKind): string {
  return curatedLeafIdForKind(k) ?? `${apiGroupOf(k.apiVersion)}/${k.resource}`;
}

// Merge discovered kinds into the curated base: non-curated mapped kinds hang off
// their group's `moreChildren` (label-sorted); every kind's `count` joins onto its
// leaf node via `withCount`, keyed by `navIdForKind` — curated leaves included.
export function buildDashboardNav(serverKinds: readonly ServerKind[]): DashboardNavNode[] {
  const buckets = new Map<string, DashboardNavNode[]>();
  const countById = new Map<string, number>();
  const seen = new Set<string>();
  serverKinds.forEach((k) => {
    const id = navIdForKind(k);
    countById.set(id, k.count);
    const group = API_GROUP_TO_GROUP[apiGroupOf(k.apiVersion)];
    if (!group) return; // unmapped api group (incl. core): deferred to Custom Resources
    if (curatedLeafIdForKind(k) !== undefined) return; // already curated — don't duplicate
    if (seen.has(id)) return; // guard against duplicate discovery rows
    seen.add(id);
    const list = buckets.get(group) ?? [];
    list.push({ id, label: k.kind });
    buckets.set(group, list);
  });
  buckets.forEach((list) => list.sort((a, b) => a.label.localeCompare(b.label)));

  const withCount = (node: DashboardNavNode): DashboardNavNode => {
    const count = countById.get(node.id);
    return {
      ...node,
      ...(count !== undefined ? { count } : {}),
      ...(node.children ? { children: node.children.map(withCount) } : {}),
      ...(node.moreChildren ? { moreChildren: node.moreChildren.map(withCount) } : {}),
    };
  };

  return DASHBOARD_NAV.map((node): DashboardNavNode => {
    const bucket = buckets.get(node.id); // never empty: a bucket exists only once a kind is pushed
    return bucket ? { ...node, moreChildren: bucket } : node;
  }).map(withCount);
}

// Resolves a selected nav id back to its full kind row via the same `navIdForKind`
// rule the tree is built with (a curated id omits its apiVersion). Undefined for
// group/overview rows or an unloaded catalog.
export function serverKindForResource(kinds: readonly ServerKind[], id: DashboardResource): ServerKind | undefined {
  return kinds.find((k) => navIdForKind(k) === id);
}

// Label for an id, resolved against a built tree (so dynamic kinds resolve too);
// falls back to the raw id (unloaded catalog, stale deep link).
export function dashboardResourceLabel(nav: readonly DashboardNavNode[], resource: DashboardResource): string {
  return findNode(nav, resource)?.label ?? resource;
}
