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

// The dashboard's resource catalog. `DASHBOARD_NAV` is the curated base — the
// hand-picked kinds that always appear, in reading order. The rendered tree augments it
// at runtime with the cluster's remaining kinds (from `clusterDataKinds` discovery),
// bucketed into a group by api group (`API_GROUP_TO_GROUP`) and revealed behind a
// "Show more…" toggle. `buildDashboardNav` does the merge; kinds whose api group isn't
// mapped (including the core group) are left for Custom Resources.
//
// The `resource` URL search param can therefore hold a dynamic kind's id not in the
// static tree, so `DashboardResource` is a plain string, validated leniently.
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

// A selectable id: a curated node id or a dynamic kind's id (`<group>/<resource>`,
// e.g. "apps/replicasets"). Open string because dynamic kinds are data-driven.
export type DashboardResource = string;

// A node in the rendered tree. Discovered kinds always live in `moreChildren`, so the
// disclosure style is derived from the shape rather than stored: a group with curated
// `children` hides its `moreChildren` behind a "Show more…" toggle; a childless group
// (e.g. System) hides them behind a parent-level chevron. Either way `moreChildren` sit
// at the same depth as `children`.
//
// `count` (objects of this kind in the active cache, from `ServerKind.count`) is set
// only on a leaf kind node — undefined on group/overview rows — so the renderer badges
// only where a real count exists.
export type DashboardNavNode = {
  readonly id: DashboardResource;
  readonly label: string;
  readonly count?: number;
  readonly children?: readonly DashboardNavNode[];
  readonly moreChildren?: readonly DashboardNavNode[];
};

// One kind advertised by a cluster's API server (a `clusterDataKinds` row). Declared
// here, not imported from `@/gql`, so this module stays pure and testable.
export type ServerKind = {
  apiVersion: string;
  kind: string;
  resource: string;
  scope: string;
  isCRD: boolean;
  count: number;
};

// The curated groups that can receive discovered kinds. `Extract<…>` of the tree's
// top-level ids ties it to `DASHBOARD_NAV`: rename or remove a group there and it drops
// out of this union, failing the `API_GROUP_TO_GROUP` value check — a build error
// instead of silently dropped kinds.
type TopLevelId = (typeof DASHBOARD_NAV)[number]['id'];
export type DashboardGroupId = Extract<
  TopLevelId,
  'workloads' | 'network' | 'config-and-storage' | 'security-and-access' | 'system'
>;

// api group → the curated group its non-curated kinds fall under. Unlisted api groups
// (including the core group "") are deferred to Custom Resources and produce no entries.
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

// Depth-first, parent-before-children, preserving display order. Walks both `children`
// and `moreChildren` so a discovered kind behind a "Show more…" toggle still resolves.
export function flattenNav(nodes: readonly DashboardNavNode[]): DashboardNavNode[] {
  return nodes.flatMap((node) => [
    node,
    ...(node.children ? flattenNav(node.children) : []),
    ...(node.moreChildren ? flattenNav(node.moreChildren) : []),
  ]);
}

// Depth-first search for the node with `id`, walking `children` and `moreChildren`,
// short-circuiting at the first match — answers "which node is this id" without flattening.
export function findNode(nodes: readonly DashboardNavNode[], id: DashboardResource): DashboardNavNode | undefined {
  let match: DashboardNavNode | undefined;
  nodes.some((node) => {
    match = node.id === id ? node : (findNode(node.children ?? [], id) ?? findNode(node.moreChildren ?? [], id));
    return match !== undefined;
  });
  return match;
}

// The api group each curated kind leaf belongs to, keyed by its resource plural (node
// id). Pins a curated leaf to one built-in kind: a discovered kind joins only when both
// api group and resource plural match, so a CRD reusing a built-in's plural in another
// group can't hijack its row or count. Group/overview rows are absent (they stand for
// no kind). The core group is "".
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

// The kind shown when the URL names none: the first node in reading order.
export const DEFAULT_DASHBOARD_RESOURCE: DashboardResource = DASHBOARD_NAV[0].id;

// Fold the optional `resource` URL param into a concrete id: absent ⇒ default, present
// passes through untouched (it may be a dynamic kind not yet in the tree).
export function resolveDashboardResource(resource: DashboardResource | undefined): DashboardResource {
  return resource && resource.length > 0 ? resource : DEFAULT_DASHBOARD_RESOURCE;
}

// The curated leaf id a discovered kind is, or undefined when it isn't a curated
// built-in. Matches on both api group and resource plural, so a CRD reusing a built-in's
// plural in another group doesn't collide with it.
function curatedLeafIdForKind(k: ServerKind): string | undefined {
  const group = CURATED_LEAF_API_GROUP[k.resource];
  return group !== undefined && group === apiGroupOf(k.apiVersion) ? k.resource : undefined;
}

// The nav node id a discovered kind maps to: a curated built-in by its bare resource
// plural ("pods"), any other kind by `group/resource` ("apps/replicasets"). The single
// home for this rule — both the bucketing and the count join key nodes by it.
function navIdForKind(k: ServerKind): string {
  return curatedLeafIdForKind(k) ?? `${apiGroupOf(k.apiVersion)}/${k.resource}`;
}

// Merge the cluster's discovered kinds into the curated base: each non-curated kind
// whose api group maps to a group is hung off that group's `moreChildren` (sorted by
// label); unmapped groups (incl. core) are skipped for Custom Resources. Every kind's
// `count` is joined onto its leaf node — curated leaves and discovered kinds alike — in
// a single `withCount` pass keyed by `navIdForKind`.
export function buildDashboardNav(serverKinds: readonly ServerKind[]): DashboardNavNode[] {
  // `buckets`: discovered kinds under their group. `countById`: every kind's count keyed
  // by the node id it lands on, so `withCount` reaches curated leaves too.
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

// The human label for an id, resolved against a built tree (so dynamic kinds
// resolve too). Falls back to the raw id when no node matches — e.g. a dynamic
// kind whose catalog hasn't loaded yet, or a stale deep link.
export function dashboardResourceLabel(nav: readonly DashboardNavNode[], resource: DashboardResource): string {
  return findNode(nav, resource)?.label ?? resource;
}
