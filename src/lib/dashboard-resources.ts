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

// The dashboard's resource catalog. `DASHBOARD_NAV` is the **curated** base — the
// hand-picked kinds that always appear in the sidebar, in the order they should
// read. The rendered tree is `DASHBOARD_NAV` *augmented at runtime* with the
// cluster's remaining kinds (from `clusterDataKinds`, live API discovery): every
// server kind not already curated is bucketed into a group by its api group (see
// `API_GROUP_TO_GROUP`) and revealed behind a "Show more…" toggle (or, for a
// childless group, the group's own disclosure). `buildDashboardNav` does that
// merge; kinds whose api group isn't mapped (including the core group) are left
// for the Custom Resources work and not rendered here yet.
//
// The `resource` URL search param (see `routes/dashboard.tsx`) can therefore hold
// an id that isn't in the static curated tree — a dynamic kind's id — so
// `DashboardResource` is a plain string, validated leniently (any non-empty
// string) rather than against a closed union.
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

// A node in the rendered tree. Discovered (non-curated) kinds always live in
// `moreChildren`, so the disclosure *style* is derived from the shape rather than
// stored: a group with curated `children` keeps them visible and hides its
// `moreChildren` behind a "Show more…" toggle; a group with no curated children
// (e.g. System) hides its `moreChildren` behind a parent-level chevron (see
// `DashboardResourceNav`). Either way `moreChildren` sit at the same depth as
// `children` would.
export type DashboardNavNode = {
  readonly id: DashboardResource;
  readonly label: string;
  readonly children?: readonly DashboardNavNode[];
  readonly moreChildren?: readonly DashboardNavNode[];
};

// One kind advertised by a cluster's API server (a `clusterDataKinds` row). Declared
// here — not imported from `@/gql` — so this module stays pure and testable.
export type ServerKind = {
  apiVersion: string;
  kind: string;
  resource: string;
  scope: string;
  isCRD: boolean;
};

// The curated groups that can receive discovered kinds. Written as `Extract<…>` of
// the tree's top-level ids so it stays tied to `DASHBOARD_NAV`: rename or remove a
// group there and it drops out of this union, which then fails the value check on
// `API_GROUP_TO_GROUP` below — a build error instead of silently dropped kinds.
type TopLevelId = (typeof DASHBOARD_NAV)[number]['id'];
export type DashboardGroupId = Extract<
  TopLevelId,
  'workloads' | 'network' | 'config-and-storage' | 'security-and-access' | 'system'
>;

// api group → the curated group its non-curated kinds fall under. api groups not
// listed here — including the core group ("") — are deferred to the Custom
// Resources work and produce no entries yet.
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

// Depth-first, parent-before-children, preserving display order — the flat view
// the id-based helpers below index. Walks both `children` and `moreChildren` so a
// discovered kind behind a "Show more…" toggle still resolves (label lookup, etc.).
// Exported so the traversal is testable against an arbitrary tree, not just the
// live catalog.
export function flattenNav(nodes: readonly DashboardNavNode[]): DashboardNavNode[] {
  return nodes.flatMap((node) => [
    node,
    ...(node.children ? flattenNav(node.children) : []),
    ...(node.moreChildren ? flattenNav(node.moreChildren) : []),
  ]);
}

// Depth-first search for the node with `id`, walking `children` and `moreChildren`,
// short-circuiting at the first match — the cheap way to answer "which node is this
// id" (and, over a subtree, "does this id live here") without flattening the tree.
export function findNode(nodes: readonly DashboardNavNode[], id: DashboardResource): DashboardNavNode | undefined {
  let match: DashboardNavNode | undefined;
  nodes.some((node) => {
    match = node.id === id ? node : (findNode(node.children ?? [], id) ?? findNode(node.moreChildren ?? [], id));
    return match !== undefined;
  });
  return match;
}

// Every curated id (group + leaf), so a server kind that's already curated is not
// duplicated among a group's discovered kinds.
const CURATED_IDS = new Set<string>(flattenNav(DASHBOARD_NAV).map((n) => n.id));

// The kind shown when the URL names none: the first node in reading order, so the
// sidebar's default highlight and the panel's default view agree.
export const DEFAULT_DASHBOARD_RESOURCE: DashboardResource = DASHBOARD_NAV[0].id;

// Fold the optional `resource` URL param into a concrete id: absent ⇒ the default.
// A present value passes through untouched (it may be a dynamic kind not in the
// curated tree), so a deep link to a server kind survives; an id that resolves to
// no node just highlights nothing until its group's kinds load.
export function resolveDashboardResource(resource: DashboardResource | undefined): DashboardResource {
  return resource && resource.length > 0 ? resource : DEFAULT_DASHBOARD_RESOURCE;
}

// Merge the cluster's discovered kinds into the curated base: each non-curated
// kind whose api group maps to a group is hung off that group's `moreChildren`
// (sorted by label). The renderer derives the disclosure style from the shape — a
// group with curated children reveals them behind "Show more…", a childless group
// (e.g. System) behind a parent-level chevron — so this doesn't decide it here.
// Kinds in unmapped api groups (incl. core) are skipped — reserved for Custom
// Resources.
export function buildDashboardNav(serverKinds: readonly ServerKind[]): DashboardNavNode[] {
  // Keyed by group id (a plain string): the ids are `DASHBOARD_NAV` node ids, so a
  // lookup with `node.id` below needs no cast.
  const buckets = new Map<string, DashboardNavNode[]>();
  const seen = new Set<string>();
  serverKinds.forEach((k) => {
    const apiGroup = apiGroupOf(k.apiVersion);
    const group = API_GROUP_TO_GROUP[apiGroup];
    if (!group) return; // unmapped api group (incl. core): deferred to Custom Resources
    if (CURATED_IDS.has(k.resource)) return; // already curated — don't duplicate
    const id = `${apiGroup}/${k.resource}`;
    if (seen.has(id)) return; // guard against duplicate discovery rows
    seen.add(id);
    const list = buckets.get(group) ?? [];
    list.push({ id, label: k.kind });
    buckets.set(group, list);
  });
  buckets.forEach((list) => list.sort((a, b) => a.label.localeCompare(b.label)));

  return DASHBOARD_NAV.map((node): DashboardNavNode => {
    const bucket = buckets.get(node.id); // never empty: a bucket exists only once a kind is pushed
    return bucket ? { ...node, moreChildren: bucket } : node;
  });
}

// The human label for an id, resolved against a built tree (so dynamic kinds
// resolve too). Falls back to the raw id when no node matches — e.g. a dynamic
// kind whose catalog hasn't loaded yet, or a stale deep link.
export function dashboardResourceLabel(nav: readonly DashboardNavNode[], resource: DashboardResource): string {
  return findNode(nav, resource)?.label ?? resource;
}
