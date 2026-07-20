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

import { useMemo } from 'react';

import { createRoute } from '@tanstack/react-router';

import { EventsTable } from '@/components/widgets/events-table';
import { ObjectsTable } from '@/components/widgets/objects-table';
import { useClusterDataKinds } from '@/lib/cluster-data-kinds';
import {
  buildDashboardNav,
  dashboardResourceLabel,
  resolveDashboardResource,
  serverKindForResource,
} from '@/lib/dashboard-resources';
import type { DashboardResource } from '@/lib/dashboard-resources';
import { Route as appRoute } from '@/routes/_app';

// The focused resource kind lives in the `resource` search param, so a selection
// is deep-linkable and each change is its own history entry. It's optional (absent
// resolves to the default in the component, so a bare `/dashboard` isn't rewritten
// on load) and may be a curated or dynamic kind's id, so it's validated leniently
// (any non-empty string) rather than against a closed union. The picker is
// `DashboardResourceNav` (mounted by `AppLayout`); this route reads the param back
// and renders the matching panel.
type DashboardSearch = { resource?: DashboardResource };

export const Route = createRoute({
  getParentRoute: () => appRoute,
  path: '/dashboard',
  validateSearch: (search: Record<string, unknown>): DashboardSearch =>
    typeof search.resource === 'string' && search.resource.length > 0 ? { resource: search.resource } : {},
  component: Dashboard,
});

// The panel reflects the resource kind chosen in the sidebar. `events` has a bespoke
// typed table; every other discovered kind renders the generic `ObjectsTable` (resolved
// from the live catalog to its apiVersion/resource/scope); a selection that names no kind
// (a group/overview row, or a not-yet-loaded catalog) shows a placeholder. The label and
// kind both resolve against the same live catalog the sidebar uses, so dynamic kinds name
// and render themselves too.
function Dashboard() {
  const { resource } = Route.useSearch();
  const { kinds } = useClusterDataKinds();
  const resolved = resolveDashboardResource(resource);
  // Both the label (needs the built tree so dynamic kinds name themselves) and the selected
  // kind derive from the live catalog, which re-emits on count churn — memoize so a Modified
  // frame doesn't rebuild the tree / rescan the catalog on every render.
  const label = useMemo(() => dashboardResourceLabel(buildDashboardNav(kinds), resolved), [kinds, resolved]);
  const serverKind = useMemo(() => serverKindForResource(kinds, resolved), [kinds, resolved]);

  let panel;
  if (resolved === 'events') {
    panel = <EventsTable />;
  } else if (serverKind) {
    panel = (
      <ObjectsTable
        apiVersion={serverKind.apiVersion}
        resource={serverKind.resource}
        kind={serverKind.kind}
        namespaced={serverKind.scope === 'Namespaced'}
      />
    );
  } else {
    panel = <p className="text-sm text-muted-foreground">The {label.toLowerCase()} view is coming soon.</p>;
  }

  return (
    <section className="min-w-0 flex-1 p-6">
      <h1 className="mb-4 text-lg font-semibold">{label}</h1>
      {panel}
    </section>
  );
}
