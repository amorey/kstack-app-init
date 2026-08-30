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

import { useMemo } from 'react';

import { createRoute } from '@tanstack/react-router';

import { EventsTable } from '@/components/widgets/events-table';
import { ObjectsTable } from '@/components/widgets/objects-table';
import { useClusterCachedDataKinds } from '@/lib/cluster-cached-data-kinds';
import {
  buildDashboardNav,
  dashboardResourceLabel,
  resolveDashboardResource,
  serverKindForResource,
} from '@/lib/dashboard-resources';
import type { DashboardResource } from '@/lib/dashboard-resources';
import { Route as appRoute } from '@/routes/_app';

// The focused kind lives in the `resource` search param (deep-linkable, each change
// a history entry — see docs/adr/2026-08-09-url-params-as-window-state.md). Optional
// (absent resolves to the default without rewriting a bare `/dashboard`) and
// validated leniently — it may name a dynamic kind, not a closed union.
type DashboardSearch = { resource?: DashboardResource };

export const Route = createRoute({
  getParentRoute: () => appRoute,
  path: '/dashboard',
  validateSearch: (search: Record<string, unknown>): DashboardSearch =>
    typeof search.resource === 'string' && search.resource.length > 0 ? { resource: search.resource } : {},
  component: Dashboard,
});

// Panel pick: `events` → bespoke `EventsTable`; any resolved discovered kind →
// generic `ObjectsTable`; no kind (group/overview row, unloaded catalog) →
// placeholder. Label and kind resolve against the same live catalog the sidebar uses.
function Dashboard() {
  const { resource } = Route.useSearch();
  const { kinds } = useClusterCachedDataKinds();
  const resolved = resolveDashboardResource(resource);
  // The catalog re-emits on count churn — memoize so a Modified frame doesn't
  // rebuild the tree / rescan the catalog every render.
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
        printerColumns={serverKind.printerColumns}
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
