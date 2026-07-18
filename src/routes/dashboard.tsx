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

import { createRoute } from '@tanstack/react-router';

import { useDashboardNav } from '@/lib/dashboard-nav';
import { dashboardResourceLabel, resolveDashboardResource } from '@/lib/dashboard-resources';
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

// The panel reflects the resource kind chosen in the sidebar. Real content (a
// live resource table) lands later; for now it names the selection so the wiring
// from sidebar → URL → panel is visible. The label resolves against the same
// built nav the sidebar uses, so dynamic kinds name themselves too.
function Dashboard() {
  const { resource } = Route.useSearch();
  const { nav } = useDashboardNav();
  const label = dashboardResourceLabel(nav, resolveDashboardResource(resource));
  return (
    <section className="min-w-0 flex-1 p-6">
      <h1 className="text-lg font-semibold">{label}</h1>
      <p className="text-sm text-muted-foreground">The {label.toLowerCase()} view is coming soon.</p>
    </section>
  );
}
