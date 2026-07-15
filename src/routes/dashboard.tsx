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

import { dashboardResourceLabel, isDashboardResource, resolveDashboardResource } from '@/lib/dashboard-resources';
import type { DashboardResource } from '@/lib/dashboard-resources';
import { Route as appRoute } from '@/routes/_app';

// The focused resource kind lives in the URL, so a selection is deep-linkable
// and each change is its own history entry (see `dashboard-resources.ts`). It's
// optional in the URL — an absent or unknown value resolves to the default in
// the component — so we never rewrite a bare `/dashboard` on load. The picker
// itself is `DashboardResourceNav`, which the app shell mounts in the floating
// sidebar while dashboard mode is active (see `AppLayout`); this route just
// reads the param back and renders the matching panel.
type DashboardSearch = { resource?: DashboardResource };

export const Route = createRoute({
  getParentRoute: () => appRoute,
  path: '/dashboard',
  validateSearch: (search: Record<string, unknown>): DashboardSearch =>
    isDashboardResource(search.resource) ? { resource: search.resource } : {},
  component: Dashboard,
});

// The panel reflects the resource kind chosen in the sidebar. Real content (a
// live resource table) lands later; for now it names the selection so the wiring
// from sidebar → URL → panel is visible.
function Dashboard() {
  const { resource } = Route.useSearch();
  const label = dashboardResourceLabel(resolveDashboardResource(resource));
  return (
    <section className="min-w-0 flex-1 p-6">
      <h1 className="text-lg font-semibold">{label}</h1>
      <p className="text-sm text-muted-foreground">The {label.toLowerCase()} view is coming soon.</p>
    </section>
  );
}
