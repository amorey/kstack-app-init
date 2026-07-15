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

// The dashboard's resource navigation — the tree that lets the user jump between
// Kubernetes resource kinds. It lives in the app's floating sidebar and is
// mounted only while dashboard mode is active (see `AppLayout`). It renders
// `DASHBOARD_NAV` recursively: every node is a router `Link` that writes the
// `resource` search param on `/dashboard`, and a group's children are indented
// beneath it (always expanded). Because each node — group or leaf — is selectable,
// there's no header-vs-item split: they're all links.
//
// A selection is a real, history-pushing navigation, so it lands in the router
// history the context bar's back/forward walks. The functional `search` preserves
// the rest of the URL (notably the window's `kubeContext`), and active
// highlighting reads the param straight off the current match (`strict: false`
// decouples it from the owning route id), folding the absent/unknown case into
// the default so the default kind highlights even before the param is in the URL.
import { Link, useSearch } from '@tanstack/react-router';

import { DASHBOARD_NAV, resolveDashboardResource } from '@/lib/dashboard-resources';
import type { DashboardNavNode, DashboardResource } from '@/lib/dashboard-resources';

const ITEM =
  'block rounded-md py-1.5 pr-3 text-sm font-medium text-muted-foreground transition-colors ' +
  'hover:bg-sidebar-accent hover:text-foreground ' +
  'data-[active=true]:bg-sidebar-accent data-[active=true]:text-foreground';

// Left padding per nesting depth (rem): a base inset plus one step per level, so
// children sit visibly under their group.
const INDENT_BASE = 0.75;
const INDENT_STEP = 0.75;

function NavItem({ node, active, depth }: { node: DashboardNavNode; active: DashboardResource; depth: number }) {
  return (
    <>
      <Link
        to="/dashboard"
        search={(prev) => ({ ...prev, resource: node.id })}
        data-active={node.id === active}
        aria-current={node.id === active ? 'page' : undefined}
        className={ITEM}
        style={{ paddingLeft: `${INDENT_BASE + depth * INDENT_STEP}rem` }}
      >
        {node.label}
      </Link>
      {node.children?.map((child) => (
        <NavItem key={child.id} node={child} active={active} depth={depth + 1} />
      ))}
    </>
  );
}

export function DashboardResourceNav() {
  const { resource } = useSearch({ strict: false });
  const active = resolveDashboardResource(resource);

  return (
    <nav aria-label="Resources" className="flex flex-col gap-0.5">
      {DASHBOARD_NAV.map((node) => (
        <NavItem key={node.id} node={node} active={active} depth={0} />
      ))}
    </nav>
  );
}
