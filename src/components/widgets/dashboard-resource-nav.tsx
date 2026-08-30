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

// The dashboard's resource tree (sidebar, dashboard mode only). Selectable nodes
// are router `Link`s writing the `resource` search param — history-pushing, with a
// functional `search` preserving the rest of the URL. Discovered kinds render
// behind "Show more…" (curated children present) or a parent-level chevron
// (childless group); both start closed and auto-open when they hold the active
// resource. See docs/adr/2026-08-09-dashboard-nav-merge.md
import { useState } from 'react';
import { Link, useSearch } from '@tanstack/react-router';
import { ChevronRight } from 'lucide-react';

import { Spinner } from '@kubetail/ui/elements/spinner';

import { useDashboardNav } from '@/lib/dashboard-nav';
import { findNode, resolveDashboardResource } from '@/lib/dashboard-resources';
import type { DashboardNavNode, DashboardResource } from '@/lib/dashboard-resources';

// One flex layout with a fixed-width leading slot (chevron or spacer) so labels
// align across siblings at each depth.
const ITEM =
  'flex w-full items-center gap-1 rounded-md py-1.5 pr-3 text-left text-sm font-medium text-muted-foreground ' +
  'transition-colors hover:bg-sidebar-accent hover:text-foreground ' +
  'data-[active=true]:bg-sidebar-accent data-[active=true]:text-foreground';

const SLOT = 'size-3 shrink-0';

// Right-aligned count badge, tabular so digits align down the column.
const COUNT = 'ml-auto shrink-0 pl-2 text-xs tabular-nums text-muted-foreground/70';

// Rendered only where a count is known (leaf kinds).
function CountBadge({ count }: { count: number | undefined }) {
  if (count === undefined) return null;
  return <span className={COUNT}>{count.toLocaleString()}</span>;
}

// "Show more…"/"Show less" row; reserves the leading slot so its label aligns.
const MORE =
  'flex w-full items-center gap-1 rounded-md py-1 pr-3 text-left text-xs font-medium text-muted-foreground/80 ' +
  'transition-colors hover:bg-sidebar-accent hover:text-foreground';

const INDENT_BASE = 0.75;
const INDENT_STEP = 0.75;

// Is `resource` in this node's subtree (child buckets only, not the node itself)?
// Drives the auto-reveal of the group holding the active kind.
function containsResource(node: DashboardNavNode, resource: DashboardResource): boolean {
  return !!findNode(node.children ?? [], resource) || !!findNode(node.moreChildren ?? [], resource);
}

// No curated children but some discovered kinds (e.g. System): kinds hide behind a
// parent-level chevron, not a "Show more…" row.
function isChildlessGroup(node: DashboardNavNode): boolean {
  return !node.children?.length && !!node.moreChildren?.length;
}

type NavItemProps = {
  node: DashboardNavNode;
  active: DashboardResource;
  depth: number;
  isExpanded: (node: DashboardNavNode) => boolean;
  toggle: (node: DashboardNavNode) => void;
};

function NavItem({ node, active, depth, isExpanded, toggle }: NavItemProps) {
  const pad = { paddingLeft: `${INDENT_BASE + depth * INDENT_STEP}rem` };
  const renderNodes = (nodes: readonly DashboardNavNode[] | undefined) =>
    nodes?.map((child) => (
      <NavItem key={child.id} node={child} active={active} depth={depth + 1} isExpanded={isExpanded} toggle={toggle} />
    ));

  // Childless group: two targets in one row — the chevron `button`
  // expands/collapses, the label `Link` navigates.
  if (isChildlessGroup(node)) {
    const expanded = isExpanded(node);
    return (
      <>
        <div data-active={node.id === active} className={ITEM} style={pad}>
          <button
            type="button"
            onClick={() => toggle(node)}
            aria-expanded={expanded}
            aria-label={`${expanded ? 'Collapse' : 'Expand'} ${node.label}`}
            className="flex shrink-0 items-center text-inherit"
          >
            <ChevronRight className={`${SLOT} transition-transform ${expanded ? 'rotate-90' : ''}`} aria-hidden />
          </button>
          <Link
            to="/dashboard"
            search={(prev) => ({ ...prev, resource: node.id })}
            aria-current={node.id === active ? 'page' : undefined}
            className="min-w-0 flex-1 truncate text-inherit"
          >
            {node.label}
          </Link>
          <CountBadge count={node.count} />
        </div>
        {expanded && renderNodes(node.moreChildren)}
      </>
    );
  }

  const more = node.moreChildren;
  const showingMore = isExpanded(node);
  return (
    <>
      <Link
        to="/dashboard"
        search={(prev) => ({ ...prev, resource: node.id })}
        data-active={node.id === active}
        aria-current={node.id === active ? 'page' : undefined}
        className={ITEM}
        style={pad}
      >
        {/* Spacer matching a disclosure's chevron. */}
        <span className={SLOT} aria-hidden />
        <span className="truncate">{node.label}</span>
        <CountBadge count={node.count} />
      </Link>
      {renderNodes(node.children)}
      {more && more.length > 0 && (
        <>
          {showingMore && renderNodes(more)}
          <button
            type="button"
            onClick={() => toggle(node)}
            aria-expanded={showingMore}
            className={MORE}
            style={{ paddingLeft: `${INDENT_BASE + (depth + 1) * INDENT_STEP}rem` }}
          >
            <span className={SLOT} aria-hidden />
            <span>{showingMore ? 'Show less' : `Show more (${more.length})…`}</span>
          </button>
        </>
      )}
    </>
  );
}

export function DashboardResourceNav() {
  const { nav, active: watchActive, phase } = useDashboardNav();
  const { resource } = useSearch({ strict: false });
  const active = resolveDashboardResource(resource);

  // Flag stale/still-dialing discovered kinds — but only while the subscription is
  // live (`watchActive`): a paused watch on an unsynced cluster is legitimately
  // curated-only, not "reconnecting".
  let status: string | null = null;
  if (watchActive && phase === 'reconnecting') status = 'Reconnecting…';
  else if (watchActive && phase === 'connecting') status = 'Loading resources…';

  // An explicit user toggle wins; absent one, a group is open iff it holds the
  // active kind (so a deep link reveals its section).
  const [overrides, setOverrides] = useState<Record<string, boolean>>({});
  const isExpanded = (node: DashboardNavNode) => overrides[node.id] ?? containsResource(node, active);
  const toggle = (node: DashboardNavNode) => setOverrides((prev) => ({ ...prev, [node.id]: !isExpanded(node) }));

  return (
    <nav aria-label="Resources" className="flex flex-col gap-0.5">
      {nav.map((node) => (
        <NavItem key={node.id} node={node} active={active} depth={0} isExpanded={isExpanded} toggle={toggle} />
      ))}
      {/* Transport hint below the tree so it never shifts the resource rows. */}
      {status !== null && (
        <div className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-muted-foreground" data-testid="nav-status">
          <Spinner size="xs" className="mr-0" />
          {status}
        </div>
      )}
    </nav>
  );
}
