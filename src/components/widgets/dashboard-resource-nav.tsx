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

// The dashboard's resource-navigation tree, in the floating sidebar, mounted only in
// dashboard mode (see `AppLayout`). The tree is the curated base merged with the
// cluster's discovered kinds (`useDashboardNav`); every selectable node is a router
// `Link` writing the `resource` search param on `/dashboard`, so a selection is a
// history-pushing navigation, and the functional `search` preserves the rest of the URL.
//
// A group's curated children are always visible; its discovered kinds (`moreChildren`)
// render at the same depth behind a "Show more…" toggle. A childless group that gained
// kinds (System) stays a navigable label but grows a chevron toggling its kinds. Both
// reveal states start closed and auto-open when they hold the active resource.
import { useState } from 'react';
import { Link, useSearch } from '@tanstack/react-router';
import { ChevronRight } from 'lucide-react';

import { Spinner } from '@kubetail/ui/elements/spinner';

import { useDashboardNav } from '@/lib/dashboard-nav';
import { findNode, resolveDashboardResource } from '@/lib/dashboard-resources';
import type { DashboardNavNode, DashboardResource } from '@/lib/dashboard-resources';

// Every row uses the same flex layout with a fixed-width leading slot, so a chevron and
// a link's spacer occupy the same space and labels line up across siblings at each depth.
const ITEM =
  'flex w-full items-center gap-1 rounded-md py-1.5 pr-3 text-left text-sm font-medium text-muted-foreground ' +
  'transition-colors hover:bg-sidebar-accent hover:text-foreground ' +
  'data-[active=true]:bg-sidebar-accent data-[active=true]:text-foreground';

// The leading slot every row reserves (chevron or equal-size spacer) so labels align.
const SLOT = 'size-3 shrink-0';

// The object-count badge: right edge, subdued and tabular so digits align down the column.
const COUNT = 'ml-auto shrink-0 pl-2 text-xs tabular-nums text-muted-foreground/70';

// A kind's object count, rendered only where known (leaf kinds; group/overview rows none).
function CountBadge({ count }: { count: number | undefined }) {
  if (count === undefined) return null;
  return <span className={COUNT}>{count.toLocaleString()}</span>;
}

// The "Show more…"/"Show less" toggle: a subdued action row that reserves the leading
// slot so its label aligns with the resource labels above it.
const MORE =
  'flex w-full items-center gap-1 rounded-md py-1 pr-3 text-left text-xs font-medium text-muted-foreground/80 ' +
  'transition-colors hover:bg-sidebar-accent hover:text-foreground';

// Left padding per nesting depth (rem): a base inset plus one step per level.
const INDENT_BASE = 0.75;
const INDENT_STEP = 0.75;

// Whether `resource` lives in this node's subtree — used to auto-reveal the group
// holding the active kind. Searches the child buckets, not the node itself.
function containsResource(node: DashboardNavNode, resource: DashboardResource): boolean {
  return !!findNode(node.children ?? [], resource) || !!findNode(node.moreChildren ?? [], resource);
}

// A group with no curated children but some discovered kinds (e.g. System): its whole
// kind list hides behind a parent-level chevron, not a "Show more…" row.
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

  // A childless group that gained kinds (e.g. System) stays a navigable label, but its
  // chevron toggles its discovered kinds. Two targets in one row: the chevron `button`
  // expands/collapses, the label `Link` navigates; the row bg follows the link's state.
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

  // Discovered kinds render at the same depth as the curated children, behind a
  // "Show more…"/"Show less" toggle.
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
        {/* Empty slot matching a disclosure's chevron, so labels align. */}
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

  // The curated tree always renders, so the discovered kinds below it may be stale
  // (a dropped watch) or not yet in (still dialing). Flag that — but only when the
  // subscription is live (`watchActive`); a paused watch on an unsynced cluster is
  // legitimately curated-only, not "reconnecting". `empty`/`live` show nothing.
  let status: string | null = null;
  if (watchActive && phase === 'reconnecting') status = 'Reconnecting…';
  else if (watchActive && phase === 'connecting') status = 'Loading resources…';

  // Per-group reveal state. An explicit user toggle wins; absent one, a group is open
  // iff it holds the active kind, so a deep link to a discovered kind reveals it.
  const [overrides, setOverrides] = useState<Record<string, boolean>>({});
  const isExpanded = (node: DashboardNavNode) => overrides[node.id] ?? containsResource(node, active);
  const toggle = (node: DashboardNavNode) => setOverrides((prev) => ({ ...prev, [node.id]: !isExpanded(node) }));

  return (
    <nav aria-label="Resources" className="flex flex-col gap-0.5">
      {nav.map((node) => (
        <NavItem key={node.id} node={node} active={active} depth={0} isExpanded={isExpanded} toggle={toggle} />
      ))}
      {/* A transport hint under the tree (below, so it never shifts the resource rows):
          the discovered kinds may be stale or incomplete while it shows. */}
      {status !== null && (
        <p className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-muted-foreground" data-testid="nav-status">
          <Spinner size="xs" className="mr-0" />
          {status}
        </p>
      )}
    </nav>
  );
}
