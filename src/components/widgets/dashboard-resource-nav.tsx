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
// Kubernetes resource kinds. It lives in the app's floating sidebar and is mounted
// only while dashboard mode is active (see `AppLayout`). The tree is the curated
// base merged with the active cluster's discovered kinds (`useDashboardNav`): every
// selectable node is a router `Link` that writes the `resource` search param on
// `/dashboard`, so a selection is a real, history-pushing navigation the context
// bar's back/forward walks, and the functional `search` preserves the rest of the
// URL (notably the window's `kubeContext`).
//
// A group's curated children are always visible; its discovered kinds
// (`moreChildren`) render at the same depth behind a "Show more…"/"Show less"
// toggle. A childless group that gained kinds (System) instead stays a navigable
// label but grows a chevron that toggles its kinds. Both reveal states start closed
// and auto-open when they hold the active resource, so a deep link to a server kind
// opens its section.
import { useState } from 'react';
import { Link, useSearch } from '@tanstack/react-router';
import { ChevronRight } from 'lucide-react';

import { useDashboardNav } from '@/lib/dashboard-nav';
import { findNode, resolveDashboardResource } from '@/lib/dashboard-resources';
import type { DashboardNavNode, DashboardResource } from '@/lib/dashboard-resources';

// Every row — link or disclosure — uses the same flex layout with a fixed-width
// leading slot, so a disclosure's chevron and a link's spacer occupy the same space
// and the label text lines up across siblings at each depth.
const ITEM =
  'flex w-full items-center gap-1 rounded-md py-1.5 pr-3 text-left text-sm font-medium text-muted-foreground ' +
  'transition-colors hover:bg-sidebar-accent hover:text-foreground ' +
  'data-[active=true]:bg-sidebar-accent data-[active=true]:text-foreground';

// The leading slot every row reserves: the chevron on a disclosure, an equal-size
// spacer on a plain link, so both labels start at the same x.
const SLOT = 'size-3 shrink-0';

// The "Show more…"/"Show less" toggle: a subdued action row that still reserves the
// leading slot, so its label aligns with the resource labels above it.
const MORE =
  'flex w-full items-center gap-1 rounded-md py-1 pr-3 text-left text-xs font-medium text-muted-foreground/80 ' +
  'transition-colors hover:bg-sidebar-accent hover:text-foreground';

// Left padding per nesting depth (rem): a base inset plus one step per level, so
// children sit visibly under their group.
const INDENT_BASE = 0.75;
const INDENT_STEP = 0.75;

// Whether `resource` lives somewhere in this node's subtree (`children` or the
// "Show more…" `moreChildren`) — used to auto-reveal the group holding the active
// kind. Searches the child buckets (not the node itself) via the shared `findNode`.
function containsResource(node: DashboardNavNode, resource: DashboardResource): boolean {
  return !!findNode(node.children ?? [], resource) || !!findNode(node.moreChildren ?? [], resource);
}

// A group with no curated children but some discovered kinds (e.g. System): its
// whole kind list hides behind a parent-level chevron, rather than a "Show more…"
// row under always-visible children.
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

  // A childless group that gained kinds (e.g. System) stays a navigable label like
  // any group, but its chevron — filling the leading slot — toggles its discovered
  // kinds (its `moreChildren`). Two targets in one row: the chevron `button`
  // expands/collapses, the label `Link` navigates; the row bg follows the link's
  // active/hover state (`data-active` on the wrapper).
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
        </div>
        {expanded && renderNodes(node.moreChildren)}
      </>
    );
  }

  // The extra (discovered) kinds render at the same depth as the curated children,
  // between them and a "Show more…"/"Show less" toggle that reveals/hides them.
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
        <span>{node.label}</span>
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
            <span>{showingMore ? 'Show less' : 'Show more…'}</span>
          </button>
        </>
      )}
    </>
  );
}

export function DashboardResourceNav() {
  const { nav } = useDashboardNav();
  const { resource } = useSearch({ strict: false });
  const active = resolveDashboardResource(resource);

  // Per-group reveal state (a childless group's chevron, or a group's "Show more…").
  // An explicit user toggle wins; absent one, a group is open iff it holds the active
  // kind, so a deep link to a discovered kind reveals it.
  const [overrides, setOverrides] = useState<Record<string, boolean>>({});
  const isExpanded = (node: DashboardNavNode) => overrides[node.id] ?? containsResource(node, active);
  const toggle = (node: DashboardNavNode) => setOverrides((prev) => ({ ...prev, [node.id]: !isExpanded(node) }));

  return (
    <nav aria-label="Resources" className="flex flex-col gap-0.5">
      {nav.map((node) => (
        <NavItem key={node.id} node={node} active={active} depth={0} isExpanded={isExpanded} toggle={toggle} />
      ))}
    </nav>
  );
}
