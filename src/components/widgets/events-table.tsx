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

// The dashboard's cached Kubernetes Events table (newest first), fed by
// `useClusterCachedDataEvents`. Unfiltered for now. Gates on `active`/`phase` — the
// pattern every whole-screen watch view follows.
// See docs/adr/2026-08-09-delta-watch-protocol.md
import ReactTimeAgo from 'react-timeago';

import { Spinner } from '@kubetail/ui/elements/spinner';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@kubetail/ui/elements/table';

import { useClusterCachedDataEvents } from '@/lib/cluster-cached-data-events';
import type { ClusterCachedDataEvent } from '@/lib/cluster-cached-data-events';

// kubectl-style "Kind namespace/name"; any part may be absent (cluster-scoped or
// name-only reference).
function involvedRef(e: ClusterCachedDataEvent): string {
  const path = e.involvedNamespace ? `${e.involvedNamespace}/${e.involvedName}` : e.involvedName;
  return [e.involvedKind, path].filter(Boolean).join(' ');
}

// `type` is an open Kubernetes string — a cluster may omit it or supply a custom
// value, so everything but Warning stays muted (empty renders "Unknown").
function TypeBadge({ type }: { type: ClusterCachedDataEvent['type'] }) {
  const warning = type === 'Warning';
  return (
    <span
      className={`inline-block rounded px-1.5 py-0.5 text-xs font-medium ${
        warning ? 'bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300' : 'bg-muted text-muted-foreground'
      }`}
    >
      {type || 'Unknown'}
    </span>
  );
}

export function EventsTable() {
  const { events, active, phase } = useClusterCachedDataEvents();

  // No active cache: an unsynced/paused cluster streams nothing — say so rather
  // than spin forever.
  if (!active) {
    return (
      <p className="text-sm text-muted-foreground">
        No synced cache for this cluster yet — events appear once syncing has started.
      </p>
    );
  }

  // Distinct from a genuinely empty snapshot (below).
  if (phase === 'connecting') {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Spinner size="sm" />
        Loading events…
      </div>
    );
  }

  if (events.length === 0) {
    return <p className="text-sm text-muted-foreground">No events.</p>;
  }

  return (
    <div className="overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-24">Type</TableHead>
            <TableHead className="w-48">Reason</TableHead>
            <TableHead className="w-64">Object</TableHead>
            <TableHead>Message</TableHead>
            <TableHead className="w-16 text-right">Count</TableHead>
            <TableHead className="w-32">Last Seen</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {events.map((e) => {
            // lastSeen is null when the source Event carried no timestamp.
            const lastSeenMs = e.lastSeen ? Date.parse(e.lastSeen) : NaN;
            const ref = involvedRef(e);
            return (
              <TableRow key={e.uid}>
                <TableCell className="align-top">
                  <TypeBadge type={e.type} />
                </TableCell>
                <TableCell className="align-top font-medium">{e.reason || '—'}</TableCell>
                <TableCell className="max-w-0 truncate align-top" title={ref}>
                  {ref || '—'}
                </TableCell>
                <TableCell className="align-top text-muted-foreground">{e.message || '—'}</TableCell>
                <TableCell className="align-top text-right tabular-nums">{e.count}</TableCell>
                <TableCell className="align-top tabular-nums">
                  {Number.isNaN(lastSeenMs) ? '—' : <ReactTimeAgo date={lastSeenMs} component="span" maxPeriod={60} />}
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
}
