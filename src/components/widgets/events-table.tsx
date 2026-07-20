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

// The dashboard's raw events view: a simple table of the active cluster's cached
// Kubernetes Events (newest first), fed by `useClusterDataEvents`. Filtering comes later;
// for now every cached event is shown. The four watch phases render distinctly —
// curated-only fallback (no active cache), connecting spinner, empty snapshot, and the
// live table — mirroring how a whole-screen watch is expected to gate on `active`/`phase`.
import ReactTimeAgo from 'react-timeago';

import { Spinner } from '@kubetail/ui/elements/spinner';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@kubetail/ui/elements/table';

import { useClusterDataEvents } from '@/lib/cluster-data-events';
import type { ClusterDataEvent } from '@/lib/cluster-data-events';

// The involved object as "Kind namespace/name" (kubectl-style), tolerating any part
// being absent (a cluster-scoped or name-only reference).
function involvedRef(e: ClusterDataEvent): string {
  const path = e.involvedNamespace ? `${e.involvedNamespace}/${e.involvedName}` : e.involvedName;
  return [e.involvedKind, path].filter(Boolean).join(' ');
}

// A severity pill. `type` is an open Kubernetes string (usually Normal/Warning, but a
// cluster may omit it or supply a custom value): Warning stands out amber, anything else
// — including an empty type, shown as "Unknown" — stays muted.
function TypeBadge({ type }: { type: ClusterDataEvent['type'] }) {
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
  const { events, active, phase } = useClusterDataEvents();

  // No active cache: an unsynced / sync-paused cluster streams nothing. Say so rather
  // than showing an eternal spinner (which would read as "connecting" forever).
  if (!active) {
    return (
      <p className="text-sm text-muted-foreground">
        No synced cache for this cluster yet — events appear once syncing has started.
      </p>
    );
  }

  // First frame not yet in from this connection: distinguish connecting (spinner) from a
  // genuinely empty snapshot (below).
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
            // lastSeen is null when the source Event carried no timestamp → render "—".
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
