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

// The dashboard's generic per-kind object table: one row per cached object of the selected
// kind, fed by `useClusterDataObjects`. For now it shows the universal columns only —
// Name (and Namespace for a namespaced kind) from the object's typed identity, and Age from
// `creationTimestamp`. Kind-specific columns (Ready/Status/…) come with the nested object
// body in a follow-up — see the "ClusterDataObject — native nested body" item in TODO.md.
// The four watch phases render distinctly — not-synced note, connecting spinner, empty
// snapshot, and the live table — mirroring `EventsTable`.
import ReactTimeAgo from 'react-timeago';

import { Spinner } from '@kubetail/ui/elements/spinner';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@kubetail/ui/elements/table';

import { useClusterDataObjects } from '@/lib/cluster-data-objects';

// The kind to show — its group/version + plural resource (streamed), its display Kind name
// (empty states), and whether it's namespaced (whether to show the Namespace column).
type ObjectsTableProps = {
  apiVersion: string;
  resource: string;
  kind: string;
  namespaced: boolean;
};

export function ObjectsTable({ apiVersion, resource, kind, namespaced }: ObjectsTableProps) {
  const { objects, active, phase } = useClusterDataObjects({ apiVersion, resource });

  // No active cache: an unsynced / sync-paused cluster streams nothing. Say so rather than
  // showing an eternal spinner (which would read as "connecting" forever).
  if (!active) {
    return (
      <p className="text-sm text-muted-foreground">
        No synced cache for this cluster yet — {kind.toLowerCase()} objects appear once syncing has started.
      </p>
    );
  }

  // First frame not yet in from this connection: distinguish connecting (spinner) from a
  // genuinely empty snapshot (below).
  if (phase === 'connecting') {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Spinner size="sm" />
        Loading {kind.toLowerCase()}…
      </div>
    );
  }

  if (objects.length === 0) {
    return <p className="text-sm text-muted-foreground">No {kind.toLowerCase()} objects.</p>;
  }

  return (
    <div className="overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            {namespaced && <TableHead className="w-48">Namespace</TableHead>}
            <TableHead>Name</TableHead>
            <TableHead className="w-32">Age</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {objects.map((o) => {
            const createdMs = o.creationTimestamp ? Date.parse(o.creationTimestamp) : NaN;
            return (
              <TableRow key={o.uid}>
                {namespaced && <TableCell className="align-top text-muted-foreground">{o.namespace || '—'}</TableCell>}
                <TableCell className="align-top font-medium">{o.name}</TableCell>
                <TableCell className="align-top tabular-nums">
                  {Number.isNaN(createdMs) ? '—' : <ReactTimeAgo date={createdMs} component="span" maxPeriod={60} />}
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
}
