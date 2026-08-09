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

// Generic per-kind object table, fed by `useClusterDataObjects`. Universal columns
// (Namespace?/Name/Age) plus kubectl-style extras from the `object-columns`
// registry. The four watch phases render distinctly — not-synced note, connecting
// spinner, empty snapshot, live table — mirroring `EventsTable`.
import ReactTimeAgo from 'react-timeago';

import { Spinner } from '@kubetail/ui/elements/spinner';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@kubetail/ui/elements/table';
import { cn } from '@kubetail/ui/lib/utils';

import { columnsForKind } from '@/components/widgets/object-columns';
import { useClusterDataObjects } from '@/lib/cluster-data-objects';

type ObjectsTableProps = {
  apiVersion: string;
  resource: string;
  kind: string;
  namespaced: boolean;
};

export function ObjectsTable({ apiVersion, resource, kind, namespaced }: ObjectsTableProps) {
  const gvr = { apiVersion, resource };
  const { objects, active, phase } = useClusterDataObjects(gvr);
  // Kind-specific columns between Name and Age; [] when none registered.
  const extraColumns = columnsForKind(gvr);

  // No active cache streams nothing — say so rather than spin forever.
  if (!active) {
    return (
      <p className="text-sm text-muted-foreground">
        No synced cache for this cluster yet — {kind.toLowerCase()} objects appear once syncing has started.
      </p>
    );
  }

  // Connecting (spinner) is distinct from a genuinely empty snapshot (below).
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
            {extraColumns.map((c) => (
              <TableHead key={c.header} className={c.className}>
                {c.header}
              </TableHead>
            ))}
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
                {extraColumns.map((c) => (
                  <TableCell key={c.header} className={cn('align-top', c.className)}>
                    {c.cell(o)}
                  </TableCell>
                ))}
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
