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

// Generic per-kind object table, fed by `useClusterCachedDataObjects`. Universal columns
// (Namespace?/Name/Age) plus kubectl-style extras from the `object-columns`
// registry. The states render distinctly — not-synced note, connecting spinner,
// empty snapshot, live table — mirroring `EventsTable`.
import ReactTimeAgo from 'react-timeago';

import { Spinner } from '@kubetail/ui/elements/spinner';

import { columnsForKind } from '@/components/widgets/object-columns';
import { VirtualTable } from '@/components/widgets/virtual-table';
import type { VirtualColumn } from '@/components/widgets/virtual-table';

import { useClusterCachedDataObjects } from '@/lib/cluster-cached-data-objects';
import type { ClusterCachedDataObject } from '@/lib/cluster-cached-data-objects';
import type { PrinterColumn } from '@/lib/dashboard-resources';

type ObjectsTableProps = {
  apiVersion: string;
  resource: string;
  kind: string;
  namespaced: boolean;
  // A CRD's declared columns, off the kinds watch. Empty for a built-in, and for a kind whose
  // hand-written registry entry wins anyway.
  printerColumns?: readonly PrinterColumn[];
};

const rowKey = (o: ClusterCachedDataObject) => o.uid;

const NAMESPACE_COLUMN: VirtualColumn<ClusterCachedDataObject> = {
  key: 'namespace',
  header: 'Namespace',
  className: 'w-48',
  cell: (o) => <span className="text-muted-foreground">{o.namespace || '—'}</span>,
};

// Takes the width the sized columns leave; a long name truncates, with the full one on hover.
const NAME_COLUMN: VirtualColumn<ClusterCachedDataObject> = {
  key: 'name',
  header: 'Name',
  className: 'truncate',
  cell: (o) => (
    <span className="font-medium" title={o.name}>
      {o.name}
    </span>
  ),
};

const AGE_COLUMN: VirtualColumn<ClusterCachedDataObject> = {
  key: 'age',
  header: 'Age',
  className: 'w-32 tabular-nums',
  cell: (o) => {
    const createdMs = o.creationTimestamp ? Date.parse(o.creationTimestamp) : NaN;
    return Number.isNaN(createdMs) ? '—' : <ReactTimeAgo date={createdMs} component="span" maxPeriod={60} />;
  },
};

export function ObjectsTable({ apiVersion, resource, kind, namespaced, printerColumns = [] }: ObjectsTableProps) {
  const gvr = { apiVersion, resource };
  const { objects, active, phase } = useClusterCachedDataObjects(gvr);
  // Kind-specific columns between Name and Age; [] when neither the registry nor the kind
  // itself declares any.
  const extraColumns = columnsForKind(gvr, printerColumns);

  // No active cache streams nothing — say so rather than spin forever.
  if (!active) {
    return (
      <p className="text-sm text-muted-foreground">
        No synced cache for this cluster yet — {kind.toLowerCase()} objects appear once syncing has started.
      </p>
    );
  }

  // Held until the snapshot's Bookmark lands, so the empty state below can only be
  // reached once the kind is genuinely known to have no objects.
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

  const columns = [...(namespaced ? [NAMESPACE_COLUMN] : []), NAME_COLUMN, ...extraColumns, AGE_COLUMN];
  return <VirtualTable rows={objects} rowKey={rowKey} columns={columns} />;
}
