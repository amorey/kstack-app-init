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

// Per-kind extra columns for ObjectsTable, derived client-side from the object's
// native `rawJSON` body; unregistered kinds (CRDs, rarer built-ins) get [] and
// keep only the universal columns.
//
// `rawJSON` is `unknown` off the wire — the server does no per-field typing — so
// accessors cast to a narrow local shape and must degrade to "—" rather than
// throw on a partial body (e.g. a Deleted row's last-known state).
import type { ReactNode } from 'react';

import ReactTimeAgo from 'react-timeago';

import type { ClusterCachedDataObject } from '@/lib/cluster-cached-data-objects';
import { gvrKey } from '@/lib/gvr';
import type { GVR } from '@/lib/gvr';
import type { PrinterColumn } from '@/lib/dashboard-resources';
import { readPath } from '@/lib/jsonpath';

export type ObjectColumn = {
  // React identity. Not the header: a CRD may declare the same one twice, and position is the
  // only identity a descriptor column has.
  key: string;
  header: string;
  cell: (o: ClusterCachedDataObject) => ReactNode;
  // Applied to both the header and its cells.
  className?: string;
};

// Defaults to {} so a missing body flows through the optional chains to "—".
function body<T>(o: ClusterCachedDataObject): T {
  return (o.rawJSON ?? {}) as T;
}

const DASH = '—';

type ContainerStatus = {
  name?: string;
  ready?: boolean;
  started?: boolean;
  restartCount?: number;
  state?: {
    waiting?: { reason?: string };
    terminated?: { reason?: string; exitCode?: number; signal?: number };
    running?: unknown;
  };
};

type PodBody = {
  metadata?: { deletionTimestamp?: string };
  spec?: { containers?: unknown[]; initContainers?: { name?: string; restartPolicy?: string }[] };
  status?: {
    phase?: string;
    reason?: string;
    conditions?: { type?: string; status?: string }[];
    containerStatuses?: ContainerStatus[];
    initContainerStatuses?: ContainerStatus[];
  };
};

function hasCondition(b: PodBody, type: string): boolean {
  return (b.status?.conditions ?? []).some((c) => c.type === type && c.status === 'True');
}

// A native sidecar (init container with restartPolicy Always) keeps running past
// initialization, so a started one doesn't hold the pod in an Init: state.
function isRestartableInit(b: PodBody, c: ContainerStatus): boolean {
  return (b.spec?.initContainers ?? []).some((s) => s.name === c.name && s.restartPolicy === 'Always');
}

// Mirrors kubectl's `printPod`. The bare `status.phase` misreads a crash-looping
// pod (phase=Running), an initializing one, and a terminating one — the
// actionable status lives in container state.
function podStatus(b: PodBody): string {
  const st = b.status;
  let reason = st?.reason || st?.phase || '';

  // Init containers first: the first one not cleanly finished decides the status.
  const initStatuses = st?.initContainerStatuses ?? [];
  const initTotal = b.spec?.initContainers?.length ?? initStatuses.length;
  let initializing = false;
  for (let i = 0; i < initStatuses.length; i += 1) {
    const c = initStatuses[i];
    const term = c.state?.terminated;
    const waiting = c.state?.waiting;
    // Skip init containers that finished cleanly, and sidecars already up.
    const settled = (term && term.exitCode === 0) || (isRestartableInit(b, c) && c.started);
    if (!settled) {
      if (term) {
        if (term.reason) reason = `Init:${term.reason}`;
        else if (term.signal) reason = `Init:Signal:${term.signal}`;
        else reason = `Init:ExitCode:${term.exitCode ?? 0}`;
      } else if (waiting?.reason && waiting.reason !== 'PodInitializing') {
        reason = `Init:${waiting.reason}`;
      } else {
        reason = `Init:${i}/${initTotal}`;
      }
      initializing = true;
      break;
    }
  }

  // Regular containers: last wins, as kubectl scans in reverse.
  if (!initializing || hasCondition(b, 'Initialized')) {
    let hasRunning = false;
    const cs = st?.containerStatuses ?? [];
    for (let i = cs.length - 1; i >= 0; i -= 1) {
      const c = cs[i];
      const term = c.state?.terminated;
      const waiting = c.state?.waiting;
      if (waiting?.reason) reason = waiting.reason;
      else if (term?.reason) reason = term.reason;
      else if (term) reason = term.signal ? `Signal:${term.signal}` : `ExitCode:${term.exitCode ?? 0}`;
      else if (c.ready && c.state?.running) hasRunning = true;
    }
    // A restarted-and-recovered container leaves a stale Completed from its last termination.
    if (reason === 'Completed' && hasRunning) reason = hasCondition(b, 'Ready') ? 'Running' : 'NotReady';
  }

  // Deletion wins over everything except a pod already in a terminal phase.
  if (b.metadata?.deletionTimestamp) {
    if (st?.reason === 'NodeLost') reason = 'Unknown';
    else if (st?.phase !== 'Succeeded' && st?.phase !== 'Failed') reason = 'Terminating';
  }
  return reason || DASH;
}

type WorkloadBody = {
  spec?: { replicas?: number };
  status?: { readyReplicas?: number; updatedReplicas?: number; availableReplicas?: number };
};

const podColumns: ObjectColumn[] = [
  {
    key: 'Ready',
    header: 'Ready',
    className: 'tabular-nums',
    cell: (o) => {
      const b = body<PodBody>(o);
      const cs = b.status?.containerStatuses ?? [];
      // Denominator is the spec count (kubectl's), so a pod whose statuses
      // haven't all landed still reads against the expected total.
      const total = b.spec?.containers?.length ?? cs.length;
      if (!total) return DASH;
      return `${cs.filter((c) => c.ready).length}/${total}`;
    },
  },
  {
    key: 'Status',
    header: 'Status',
    cell: (o) => podStatus(body<PodBody>(o)),
  },
  {
    key: 'Restarts',
    header: 'Restarts',
    className: 'tabular-nums',
    cell: (o) => {
      const st = body<PodBody>(o).status;
      // Init-container retries count too: during init they're the only restarts
      // recorded, and a sidecar keeps accruing them afterwards.
      const all = [...(st?.initContainerStatuses ?? []), ...(st?.containerStatuses ?? [])];
      if (all.length === 0) return DASH;
      return all.reduce((n, c) => n + (c.restartCount ?? 0), 0);
    },
  },
];

const workloadColumns: ObjectColumn[] = [
  {
    key: 'Ready',
    header: 'Ready',
    className: 'tabular-nums',
    cell: (o) => {
      const b = body<WorkloadBody>(o);
      return `${b.status?.readyReplicas ?? 0}/${b.spec?.replicas ?? 0}`;
    },
  },
  {
    key: 'Up-to-date',
    header: 'Up-to-date',
    className: 'tabular-nums',
    cell: (o) => body<WorkloadBody>(o).status?.updatedReplicas ?? 0,
  },
  {
    key: 'Available',
    header: 'Available',
    className: 'tabular-nums',
    cell: (o) => body<WorkloadBody>(o).status?.availableReplicas ?? 0,
  },
];

// Keyed by gvrKey — the canonical kind key the watch provenance and kinds
// catalog share.
const OBJECT_COLUMNS: Record<string, ObjectColumn[]> = {
  [gvrKey({ apiVersion: 'v1', resource: 'pods' })]: podColumns,
  [gvrKey({ apiVersion: 'apps/v1', resource: 'deployments' })]: workloadColumns,
};

// One column built from a CRD's descriptor: read the path, render by declared type.
//
// **Only scalars render.** A path landing on an object or an array has no cell form — kubectl
// would print its JSON, which is unreadable in a table — so it reads as absent.
function descriptorColumn(d: PrinterColumn, i: number): ObjectColumn {
  const numeric = d.type === 'integer' || d.type === 'number';
  return {
    key: `${i}-${d.name}`,
    header: d.name,
    className: numeric || d.type === 'date' ? 'tabular-nums' : undefined,
    cell: (o) => {
      const value = readPath(o.rawJSON, d.jsonPath);
      if (value === null || value === undefined || typeof value === 'object') return DASH;
      if (d.type === 'date') {
        const ms = Date.parse(String(value));
        return Number.isNaN(ms) ? DASH : <ReactTimeAgo date={ms} component="span" maxPeriod={60} />;
      }
      return String(value);
    },
  };
}

// Two tiers: a hand-written entry wins, else the kind's own descriptors, else [] (universal
// columns only).
//
// **The registry wins on purpose** — an accessor is hand-written precisely because it says
// something a jsonPath cannot, the way podStatus reads container state to correct a
// crash-looping pod that reports `phase: Running`.
//
// `priority > 0` is dropped: kubectl hides those behind `-o wide`, and this table has no
// equivalent yet.
export function columnsForKind(gvr: GVR, printerColumns: readonly PrinterColumn[] = []): ObjectColumn[] {
  const registered = OBJECT_COLUMNS[gvrKey(gvr)];
  if (registered) return registered;
  return printerColumns.filter((d) => !d.priority).map(descriptorColumn);
}
