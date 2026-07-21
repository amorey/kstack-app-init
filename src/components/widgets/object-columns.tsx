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

// The per-kind column registry for ObjectsTable. Every kind gets the universal
// Namespace?/Name/Age columns; a built-in kind additionally gets the kubectl-style columns
// registered here (Ready/Status/…), computed client-side from the object's native body
// (`rawJSON`). Unregistered kinds — CRDs, less-common built-ins — fall back to the universal
// columns alone (an empty extra-column list).
//
// Accessors read `rawJSON` (typed `unknown` off the wire) by casting to a narrow local shape
// naming only the fields they touch — the server does no per-field typing. A missing/partial
// body degrades to "—" rather than throwing, so a Deleted row's last-known body or an
// as-yet-unpopulated field never breaks a cell.
import type { ReactNode } from 'react';

import type { ClusterDataObject } from '@/lib/cluster-data-objects';
import { gvrKey } from '@/lib/gvr';
import type { GVR } from '@/lib/gvr';

// One kind-specific column: a header and a cell derived from an object's body.
export type ObjectColumn = {
  header: string;
  cell: (o: ClusterDataObject) => ReactNode;
  // Extra classes for both the header and cells (e.g. width, tabular-nums).
  className?: string;
};

// The object's native body as a narrow shape (only the fields a cell reads), defaulting to
// {} so a missing body flows through the optional chains to "—".
function body<T>(o: ClusterDataObject): T {
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

// A native sidecar: an init container whose restartPolicy is Always. It keeps running past
// initialization, so a started one doesn't hold the pod in an Init: state.
function isRestartableInit(b: PodBody, c: ContainerStatus): boolean {
  return (b.spec?.initContainers ?? []).some((s) => s.name === c.name && s.restartPolicy === 'Always');
}

// Mirrors kubectl's `printPod` status derivation. `status.phase` is only the starting point —
// the actionable status lives in container state: a crash-looping pod is phase=Running but
// should read CrashLoopBackOff, an initializing pod should read its init progress, and a pod
// being deleted should read Terminating. Reporting the bare phase misreads all three.
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
    // Skip past init containers that finished cleanly, and sidecars that are already up.
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

  // Regular containers (last wins, as kubectl scans in reverse) once init is done.
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
    header: 'Ready',
    className: 'tabular-nums',
    cell: (o) => {
      const b = body<PodBody>(o);
      const cs = b.status?.containerStatuses ?? [];
      // Denominator is the spec's container count (kubectl's), so a pod whose statuses
      // haven't all landed yet still reads against the expected total.
      const total = b.spec?.containers?.length ?? cs.length;
      if (!total) return DASH;
      return `${cs.filter((c) => c.ready).length}/${total}`;
    },
  },
  {
    header: 'Status',
    cell: (o) => podStatus(body<PodBody>(o)),
  },
  {
    header: 'Restarts',
    className: 'tabular-nums',
    cell: (o) => {
      const st = body<PodBody>(o).status;
      // Init-container retries count too: while a pod initializes they're the only restarts
      // recorded (containerStatuses is still empty), and a restartable init container
      // (sidecar) keeps accruing them afterwards.
      const all = [...(st?.initContainerStatuses ?? []), ...(st?.containerStatuses ?? [])];
      if (all.length === 0) return DASH;
      return all.reduce((n, c) => n + (c.restartCount ?? 0), 0);
    },
  },
];

const workloadColumns: ObjectColumn[] = [
  {
    header: 'Ready',
    className: 'tabular-nums',
    cell: (o) => {
      const b = body<WorkloadBody>(o);
      return `${b.status?.readyReplicas ?? 0}/${b.spec?.replicas ?? 0}`;
    },
  },
  {
    header: 'Up-to-date',
    className: 'tabular-nums',
    cell: (o) => body<WorkloadBody>(o).status?.updatedReplicas ?? 0,
  },
  {
    header: 'Available',
    className: 'tabular-nums',
    cell: (o) => body<WorkloadBody>(o).status?.availableReplicas ?? 0,
  },
];

// Keyed by gvrKey — the same canonical kind key the watch provenance and kinds catalog use.
const OBJECT_COLUMNS: Record<string, ObjectColumn[]> = {
  [gvrKey({ apiVersion: 'v1', resource: 'pods' })]: podColumns,
  [gvrKey({ apiVersion: 'apps/v1', resource: 'deployments' })]: workloadColumns,
};

// The kind-specific columns for a GVR, or [] for an unregistered kind (universal columns only).
export function columnsForKind(gvr: GVR): ObjectColumn[] {
  return OBJECT_COLUMNS[gvrKey(gvr)] ?? [];
}
