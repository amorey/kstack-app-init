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

// The active cluster's cached objects of one kind as a live table feed — the data behind
// the dashboard's per-kind object tables. It's the objects counterpart of
// `useClusterDataEvents`: the active kube-context resolves to a cluster (via the registry's
// kubeconfig source), and `clusterDataObjectsWatch`, keyed by that cluster's active cache
// plus the target kind (`apiVersion` + `resource`), streams the kind's cached objects — the
// newest set as an `Added` burst, then `Added`/`Modified`/`Deleted` keyed by `uid`.
//
// For now each object carries only its typed universal identity (uid/namespace/name/
// creationTimestamp), enough for a Name/Namespace/Age table. The nested object body (and
// kind-specific columns computed from it) is a follow-up — see the "ClusterDataObject —
// native nested body" item in TODO.md.
//
// The delta reducer is cache-aware in the exact same way as `useClusterDataEvents`: every
// frame carries its cache id, so a straggler from a superseded subscription is dropped and
// the active cache's own first frame after a swap starts fresh — two caches' objects never
// mix. Rows are sorted by (namespace, name), kubectl-style.
import { useMemo } from 'react';

import { graphql } from '@/gql';
import type { ClusterDataObjectsWatchSubscription as ClusterDataObjectsWatchSubscriptionType } from '@/gql/graphql';
import { useActiveKubeContext } from '@/lib/active-kube-context';
import { useClusters, applyChange } from '@/lib/clusters';
import type { Keyed } from '@/lib/clusters';
import { useWatchSubscription, watchPhase } from '@/lib/graphql/use-watch-subscription';
import type { WatchPhase } from '@/lib/graphql/use-watch-subscription';

const ClusterDataObjectsWatchSubscription = graphql(`
  subscription ClusterDataObjectsWatch($id: ObjectID!, $cacheID: ObjectID!, $apiVersion: String!, $resource: String!) {
    clusterDataObjectsWatch(id: $id, cacheID: $cacheID, apiVersion: $apiVersion, resource: $resource) {
      type
      cacheID
      object {
        uid
        apiVersion
        kind
        namespace
        name
        creationTimestamp
      }
    }
  }
`);

// One cached object (the `object` payload of a change), as the table renders a row from.
export type ClusterDataObject = ClusterDataObjectsWatchSubscriptionType['clusterDataObjectsWatch']['object'];

// The reduced set: objects keyed by uid, tagged with the cache id the frames came from
// (read off each frame, not inferred from render state) so the reducer/read can reject a
// previous cache's objects that urql retains across a swap.
type ObjectSet = { cacheID: string; objects: Keyed<ClusterDataObject> };

// The kind whose objects to stream — its group/version and plural resource.
export type ObjectKind = { apiVersion: string; resource: string };

// The active context's cached objects of `kind`, updated live. Empty while
// clusters/objects haven't loaded (no active cluster, or an unsynced one — no active cache,
// so the watch is paused). `active` = the watch is live (a cluster + active cache to stream
// from); `phase` classifies connecting vs. empty-snapshot for a spinner, mirroring
// `useClusterDataEvents`.
export function useClusterDataObjects(kind: ObjectKind): {
  objects: ClusterDataObject[];
  active: boolean;
  phase: WatchPhase;
} {
  const { context } = useActiveKubeContext();
  const { clusters } = useClusters();

  const cluster = useMemo(
    () => clusters?.find((c) => c.spec.source.kubeconfig?.context === context),
    [clusters, context],
  );
  const clusterID = cluster?.id;
  const cacheID = cluster?.activeCache?.id;
  const paused = !clusterID || !cacheID;

  // The same cache-aware reduction as useClusterDataEvents: drop a straggler from a
  // superseded subscription (leaving the active cache's set untouched), and start fresh on
  // the active cache's first frame after a swap. A transport reconnect (same cacheID, full
  // replay) is handled by useWatchSubscription resetting the set to `undefined`.
  const [{ data, connected }] = useWatchSubscription(
    {
      query: ClusterDataObjectsWatchSubscription,
      variables: { id: clusterID ?? '', cacheID: cacheID ?? '', apiVersion: kind.apiVersion, resource: kind.resource },
      pause: paused,
    },
    (prev: ObjectSet | undefined, res) => {
      const { type, object, cacheID: frameCacheID } = res.clusterDataObjectsWatch;
      if (frameCacheID !== cacheID) return prev ?? { cacheID: cacheID ?? '', objects: new Map() };
      const objects = prev && prev.cacheID === frameCacheID ? prev.objects : undefined;
      return { cacheID: frameCacheID, objects: applyChange(objects, type, object.uid, object) };
    },
  );

  // The accumulated set, but only when it's tagged for the active cache — urql retains the
  // previous cache's `data` across a swap, so reject anything not tagged for it. This one
  // guard feeds both the rendered rows and the watch phase.
  const activeSet = data && cacheID && data.cacheID === cacheID ? data.objects : undefined;

  // Sort by (namespace, name) so the table order is stable across delta churn.
  const objects = useMemo(
    () =>
      activeSet
        ? [...activeSet.values()].sort((a, b) => a.namespace.localeCompare(b.namespace) || a.name.localeCompare(b.name))
        : [],
    [activeSet],
  );

  return { objects, active: !paused, phase: watchPhase(!!activeSet, connected) };
}
