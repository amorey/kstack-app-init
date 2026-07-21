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
// Each object carries its typed universal identity (uid/namespace/name/creationTimestamp)
// plus `rawJSON`, the full native body — the frontend casts the body to a typed Kubernetes
// object and derives kind-specific columns from it (see ObjectsTable).
//
// Reduction runs through the shared `useCacheDeltaWatch`, but with a fuller provenance key
// than the kinds/events watches: cacheID + apiVersion + resource, because this watch is
// keyed by kind too. Every frame carries that triple, so a straggler from a superseded cache
// *or* from the previous kind's still-draining subscription (same cache, different resource)
// is dropped, and the first frame after a cache swap or resource switch starts fresh — two
// caches' *or* two kinds' objects never mix. Rows are sorted by (namespace, name),
// kubectl-style.
import { useMemo } from 'react';

import { graphql } from '@/gql';
import type { ClusterDataObjectsWatchSubscription as ClusterDataObjectsWatchSubscriptionType } from '@/gql/graphql';
import { useActiveCluster } from '@/lib/active-cluster';
import { useCacheDeltaWatch, joinProvenance } from '@/lib/graphql/use-cache-delta-watch';
import type { WatchPhase } from '@/lib/graphql/use-watch-subscription';
import { gvrKey } from '@/lib/gvr';
import type { GVR } from '@/lib/gvr';

const ClusterDataObjectsWatchSubscription = graphql(`
  subscription ClusterDataObjectsWatch($id: ObjectID!, $cacheID: ObjectID!, $apiVersion: String!, $resource: String!) {
    clusterDataObjectsWatch(id: $id, cacheID: $cacheID, apiVersion: $apiVersion, resource: $resource) {
      type
      cacheID
      apiVersion
      resource
      object {
        uid
        apiVersion
        kind
        namespace
        name
        creationTimestamp
        rawJSON
      }
    }
  }
`);

// One cached object (the `object` payload of a change), as the table renders a row from.
export type ClusterDataObject = ClusterDataObjectsWatchSubscriptionType['clusterDataObjectsWatch']['object'];

// One collator for the (namespace, name) sort, hoisted to module scope: the reducer hands
// back a new array on every delta frame, so the whole list re-sorts per frame — and
// `String.prototype.localeCompare` re-derives a collator on each call, which is the slowest
// way to compare strings in JS. Reusing one instance keeps a large kind's re-sort cheap.
const COLLATOR = new Intl.Collator();

// The active context's cached objects of `kind`, updated live. Empty while
// clusters/objects haven't loaded (no active cluster, or an unsynced one — no active cache,
// so the watch is paused). `active` = the watch is live (a cluster + active cache to stream
// from); `phase` classifies connecting vs. empty-snapshot for a spinner, mirroring
// `useClusterDataEvents`.
//
// Provenance carries the FULL key this watch is keyed on — cacheID + apiVersion + resource —
// not just cacheID like the kinds/events watches, because switching resources within one
// cache keeps the cacheID: without the kind in the key, the previous kind's retained set /
// stragglers would leak into the new kind's table. `useCacheDeltaWatch` drops any frame
// whose provenance doesn't match and starts fresh on a change.
export function useClusterDataObjects(kind: GVR): {
  objects: ClusterDataObject[];
  active: boolean;
  phase: WatchPhase;
} {
  const { clusterID, cacheID, active } = useActiveCluster();

  const { items, phase } = useCacheDeltaWatch<ClusterDataObjectsWatchSubscriptionType, ClusterDataObject>(
    {
      query: ClusterDataObjectsWatchSubscription,
      variables: { id: clusterID ?? '', cacheID: cacheID ?? '', apiVersion: kind.apiVersion, resource: kind.resource },
      pause: !active,
    },
    {
      select: (d) => {
        const f = d.clusterDataObjectsWatch;
        return { type: f.type, entity: f.object, provenance: joinProvenance(f.cacheID, gvrKey(f)) };
      },
      keyOf: (o) => o.uid,
      currentProvenance: joinProvenance(cacheID ?? '', gvrKey(kind)),
    },
  );

  // Sort by (namespace, name) so the table order is stable across delta churn.
  const objects = useMemo(
    () => [...items].sort((a, b) => COLLATOR.compare(a.namespace, b.namespace) || COLLATOR.compare(a.name, b.name)),
    [items],
  );

  return { objects, active, phase };
}
