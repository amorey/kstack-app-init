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

// The active cluster's cached Kubernetes Events (the synced `events.k8s.io`/core Event
// rows, not kstack's own control-plane `Event` timeline) as a live list.
// `clusterDataEventsWatch` is a per-cache delta watch (a re-firing event bumps
// count/lastSeen → `Modified`). Reduced via `useCacheDeltaWatch`, keyed by `uid` with
// cacheID provenance so two caches' events never mix —
// see docs/adr/2026-08-09-delta-watch-protocol.md
import { useMemo } from 'react';

import { graphql } from '@/gql';
import type { ClusterDataEventsWatchSubscription as ClusterDataEventsWatchSubscriptionType } from '@/gql/graphql';
import { useActiveCluster } from '@/lib/active-cluster';
import { useCacheDeltaWatch, joinProvenance } from '@/lib/graphql/use-cache-delta-watch';
import type { WatchPhase } from '@/lib/graphql/use-watch-subscription';

const ClusterDataEventsWatchSubscription = graphql(`
  subscription ClusterDataEventsWatch($id: ObjectID!, $cacheID: ObjectID!) {
    clusterDataEventsWatch(id: $id, cacheID: $cacheID) {
      type
      cacheID
      event {
        uid
        type
        reason
        message
        count
        firstSeen
        lastSeen
        involvedKind
        involvedNamespace
        involvedName
      }
    }
  }
`);

// One cached event (the `event` payload of a change), as the table renders it.
export type ClusterDataEvent = NonNullable<ClusterDataEventsWatchSubscriptionType['clusterDataEventsWatch']['event']>;

// The active context's cached events, newest first (by lastSeen), live. Paused
// (empty) without an active cache. Provenance is just the cacheID — the variables
// carry no kind.
export function useClusterDataEvents(): { events: ClusterDataEvent[]; active: boolean; phase: WatchPhase } {
  const { clusterID, cacheID, active } = useActiveCluster();

  const { items, phase } = useCacheDeltaWatch<ClusterDataEventsWatchSubscriptionType, ClusterDataEvent>(
    {
      query: ClusterDataEventsWatchSubscription,
      variables: { id: clusterID ?? '', cacheID: cacheID ?? '' },
      pause: !active,
    },
    {
      select: (d) => {
        const f = d.clusterDataEventsWatch;
        return { type: f.type, entity: f.event, provenance: joinProvenance(f.cacheID) };
      },
      keyOf: (e) => e.uid,
      currentProvenance: joinProvenance(cacheID ?? ''),
    },
  );

  // Newest first by lastSeen; decorate–sort–undecorate so Date.parse runs once per row.
  // A lexicographic string sort is unsafe (RFC3339 mixes fractional and whole-second
  // forms). Null lastSeen sorts oldest.
  const events = useMemo(
    () =>
      [...items]
        .map((e) => ({ e, ms: e.lastSeen ? Date.parse(e.lastSeen) : 0 }))
        .sort((a, b) => b.ms - a.ms)
        .map((x) => x.e),
    [items],
  );

  return { events, active, phase };
}
