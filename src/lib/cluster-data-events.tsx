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
// rows, not kstack's own control-plane `Event` timeline) as a live list — consumed by
// the dashboard events table today, and reusable anywhere else that needs them (e.g.
// chat). This is the events counterpart of `useDashboardNav`: the active kube-context
// resolves to a cluster (via the registry's kubeconfig source), and
// `clusterDataEventsWatch` streams that cache's events as a delta watch — the newest
// window as an `Added` burst on subscribe, then per-event `Added`/`Modified`/`Deleted`
// (a re-firing event bumps its count/lastSeen → `Modified`). Reduction runs through the
// shared `useCacheDeltaWatch`, keyed by event `uid` with cacheID provenance, so a straggler
// from a superseded cache is dropped and the active cache's first frame after a swap starts
// fresh — two caches' events never mix.
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
export type ClusterDataEvent = ClusterDataEventsWatchSubscriptionType['clusterDataEventsWatch']['event'];

// The active context's cached events, newest first (by lastSeen), updated live. Empty
// while clusters/events haven't loaded (no active cluster, or an unsynced one — it has no
// active cache, so the subscription is paused). `active` = the subscription is live (a
// cluster + active cache to stream from); `phase` classifies connecting vs. empty-snapshot
// for a spinner, mirroring `useDashboardNav`. This watch's variables carry no kind, so its
// provenance is just the cacheID.
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

  // Newest first by lastSeen. Parse each lastSeen once (a decorate–sort–undecorate), not per
  // comparison — a plain comparator would call Date.parse ~2·n·log n times, and a
  // lexicographic string sort is unsafe here since RFC3339 millis timestamps mix fractional
  // and whole-second forms. A null lastSeen (source Event with no timestamp) sorts oldest
  // (last, descending).
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
