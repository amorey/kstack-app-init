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
// (a re-firing event bumps its count/lastSeen → `Modified`). The reducer keys by event
// `uid` and is cache-aware in the exact same way as the nav hook: every frame carries
// its cache id, so a straggler from a superseded subscription is dropped and the active
// cache's own first frame after a swap starts fresh, so two caches' events never mix.
import { useMemo } from 'react';

import { graphql } from '@/gql';
import type { ClusterDataEventsWatchSubscription as ClusterDataEventsWatchSubscriptionType } from '@/gql/graphql';
import { useActiveKubeContext } from '@/lib/active-kube-context';
import { useClusters, applyChange } from '@/lib/clusters';
import type { Keyed } from '@/lib/clusters';
import { useWatchSubscription, watchPhase } from '@/lib/graphql/use-watch-subscription';
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

// The reduced set: events keyed by uid, tagged with the cache id the frames came from
// (read off each frame, not inferred from render state) so the reducer/read can reject a
// previous cache's events that urql retains across a swap.
type EventSet = { cacheID: string; events: Keyed<ClusterDataEvent> };

// The active context's cached events, newest first (by lastSeen), updated live. Empty
// while clusters/events haven't loaded (no active cluster, or an unsynced one — it has no
// active cache, so the subscription is paused). `active` = the subscription is live (a
// cluster + active cache to stream from); `phase` classifies connecting vs. empty-snapshot
// for a spinner, mirroring `useDashboardNav`.
export function useClusterDataEvents(): { events: ClusterDataEvent[]; active: boolean; phase: WatchPhase } {
  const { context } = useActiveKubeContext();
  const { clusters } = useClusters();

  const cluster = useMemo(
    () => clusters?.find((c) => c.spec.source.kubeconfig?.context === context),
    [clusters, context],
  );
  const clusterID = cluster?.id;
  const cacheID = cluster?.activeCache?.id;

  // The same cache-aware reduction as useDashboardNav: drop a straggler from a superseded
  // subscription (leaving the active cache's set untouched), and start fresh on the active
  // cache's first frame after a swap. A transport reconnect (same cacheID, full replay) is
  // handled by useWatchSubscription resetting the set to `undefined` before the replay.
  const [{ data, connected }] = useWatchSubscription(
    {
      query: ClusterDataEventsWatchSubscription,
      variables: { id: clusterID ?? '', cacheID: cacheID ?? '' },
      pause: !clusterID || !cacheID,
    },
    (prev: EventSet | undefined, res) => {
      const { type, event, cacheID: frameCacheID } = res.clusterDataEventsWatch;
      if (frameCacheID !== cacheID) return prev ?? { cacheID: cacheID ?? '', events: new Map() };
      const events = prev && prev.cacheID === frameCacheID ? prev.events : undefined;
      return { cacheID: frameCacheID, events: applyChange(events, type, event.uid, event) };
    },
  );

  // Read only from a set tagged for the active cache — urql retains the previous cache's
  // `data` across a swap, so reject anything not tagged for the active cache.
  const events = useMemo(() => {
    if (!data || !cacheID || data.cacheID !== cacheID) return [];
    // Parse each lastSeen once (a decorate–sort–undecorate), not per comparison — a plain
    // comparator would call Date.parse ~2·n·log n times, and a lexicographic string sort is
    // unsafe here since RFC3339 millis timestamps mix fractional and whole-second forms.
    // A null lastSeen (source Event with no timestamp) sorts oldest (last, descending).
    return [...data.events.values()]
      .map((e) => ({ e, ms: e.lastSeen ? Date.parse(e.lastSeen) : 0 }))
      .sort((a, b) => b.ms - a.ms)
      .map((x) => x.e);
  }, [cacheID, data]);

  const active = !!(clusterID && cacheID);
  const hasEvents = !!(data && cacheID && data.cacheID === cacheID);
  const phase = watchPhase(hasEvents, connected);
  return { events, active, phase };
}
