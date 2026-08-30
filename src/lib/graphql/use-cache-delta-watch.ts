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

// Frontend mirror of the sidecar's `cacheDeltaWatch[T]`: the shared engine behind
// every per-cache delta watch (`useClusterDataKinds`/`Events`/`Objects`). Folds the
// delta stream into an id-keyed map and owns the correctness-critical provenance
// guard: a frame whose provenance ≠ `currentProvenance` is dropped (straggler from a
// superseded subscription), and the accumulator restarts when the current provenance
// changes. The provenance string must carry EVERY dimension the watch is keyed on
// (cacheID for kinds/events; cacheID + apiVersion + resource for objects) — miss one
// and that dimension's stragglers mis-attribute. Transport reconnects (same
// provenance, full replay) are handled underneath by `useWatchSubscription`.
// See docs/adr/2026-08-09-delta-watch-protocol.md
//
// Items are returned in insertion order; sorting stays with the caller.
import { useMemo } from 'react';
import type { AnyVariables, UseSubscriptionArgs } from 'urql';

import { applyChange } from '@/lib/clusters';
import type { Keyed } from '@/lib/clusters';
import { useWatchSubscription, watchPhase } from '@/lib/graphql/use-watch-subscription';
import type { WatchPhase } from '@/lib/graphql/use-watch-subscription';

// The reduced set, tagged with the provenance its items were folded under.
// `synced` flips on the Bookmark closing the snapshot; until then `items` is a
// partial snapshot, not the collection.
type DeltaSet<T> = { provenance: string; items: Keyed<T>; synced: boolean };

// One frame as `select`ed: change kind, entity, and the frame's own provenance
// (built off the same dimensions as `currentProvenance`). The Bookmark is the only
// frame the server sends with no entity, but it is identified by `type` — a null
// entity on a change is a server-side field error, not a snapshot boundary.
export type DeltaFrame<T> = { type: string; entity: T | null; provenance: string };

// Joins provenance dimensions into one opaque key. The separator is the unit
// separator rather than anything a dimension could contain — written as an escape,
// since a literal control byte would make git read this file as binary.
export function joinProvenance(...parts: string[]): string {
  return parts.join('\u001f');
}

// Runs a per-cache delta watch; returns reduced items (insertion order) plus `phase`.
// Liveness (`active`) stays with the caller, which derived `pause` from it.
export function useCacheDeltaWatch<Data, T>(
  args: UseSubscriptionArgs<AnyVariables, Data>,
  opts: {
    // Extract the change kind, entity, and frame provenance from a raw frame.
    select: (data: Data) => DeltaFrame<T>;
    // The entity's stable map key (uid, or apiVersion/resource for kinds).
    keyOf: (entity: T) => string;
    // The active subscription's provenance — built off the same dimensions `select` reads.
    currentProvenance: string;
  },
): { items: T[]; phase: WatchPhase } {
  const { select, keyOf, currentProvenance } = opts;

  const { data, connected } = useWatchSubscription(args, (prev: DeltaSet<T> | undefined, frames) => {
    // Restart when the current provenance changed under us (cache swap / kind switch).
    const base = prev && prev.provenance === currentProvenance ? prev : undefined;
    const items = new Map(base?.items);
    let synced = base?.synced ?? false;
    frames.forEach((res) => {
      const { type, entity, provenance } = select(res);
      // Drop stragglers from a superseded subscription.
      if (provenance !== currentProvenance) return;
      // The Bookmark closes the snapshot. Keyed on `type`, never on a missing entity:
      // a nested non-null field erroring nulls its parent, and reading that as the
      // snapshot boundary would declare a still-loading collection complete.
      if (type === 'Bookmark') {
        synced = true;
        return;
      }
      if (!entity) return;
      applyChange(items, type, keyOf(entity), entity);
    });
    return { provenance: currentProvenance, items, synced };
  });

  // Read only a set tagged for the current provenance: on a swap/switch the accumulator
  // still holds the previous one's set for the render in which `currentProvenance` flips,
  // before the resubscribe clears it. Feeds both items and phase.
  const active = data && data.provenance === currentProvenance ? data : undefined;
  const activeItems = active?.items;
  const items = useMemo(() => (activeItems ? [...activeItems.values()] : []), [activeItems]);

  return { items, phase: watchPhase(!!active?.synced, connected) };
}
