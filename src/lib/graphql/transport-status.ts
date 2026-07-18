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

// Per-operation transport-status registry, keyed by urql's `operation.key`. Carries
// a subscription's connection state from subscribe-exchange (which owns the SSE
// lifecycle) to useWatchSubscription.
//
//   - `connected` — is the connection up right now?
//   - `generation` — a process-wide monotonic serial stamped on each successful open.
//     The hook rebuilds its accumulator when the generation it folded under stops
//     matching, so a reconnect's snapshot replaces prior state. It's global rather
//     than per-key because urql retains accumulators across variable changes and
//     pause/re-execute, where a per-key counter could alias a carried-over tag.
export type TransportStatus = { connected: boolean; generation: number };

// A single shared frozen default so an unknown key returns a referentially
// stable snapshot — useSyncExternalStore's getSnapshot must not return a fresh
// object each read or it re-renders forever.
const DEFAULT: TransportStatus = Object.freeze({ connected: false, generation: 0 });

type Entry = {
  // The current snapshot, replaced (not mutated) on every change so identity
  // tracks change — again for useSyncExternalStore.
  snapshot: TransportStatus;
  listeners: Set<() => void>;
};

const registry = new Map<number, Entry>();

function entryFor(key: number): Entry {
  let entry = registry.get(key);
  if (!entry) {
    entry = { snapshot: DEFAULT, listeners: new Set() };
    registry.set(key, entry);
  }
  return entry;
}

function set(key: number, next: TransportStatus) {
  const entry = entryFor(key);
  // No-op if nothing actually changed, so a repeated drop (each failed reconnect
  // dial calls markDisconnected) doesn't churn a fresh snapshot and re-render.
  if (entry.snapshot.connected === next.connected && entry.snapshot.generation === next.generation) return;
  entry.snapshot = next;
  entry.listeners.forEach((cb) => cb());
}

// The current status for a key (the disconnected default if never touched).
export function getStatus(key: number): TransportStatus {
  return registry.get(key)?.snapshot ?? DEFAULT;
}

// Register a change listener (useSyncExternalStore's subscribe); returns an
// unsubscribe. Safe to call before the exchange has ever marked the key.
export function subscribeStatus(key: number, cb: () => void): () => void {
  const entry = entryFor(key);
  entry.listeners.add(cb);
  return () => {
    entry.listeners.delete(cb);
    // Reclaim a keep-alive entry once its last consumer detaches. `clearStatus`
    // can't drop the entry when it runs before the store listener (it resets to
    // DEFAULT instead), so whichever teardown removes the final listener owns
    // reclamation — otherwise the registry grows one entry per distinct op key.
    if (entry.listeners.size === 0) registry.delete(key);
  };
}

// Monotonic across the whole process, so every open is globally unique — see
// `generation` on TransportStatus.
let connectionSerial = 0;

// Mark up with a fresh globally-unique generation (the hook's reset trigger).
export function markConnected(key: number) {
  connectionSerial += 1;
  set(key, { connected: true, generation: connectionSerial });
}

// Mark down but keep the generation — a drop isn't a new connection, so the hook
// keeps its last-known data through the outage.
export function markDisconnected(key: number) {
  set(key, { connected: false, generation: getStatus(key).generation });
}

// Forget a key on consumer teardown. In the common case the last listener is
// already gone (the hook unsubscribed on unmount), so the entry is dropped
// outright. If a straggler consumer is still mounted, keep the entry but return
// it to disconnected (via `set`, reusing its notify + change-guard).
export function clearStatus(key: number) {
  const entry = registry.get(key);
  if (!entry) return;
  if (entry.listeners.size === 0) registry.delete(key);
  else set(key, DEFAULT);
}
