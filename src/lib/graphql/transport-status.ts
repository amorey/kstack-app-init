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

// The per-operation transport-status registry: the side-channel that carries a
// subscription's connection state from subscribe-exchange (which owns the
// host-side SSE lifecycle) to useWatchSubscription (which renders it), keyed by
// urql's `operation.key`. It replaces the old in-band synthetic reset frame:
// the reset signal now travels here, off the data channel, so a subscription's
// `data` is only ever real GraphQL data (or `undefined`).
//
// Two fields per key:
//   - `connected` — up now? Flipped true on the host's `open` frame, false on
//     any drop. Consumers render "connecting/reconnecting" from it.
//   - `generation` — a **process-wide monotonic serial** stamped at each
//     successful `open` (never bumped on a drop). It's the reset primitive: the
//     hook rebuilds its accumulator whenever the generation it folded under no
//     longer matches the current one, so a reconnect's snapshot replaces
//     prior-connection state without a data frame. It is deliberately *global*,
//     not a per-key counter: urql retains a `useSubscription` accumulator across
//     a variables change (a new op key) and across a same-key pause/re-execute,
//     so a per-key counter restarting at 1 could match that carried-over tag and
//     fold new frames onto stale state. A globally-unique serial guarantees the
//     retained tag can never alias a different (or reopened) connection's.
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

// The next connection-open serial. Monotonic across the whole process (all
// keys), so every open is globally unique — see `generation` on TransportStatus.
let connectionSerial = 0;

// A connection opened: up, and a fresh globally-unique generation (the hook's
// reset trigger).
export function markConnected(key: number) {
  connectionSerial += 1;
  set(key, { connected: true, generation: connectionSerial });
}

// A connection dropped: down, but the generation is untouched — a drop is not a
// new connection, so the hook keeps its last-known data through the outage.
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
