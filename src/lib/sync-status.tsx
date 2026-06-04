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

// Engine sync-health, surfaced to the renderer.
//
// TODO(sync-status): the `syncStatusWatch` GraphQL surface this provider used to
// consume was removed from the sidecar schema during the auth/cloud refactor and
// not yet re-added. The prefsync engine still tracks status (Connecting / Live /
// Backoff / Offline + LastError + LastSyncedAt via Engine.Status()), but it isn't
// exposed over GraphQL — re-wiring it needs a schema SyncStatus type + a resolver
// over a status watch hub. Until then this provider reports `null` (the
// SyncHealthBadge degrades to its "Connecting…" muted state). Pre-existing on this
// branch; stubbed here only to unblock codegen, tracked as separate work.
import { createContext, useContext, useMemo } from 'react';

// Mirrors the engine's status snapshot the SyncHealthBadge renders, kept as a
// hand-written type until the GraphQL surface is restored (see TODO above).
export type SyncStatus = {
  state: 'CONNECTING' | 'LIVE' | 'BACKOFF' | 'OFFLINE';
  lastError: string | null;
  lastSyncedAt: number;
  retryAt: number;
};

type SyncStatusContextValue = {
  status: SyncStatus | null;
};

const SyncStatusContext = createContext<SyncStatusContextValue | null>(null);

export function SyncStatusProvider({ children }: { children: React.ReactNode }) {
  // Stubbed: no status source wired yet (see TODO above). null = "not reported".
  const value = useMemo<SyncStatusContextValue>(() => ({ status: null }), []);
  return <SyncStatusContext.Provider value={value}>{children}</SyncStatusContext.Provider>;
}

export function useSyncStatus(): SyncStatusContextValue {
  const ctx = useContext(SyncStatusContext);
  if (!ctx) throw new Error('useSyncStatus must be used inside <SyncStatusProvider>');
  return ctx;
}

const SECOND = 1_000;
const MINUTE = 60 * SECOND;
const HOUR = 60 * MINUTE;

/**
 * Relative age of the last successful reconcile. `lastSyncedAtMs` is the
 * engine's Unix-millis (0 = never). Bucketed and computed at render time.
 * Deliberately not ticking: the engine coalesces and stops pushing once
 * healthy, so on a long-idle (but fine) app this label can read staler
 * than reality. Accepted — a low-stakes badge isn't worth a timer and
 * per-second app-wide re-renders.
 */
export function formatSyncFreshness(lastSyncedAtMs: number, now: number = Date.now()): string {
  if (lastSyncedAtMs === 0) return 'never synced';
  const diff = Math.max(0, now - lastSyncedAtMs);
  if (diff < 5 * SECOND) return 'just now';
  if (diff < MINUTE) return `${Math.floor(diff / SECOND)}s ago`;
  if (diff < HOUR) return `${Math.floor(diff / MINUTE)}m ago`;
  return `${Math.floor(diff / HOUR)}h ago`;
}
