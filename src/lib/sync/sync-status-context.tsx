// Engine sync-health, surfaced to the renderer. The always-on sidecar
// engine owns the single upstream connection and publishes its health on
// the `syncStatusWatch` subscription (snapshot first, then live changes
// fanned out from the engine's hub — same shape as settingsWatch). This
// provider just adapts that stream into context; the subscription's own
// auto-reconnect (subscribe-exchange) keeps it attached across sleep.
import { createContext, useContext, useMemo } from 'react';
import { useSubscription } from 'urql';

import { graphql } from '@/gql';
import type { SyncStatusWatchSubscription } from '@/gql/graphql';

/** Engine sync health, as delivered by `syncStatusWatch`. */
export type SyncStatus = SyncStatusWatchSubscription['syncStatusWatch'];

const SyncStatusWatchSubscription = graphql(`
  subscription SyncStatusWatch {
    syncStatusWatch {
      state
      lastError
      lastSyncedAt
      retryAt
    }
  }
`);

type SyncStatusContextValue = {
  /** Latest engine status; null until the first push lands. */
  status: SyncStatus | null;
};

const SyncStatusContext = createContext<SyncStatusContextValue | null>(null);

export function SyncStatusProvider({ children }: { children: React.ReactNode }) {
  const [{ data }] = useSubscription({ query: SyncStatusWatchSubscription });
  const status = data?.syncStatusWatch ?? null;
  // No `error` branch on purpose: subscribe-exchange reconnects transport
  // drops internally and never surfaces a terminal error here, so a null
  // status means "not reported yet", never "failed". The memo keeps the
  // context value stable across parent re-renders that aren't a new push
  // (urql returns the same result identity between pushes).
  const value = useMemo<SyncStatusContextValue>(() => ({ status }), [status]);
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
