// Compact, always-visible engine sync indicator. Unlike ConnectionStatus
// (a transient, auto-dismissing error banner), this is a persistent pill
// reflecting the always-on engine's current health. Layout-neutral — the
// caller positions it.
import { formatSyncFreshness, useSyncStatus } from './sync-status-context';

type Tone = 'ok' | 'warn' | 'bad' | 'muted';

const TONE_CLASS: Record<Tone, string> = {
  ok: 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-400',
  warn: 'bg-amber-500/15 text-amber-700 dark:text-amber-400',
  bad: 'bg-destructive/15 text-destructive',
  muted: 'bg-muted text-muted-foreground',
};

export function SyncHealthBadge() {
  const { status } = useSyncStatus();

  // Default covers both CONNECTING and the pre-first-push null state.
  let tone: Tone = 'muted';
  let label = 'Connecting…';

  if (status) {
    switch (status.state) {
      case 'LIVE':
        tone = 'ok';
        label = `Synced · ${formatSyncFreshness(status.lastSyncedAt)}`;
        break;
      case 'BACKOFF':
        tone = 'warn';
        label = 'Reconnecting…';
        break;
      case 'OFFLINE':
        tone = 'bad';
        label = status.lastError ? `Offline — ${status.lastError}` : 'Offline';
        break;
      default:
        break;
    }
  }

  return (
    <div
      role="status"
      aria-live="polite"
      className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium ${TONE_CLASS[tone]}`}
    >
      <span aria-hidden className="size-1.5 rounded-full bg-current opacity-70" />
      {label}
    </div>
  );
}
