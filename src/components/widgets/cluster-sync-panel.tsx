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

// Slide-out panel listing every cluster the sync engine is mirroring into
// the local cache: its sync state, freshness, and live download rate. The
// caller positions the trigger (today: the top-right toolbar, next to
// SyncHealthBadge). Data comes from useClusterSync(); a follow-up wires the
// backend resolver to the engine — until then the list is empty.
import { useState } from 'react';
import { Database } from 'lucide-react';

import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from '@kubetail/ui/elements/sheet';
import { Switch } from '@kubetail/ui/elements/switch';

import { type ClusterSyncStatus, formatDownloadRate, useClusterSync } from '@/lib/cluster-sync';
import { formatSyncFreshness } from '@/lib/sync-status';

type Tone = 'ok' | 'warn' | 'bad' | 'muted';

// Per-state dot color, mirroring SyncHealthBadge's tone palette. PENDING is
// muted (nothing flowing yet), SYNCING/BACKOFF warn (in motion / retrying),
// LIVE ok, OFFLINE bad.
const STATE_TONE: Record<ClusterSyncStatus['state'], Tone> = {
  PENDING: 'muted',
  SYNCING: 'warn',
  LIVE: 'ok',
  BACKOFF: 'warn',
  OFFLINE: 'bad',
};

const DOT_CLASS: Record<Tone, string> = {
  ok: 'bg-emerald-500',
  warn: 'bg-amber-500',
  bad: 'bg-destructive',
  muted: 'bg-muted-foreground/50',
};

function ClusterRow({
  cluster,
  enabled,
  onToggle,
}: {
  cluster: ClusterSyncStatus;
  enabled: boolean;
  onToggle: (enabled: boolean) => void;
}) {
  // A disabled cluster reads as muted regardless of its last reported state.
  const tone = enabled ? STATE_TONE[cluster.state] : 'muted';
  const detail = [cluster.lastSyncedAt ? formatSyncFreshness(cluster.lastSyncedAt) : null, cluster.lastError || null]
    .filter(Boolean)
    .join(' · ');
  return (
    <li className="flex items-center gap-3 py-2">
      <span aria-hidden className={`size-2 shrink-0 rounded-full ${DOT_CLASS[tone]}`} />
      <div className="min-w-0 flex-1">
        <div className="truncate text-sm font-medium">{cluster.context}</div>
        <div className="text-xs text-muted-foreground">
          {/* State in its own span so it stays individually addressable. */}
          <span>{cluster.state}</span>
          {detail ? <span>{` · ${detail}`}</span> : null}
        </div>
      </div>
      <span className="shrink-0 text-xs tabular-nums text-muted-foreground">
        {enabled ? formatDownloadRate(cluster.downloadRateBps) : '—'}
      </span>
      <Switch
        size="sm"
        aria-label={`Sync ${cluster.context}`}
        checked={enabled}
        onCheckedChange={onToggle}
        className="shrink-0"
      />
    </li>
  );
}

export function ClusterSyncPanel() {
  const { clusters } = useClusterSync();
  const rows = clusters ?? [];

  // Per-cluster sync enable/disable. Local-only and no-op for now: toggling
  // flips the UI but doesn't touch the sidecar. A follow-up wires this to a
  // mutation that pauses/resumes the engine's per-cluster sync. We track the
  // *disabled* set (default = all enabled) so clusters that appear later
  // start enabled without seeding state.
  const [disabled, setDisabled] = useState<ReadonlySet<string>>(new Set());
  const setEnabled = (context: string, enabled: boolean) => {
    setDisabled((prev) => {
      const next = new Set(prev);
      if (enabled) next.delete(context);
      else next.add(context);
      return next;
    });
  };

  return (
    <Sheet>
      <SheetTrigger className="inline-flex items-center gap-1.5 rounded-full bg-muted px-2.5 py-1 text-xs font-medium text-muted-foreground outline-none hover:bg-muted/80 focus-visible:ring-2 focus-visible:ring-ring">
        <Database className="size-3.5" aria-hidden />
        Clusters
      </SheetTrigger>
      <SheetContent side="right" className="w-80">
        <SheetHeader>
          <SheetTitle>Cluster sync</SheetTitle>
          <SheetDescription>Clusters mirrored into the local cache and their download rate.</SheetDescription>
        </SheetHeader>
        {rows.length === 0 ? (
          <p className="px-4 py-6 text-sm text-muted-foreground">No clusters syncing.</p>
        ) : (
          <ul className="divide-y px-4">
            {rows.map((c) => (
              <ClusterRow
                key={c.context}
                cluster={c}
                enabled={!disabled.has(c.context)}
                onToggle={(enabled) => setEnabled(c.context, enabled)}
              />
            ))}
          </ul>
        )}
      </SheetContent>
    </Sheet>
  );
}
