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

// Slide-out panel listing every cluster in the app's registry — those in the
// current kubeconfig plus any with a leftover cache. Each row shows its status
// (syncing / paused / orphaned), freshness, and cache size; the toggle enables
// or disables syncing, and orphaned/cached rows can have their cache deleted.
// The caller positions the trigger (today: the top-right toolbar). Data + state
// come from the sidecar via useClusters() — the toggle and delete write through
// to it, and the resulting clustersWatch push updates the row.
import { Database, Trash2 } from 'lucide-react';
import { useMutation } from 'urql';

import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from '@kubetail/ui/elements/sheet';
import { Switch } from '@kubetail/ui/elements/switch';

import { graphql } from '@/gql';
import { type Cluster, formatBytes, useClusters } from '@/lib/clusters';
import { errorMessage, reportError } from '@/lib/error-bus';
import { formatSyncFreshness } from '@/lib/sync-status';

const SetClusterEnabledMutation = graphql(`
  mutation SetClusterEnabled($uuid: String!, $enabled: Boolean!) {
    setClusterEnabled(uuid: $uuid, enabled: $enabled) {
      uuid
      enabled
    }
  }
`);

const DeleteClusterCacheMutation = graphql(`
  mutation DeleteClusterCache($uuid: String!) {
    deleteClusterCache(uuid: $uuid)
  }
`);

type Tone = 'ok' | 'warn' | 'muted';

const DOT_CLASS: Record<Tone, string> = {
  ok: 'bg-emerald-500',
  warn: 'bg-amber-500',
  muted: 'bg-muted-foreground/50',
};

// A cluster's status is derived from its flags: a disabled cluster is Paused;
// an enabled one whose context has left the kubeconfig is an Orphaned cache
// (a cleanup candidate, hence amber); otherwise it's actively Syncing.
function statusOf(c: Cluster): { label: string; tone: Tone } {
  if (!c.enabled) return { label: 'Paused', tone: 'muted' };
  if (!c.present) return { label: 'Orphaned', tone: 'warn' };
  return { label: 'Syncing', tone: 'ok' };
}

function ClusterRow({
  cluster,
  onToggle,
  onDelete,
}: {
  cluster: Cluster;
  onToggle: (enabled: boolean) => void;
  onDelete: () => void;
}) {
  const status = statusOf(cluster);
  const detail = [
    cluster.cached ? formatBytes(cluster.cacheBytes) : null,
    cluster.lastSyncedAt ? formatSyncFreshness(cluster.lastSyncedAt) : null,
  ]
    .filter(Boolean)
    .join(' · ');
  return (
    <li className="flex items-center gap-3 py-2">
      <span aria-hidden className={`size-2 shrink-0 rounded-full ${DOT_CLASS[status.tone]}`} />
      <div className="min-w-0 flex-1">
        <div className="truncate text-sm font-medium">{cluster.name || cluster.uuid}</div>
        <div className="text-xs text-muted-foreground">
          {/* Status in its own span so it stays individually addressable. */}
          <span>{status.label}</span>
          {detail ? <span>{` · ${detail}`}</span> : null}
        </div>
      </div>
      {cluster.cached ? (
        <button
          type="button"
          aria-label={`Delete cache for ${cluster.name || cluster.uuid}`}
          onClick={onDelete}
          className="shrink-0 rounded p-1 text-muted-foreground outline-none hover:text-destructive focus-visible:ring-2 focus-visible:ring-ring"
        >
          <Trash2 className="size-3.5" aria-hidden />
        </button>
      ) : null}
      <Switch
        size="sm"
        aria-label={`Sync ${cluster.name || cluster.uuid}`}
        checked={cluster.enabled}
        onCheckedChange={onToggle}
        className="shrink-0"
      />
    </li>
  );
}

export function ClusterSyncPanel() {
  const { clusters } = useClusters();
  const rows = clusters ?? [];

  // Toggle/delete write through to the sidecar; the resulting clustersWatch
  // push is the source of truth for each row's state (no local mirror). urql's
  // execute resolves (never rejects) with an OperationResult, so a failed
  // mutation would otherwise be swallowed — surface its error on the bus.
  const [, setClusterEnabledMut] = useMutation(SetClusterEnabledMutation);
  const [, deleteClusterCacheMut] = useMutation(DeleteClusterCacheMutation);

  const setClusterEnabled = (uuid: string, enabled: boolean) => {
    setClusterEnabledMut({ uuid, enabled }).then((result) => {
      if (result.error) {
        reportError({ source: 'graphql', message: errorMessage(result.error), cause: result.error });
      }
    });
  };

  const deleteClusterCache = (uuid: string) => {
    deleteClusterCacheMut({ uuid }).then((result) => {
      if (result.error) {
        reportError({ source: 'graphql', message: errorMessage(result.error), cause: result.error });
      }
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
          <SheetTitle>Clusters</SheetTitle>
          <SheetDescription>Clusters in your kubeconfig and any leftover local caches.</SheetDescription>
        </SheetHeader>
        {rows.length === 0 ? (
          <p className="px-4 py-6 text-sm text-muted-foreground">No clusters yet.</p>
        ) : (
          <ul className="divide-y px-4">
            {rows.map((c) => (
              <ClusterRow
                key={c.uuid}
                cluster={c}
                onToggle={(enabled) => setClusterEnabled(c.uuid, enabled)}
                onDelete={() => deleteClusterCache(c.uuid)}
              />
            ))}
          </ul>
        )}
      </SheetContent>
    </Sheet>
  );
}
