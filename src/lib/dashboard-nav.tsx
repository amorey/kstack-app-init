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

// The live dashboard resource tree: curated base merged with the active cluster's
// discovered kinds (thin builder over `useClusterCachedDataKinds`). Unsynced cluster →
// curated-only. See docs/adr/2026-08-09-dashboard-nav-merge.md
import { useMemo } from 'react';

import { useClusterCachedDataKinds } from '@/lib/cluster-cached-data-kinds';
import { buildDashboardNav } from '@/lib/dashboard-resources';
import type { DashboardNavNode } from '@/lib/dashboard-resources';
import type { WatchPhase } from '@/lib/graphql/use-watch-subscription';

// While paused (`active` false), `phase`'s `connected` is meaninglessly false — a
// "reconnecting/loading" affordance must gate on `active`, not `phase` alone, to
// stay silent on the curated-only fallback.
export function useDashboardNav(): { nav: DashboardNavNode[]; active: boolean; phase: WatchPhase } {
  const { kinds, active, phase } = useClusterCachedDataKinds();
  const nav = useMemo(() => buildDashboardNav(kinds), [kinds]);
  return { nav, active, phase };
}
