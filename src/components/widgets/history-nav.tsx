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

// Browser-style back/forward buttons walking the router's history stack.
//
// Availability must derive from TanStack's per-entry `__TSR_index`, never
// `window.history.length` — as a standalone site, that count includes entries
// predating the app. Forward compares the index against a ceiling of the highest
// still-reachable index, persisted in `sessionStorage` because the browser keeps
// forward entries across a reload (resetting it would disable Forward while
// `history.forward()` still works). The stored value is tagged with `__TSR_key`
// and trusted only on a match, so a new visit in the same tab can't inherit a
// dead Forward.
// See docs/adr/2026-08-09-url-params-as-window-state.md
import { useCallback, useRef, useSyncExternalStore } from 'react';

import { useRouter } from '@tanstack/react-router';
import type { RouterHistory } from '@tanstack/react-router';
import { ArrowLeft, ArrowRight } from 'lucide-react';

import { Button } from '@kubetail/ui/elements/button';

// Exported so tests can seed a pre-reload value.
export const FORWARD_CEILING_STORAGE_KEY = 'kstack:history-forward-ceiling';

type StoredCeiling = { ceiling: number; key: string };

// Never memoize on `history`: its identity is stable while index/key change on
// every navigation, so anything captured goes stale.
function getCurrentIndex(history: RouterHistory): number {
  return history.location.state.__TSR_index ?? 0;
}
function getCurrentKey(history: RouterHistory): string {
  return history.location.state.__TSR_key ?? '';
}

// 0 unless the stored ceiling belongs to the current entry (`entryKey`).
function readStoredCeiling(entryKey: string): number {
  const raw = sessionStorage.getItem(FORWARD_CEILING_STORAGE_KEY);
  if (!raw) return 0;
  try {
    const { ceiling, key } = JSON.parse(raw) as StoredCeiling;
    return key === entryKey && Number.isFinite(ceiling) ? ceiling : 0;
  } catch {
    return 0;
  }
}

function useHistoryNav() {
  const { history } = useRouter();

  // Highest index still reachable forward; never below the current entry.
  // Lazily initialized once per mount.
  const ceilingRef = useRef<number | null>(null);
  if (ceilingRef.current === null) {
    ceilingRef.current = Math.max(readStoredCeiling(getCurrentKey(history)), getCurrentIndex(history));
  }

  const subscribe = useCallback(
    (onChange: () => void) =>
      history.subscribe(({ action }) => {
        // A push truncates forward entries; every other action leaves the
        // reachable top intact.
        const index = getCurrentIndex(history);
        const ceiling = action.type === 'PUSH' ? index : Math.max(ceilingRef.current ?? 0, index);
        ceilingRef.current = ceiling;
        sessionStorage.setItem(FORWARD_CEILING_STORAGE_KEY, JSON.stringify({ ceiling, key: getCurrentKey(history) }));
        onChange();
      }),
    [history],
  );

  const canGoBack = useSyncExternalStore(subscribe, () => getCurrentIndex(history) > 0);
  const canGoForward = useSyncExternalStore(subscribe, () => getCurrentIndex(history) < (ceilingRef.current ?? 0));

  return {
    canGoBack,
    canGoForward,
    goBack: () => history.back(),
    goForward: () => history.forward(),
  };
}

export function HistoryNav() {
  const { canGoBack, canGoForward, goBack, goForward } = useHistoryNav();

  const buttons = [
    { label: 'Go back', Icon: ArrowLeft, enabled: canGoBack, onClick: goBack },
    {
      label: 'Go forward',
      Icon: ArrowRight,
      enabled: canGoForward,
      onClick: goForward,
    },
  ];

  return (
    <div className="flex shrink-0 items-center gap-0.5">
      {buttons.map(({ label, Icon, enabled, onClick }) => (
        <Button
          key={label}
          variant="ghost"
          size="icon-sm"
          aria-label={label}
          disabled={!enabled}
          onClick={onClick}
          className="text-muted-foreground"
        >
          <Icon className="h-4 w-4" aria-hidden />
        </Button>
      ))}
    </div>
  );
}
