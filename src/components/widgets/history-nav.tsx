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

// Browser-style back/forward buttons for the context bar (left of the picker).
// They walk the router's history stack, so any history-pushing navigation is
// reversible — today that's the kube-context switch and the dashboard resource
// nav (see `dashboard-resource-nav.tsx`), and anything added later gets it for
// free.
//
// Availability is derived purely from the *router-managed* stack, never the
// browser's total history length — the frontend may run as a standalone site
// reached via a link, where `window.history` holds entries predating the app
// that must not count. TanStack stamps a 0-based `__TSR_index` on each entry, so
// Back is available past index 0. Forward is available while the current index is
// below the highest still-reachable index — a ceiling we track from the actions
// themselves: a `push` truncates the forward entries (ceiling drops to the new
// index); every other action (back/forward/go/replace) leaves the reachable top
// intact. Both are read through `useSyncExternalStore` subscribed to
// `router.history`, so the buttons re-enable and disable as navigation moves.
//
// The ceiling is persisted in `sessionStorage`, because the browser keeps the
// forward entries across a page reload: reloading from a rewound position must
// restore the ceiling rather than reset it to the (lower) current index, or
// Forward would be wrongly disabled even though `history.forward()` still works.
// `sessionStorage` is scoped to the browsing context (one per window in the app,
// the tab on a standalone site), so it mirrors the per-context history and clears
// when that context closes.
//
// The persisted value is tagged with the current entry's `__TSR_key` (TanStack's
// per-entry id) and trusted only while it still matches. A reload preserves that
// key, so the ceiling survives; but starting a *new* visit in the same tab (leave
// the app, follow a link back) lands on a fresh entry with a new key, so the
// earlier visit's stale ceiling is discarded rather than exposing a dead Forward.
import { useCallback, useRef, useSyncExternalStore } from 'react';

import { useRouter } from '@tanstack/react-router';
import type { RouterHistory } from '@tanstack/react-router';
import { ArrowLeft, ArrowRight } from 'lucide-react';

import { Button } from '@kubetail/ui/elements/button';

// sessionStorage key for the persisted forward ceiling (see the header comment).
// Exported so tests can seed a pre-reload value.
export const FORWARD_CEILING_STORAGE_KEY = 'kstack:history-forward-ceiling';

type StoredCeiling = { ceiling: number; key: string };

// Live reads of the current entry's TanStack index/key. Plain functions of
// `history`, not memoized values: the index and key change on every navigation
// while the `history` object identity is stable, so anything captured with
// `useMemo`/`useCallback` keyed on it would go stale — these must re-read
// `history.location` on each call.
function getCurrentIndex(history: RouterHistory): number {
  return history.location.state.__TSR_index ?? 0;
}
function getCurrentKey(history: RouterHistory): string {
  return history.location.state.__TSR_key ?? '';
}

// The stored ceiling, but only if it belongs to the current history entry
// (`entryKey`); otherwise 0, so a stale value from an earlier visit is ignored.
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

  // The highest index still reachable forward from the current position, seeded
  // from the persisted value (scoped to this entry's key) so it survives a reload
  // but not a new visit, and never below the current entry. Lazily initialized
  // once per mount.
  const ceilingRef = useRef<number | null>(null);
  if (ceilingRef.current === null) {
    ceilingRef.current = Math.max(readStoredCeiling(getCurrentKey(history)), getCurrentIndex(history));
  }

  const subscribe = useCallback(
    (onChange: () => void) =>
      history.subscribe(({ action }) => {
        // A push truncates the forward entries (ceiling drops to the new index);
        // every other action leaves the reachable top intact. Persist under the
        // current entry's key so the ceiling survives a reload of this stack.
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
