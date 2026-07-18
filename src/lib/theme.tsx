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

// The app's theming controller, structured around two orthogonal axes so the second
// slots in later without reworking the first:
//
//   1. Color scheme — the user picks a `ColorSchemePreference` (`system`/`light`/
//      `dark`), resolved to a concrete `ColorScheme` (system defers to
//      `prefers-color-scheme`). The resolved scheme toggles the `.dark` class on
//      `<html>` that Tailwind's dark variant keys on (`@custom-variant dark` in
//      `index.css`; `@kubetail/ui` ships the `.dark` token overrides).
//   2. Theme (skin) — the named palette used within a resolved scheme (e.g.
//      "github-light"). Not built yet: it would be a per-scheme choice applied as a
//      `data-theme` attribute, indexed by the resolved scheme (`resolveColorScheme`
//      is the seam). "theme" is reserved for that skin; this axis is "(color) scheme".
//
// The preference is one field of the host's `host.json`, its source of truth (see
// `lib/host-file.ts`, which owns the boot/change/sync protocol). It reads synchronously
// at boot, so the first paint carries the right scheme — the inline script in
// `index.html` hand-mirrors `resolveColorScheme` against the same injected global, and
// the host paints the window's native background from the same file.
import type { ReactNode } from 'react';
import { createContext, useContext, useEffect, useMemo, useState } from 'react';

import { readInjectedHostFile, subscribeHostFile, updateHostFile } from '@/lib/host-file';

// The user's choice (includes "system"); resolves to a concrete `ColorScheme`.
export type ColorSchemePreference = 'system' | 'light' | 'dark';
// The resolved color scheme actually painted.
export type ColorScheme = 'light' | 'dark';

const DEFAULT_PREFERENCE: ColorSchemePreference = 'system';

function isColorSchemePreference(value: unknown): value is ColorSchemePreference {
  return value === 'system' || value === 'light' || value === 'dark';
}

// The preference from the host-injected `host.json` snapshot, or the default
// ("system", follow the OS) when the file is absent (plain-browser dev), the
// setting is unset, or the value is unrecognized.
export function readInjectedPreference(): ColorSchemePreference {
  const value = readInjectedHostFile().colorSchemePreference;
  return isColorSchemePreference(value) ? value : DEFAULT_PREFERENCE;
}

// The OS-level scheme, used to resolve the "system" preference.
function systemPrefersDark(): boolean {
  return window.matchMedia('(prefers-color-scheme: dark)').matches;
}

// Resolve a preference to the concrete scheme to paint: "system" defers to the OS.
// The seam for the future skin axis (the resolved scheme would index the skin).
export function resolveColorScheme(preference: ColorSchemePreference): ColorScheme {
  if (preference === 'system') return systemPrefersDark() ? 'dark' : 'light';
  return preference;
}

// Mirror a preference's resolved scheme onto the document root as the `.dark` class
// Tailwind keys on. When the skin axis lands, this also sets `data-theme`.
export function applyPreference(preference: ColorSchemePreference): void {
  document.documentElement.classList.toggle('dark', resolveColorScheme(preference) === 'dark');
}

type ThemeContextValue = {
  preference: ColorSchemePreference;
  setPreference: (preference: ColorSchemePreference) => void;
};

const ThemeContext = createContext<ThemeContextValue | null>(null);

// Owns the color-scheme preference; the future skin selection would live here too
// (hence the "Theme" umbrella name, distinct from the scheme it exposes).
export function ThemeProvider({ children }: { children: ReactNode }) {
  const [preference, setPreferenceState] = useState<ColorSchemePreference>(() => readInjectedPreference());

  // Re-apply whenever the preference changes, and — while it's "system" — whenever
  // the OS scheme flips underneath us. The media-query listener only exists for the
  // "system" preference; an explicit Light/Dark choice ignores the OS entirely.
  useEffect(() => {
    applyPreference(preference);
    if (preference !== 'system') return undefined;
    const mql = window.matchMedia('(prefers-color-scheme: dark)');
    const onChange = () => applyPreference('system');
    mql.addEventListener('change', onChange);
    return () => mql.removeEventListener('change', onChange);
  }, [preference]);

  // Track the source of truth: the host broadcasts the merged `host.json` after every
  // write, so a change made in any window (this one included — redundant but idempotent)
  // lands here. An unrecognized value is ignored rather than reset, so a newer host
  // format can't yank the scheme out from under us.
  useEffect(
    () =>
      subscribeHostFile((file) => {
        const value = file.colorSchemePreference;
        if (isColorSchemePreference(value)) setPreferenceState(value);
      }),
    [],
  );

  const value = useMemo<ThemeContextValue>(
    () => ({
      preference,
      setPreference: (next) => {
        // Optimistic: apply locally now, persist in the background.
        setPreferenceState(next);
        updateHostFile({ colorSchemePreference: next });
      },
    }),
    [preference],
  );

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

// Read/set the color-scheme preference. Named for the scheme axis specifically,
// leaving room for a future `useTheme`/skin hook off the same provider.
export function useColorScheme(): ThemeContextValue {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error('useColorScheme must be used within a ThemeProvider');
  return ctx;
}
