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

// The window's outer frame for the frameless Linux/Windows window. Those windows
// are `decorations(false)` *and* `transparent`, so the OS draws no border or drop
// shadow — with a same-color desktop behind them the edge is invisible. This
// wraps the whole app in a surface inset a few px from the window edge, clipped to
// rounded corners, with a thin border and a soft outer shadow that paints into the
// transparent gutter — so the window reads as a distinct floating surface.
//
// The gutter (`inset-4`) must be at least as wide as the shadow's reach (blur +
// offset). The OS window rectangle ends at the gutter's outer edge and clips
// anything beyond it, so a shadow that reaches farther than the gutter gets cut off
// into a hard band. Keep the shadow blur/offset small enough to fade out within the
// inset.
//
// The inset works without touching any of the app's fixed chrome (custom title
// bar, floating sidebar): `contain: paint` makes this element the containing block
// for its `position: fixed` descendants, so they anchor to the frame's inset box
// rather than the window edge — without promoting the whole app to a GPU layer
// (and disturbing subpixel text) the way a `transform` would. `overflow-hidden`
// then clips that chrome to the rounded corners.
//
// macOS keeps its native window decorations (the OS draws the rounded corners and
// shadow itself), so the webview stays full-bleed there and this is a passthrough.
// It must wrap the app at the outermost level so the loading and error states get
// the same frame and opaque background as the mounted app.
//
// When the window is maximized there's no off-window space to paint into, so the
// gutter would just read as an empty transparent band around the app (native apps
// like Firefox drop their rounded corners + shadow here too). We collapse to a
// full-bleed `inset-0` surface — no inset, corners, border, or shadow — while
// maximized, and `WindowResizeHandles` hides its grips to match.
import type { ReactNode } from 'react';
import { useEffect } from 'react';

import { isMacOS } from '@/lib/platform';
import { useWindowMaximized } from '@/lib/window-maximized';

export function WindowFrame({ children }: { children?: ReactNode }) {
  const maximized = useWindowMaximized();

  // Mirror the maximized state onto <html> so portaled chrome that can't reach
  // into the frame — chiefly the dialog scrim, which base-ui portals to <body>
  // outside this frame — can match the frame's geometry from CSS (see the
  // `[data-slot='dialog-overlay']` rule in `index.css`). Always false on macOS.
  useEffect(() => {
    document.documentElement.classList.toggle('frame-maximized', maximized);
  }, [maximized]);

  if (isMacOS()) return <>{children}</>;
  const className = maximized
    ? 'fixed inset-0 overflow-hidden bg-background contain-[paint]'
    : 'fixed inset-4 overflow-hidden rounded-lg border border-black/5 bg-background shadow-[0_6px_16px_rgba(0,0,0,0.35)] contain-[paint] dark:border-white/5';
  return (
    <div data-testid="window-frame" className={className}>
      {children}
    </div>
  );
}
