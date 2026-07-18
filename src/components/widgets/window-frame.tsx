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

// The window's outer frame, for the frameless Linux window only. That window is
// `decorations(false)` *and* `transparent`, so the OS draws no border or shadow —
// against a same-color desktop the edge is invisible. This wraps the whole app in a
// surface inset a few px from the window edge, clipped to rounded corners, with a
// thin border and soft outer shadow painted into the transparent gutter, so the
// window reads as a distinct floating surface.
//
// macOS and Windows are passthroughs (full-bleed). macOS keeps native decorations;
// Windows is frameless but opaque, so DWM draws the borderless window's own shadow
// (and Win11 corners) — a second custom shadow would just double-stack.
//
// The gutter (`inset-4`) must be at least as wide as the shadow's reach (blur +
// offset): the OS window rectangle ends at the gutter's outer edge and clips
// anything beyond it, so a shadow reaching farther gets cut into a hard band.
//
// `contain: paint` makes this the containing block for the app's `position: fixed`
// chrome (title bar, sidebar), so it anchors to the inset box rather than the
// window edge — without promoting the app to a GPU layer (disturbing subpixel text)
// the way `transform` would. `overflow-hidden` then clips that chrome to the corners.
//
// It wraps the app at the outermost level so loading and error states get the same
// frame and opaque background as the mounted app.
//
// When maximized there's no off-window space to paint into, so we collapse to a
// full-bleed `inset-0` surface (no inset, corners, border, or shadow), and
// `WindowResizeHandles` hides its grips to match.
import type { ReactNode } from 'react';
import { useEffect } from 'react';

import { isLinux } from '@/lib/platform';
import { useWindowMaximized } from '@/lib/window-maximized';

export function WindowFrame({ children }: { children?: ReactNode }) {
  const maximized = useWindowMaximized();

  // Mirror the maximized state onto <html> so portaled chrome that can't reach into
  // the frame — chiefly the dialog scrim, base-ui-portaled to <body> outside it —
  // can match the frame's geometry from CSS (the `[data-slot='dialog-overlay']` rule
  // in `index.css`). Only meaningful on frameless Linux; the scrim rules key off
  // `html.frameless`, which macOS and Windows don't set.
  useEffect(() => {
    document.documentElement.classList.toggle('frame-maximized', maximized);
  }, [maximized]);

  if (!isLinux()) return <>{children}</>;
  const className = maximized
    ? 'fixed inset-0 overflow-hidden bg-background contain-[paint]'
    : 'fixed inset-4 overflow-hidden rounded-lg border border-black/5 bg-background shadow-[0_6px_16px_rgba(0,0,0,0.35)] contain-[paint] dark:border-white/5';
  return (
    <div data-testid="window-frame" className={className}>
      {children}
    </div>
  );
}
