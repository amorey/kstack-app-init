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
import type { ReactNode } from 'react';

import { isMacOS } from '@/lib/platform';

export function WindowFrame({ children }: { children?: ReactNode }) {
  if (isMacOS()) return <>{children}</>;
  return (
    <div
      data-testid="window-frame"
      className="fixed inset-2 overflow-hidden rounded-lg border border-black/5 bg-background shadow-[0_10px_30px_rgba(0,0,0,0.45)] contain-[paint] dark:border-white/5"
    >
      {children}
    </div>
  );
}
