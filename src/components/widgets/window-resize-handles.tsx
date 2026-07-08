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

// Edge/corner resize grips for the frameless Linux/Windows window. Dropping the
// OS decorations (`decorations(false)`) also drops the native resize borders, so
// the app draws its own: invisible fixed strips over the transparent gutter that
// `WindowFrame` leaves around the app (see `window-frame.tsx`). Each strip starts
// a native resize drag via the Tauri core window API on pointer-down. Corners sit
// above the edges (later in DOM order + wider) so a corner grab resizes on both
// axes. macOS keeps its native decorations, so this renders nothing there.
import { getCurrentWindow } from '@tauri-apps/api/window';

import { isMacOS } from '@/lib/platform';

// `@tauri-apps/api` declares this union but doesn't export it; mirror it here so
// the direction constants below stay typed. Structurally matches the argument
// `startResizeDragging` accepts.
type ResizeDirection = 'North' | 'South' | 'East' | 'West' | 'NorthEast' | 'NorthWest' | 'SouthEast' | 'SouthWest';

function startResize(direction: ResizeDirection) {
  getCurrentWindow()
    .startResizeDragging(direction)
    .catch(() => {});
}

// Grip footprint: edges span the ~16px transparent gutter; corners are a bit
// larger so the diagonal target is easy to hit. `select-none` + `touch-none`
// keep the webview from claiming the press (see the mousedown handler).
const EDGE = 'fixed z-50 select-none touch-none';
const CORNER = 'fixed z-50 h-5 w-5 select-none touch-none';

const HANDLES: { key: string; className: string; cursor: string; direction: ResizeDirection }[] = [
  // Edges.
  { key: 'n', className: `${EDGE} inset-x-0 top-0 h-4`, cursor: 'ns-resize', direction: 'North' },
  { key: 's', className: `${EDGE} inset-x-0 bottom-0 h-4`, cursor: 'ns-resize', direction: 'South' },
  { key: 'w', className: `${EDGE} inset-y-0 left-0 w-4`, cursor: 'ew-resize', direction: 'West' },
  { key: 'e', className: `${EDGE} inset-y-0 right-0 w-4`, cursor: 'ew-resize', direction: 'East' },
  // Corners (above the edges).
  { key: 'nw', className: `${CORNER} left-0 top-0`, cursor: 'nwse-resize', direction: 'NorthWest' },
  { key: 'ne', className: `${CORNER} right-0 top-0`, cursor: 'nesw-resize', direction: 'NorthEast' },
  { key: 'sw', className: `${CORNER} left-0 bottom-0`, cursor: 'nesw-resize', direction: 'SouthWest' },
  { key: 'se', className: `${CORNER} right-0 bottom-0`, cursor: 'nwse-resize', direction: 'SouthEast' },
];

export function WindowResizeHandles() {
  if (isMacOS()) return null;
  return (
    <>
      {HANDLES.map(({ key, className, cursor, direction }) => (
        <div
          key={key}
          aria-hidden
          className={className}
          style={{ cursor }}
          onMouseDown={(e) => {
            // Only the primary button; ignore right/middle so context menus etc. work.
            if (e.button !== 0) return;
            // Critical on Linux/WebKitGTK: `startResizeDragging` is async (it
            // IPCs to Rust, which then asks GTK to begin the resize grab). If the
            // webview starts its own selection grab on this press first, GTK's
            // grab loses the race and the drag silently fails into a text
            // selection. Preventing the default press keeps the button free for
            // GTK. Uses `mousedown` (not `pointerdown`) because that's what
            // reliably suppresses the selection.
            e.preventDefault();
            startResize(direction);
          }}
        />
      ))}
    </>
  );
}
