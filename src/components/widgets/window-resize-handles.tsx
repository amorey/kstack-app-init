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

// Edge/corner resize grips for the frameless Linux/Windows window. `decorations(false)`
// drops the native resize borders, so the app draws its own: invisible fixed strips
// along the window edges. On Linux they sit over the transparent gutter `WindowFrame`
// leaves; on Windows (full-bleed, no gutter) they overlay the outermost few px of the
// app. Each strip starts a native resize drag via the Tauri core window API on
// pointer-down. Corners sit above the edges (later in DOM order + wider) so a corner
// grab resizes both axes. Renders nothing on macOS or while maximized.
import { getCurrentWindow } from '@tauri-apps/api/window';

import { isLinux, isMacOS } from '@/lib/platform';
import { useWindowMaximized } from '@/lib/window-maximized';

// `@tauri-apps/api` declares this union but doesn't export it; mirror it here to
// keep the direction constants typed. Structurally matches `startResizeDragging`'s
// argument.
type ResizeDirection = 'North' | 'South' | 'East' | 'West' | 'NorthEast' | 'NorthWest' | 'SouthEast' | 'SouthWest';

function startResize(direction: ResizeDirection) {
  getCurrentWindow()
    .startResizeDragging(direction)
    .catch(() => {});
}

// Grip footprint. `select-none` + `touch-none` keep the webview from claiming the
// press (see the mousedown handler). On Linux the strips fill the transparent
// gutter (~16px) for free; on Windows they overlay real app content, so a thin
// (~5px) edge-hugging strip keeps the resize cursor near the very edge like native
// windows.
const EDGE = 'fixed z-50 select-none touch-none';
const CORNER = 'fixed z-50 select-none touch-none';

function buildHandles(edge: { h: string; w: string }, corner: string) {
  return [
    // Edges.
    { key: 'n', className: `${EDGE} inset-x-0 top-0 ${edge.h}`, cursor: 'ns-resize', direction: 'North' },
    { key: 's', className: `${EDGE} inset-x-0 bottom-0 ${edge.h}`, cursor: 'ns-resize', direction: 'South' },
    { key: 'w', className: `${EDGE} inset-y-0 left-0 ${edge.w}`, cursor: 'ew-resize', direction: 'West' },
    { key: 'e', className: `${EDGE} inset-y-0 right-0 ${edge.w}`, cursor: 'ew-resize', direction: 'East' },
    // Corners (above the edges).
    { key: 'nw', className: `${CORNER} ${corner} left-0 top-0`, cursor: 'nwse-resize', direction: 'NorthWest' },
    { key: 'ne', className: `${CORNER} ${corner} right-0 top-0`, cursor: 'nesw-resize', direction: 'NorthEast' },
    { key: 'sw', className: `${CORNER} ${corner} left-0 bottom-0`, cursor: 'nesw-resize', direction: 'SouthWest' },
    { key: 'se', className: `${CORNER} ${corner} right-0 bottom-0`, cursor: 'nwse-resize', direction: 'SouthEast' },
  ] satisfies { key: string; className: string; cursor: string; direction: ResizeDirection }[];
}

// Linux: wide strips filling the gutter. Windows (and other frameless platforms):
// thin edge-hugging strips.
const LINUX_HANDLES = buildHandles({ h: 'h-4', w: 'w-4' }, 'h-5 w-5');
const NARROW_HANDLES = buildHandles({ h: 'h-[5px]', w: 'w-[5px]' }, 'h-2.5 w-2.5');

export function WindowResizeHandles() {
  const maximized = useWindowMaximized();
  if (isMacOS() || maximized) return null;
  const handles = isLinux() ? LINUX_HANDLES : NARROW_HANDLES;
  return (
    <>
      {handles.map(({ key, className, cursor, direction }) => (
        <div
          key={key}
          aria-hidden
          className={className}
          style={{ cursor }}
          onMouseDown={(e) => {
            // Only the primary button; ignore right/middle so context menus etc. work.
            if (e.button !== 0) return;
            // Critical on Linux/WebKitGTK: `startResizeDragging` is async (it IPCs
            // to Rust, which asks GTK to begin the resize grab). If the webview
            // starts its own selection grab on this press first, GTK's grab loses
            // the race and the drag fails into a text selection. Preventing the
            // default press keeps the button free for GTK. `mousedown` (not
            // `pointerdown`) is what reliably suppresses the selection.
            e.preventDefault();
            startResize(direction);
          }}
        />
      ))}
    </>
  );
}
