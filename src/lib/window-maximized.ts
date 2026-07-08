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

// Tracks whether the current window is maximized, so the frameless Linux/Windows
// chrome (`WindowFrame`, `WindowResizeHandles`) can collapse its border/shadow
// gutter and hide the resize grips when there's no off-window space to draw into
// — matching how native apps drop their rounded corners + drop shadow while
// maximized. The Tauri JS API has no dedicated maximize event, so we re-query
// `isMaximized()` on every `onResized` (which fires on maximize/unmaximize/resize).
// Always false on macOS, where the native decorations own this behavior.
import { useEffect, useState } from 'react';
import { getCurrentWindow } from '@tauri-apps/api/window';

import { isMacOS } from '@/lib/platform';

export function useWindowMaximized(): boolean {
  const [maximized, setMaximized] = useState(false);

  useEffect(() => {
    if (isMacOS()) return undefined;

    const win = getCurrentWindow();
    let active = true;
    let unlisten: (() => void) | undefined;

    const sync = () => {
      win
        .isMaximized()
        .then((value) => {
          if (active) setMaximized(value);
        })
        .catch(() => {});
    };

    sync();
    win
      .onResized(sync)
      .then((fn) => {
        if (active) unlisten = fn;
        else fn();
      })
      .catch(() => {});

    return () => {
      active = false;
      unlisten?.();
    };
  }, []);

  return maximized;
}
