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

// Title-bar button that collapses/expands the floating sidebar. Sits next to the
// traffic lights on macOS and next to the `AppMenu` hamburger on Linux/Windows.
// By default it drives the same `@kubetail/ui` sidebar state as the built-in
// `Cmd/Ctrl+B` shortcut, so both stay in sync: a click pins the full sidebar open
// or collapses it. While the sidebar is collapsed, hovering the button previews
// it as a popup — the shell wires that up through the optional
// `onHoverStart`/`onHoverEnd` handlers. When the frame is too narrow to pin the
// real sidebar, the shell overrides the click via `onClick` so it dismisses the
// peeked popup instead.
import { PanelLeft } from 'lucide-react';
import { useSidebar } from '@kubetail/ui/elements/sidebar';

type SidebarToggleProps = {
  onHoverStart?: () => void;
  onHoverEnd?: () => void;
  /** Overrides the default pin/collapse click (narrow mode toggles the popup). */
  onClick?: () => void;
};

export function SidebarToggle({ onHoverStart, onHoverEnd, onClick }: SidebarToggleProps = {}) {
  const { toggleSidebar } = useSidebar();
  return (
    <button
      type="button"
      aria-label="Toggle sidebar"
      onClick={onClick ?? toggleSidebar}
      onMouseEnter={onHoverStart}
      onMouseLeave={onHoverEnd}
      className="flex h-7 w-7 shrink-0 self-center items-center justify-center rounded text-muted-foreground outline-none hover:bg-accent hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring"
    >
      <PanelLeft className="h-4 w-4" aria-hidden />
    </button>
  );
}
