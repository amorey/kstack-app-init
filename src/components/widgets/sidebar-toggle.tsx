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

// Title-bar button that collapses/expands the floating sidebar. By default it
// drives the same `@kubetail/ui` sidebar state as the built-in `Cmd/Ctrl+B`
// shortcut, so both stay in sync. While collapsed, hovering previews the sidebar
// as a popup via the optional `onHoverStart`/`onHoverEnd` handlers. When the frame
// is too narrow to pin the real sidebar, the shell overrides the click via
// `onClick` to dismiss the peeked popup instead.
import { PanelLeft } from 'lucide-react';
import { Button } from '@kubetail/ui/elements/button';
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
    <Button
      variant="ghost"
      size="icon-sm"
      aria-label="Toggle sidebar"
      onClick={onClick ?? toggleSidebar}
      onMouseEnter={onHoverStart}
      onMouseLeave={onHoverEnd}
      className="shrink-0 self-center text-muted-foreground"
    >
      <PanelLeft className="h-4 w-4" aria-hidden />
    </Button>
  );
}
