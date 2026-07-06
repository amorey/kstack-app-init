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

// The app's floating sidebar shell. Wraps the page in a `floating`-variant
// sidebar whose header doubles as the window title bar: on macOS it reserves a
// gutter for the native traffic lights (positioned by the host via the Overlay
// title bar); on Linux/Windows the frameless window has no native controls, so
// the header renders `WindowControls` instead. The header carries a draggable
// strip (`data-tauri-drag-region`) so the whole title-bar area moves the window.
import type { ReactNode } from 'react';

import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarInset,
  SidebarProvider,
} from '@kubetail/ui/elements/sidebar';

import { isMacOS } from '@/lib/platform';
import { WindowControls } from '@/components/widgets/window-controls';

// Horizontal space (px) reserved at the header's left edge for the cluster of
// three macOS traffic lights. This is an independent visual reservation for the
// lights' full width — not derived from the host's `trafficLightPosition`
// (which sets only the first light's origin); eyeball it against the native
// buttons when tuning.
const TRAFFIC_LIGHT_GUTTER = 62;

// Title-bar band height (Tailwind class). The sidebar header and the window
// drag band must cover the same strip, so both take their height from here
// (44px — matches the host's `TITLE_BAR_HEIGHT` in `window_manager.rs`).
const TITLE_BAR_HEIGHT = 'h-11';

// A window-wide drag strip across the top, spanning the whole title-bar band —
// including the area beside the floating sidebar (over the page). Its `z-0`
// keeps it below the sidebar's `z-10` so the sidebar's own controls stay
// clickable; rendered after the inset it still paints over the page background,
// so clicking the empty top background there moves the window. Pages reserve
// this band with top padding, so it never covers interactive page content.
function WindowDragBand() {
  return (
    <div
      data-testid="window-drag-region"
      data-tauri-drag-region
      aria-hidden
      className={`fixed inset-x-0 top-0 z-0 ${TITLE_BAR_HEIGHT}`}
    />
  );
}

function TitleBar() {
  const mac = isMacOS();
  return (
    <SidebarHeader className={`${TITLE_BAR_HEIGHT} flex-row items-center gap-1 p-2`}>
      {mac && (
        <div
          data-testid="traffic-light-gutter"
          aria-hidden
          className="shrink-0"
          style={{ width: TRAFFIC_LIGHT_GUTTER }}
        />
      )}
      {/* Draggable strip fills the free space; interactive siblings (controls,
          and the header widgets added later) stay outside it so they still
          receive clicks. */}
      <div data-tauri-drag-region className="h-full flex-1" />
      {!mac && <WindowControls />}
    </SidebarHeader>
  );
}

type AppSidebarProps = {
  /** Sidebar body (navigation, pickers) — rendered in `SidebarContent`. */
  nav?: ReactNode;
  /** Sidebar footer (status, account) — rendered in `SidebarFooter`. */
  footer?: ReactNode;
  /** Page content — rendered in the `SidebarInset` beside the sidebar. */
  children?: ReactNode;
};

export function AppSidebar({ nav, footer, children }: AppSidebarProps) {
  return (
    <SidebarProvider>
      <Sidebar variant="floating">
        <TitleBar />
        <SidebarContent>{nav}</SidebarContent>
        <SidebarFooter>{footer}</SidebarFooter>
      </Sidebar>
      <SidebarInset>{children}</SidebarInset>
      {/* Must render after SidebarInset — see WindowDragBand for why. */}
      <WindowDragBand />
    </SidebarProvider>
  );
}
