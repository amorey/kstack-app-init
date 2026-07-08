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

// The app's floating sidebar shell. The window is chromeless on every platform,
// but the title bar takes a different shape per OS:
//
//   • macOS keeps its native traffic lights (drawn over the Overlay title bar by
//     the host). The sidebar's header doubles as the title bar: it reserves a
//     gutter for the lights, and a full-width drag band covers the rest of the
//     top strip so the whole band moves the window.
//   • Linux/Windows have no native window controls, so a full-width custom title
//     bar sits across the top — hamburger `AppMenu` at the left, a draggable
//     strip in the middle, and `WindowControls` (minimize/maximize/close) at the
//     right. The floating sidebar is offset to start just below it.
import type { CSSProperties, ReactNode } from 'react';

import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarInset,
  SidebarProvider,
} from '@kubetail/ui/elements/sidebar';

import { isMacOS } from '@/lib/platform';
import { AppMenu } from '@/components/widgets/app-menu';
import { WindowControls } from '@/components/widgets/window-controls';

// Horizontal space (px) reserved at the macOS header's left edge for the cluster
// of three traffic lights. This is an independent visual reservation for the
// lights' full width — not derived from the host's `trafficLightPosition`
// (which sets only the first light's origin); eyeball it against the native
// buttons when tuning.
const TRAFFIC_LIGHT_GUTTER = 62;

// macOS title-bar band height (Tailwind class). The sidebar header and the
// window drag band must cover the same strip, so both take their height from
// here (44px — matches the host's `TITLE_BAR_HEIGHT` in `window_manager.rs`).
const MAC_TITLE_BAR_HEIGHT = 'h-11';

// Linux/Windows custom title-bar band height. 32px is close to a native Windows
// caption bar. This is the single source of truth: it's published as the
// `--win-titlebar-h` CSS variable on the sidebar wrapper (see `AppSidebar`), and
// both the title bar's own height and the sidebar's offset read from that
// variable — so the band height is stated in exactly one place.
const WIN_TITLE_BAR_HEIGHT_PX = '32px';

// Title-bar height and sidebar offset, both derived from `--win-titlebar-h`.
const WIN_TITLE_BAR_HEIGHT = 'h-[var(--win-titlebar-h)]';

// Offset applied to the floating sidebar container off macOS so it starts just
// below the custom title bar instead of at the window top. Overrides the
// sidebar's default `inset-y-0`/`h-svh` via tailwind-merge (`top-*` wins the top
// edge, the explicit height replaces `h-svh`).
const WIN_SIDEBAR_OFFSET = 'top-[var(--win-titlebar-h)] h-[calc(100svh-var(--win-titlebar-h))]';

// macOS: a window-wide drag strip across the top, spanning the whole title-bar
// band — including the area beside the floating sidebar (over the page). Its
// `z-0` keeps it below the sidebar's `z-10` so the sidebar's own controls stay
// clickable; rendered after the inset it still paints over the page background,
// so clicking the empty top background there moves the window. Pages reserve
// this band with top padding, so it never covers interactive page content.
function MacWindowDragBand() {
  return (
    <div
      data-testid="window-drag-region"
      data-tauri-drag-region
      aria-hidden
      className={`fixed inset-x-0 top-0 z-0 ${MAC_TITLE_BAR_HEIGHT}`}
    />
  );
}

// macOS: the sidebar header doubles as the title bar. Reserves the traffic-light
// gutter, then a draggable strip fills the rest.
function MacTitleBar() {
  return (
    <SidebarHeader className={`${MAC_TITLE_BAR_HEIGHT} flex-row items-center gap-1 p-2`}>
      <div
        data-testid="traffic-light-gutter"
        aria-hidden
        className="shrink-0"
        style={{ width: TRAFFIC_LIGHT_GUTTER }}
      />
      {/* Draggable strip fills the free space; interactive siblings (added
          later) stay outside it so they still receive clicks. */}
      <div data-tauri-drag-region className="h-full flex-1" />
    </SidebarHeader>
  );
}

// Linux/Windows: a full-width custom title bar across the top of the window.
// Fixed and above the sidebar (`z-30` over the sidebar's `z-10`) so its controls
// stay clickable; its middle strip is the window's drag region.
function WinTitleBar() {
  return (
    <div className={`fixed inset-x-0 top-0 z-30 flex ${WIN_TITLE_BAR_HEIGHT} items-stretch bg-background`}>
      <AppMenu />
      <div data-testid="window-drag-region" data-tauri-drag-region aria-hidden className="h-full flex-1" />
      <WindowControls />
    </div>
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
  const mac = isMacOS();
  return (
    // Off macOS, publish the custom title-bar height so the bar and the sidebar
    // offset can both read it from one place (see `WIN_TITLE_BAR_HEIGHT_PX`).
    <SidebarProvider style={mac ? undefined : ({ '--win-titlebar-h': WIN_TITLE_BAR_HEIGHT_PX } as CSSProperties)}>
      {!mac && <WinTitleBar />}
      <Sidebar variant="floating" className={mac ? undefined : WIN_SIDEBAR_OFFSET}>
        {mac && <MacTitleBar />}
        <SidebarContent>{nav}</SidebarContent>
        <SidebarFooter>{footer}</SidebarFooter>
      </Sidebar>
      <SidebarInset>{children}</SidebarInset>
      {/* Must render after SidebarInset — see MacWindowDragBand for why. */}
      {mac && <MacWindowDragBand />}
    </SidebarProvider>
  );
}
