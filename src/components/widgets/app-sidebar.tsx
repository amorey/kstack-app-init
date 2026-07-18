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

// The app's floating sidebar shell. The window is chromeless on every platform; the
// title bar takes a different shape per OS:
//
//   • macOS keeps its native traffic lights. The sidebar's header doubles as the
//     title bar (a gutter for the lights, then a full-width drag band).
//   • Linux/Windows have no native controls, so a full-width custom title bar sits
//     across the top — hamburger `AppMenu`, a draggable strip, and `WindowControls`
//     at the right; the sidebar starts just below it.
//
// The library's `SidebarProvider`/`useSidebar` owns open/collapsed state (cookie
// persistence + `Cmd/Ctrl+B`), but we render the shell ourselves: the library's
// `Sidebar` slides in/out over 200ms with no opt-out, and we want an instant
// show/hide (mount/unmount the card, toggle a spacer's width).
import type { CSSProperties, PointerEvent as ReactPointerEvent, ReactNode } from 'react';
import { useCallback, useEffect, useRef, useState } from 'react';

import {
  SidebarContent,
  SidebarFooter,
  SidebarInset,
  SidebarProvider,
  useSidebar,
} from '@kubetail/ui/elements/sidebar';

import { isMacOS } from '@/lib/platform';
import { AppMenu } from '@/components/widgets/app-menu';
import { SidebarToggle } from '@/components/widgets/sidebar-toggle';
import { WindowControls } from '@/components/widgets/window-controls';

// Horizontal space (px) reserved at the macOS header's left edge for the three
// traffic lights. An independent visual reservation, not derived from the host's
// `trafficLightPosition`; eyeball it against the native buttons when tuning.
const TRAFFIC_LIGHT_GUTTER = 62;

// Left offset (px, from the window edge) for the fixed macOS sidebar toggle, clearing
// the traffic-light cluster (which ends ~72px in). Measured from the window edge, not
// card-relative like `TRAFFIC_LIGHT_GUTTER`, since the toggle is `position: fixed` and
// stays put while the card can be unmounted.
const MAC_TOGGLE_LEFT = 96;

// macOS title-bar band height. The sidebar header and the window drag band cover the
// same strip, so both read from here (44px — matches the host's `TITLE_BAR_HEIGHT`).
const MAC_TITLE_BAR_HEIGHT = 'h-11';

// Top offset matching `MAC_TITLE_BAR_HEIGHT` (44px), hanging the hover popup just below
// the title-bar band. Kept in step by hand — Tailwind can't derive one from the other.
const MAC_TITLE_BAR_TOP = 'top-11';

// The hover popup is a standalone dropdown, not a preview of the pinned card, so it
// gets its own fixed footprint rather than tracking `--sidebar-width`. Height scales
// with the window; long content scrolls inside.
const PREVIEW_POPUP_WIDTH = 'w-72';
const PREVIEW_POPUP_HEIGHT = 'h-[70svh]';

// Grace delay (ms) before the hover popup closes on pointer-leave, bridging the travel
// from toggle down onto the card and absorbing a brief flick off the edge.
const PREVIEW_CLOSE_DELAY_MS = 300;

// Below Tailwind's `md` breakpoint the floating card crowds the page, so auto-collapse
// it and let the inset reclaim full width (the toggle still reopens it). Reads the
// `--breakpoint-md` theme variable and matches `< md` — the inverse of the `md:`
// variant — so it tracks the theme if the breakpoint changes.
function mediumBreakpointQuery() {
  const md = getComputedStyle(document.documentElement).getPropertyValue('--breakpoint-md').trim() || '48rem';
  return `(max-width: calc(${md} - 1px))`;
}

// Single owner of the "below the medium breakpoint" (too narrow to pin) signal.
// Seeds synchronously on first render (no post-paint flash), then follows `change`.
function useIsNarrow() {
  const [narrow, setNarrow] = useState(() => window.matchMedia(mediumBreakpointQuery()).matches);
  useEffect(() => {
    const mql = window.matchMedia(mediumBreakpointQuery());
    const onChange = (e: MediaQueryListEvent) => setNarrow(e.matches);
    mql.addEventListener('change', onChange);
    return () => mql.removeEventListener('change', onChange);
  }, []);
  return narrow;
}

// Linux/Windows custom title-bar band height (~a native Windows caption bar). Single
// source of truth: published as the `--win-titlebar-h` CSS variable (see `AppSidebar`),
// which both the bar's own height and the sidebar's top offset read from.
const WIN_TITLE_BAR_HEIGHT_PX = '32px';

// Title-bar height, derived from `--win-titlebar-h`.
const WIN_TITLE_BAR_HEIGHT = 'h-[var(--win-titlebar-h)]';

// Sidebar width bounds (px); the card drag-resizes between these. `DEFAULT` matches the
// library's 16rem. The chosen width is published as `--sidebar-width` (so card and
// spacer track it) and persisted to `localStorage` under `SIDEBAR_WIDTH_KEY`.
const MIN_SIDEBAR_WIDTH = 200;
const MAX_SIDEBAR_WIDTH = 480;
const DEFAULT_SIDEBAR_WIDTH = 256;
const SIDEBAR_WIDTH_KEY = 'sidebar_width';

// The floating card's `p-2` padding (px): `--sidebar-width` spans the whole card, so
// the visible sidebar border sits this far inside its right edge. The resize handle
// centers on that border and the drag math offsets by it, so grabbing it doesn't jump.
const CARD_PADDING = 8;

const clampWidth = (px: number) => Math.min(MAX_SIDEBAR_WIDTH, Math.max(MIN_SIDEBAR_WIDTH, px));

// Persisted, clamped sidebar width plus a `localStorage`-mirroring setter. Windows
// share one webview origin (one `localStorage`) but each keeps its own width in React
// state and never syncs live, so resizing one leaves the others untouched while a newly
// opened window inherits the last-saved width on mount.
function useSidebarWidth(): [number, (px: number) => void] {
  const [width, setWidth] = useState(() => {
    const stored = Number(localStorage.getItem(SIDEBAR_WIDTH_KEY));
    return stored ? clampWidth(stored) : DEFAULT_SIDEBAR_WIDTH;
  });
  const set = useCallback((px: number) => {
    const next = clampWidth(px);
    setWidth(next);
    localStorage.setItem(SIDEBAR_WIDTH_KEY, String(next));
  }, []);
  return [width, set];
}

// Drag handle centered on the sidebar's visible right border. On drag it maps the
// pointer's x (from the window's left edge) to the card width, plus the card padding so
// the border tracks the cursor without jumping; the parent clamps and persists. Pointer
// capture keeps the drag alive when the cursor outruns the thin handle.
function ResizeHandle({ onResize }: { onResize: (px: number) => void }) {
  const handlePointerDown = useCallback(
    (e: ReactPointerEvent<HTMLDivElement>) => {
      e.preventDefault();
      e.currentTarget.setPointerCapture(e.pointerId);
      // Force the resize cursor for the whole drag (else it reverts when the pointer
      // strays off the thin handle); `user-select: none` stops text selection.
      const { body } = document;
      const prevCursor = body.style.cursor;
      const prevSelect = body.style.userSelect;
      body.style.cursor = 'col-resize';
      body.style.userSelect = 'none';
      const onMove = (ev: PointerEvent) => onResize(ev.clientX + CARD_PADDING);
      const onUp = () => {
        body.style.cursor = prevCursor;
        body.style.userSelect = prevSelect;
        window.removeEventListener('pointermove', onMove);
        window.removeEventListener('pointerup', onUp);
      };
      window.addEventListener('pointermove', onMove);
      window.addEventListener('pointerup', onUp);
    },
    [onResize],
  );

  return (
    <div
      data-testid="sidebar-resize-handle"
      role="separator"
      aria-orientation="vertical"
      onPointerDown={handlePointerDown}
      className="absolute inset-y-0 right-1 z-20 w-2 cursor-col-resize touch-none"
    />
  );
}

// macOS: a window-wide drag strip across the top title-bar band, including the area
// beside the sidebar (over the page). `z-0` keeps it below the sidebar's `z-10` so its
// controls stay clickable, but it still paints over the page background. Pages reserve
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
    <div className={`flex ${MAC_TITLE_BAR_HEIGHT} shrink-0 flex-row items-center gap-1 p-2`}>
      <div
        data-testid="traffic-light-gutter"
        aria-hidden
        className="shrink-0"
        style={{ width: TRAFFIC_LIGHT_GUTTER }}
      />
      <div data-tauri-drag-region className="h-full flex-1" />
    </div>
  );
}

// macOS: the sidebar toggle, fixed just right of the traffic lights. Lives outside the
// sidebar so it stays put and clickable when the sidebar is hidden. `z-20` keeps it
// above the sidebar (`z-10`) and the drag band (`z-0`).
function MacSidebarToggle(handlers: HoverHandlers) {
  return (
    <div className={`fixed top-0 z-20 flex ${MAC_TITLE_BAR_HEIGHT} items-center`} style={{ left: MAC_TOGGLE_LEFT }}>
      <SidebarToggle {...handlers} />
    </div>
  );
}

// Linux/Windows: a full-width custom title bar. Fixed above the sidebar (`z-30`) so its
// controls stay clickable; its middle strip is the drag region. The toggle sits by the
// hamburger and, being part of this fixed bar, stays visible to reopen a hidden sidebar.
function WinTitleBar(handlers: HoverHandlers) {
  return (
    // `transform-gpu` pins this fixed, opaque bar to its own retained compositor layer.
    // Without it WebKitGTK re-rasters during an interactive resize and — the window
    // being transparent — the title area briefly flashes through to the desktop. Safe
    // here: no body text, so the app-wide subpixel-rendering reason to avoid transforms
    // (see `window-frame.tsx`) doesn't apply.
    <div
      className={`fixed inset-x-0 top-0 z-30 flex transform-gpu ${WIN_TITLE_BAR_HEIGHT} items-stretch gap-0.5 bg-background`}
    >
      <AppMenu />
      <SidebarToggle {...handlers} />
      <div data-testid="window-drag-region" data-tauri-drag-region aria-hidden className="h-full flex-1" />
      <WindowControls />
    </div>
  );
}

// Handlers the toggle fires to preview a collapsed sidebar as a popup:
// `onHoverStart`/`onHoverEnd` open/close it on hover. `onClick` is unset when a click
// should keep the toggle's default pin/collapse behavior (wide case), and supplied only
// when narrow, where there's no room to pin and a click dismisses the peeked popup.
type HoverHandlers = {
  onHoverStart?: () => void;
  onHoverEnd?: () => void;
  onClick?: () => void;
};

type ShellProps = {
  mac: boolean;
  onResize: (px: number) => void;
  nav?: ReactNode;
  footer?: ReactNode;
  children?: ReactNode;
};

// The floating-sidebar shell. When open, a fixed card holds the nav/footer and a
// same-width spacer reserves room for it so the inset sits beside it. When collapsed
// both vanish instantly (no slide) and the inset reclaims full width — but hovering the
// toggle re-mounts the card as an unpinned popup overlaying the page, gone once the
// pointer leaves both. Clicking the toggle pins it back open (like `Cmd/Ctrl+B`).
function SidebarShell({ mac, onResize, nav, footer, children }: ShellProps) {
  const { open } = useSidebar();

  // Narrowness is a presentation constraint, not a state change: `open` stays the true
  // intent (button / `Cmd/Ctrl+B`), `effectiveOpen` is what renders. So narrowing hides
  // the sidebar and widening restores it for free, with no shadow state to sync.
  // `isNarrow` also drives the toggle's click behavior, and is seeded synchronously so
  // the sidebar never flashes open before a narrow-at-mount is accounted for.
  const isNarrow = useIsNarrow();
  const effectiveOpen = open && !isNarrow;

  // Hover-preview state for the collapsed sidebar. `PREVIEW_CLOSE_DELAY_MS` bridges the
  // pointer's travel from toggle into the popup so it doesn't flicker shut between them.
  const [preview, setPreview] = useState(false);
  const closeTimer = useRef<ReturnType<typeof setTimeout>>(undefined);
  const showPreview = useCallback(() => {
    clearTimeout(closeTimer.current);
    setPreview(true);
  }, []);
  const hidePreview = useCallback(() => {
    clearTimeout(closeTimer.current);
    closeTimer.current = setTimeout(() => setPreview(false), PREVIEW_CLOSE_DELAY_MS);
  }, []);
  // Narrow-collapsed, clicking the toggle flips the popup on/off in place (no room to
  // pin). Cancels any pending close so a click right after a hover isn't undone.
  const togglePreview = useCallback(() => {
    clearTimeout(closeTimer.current);
    setPreview((p) => !p);
  }, []);
  useEffect(() => () => clearTimeout(closeTimer.current), []);
  // Drop any lingering hover-preview the moment the sidebar docks, so a later collapse
  // starts fresh. A render-time state adjustment (not an effect) so it lands before paint.
  const [prevEffectiveOpen, setPrevEffectiveOpen] = useState(effectiveOpen);
  if (effectiveOpen !== prevEffectiveOpen) {
    setPrevEffectiveOpen(effectiveOpen);
    if (effectiveOpen) setPreview(false);
  }

  // Only preview while collapsed; the docked card (or the width drag) takes over.
  const showingPreview = !effectiveOpen && preview;
  const cardVisible = effectiveOpen || showingPreview;
  // Docked, the toggle keeps its default collapse click. Collapsed, hover peeks the
  // popup; the click then either pins the sidebar back open (wide → leave `onClick`
  // unset) or toggles the peeked popup in place (too narrow to pin).
  const hoverHandlers: HoverHandlers = effectiveOpen
    ? {}
    : { onHoverStart: showPreview, onHoverEnd: hidePreview, onClick: isNarrow ? togglePreview : undefined };

  // Card geometry differs between pinned and popup: pinned is full-height (`bottom-0`)
  // anchored at the top (flush on macOS, below the custom title bar off macOS); the
  // popup fits content and hangs just below the title-bar band on every platform, so on
  // macOS it clears the traffic lights instead of spanning up behind them.
  const dockedTop = mac ? 'top-0' : 'top-[var(--win-titlebar-h)]';
  const previewTop = mac ? MAC_TITLE_BAR_TOP : 'top-[var(--win-titlebar-h)]';

  return (
    <>
      {!mac && <WinTitleBar {...hoverHandlers} />}

      {/* Layout spacer reserving the sidebar's width so the inset sits beside the card.
          Zero when collapsed — the hover popup overlays the page instead. */}
      <div className="shrink-0" aria-hidden style={{ width: effectiveOpen ? 'var(--sidebar-width)' : 0 }} />

      {cardVisible && (
        <div
          data-testid="app-sidebar"
          onMouseEnter={showingPreview ? showPreview : undefined}
          onMouseLeave={showingPreview ? hidePreview : undefined}
          className={`fixed left-0 z-10 flex p-2 ${
            showingPreview
              ? `${previewTop} ${PREVIEW_POPUP_WIDTH} ${PREVIEW_POPUP_HEIGHT}`
              : `bottom-0 w-(--sidebar-width) ${dockedTop}`
          }`}
        >
          <div
            className={`flex size-full min-h-0 flex-col rounded-lg bg-sidebar ring-1 ring-sidebar-border ${
              showingPreview ? 'shadow-lg' : 'shadow-sm'
            }`}
          >
            {/* The pinned macOS card doubles as the title bar; the popup drops below it. */}
            {mac && !showingPreview && <MacTitleBar />}
            <SidebarContent>{nav}</SidebarContent>
            <SidebarFooter>{footer}</SidebarFooter>
          </div>
          {/* No resize handle on the transient popup — resizing is pinned-only. */}
          {!showingPreview && <ResizeHandle onResize={onResize} />}
        </div>
      )}

      <SidebarInset data-testid="sidebar-inset">{children}</SidebarInset>

      {/* Mac-only trailing chrome, after the inset so the drag band paints over the
          page background; the toggle floats above everything (z-20). */}
      {mac && (
        <>
          <MacWindowDragBand />
          <MacSidebarToggle {...hoverHandlers} />
        </>
      )}
    </>
  );
}

type AppSidebarProps = {
  /** Sidebar body (navigation, pickers) — rendered above the footer. */
  nav?: ReactNode;
  /** Sidebar footer (status, account) — rendered below the nav. */
  footer?: ReactNode;
  /** Page content — rendered in the inset beside the sidebar. */
  children?: ReactNode;
};

export function AppSidebar({ nav, footer, children }: AppSidebarProps) {
  const mac = isMacOS();
  const [width, setWidth] = useSidebarWidth();

  // Publish the drag-chosen width as `--sidebar-width` and, off macOS, the custom
  // title-bar height, so the bar and the sidebar offset read both from one place.
  const style = {
    '--sidebar-width': `${width}px`,
    ...(mac ? {} : { '--win-titlebar-h': WIN_TITLE_BAR_HEIGHT_PX }),
  } as CSSProperties;

  return (
    // The provider wrapper hardcodes `min-h-svh`; off macOS the app is inset inside
    // `WindowFrame`, so override it to fill the frame instead (`--app-min-h`). `cn`
    // (tailwind-merge) dedupes the `min-h-*` utilities, keeping ours.
    <SidebarProvider style={style} className="min-h-(--app-min-h)">
      <SidebarShell mac={mac} onResize={setWidth} nav={nav} footer={footer}>
        {children}
      </SidebarShell>
    </SidebarProvider>
  );
}
