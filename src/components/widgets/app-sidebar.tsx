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
//
// We drive the sidebar's open/collapsed state with the library's
// `SidebarProvider`/`useSidebar` (cookie persistence + the `Cmd/Ctrl+B`
// shortcut), but render the shell ourselves rather than using the library's
// `Sidebar`. That component slides the panel in/out over 200ms with no way to
// opt out; we want an instant show/hide, so our shell simply mounts/unmounts the
// floating card and toggles a layout spacer's width.
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

// Horizontal space (px) reserved at the macOS header's left edge for the cluster
// of three traffic lights. This is an independent visual reservation for the
// lights' full width — not derived from the host's `trafficLightPosition`
// (which sets only the first light's origin); eyeball it against the native
// buttons when tuning.
const TRAFFIC_LIGHT_GUTTER = 62;

// Left offset (px, from the *window* edge) for the fixed macOS sidebar toggle.
// The three native traffic lights sit at window-left + `SIDEBAR_GAP` (8) +
// `TRAFFIC_LIGHT_LEFT_INSET` (12), 20px apart at 12px wide — so the cluster ends
// ~72px in. This clears it with a comfortable gap. Distinct from
// `TRAFFIC_LIGHT_GUTTER` (which reserves space *inside* the card header,
// card-relative): the toggle is `position: fixed`, so it must be measured from
// the window edge instead — the card can be unmounted (sidebar hidden) while
// the toggle stays put.
const MAC_TOGGLE_LEFT = 96;

// macOS title-bar band height (Tailwind class). The sidebar header and the
// window drag band must cover the same strip, so both take their height from
// here (44px — matches the host's `TITLE_BAR_HEIGHT` in `window_manager.rs`).
const MAC_TITLE_BAR_HEIGHT = 'h-11';

// Top offset matching `MAC_TITLE_BAR_HEIGHT` (44px), used to hang the hover popup
// just below the macOS title-bar band so it drops from under the toggle. Kept in
// step with `MAC_TITLE_BAR_HEIGHT` by hand — Tailwind can't derive one from the
// other.
const MAC_TITLE_BAR_TOP = 'top-11';

// The hover popup is a standalone dropdown, not a preview of the pinned card, so
// it gets its own fixed footprint (Tailwind classes) instead of tracking the
// drag-chosen `--sidebar-width`. The height is a fixed fraction of the window
// height, so it scales with the window; long content scrolls inside.
const PREVIEW_POPUP_WIDTH = 'w-72';
const PREVIEW_POPUP_HEIGHT = 'h-[70svh]';

// Grace delay (ms) before the hover popup closes once the pointer leaves the
// toggle (and the popup). Bridges the gap as the pointer travels from the toggle
// down onto the card, and keeps a brief flick off the edge from dismissing it.
const PREVIEW_CLOSE_DELAY_MS = 300;

// Below Tailwind's `md` breakpoint the floating card crowds the page, so we
// auto-collapse it there and let the inset reclaim the full width (the toggle
// stays available to reopen it). The threshold isn't hardcoded: we read
// Tailwind's `--breakpoint-md` theme variable and match `< md` — the exact
// inverse of the `md:` variant — so it tracks the theme if the breakpoint ever
// changes.
function mediumBreakpointQuery() {
  const md = getComputedStyle(document.documentElement).getPropertyValue('--breakpoint-md').trim() || '48rem';
  return `(max-width: calc(${md} - 1px))`;
}

// Linux/Windows custom title-bar band height. 32px is close to a native Windows
// caption bar. This is the single source of truth: it's published as the
// `--win-titlebar-h` CSS variable on the sidebar wrapper (see `AppSidebar`), and
// both the title bar's own height and the sidebar's top offset read from that
// variable — so the band height is stated in exactly one place.
const WIN_TITLE_BAR_HEIGHT_PX = '32px';

// Title-bar height, derived from `--win-titlebar-h`.
const WIN_TITLE_BAR_HEIGHT = 'h-[var(--win-titlebar-h)]';

// Sidebar width bounds (px). The card is drag-resizable from its right edge
// between these; `DEFAULT_SIDEBAR_WIDTH` matches the library's 16rem default.
// The chosen width is published as `--sidebar-width` (overriding the library
// default) so both the floating card and the layout spacer track it, and is
// persisted to `localStorage` under `SIDEBAR_WIDTH_KEY` across sessions.
const MIN_SIDEBAR_WIDTH = 200;
const MAX_SIDEBAR_WIDTH = 480;
const DEFAULT_SIDEBAR_WIDTH = 256;
const SIDEBAR_WIDTH_KEY = 'sidebar_width';

// The floating card's `p-2` padding (px): `--sidebar-width` spans the whole
// card, so the *visible* sidebar border sits this far inside the card's right
// edge. The resize handle centers on that visible border and the drag math
// offsets by it, so grabbing the border doesn't jump the width.
const CARD_PADDING = 8;

const clampWidth = (px: number) => Math.min(MAX_SIDEBAR_WIDTH, Math.max(MIN_SIDEBAR_WIDTH, px));

// Persisted, clamped sidebar width plus a setter that mirrors changes to
// `localStorage`. Reads the stored value lazily on first render. The app can
// open several main windows at once; they share one webview origin (hence one
// `localStorage`), but each keeps its own width in React state and never syncs
// live — so resizing one window leaves the others untouched, while a *newly*
// opened window inherits the last-saved width from `localStorage` on mount.
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

// Drag handle centered on the sidebar's visible right border, straddling it
// symmetrically. On pointer-drag it maps the pointer's x-position (measured from
// the window's left edge, where the card sits) to the card width — adding the
// card padding so the visible border tracks the cursor without jumping on grab;
// the parent clamps and persists it. Pointer capture keeps the drag alive even
// when the cursor outruns the thin handle.
function ResizeHandle({ onResize }: { onResize: (px: number) => void }) {
  const handlePointerDown = useCallback(
    (e: ReactPointerEvent<HTMLDivElement>) => {
      e.preventDefault();
      e.currentTarget.setPointerCapture(e.pointerId);
      // Force the resize cursor for the whole drag: without this it reverts
      // whenever the pointer strays off the thin handle onto other elements.
      // `user-select: none` stops text getting selected as the pointer moves.
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

// macOS: the sidebar toggle, fixed just right of the traffic lights. It lives
// outside the sidebar (not in `MacTitleBar`) so it stays put — and stays
// clickable to reopen — when the sidebar is hidden. `z-20` keeps it above both
// the sidebar (`z-10`) and the window drag band (`z-0`).
function MacSidebarToggle({ onHoverStart, onHoverEnd }: HoverHandlers) {
  return (
    <div className={`fixed top-0 z-20 flex ${MAC_TITLE_BAR_HEIGHT} items-center`} style={{ left: MAC_TOGGLE_LEFT }}>
      <SidebarToggle onHoverStart={onHoverStart} onHoverEnd={onHoverEnd} />
    </div>
  );
}

// Linux/Windows: a full-width custom title bar across the top of the window.
// Fixed and above the sidebar (`z-30` over the sidebar's `z-10`) so its controls
// stay clickable; its middle strip is the window's drag region. The sidebar
// toggle sits next to the hamburger and, being part of this fixed bar rather
// than the sidebar itself, stays visible to reopen a hidden sidebar.
function WinTitleBar({ onHoverStart, onHoverEnd }: HoverHandlers) {
  return (
    // `transform-gpu` (translateZ(0)) pins this fixed, opaque bar to its own
    // retained compositor layer. Without it, WebKitGTK re-rasters the layer
    // during an interactive resize and — because the window is transparent —
    // the title area briefly shows through to the gutter/desktop (a flash). A
    // retained backing store survives the resize. Safe here: the bar has no body
    // text, so the subpixel-rendering reason we avoid transforms app-wide (see
    // `window-frame.tsx`) doesn't apply.
    <div
      className={`fixed inset-x-0 top-0 z-30 flex transform-gpu ${WIN_TITLE_BAR_HEIGHT} items-stretch gap-0.5 bg-background`}
    >
      <AppMenu />
      <SidebarToggle onHoverStart={onHoverStart} onHoverEnd={onHoverEnd} />
      <div data-testid="window-drag-region" data-tauri-drag-region aria-hidden className="h-full flex-1" />
      <WindowControls />
    </div>
  );
}

// Handlers the toggle fires on hover, wired by the shell to preview a collapsed
// sidebar as a popup.
type HoverHandlers = {
  onHoverStart?: () => void;
  onHoverEnd?: () => void;
};

type ShellProps = {
  mac: boolean;
  onResize: (px: number) => void;
  nav?: ReactNode;
  footer?: ReactNode;
  children?: ReactNode;
};

// Our floating-sidebar shell. `SidebarProvider` (above) owns the open/collapsed
// state; here we render it. When open, a fixed card holds the nav/footer and a
// same-width spacer reserves room for it in the flex row so the page inset sits
// beside it. When collapsed, both vanish immediately (no slide) and the inset
// reclaims the full width — but hovering the toggle then re-mounts the same card
// as an unpinned popup that overlays the page (no spacer) and disappears once
// the pointer leaves both the toggle and the card.
function SidebarShell({ mac, onResize, nav, footer, children }: ShellProps) {
  const { open, setOpen } = useSidebar();

  // Follow the medium breakpoint: collapse the sidebar when the window narrows
  // below it and reopen it when the window widens back above. We act on each
  // `change` edge (crossing the breakpoint), so between crossings a manual
  // toggle still sticks. The narrow-at-mount case is handled synchronously by
  // the provider's `defaultOpen` (see `AppSidebar`), so there's no post-paint
  // collapse here that would flash the sidebar open first.
  useEffect(() => {
    const mql = window.matchMedia(mediumBreakpointQuery());
    const onChange = (e: MediaQueryListEvent) => setOpen(!e.matches);
    mql.addEventListener('change', onChange);
    return () => mql.removeEventListener('change', onChange);
  }, [setOpen]);

  // Hover-preview state for the collapsed sidebar. A short close delay
  // (`PREVIEW_CLOSE_DELAY_MS`) bridges the gap as the pointer travels from the
  // toggle down into the popup, so it doesn't flicker shut between the two.
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
  useEffect(() => () => clearTimeout(closeTimer.current), []);

  // Only preview while collapsed; pinning open (or the width drag) takes over.
  const showingPreview = !open && preview;
  const cardVisible = open || showingPreview;
  const hoverHandlers: HoverHandlers = open ? {} : { onHoverStart: showPreview, onHoverEnd: hidePreview };

  // Card geometry differs between the pinned sidebar and the hover popup:
  //   • Pinned: full window height (`bottom-0`) anchored at the top — flush to
  //     the window top on macOS (its header carries the title bar) or just below
  //     the custom title bar off macOS.
  //   • Popup: a dropdown that drops from under the toggle. It's height-fits-
  //     content (no `bottom-0`, capped so long content scrolls) and hangs just
  //     below the title-bar band on every platform, so on macOS it clears the
  //     traffic lights instead of spanning up behind them like the real sidebar.
  const dockedTop = mac ? 'top-0' : 'top-[var(--win-titlebar-h)]';
  const previewTop = mac ? MAC_TITLE_BAR_TOP : 'top-[var(--win-titlebar-h)]';

  return (
    <>
      {!mac && <WinTitleBar {...hoverHandlers} />}

      {/* Layout spacer: reserves the sidebar's width in the flex row so the inset
          sits beside the floating card. Zero width when collapsed — the hover
          popup overlays the page rather than reserving room. */}
      <div className="shrink-0" aria-hidden style={{ width: open ? 'var(--sidebar-width)' : 0 }} />

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
            {/* The pinned macOS card doubles as the title bar; the popup drops
                below it, so it needs no in-card title bar. */}
            {mac && !showingPreview && <MacTitleBar />}
            <SidebarContent>{nav}</SidebarContent>
            <SidebarFooter>{footer}</SidebarFooter>
          </div>
          {/* No resize handle on the transient popup — resizing is a pinned-only
              affordance. */}
          {!showingPreview && <ResizeHandle onResize={onResize} />}
        </div>
      )}

      <SidebarInset data-testid="sidebar-inset">{children}</SidebarInset>

      {/* Mac-only trailing chrome: rendered after the inset so the drag band
          paints over the page background (see MacWindowDragBand); the toggle
          floats above everything (z-20). */}
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

  // Start collapsed when the window opens already below the medium breakpoint.
  // Computed synchronously (once) so the provider's first render is correct and
  // the sidebar never flashes open before the breakpoint effect can close it.
  const [initialOpen] = useState(() => !window.matchMedia(mediumBreakpointQuery()).matches);

  // Publish the drag-chosen width as `--sidebar-width` (overriding the library
  // default) and, off macOS, the custom title-bar height — both so the bar and
  // the sidebar offset can read it from one place (see `WIN_TITLE_BAR_HEIGHT_PX`).
  const style = {
    '--sidebar-width': `${width}px`,
    ...(mac ? {} : { '--win-titlebar-h': WIN_TITLE_BAR_HEIGHT_PX }),
  } as CSSProperties;

  return (
    // The library's provider wrapper hardcodes `min-h-svh` (the window viewport);
    // off macOS the app is inset inside `WindowFrame`, so override it to fill the
    // frame instead (see `--app-min-h` in `index.css`). `cn` (tailwind-merge)
    // dedupes the `min-h-*` utilities, keeping ours.
    <SidebarProvider defaultOpen={initialOpen} style={style} className="min-h-[var(--app-min-h)]">
      <SidebarShell mac={mac} onResize={setWidth} nav={nav} footer={footer}>
        {children}
      </SidebarShell>
    </SidebarProvider>
  );
}
