// Copyright 2026 The Kstack Authors
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

// The app's floating sidebar shell. Per-OS title bar: macOS — sidebar header
// doubles as the title bar (traffic-light gutter + drag band); Linux/Windows —
// full-width custom bar (`AppMenu`, drag strip, `WindowControls`).
// See docs/adr/2026-08-09-per-platform-window-chrome.md
//
// The library's `SidebarProvider`/`useSidebar` owns open/collapsed state, but the
// shell is rendered here: the library's `Sidebar` always slide-animates, and we
// want instant show/hide (mount/unmount the card, toggle a spacer's width).
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

// Space (px) for the macOS traffic lights — not derived from the host's
// `trafficLightPosition`; eyeball against the native buttons when tuning.
const TRAFFIC_LIGHT_GUTTER = 62;

// Fixed macOS toggle's left offset (px) — window-edge-relative, not card-relative:
// the toggle stays put while the card can be unmounted. Clears the traffic lights (~72px).
const MAC_TOGGLE_LEFT = 96;

// macOS title-bar band height (44px — matches the host's `TITLE_BAR_HEIGHT`).
const MAC_TITLE_BAR_HEIGHT = 'h-11';

// Must match `MAC_TITLE_BAR_HEIGHT` by hand — Tailwind can't derive one from the other.
const MAC_TITLE_BAR_TOP = 'top-11';

// The hover popup is a standalone dropdown, so it has its own fixed footprint
// rather than tracking `--sidebar-width`.
const PREVIEW_POPUP_WIDTH = 'w-72';
const PREVIEW_POPUP_HEIGHT = 'h-[70svh]';

// Grace delay (ms) bridging the pointer's travel from toggle onto the popup.
const PREVIEW_CLOSE_DELAY_MS = 300;

// Matches `< md` off the `--breakpoint-md` theme variable (the inverse of the
// `md:` variant), so it tracks the theme if the breakpoint changes.
function mediumBreakpointQuery() {
  const md = getComputedStyle(document.documentElement).getPropertyValue('--breakpoint-md').trim() || '48rem';
  return `(max-width: calc(${md} - 1px))`;
}

// "Too narrow to pin" signal; seeded synchronously (no post-paint flash).
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

// Linux/Windows title-bar height (~native Windows caption bar), published as
// `--win-titlebar-h` so the bar's height and the sidebar's top offset share it.
const WIN_TITLE_BAR_HEIGHT_PX = '32px';

const WIN_TITLE_BAR_HEIGHT = 'h-[var(--win-titlebar-h)]';

// Drag-resize bounds (px); `DEFAULT` matches the library's 16rem. Width publishes
// as `--sidebar-width` and persists to localStorage.
const MIN_SIDEBAR_WIDTH = 200;
const MAX_SIDEBAR_WIDTH = 480;
const DEFAULT_SIDEBAR_WIDTH = 256;
const SIDEBAR_WIDTH_KEY = 'sidebar_width';

// The card's `p-2` padding (px): the visible border sits this far inside the card's
// right edge, and the drag math offsets by it so grabbing the handle doesn't jump.
const CARD_PADDING = 8;

const clampWidth = (px: number) => Math.min(MAX_SIDEBAR_WIDTH, Math.max(MIN_SIDEBAR_WIDTH, px));

// Persisted, clamped width. Windows share one localStorage but never sync live —
// resizing one leaves the others untouched; a new window inherits the last-saved width.
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

// Drag handle on the sidebar's visible right border; maps pointer x + card padding
// to card width (parent clamps/persists). Pointer capture keeps the drag alive when
// the cursor outruns the thin handle.
function ResizeHandle({ onResize }: { onResize: (px: number) => void }) {
  const handlePointerDown = useCallback(
    (e: ReactPointerEvent<HTMLDivElement>) => {
      e.preventDefault();
      e.currentTarget.setPointerCapture(e.pointerId);
      // Force the resize cursor for the whole drag (else it reverts off the thin
      // handle); `user-select: none` stops text selection.
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

// macOS: window-wide drag strip across the title-bar band. `z-0` keeps it below the
// sidebar (`z-10`) so its controls stay clickable; pages reserve the band with top
// padding so it never covers interactive content.
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

// macOS: the sidebar header doubles as the title bar (traffic-light gutter + drag strip).
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

// macOS: fixed toggle right of the traffic lights — outside the sidebar so it stays
// clickable when the sidebar is hidden; `z-20` above sidebar and drag band.
function MacSidebarToggle(handlers: HoverHandlers) {
  return (
    <div className={`fixed top-0 z-20 flex ${MAC_TITLE_BAR_HEIGHT} items-center`} style={{ left: MAC_TOGGLE_LEFT }}>
      <SidebarToggle {...handlers} />
    </div>
  );
}

// Linux/Windows: full-width custom title bar, fixed above the sidebar (`z-30`);
// the toggle rides this fixed bar so it can reopen a hidden sidebar.
function WinTitleBar(handlers: HoverHandlers) {
  return (
    // `transform-gpu` pins the bar to a retained compositor layer — without it
    // WebKitGTK re-rasters during an interactive resize and the transparent window
    // flashes through to the desktop. Safe here: no body text, so the app-wide
    // avoid-transforms rule (`window-frame.tsx`) doesn't apply.
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

// Toggle handlers for the collapsed-sidebar hover popup. `onClick` is supplied only
// when narrow (no room to pin — a click toggles the peeked popup); unset keeps the
// toggle's default pin/collapse click.
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

// When open, a fixed card + same-width spacer keep the inset beside it; collapsed,
// both vanish instantly and hovering the toggle peeks the card as an unpinned popup.
// Clicking the toggle pins it back open (like `Cmd/Ctrl+B`).
function SidebarShell({ mac, onResize, nav, footer, children }: ShellProps) {
  const { open } = useSidebar();

  // Narrowness is a presentation constraint, not a state change: `open` stays the
  // true intent, `effectiveOpen` is what renders — widening restores the sidebar
  // for free, with no shadow state to sync.
  const isNarrow = useIsNarrow();
  const effectiveOpen = open && !isNarrow;

  // Hover-preview state for the collapsed sidebar.
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
  // Narrow-collapsed: click flips the popup in place; cancel any pending close so a
  // click right after a hover isn't undone.
  const togglePreview = useCallback(() => {
    clearTimeout(closeTimer.current);
    setPreview((p) => !p);
  }, []);
  useEffect(() => () => clearTimeout(closeTimer.current), []);
  // Drop a lingering hover-preview the moment the sidebar docks. Render-time state
  // adjustment (not an effect) so it lands before paint.
  const [prevEffectiveOpen, setPrevEffectiveOpen] = useState(effectiveOpen);
  if (effectiveOpen !== prevEffectiveOpen) {
    setPrevEffectiveOpen(effectiveOpen);
    if (effectiveOpen) setPreview(false);
  }

  // Preview only while collapsed.
  const showingPreview = !effectiveOpen && preview;
  const cardVisible = effectiveOpen || showingPreview;
  // Docked: default collapse click. Collapsed: hover peeks; click pins back open
  // (wide, `onClick` unset) or toggles the peeked popup (narrow).
  const hoverHandlers: HoverHandlers = effectiveOpen
    ? {}
    : { onHoverStart: showPreview, onHoverEnd: hidePreview, onClick: isNarrow ? togglePreview : undefined };

  // Pinned: full-height, top-anchored (flush on macOS, below the custom bar off it).
  // Popup: hangs just below the title-bar band so it clears the macOS traffic lights.
  const dockedTop = mac ? 'top-0' : 'top-[var(--win-titlebar-h)]';
  const previewTop = mac ? MAC_TITLE_BAR_TOP : 'top-[var(--win-titlebar-h)]';

  return (
    <>
      {!mac && <WinTitleBar {...hoverHandlers} />}

      {/* Spacer reserving the sidebar's width; zero when collapsed (the popup overlays). */}
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

      {/* Mac-only trailing chrome, after the inset so the drag band paints over the page. */}
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

  // Publish `--sidebar-width` and, off macOS, `--win-titlebar-h`.
  const style = {
    '--sidebar-width': `${width}px`,
    ...(mac ? {} : { '--win-titlebar-h': WIN_TITLE_BAR_HEIGHT_PX }),
  } as CSSProperties;

  return (
    // The app is exactly the `WindowFrame` inset tall, so a route can scroll inside it.
    // The provider hardcodes `min-h-svh`, which on Linux is taller than the frame:
    // `min-h-0` displaces it (same tailwind-merge group), `h-` alone would not.
    <SidebarProvider style={style} className="h-(--app-min-h) min-h-0">
      <SidebarShell mac={mac} onResize={setWidth} nav={nav} footer={footer}>
        {children}
      </SidebarShell>
    </SidebarProvider>
  );
}
