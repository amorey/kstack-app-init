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

import { act, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  MAC_USER_AGENT,
  NON_MAC_USER_AGENT,
  mockMatchMedia,
  mockTauriWindow,
  restoreUserAgent,
  setUserAgent,
} from '@/test-utils';

// Mocks ---------------------------------------------------------------

// `WindowControls` (rendered off-macOS) drives the native window via
// `getCurrentWindow`, so the window API must be mocked before importing.
const { factory } = mockTauriWindow();
vi.mock('@tauri-apps/api/window', () => factory());

const { AppSidebar } = await import('./app-sidebar');

// Helpers -------------------------------------------------------------

beforeEach(() => {
  setUserAgent(NON_MAC_USER_AGENT);
  localStorage.clear();
});

// jsdom doesn't implement pointer capture; the handle calls it, so stub it out.
if (!HTMLElement.prototype.setPointerCapture) {
  HTMLElement.prototype.setPointerCapture = () => {};
}

const wrapperWidth = (container: HTMLElement) =>
  (container.querySelector('[data-slot="sidebar-wrapper"]') as HTMLElement).style.getPropertyValue('--sidebar-width');

// Installs a controllable `matchMedia` whose `matches` here means "below the medium
// breakpoint". Returns a `setNarrow` to drive breakpoint crossings (see
// `mockMatchMedia`).
const mockBreakpoint = mockMatchMedia;

const sidebarVisible = () => screen.queryByTestId('app-sidebar') !== null;

afterEach(() => {
  restoreUserAgent();
});

// Tests ---------------------------------------------------------------

describe('AppSidebar', () => {
  it('renders the floating sidebar card', () => {
    render(<AppSidebar />);
    expect(screen.getByTestId('app-sidebar')).toBeInTheDocument();
  });

  it('collapses the pinned sidebar via the toggle without unmounting the toggle', () => {
    render(<AppSidebar />);
    // Open by default: the floating card is mounted.
    expect(screen.getByTestId('app-sidebar')).toBeInTheDocument();
    // Collapsing unmounts the card outright (instant hide, no slide) but leaves
    // the toggle in place so the sidebar can be reopened.
    fireEvent.click(screen.getByRole('button', { name: /toggle sidebar/i }));
    expect(screen.queryByTestId('app-sidebar')).not.toBeInTheDocument();
  });

  it('pins the full sidebar back open on click while collapsed', () => {
    render(<AppSidebar />);
    const toggle = screen.getByRole('button', { name: /toggle sidebar/i });
    // Collapse first.
    fireEvent.click(toggle);
    expect(screen.queryByTestId('app-sidebar')).not.toBeInTheDocument();

    // Clicking again pins the full sidebar back open — the pinned card carries a
    // resize handle (the popup would not).
    fireEvent.click(toggle);
    expect(screen.getByTestId('app-sidebar')).toBeInTheDocument();
    expect(screen.getByTestId('sidebar-resize-handle')).toBeInTheDocument();
  });

  it('pins the sidebar open on click while its hover popup is showing', () => {
    render(<AppSidebar />);
    const toggle = screen.getByRole('button', { name: /toggle sidebar/i });
    fireEvent.click(toggle);

    // Hover opens the unpinned popup (no resize handle); a click then pins the
    // full sidebar open in its place (resize handle appears).
    fireEvent.mouseEnter(toggle);
    expect(screen.queryByTestId('sidebar-resize-handle')).not.toBeInTheDocument();
    fireEvent.click(toggle);
    expect(screen.getByTestId('app-sidebar')).toBeInTheDocument();
    expect(screen.getByTestId('sidebar-resize-handle')).toBeInTheDocument();
  });

  it('previews the collapsed sidebar as a popup while hovering the toggle, then hides it on leave', async () => {
    vi.useFakeTimers();
    try {
      render(<AppSidebar />);
      const toggle = screen.getByRole('button', { name: /toggle sidebar/i });
      // Collapse first: the card unmounts.
      fireEvent.click(toggle);
      expect(screen.queryByTestId('app-sidebar')).not.toBeInTheDocument();

      // Hovering the toggle re-mounts the card as a hover popup.
      fireEvent.mouseEnter(toggle);
      expect(screen.getByTestId('app-sidebar')).toBeInTheDocument();
      // The popup carries no resize handle (pinned-only affordance).
      expect(screen.queryByTestId('sidebar-resize-handle')).not.toBeInTheDocument();

      // Leaving the toggle closes it after the grace delay elapses.
      fireEvent.mouseLeave(toggle);
      expect(screen.getByTestId('app-sidebar')).toBeInTheDocument();
      await act(async () => {
        await vi.advanceTimersByTimeAsync(400);
      });
      expect(screen.queryByTestId('app-sidebar')).not.toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps the hover popup open when the pointer moves from the toggle onto the card', async () => {
    vi.useFakeTimers();
    try {
      render(<AppSidebar />);
      const toggle = screen.getByRole('button', { name: /toggle sidebar/i });
      fireEvent.click(toggle);

      fireEvent.mouseEnter(toggle);
      const popup = screen.getByTestId('app-sidebar');
      // Pointer leaves the toggle but lands on the card before the delay fires.
      fireEvent.mouseLeave(toggle);
      fireEvent.mouseEnter(popup);
      await act(async () => {
        await vi.advanceTimersByTimeAsync(400);
      });
      // Still open — hover moved onto the card, cancelling the close.
      expect(screen.getByTestId('app-sidebar')).toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  });

  it('renders the macOS hover popup as a dropdown below the title bar (no in-card title bar)', () => {
    setUserAgent(MAC_USER_AGENT);
    render(<AppSidebar />);
    const toggle = screen.getByRole('button', { name: /toggle sidebar/i });
    // Pinned open: the card carries the macOS title bar (traffic-light gutter).
    expect(screen.getByTestId('traffic-light-gutter')).toBeInTheDocument();

    fireEvent.click(toggle);
    fireEvent.mouseEnter(toggle);
    // The popup drops below the band, so it omits the in-card title bar and hangs
    // from the title-bar height rather than the window top.
    const popup = screen.getByTestId('app-sidebar');
    expect(screen.queryByTestId('traffic-light-gutter')).not.toBeInTheDocument();
    expect(popup.className).toContain('top-11');
    expect(popup.className).not.toContain('bottom-0');
  });

  it('exposes a draggable title-bar region', () => {
    const { container } = render(<AppSidebar />);
    expect(container.querySelector('[data-tauri-drag-region]')).not.toBeNull();
  });

  it('renders a full-width custom title bar with app menu and window controls on Linux/Windows', () => {
    render(<AppSidebar />);
    // Off macOS the title bar carries the hamburger app menu, a drag strip, and
    // the custom window controls — and there's no macOS traffic-light gutter.
    expect(screen.getByRole('button', { name: /application menu/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /minimize/i })).toBeInTheDocument();
    expect(screen.getByTestId('window-drag-region')).toHaveAttribute('data-tauri-drag-region');
    expect(screen.queryByTestId('traffic-light-gutter')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /toggle sidebar/i })).toBeInTheDocument();
  });

  it('reserves a traffic-light gutter and hides the custom title bar on macOS', () => {
    setUserAgent(MAC_USER_AGENT);
    render(<AppSidebar />);
    // macOS keeps the native traffic lights and its header-as-title-bar, so the
    // custom app menu and window controls are absent.
    expect(screen.getByTestId('traffic-light-gutter')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /file/i })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /minimize/i })).not.toBeInTheDocument();
    // The sidebar toggle sits beside the traffic lights.
    expect(screen.getByRole('button', { name: /toggle sidebar/i })).toBeInTheDocument();
  });

  it('resizes the sidebar by dragging the handle and persists the width', () => {
    const { container } = render(<AppSidebar />);
    // Defaults to the library width until dragged.
    expect(wrapperWidth(container)).toBe('256px');

    // The handle is centered on the visible border (8px inside the card's right
    // edge), so the resulting width is the pointer x plus that padding.
    fireEvent.pointerDown(screen.getByTestId('sidebar-resize-handle'), { pointerId: 1 });
    fireEvent.pointerMove(window, { clientX: 300 });
    expect(wrapperWidth(container)).toBe('308px');
    expect(localStorage.getItem('sidebar_width')).toBe('308');

    fireEvent.pointerUp(window);
    // Releasing detaches the listeners: further movement is ignored.
    fireEvent.pointerMove(window, { clientX: 400 });
    expect(wrapperWidth(container)).toBe('308px');
  });

  it('forces the resize cursor for the whole drag, then restores it', () => {
    render(<AppSidebar />);
    expect(document.body.style.cursor).toBe('');

    fireEvent.pointerDown(screen.getByTestId('sidebar-resize-handle'), { pointerId: 1 });
    // Held even when the pointer strays off the thin handle.
    fireEvent.pointerMove(window, { clientX: 300 });
    expect(document.body.style.cursor).toBe('col-resize');

    fireEvent.pointerUp(window);
    expect(document.body.style.cursor).toBe('');
  });

  it('clamps the dragged width to the min/max bounds', () => {
    const { container } = render(<AppSidebar />);
    fireEvent.pointerDown(screen.getByTestId('sidebar-resize-handle'), { pointerId: 1 });

    fireEvent.pointerMove(window, { clientX: 9999 });
    expect(wrapperWidth(container)).toBe('480px');
    fireEvent.pointerMove(window, { clientX: 10 });
    expect(wrapperWidth(container)).toBe('200px');
  });

  it('restores the last-saved width on mount (new windows inherit it)', () => {
    localStorage.setItem('sidebar_width', '320');
    const { container } = render(<AppSidebar />);
    expect(wrapperWidth(container)).toBe('320px');
  });

  it('remembers a manual collapse across a narrow→wide breakpoint round-trip', () => {
    const setNarrow = mockBreakpoint();
    render(<AppSidebar />);
    // Open by default (wide). User manually collapses it.
    expect(sidebarVisible()).toBe(true);
    fireEvent.click(screen.getByRole('button', { name: /toggle sidebar/i }));
    expect(sidebarVisible()).toBe(false);
    // Narrow below md, then widen back: the sidebar must stay hidden, honoring
    // the user's choice rather than auto-reopening.
    setNarrow(true);
    expect(sidebarVisible()).toBe(false);
    setNarrow(false);
    expect(sidebarVisible()).toBe(false);
  });

  it('auto-collapses when narrowing and restores an open sidebar when widening', () => {
    const setNarrow = mockBreakpoint();
    render(<AppSidebar />);
    expect(sidebarVisible()).toBe(true);
    setNarrow(true);
    expect(sidebarVisible()).toBe(false);
    setNarrow(false);
    expect(sidebarVisible()).toBe(true);
  });

  it('toggles the hover popup on click while too narrow to pin the sidebar', () => {
    const setNarrow = mockBreakpoint();
    render(<AppSidebar />);
    const toggle = screen.getByRole('button', { name: /toggle sidebar/i });
    // Narrow below md: the sidebar auto-collapses and there's no room to pin it.
    setNarrow(true);
    expect(sidebarVisible()).toBe(false);

    // Hover peeks the popup (unpinned: no resize handle). A click while narrow
    // dismisses that popup in place rather than pinning the full sidebar.
    fireEvent.mouseEnter(toggle);
    expect(sidebarVisible()).toBe(true);
    expect(screen.queryByTestId('sidebar-resize-handle')).not.toBeInTheDocument();
    fireEvent.click(toggle);
    expect(sidebarVisible()).toBe(false);
    // And clicking again brings the popup back (still unpinned).
    fireEvent.click(toggle);
    expect(sidebarVisible()).toBe(true);
    expect(screen.queryByTestId('sidebar-resize-handle')).not.toBeInTheDocument();
  });

  it('renders page content in the inset', () => {
    render(
      <AppSidebar>
        <div>page-content</div>
      </AppSidebar>,
    );
    expect(screen.getByText('page-content')).toBeInTheDocument();
  });
});
