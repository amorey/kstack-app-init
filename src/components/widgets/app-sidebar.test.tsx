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

import { fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { MAC_USER_AGENT, NON_MAC_USER_AGENT, mockTauriWindow, restoreUserAgent, setUserAgent } from '@/test-utils';

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

afterEach(() => {
  restoreUserAgent();
});

// Tests ---------------------------------------------------------------

describe('AppSidebar', () => {
  it('renders the floating sidebar card', () => {
    render(<AppSidebar />);
    expect(screen.getByTestId('app-sidebar')).toBeInTheDocument();
  });

  it('shows and hides the sidebar via the toggle without unmounting the toggle', () => {
    render(<AppSidebar />);
    // Open by default: the floating card is mounted.
    expect(screen.getByTestId('app-sidebar')).toBeInTheDocument();
    // Collapsing unmounts the card outright (instant hide, no slide) but leaves
    // the toggle in place so the sidebar can be reopened.
    fireEvent.click(screen.getByRole('button', { name: /toggle sidebar/i }));
    expect(screen.queryByTestId('app-sidebar')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /toggle sidebar/i }));
    expect(screen.getByTestId('app-sidebar')).toBeInTheDocument();
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

  it('renders page content in the inset', () => {
    render(
      <AppSidebar>
        <div>page-content</div>
      </AppSidebar>,
    );
    expect(screen.getByText('page-content')).toBeInTheDocument();
  });
});
