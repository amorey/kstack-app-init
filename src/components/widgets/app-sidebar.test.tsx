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

import { render, screen } from '@testing-library/react';
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
});

afterEach(() => {
  restoreUserAgent();
});

// Tests ---------------------------------------------------------------

describe('AppSidebar', () => {
  it('renders a floating-variant sidebar', () => {
    const { container } = render(<AppSidebar />);
    expect(container.querySelector('[data-slot="sidebar"][data-variant="floating"]')).not.toBeNull();
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
  });

  it('reserves a traffic-light gutter and hides the custom title bar on macOS', () => {
    setUserAgent(MAC_USER_AGENT);
    render(<AppSidebar />);
    // macOS keeps the native traffic lights and its header-as-title-bar, so the
    // custom app menu and window controls are absent.
    expect(screen.getByTestId('traffic-light-gutter')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /file/i })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /minimize/i })).not.toBeInTheDocument();
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
