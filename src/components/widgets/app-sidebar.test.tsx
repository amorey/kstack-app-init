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

  it('exposes a full-width drag band in addition to the header strip', () => {
    const { container } = render(<AppSidebar />);
    // A window-wide band over the old title-bar area lets clicking the empty
    // top background (beside/over the page) move the window. It's a *second*
    // drag region alongside the sidebar header's own strip.
    const band = screen.getByTestId('window-drag-region');
    expect(band).toHaveAttribute('data-tauri-drag-region');
    expect(container.querySelectorAll('[data-tauri-drag-region]').length).toBeGreaterThanOrEqual(2);
  });

  it('reserves a traffic-light gutter and hides custom controls on macOS', () => {
    setUserAgent(MAC_USER_AGENT);
    render(<AppSidebar />);
    expect(screen.getByTestId('traffic-light-gutter')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /minimize/i })).not.toBeInTheDocument();
  });

  it('renders custom window controls and no gutter on Linux/Windows', () => {
    render(<AppSidebar />);
    expect(screen.getByRole('button', { name: /minimize/i })).toBeInTheDocument();
    expect(screen.queryByTestId('traffic-light-gutter')).not.toBeInTheDocument();
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
