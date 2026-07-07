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
import { describe, expect, it, vi } from 'vitest';

import { mockTauriWindow } from '@/test-utils';

// Mocks ---------------------------------------------------------------

// `WindowControls` (in the custom title bar off macOS) drives the native window
// via `getCurrentWindow`.
const { factory } = mockTauriWindow();
vi.mock('@tauri-apps/api/window', () => factory());

// Replace each chrome widget with an identifiable stub so the test asserts
// *placement* (title bar vs sidebar vs inset) without dragging in their
// providers.
vi.mock('@/components/widgets/file-menu', () => ({
  FileMenu: () => <div data-testid="file-menu" />,
}));
vi.mock('@/components/widgets/kube-context-picker', () => ({
  KubeContextPicker: () => <div data-testid="kube-context-picker" />,
}));
vi.mock('@/components/widgets/account-menu', () => ({
  AccountMenu: () => <div data-testid="account-menu" />,
}));
vi.mock('@/lib/connection-status', () => ({
  ConnectionStatus: () => <div data-testid="connection-status" />,
}));

const { AppShell } = await import('./app-shell');

// Helpers -------------------------------------------------------------

function sidebarOf(container: HTMLElement) {
  const el = container.querySelector('[data-slot="sidebar"]');
  if (!el) throw new Error('sidebar not found');
  return el;
}

function insetOf(container: HTMLElement) {
  const el = container.querySelector('[data-slot="sidebar-inset"]');
  if (!el) throw new Error('sidebar inset not found');
  return el;
}

// Tests ---------------------------------------------------------------

describe('AppShell', () => {
  it('places the navigation chrome inside the sidebar', () => {
    const { container } = render(<AppShell />);
    const sidebar = sidebarOf(container);
    expect(sidebar.contains(screen.getByTestId('kube-context-picker'))).toBe(true);
  });

  it('places the File menu in the title bar, not the sidebar', () => {
    const { container } = render(<AppShell />);
    // Off macOS the File menu is a hamburger in the custom title bar, which sits
    // outside the sidebar.
    expect(sidebarOf(container).contains(screen.getByTestId('file-menu'))).toBe(false);
  });

  it('places the account chrome inside the sidebar', () => {
    const { container } = render(<AppShell />);
    const sidebar = sidebarOf(container);
    expect(sidebar.contains(screen.getByTestId('account-menu'))).toBe(true);
  });

  it('renders page content in the inset, not the sidebar', () => {
    const { container } = render(
      <AppShell>
        <div data-testid="page-content" />
      </AppShell>,
    );
    const page = screen.getByTestId('page-content');
    expect(insetOf(container).contains(page)).toBe(true);
    expect(sidebarOf(container).contains(page)).toBe(false);
  });

  it('renders the connection-status banner', () => {
    render(<AppShell />);
    expect(screen.getByTestId('connection-status')).toBeInTheDocument();
  });
});
