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

import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { mockTauriCore } from '@/test-utils';

// Mocks ---------------------------------------------------------------

const { invokeMock, factory } = mockTauriCore();
vi.mock('@tauri-apps/api/core', () => factory());

const { AppMenu } = await import('./app-menu');

// Helpers -------------------------------------------------------------

const originalUserAgent = window.navigator.userAgent;

function setUserAgent(value: string) {
  Object.defineProperty(window.navigator, 'userAgent', { value, configurable: true });
}

beforeEach(() => {
  invokeMock.mockReset();
  invokeMock.mockResolvedValue(undefined);
  // Default to a non-macOS UA so the hamburger renders.
  setUserAgent('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36');
});

afterEach(() => {
  setUserAgent(originalUserAgent);
});

// Tests ---------------------------------------------------------------

describe('AppMenu', () => {
  it('renders an application menu trigger on Linux/Windows', () => {
    render(<AppMenu />);
    expect(screen.getByRole('button', { name: /application menu/i })).toBeInTheDocument();
  });

  it('renders nothing on macOS (native menu bar owns it)', () => {
    setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15');
    const { container } = render(<AppMenu />);
    expect(container).toBeEmptyDOMElement();
  });

  it('invokes new_window when the New Window item is clicked', async () => {
    const user = userEvent.setup();
    render(<AppMenu />);
    await user.click(screen.getByRole('button', { name: /application menu/i }));
    await user.click(await screen.findByRole('menuitem', { name: /new window/i }));
    expect(invokeMock).toHaveBeenCalledWith('new_window');
  });

  it('invokes quit when the Quit item is clicked', async () => {
    const user = userEvent.setup();
    render(<AppMenu />);
    await user.click(screen.getByRole('button', { name: /application menu/i }));
    await user.click(await screen.findByRole('menuitem', { name: /quit/i }));
    expect(invokeMock).toHaveBeenCalledWith('quit');
  });

  it('invokes new_window on Ctrl+N', async () => {
    render(<AppMenu />);
    fireEvent.keyDown(window, { key: 'n', ctrlKey: true });
    await waitFor(() => expect(invokeMock).toHaveBeenCalledWith('new_window'));
  });

  it('invokes quit on Ctrl+Q', async () => {
    render(<AppMenu />);
    fireEvent.keyDown(window, { key: 'q', ctrlKey: true });
    await waitFor(() => expect(invokeMock).toHaveBeenCalledWith('quit'));
  });

  it('also accepts the Cmd (meta) modifier for shortcuts', async () => {
    render(<AppMenu />);
    fireEvent.keyDown(window, { key: 'n', metaKey: true });
    await waitFor(() => expect(invokeMock).toHaveBeenCalledWith('new_window'));
  });

  it('ignores plain keypresses without a modifier', () => {
    render(<AppMenu />);
    fireEvent.keyDown(window, { key: 'n' });
    expect(invokeMock).not.toHaveBeenCalled();
  });

  it('does not register shortcuts on macOS', () => {
    setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15');
    render(<AppMenu />);
    fireEvent.keyDown(window, { key: 'n', ctrlKey: true });
    expect(invokeMock).not.toHaveBeenCalled();
  });
});
