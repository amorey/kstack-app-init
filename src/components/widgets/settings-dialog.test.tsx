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

import { act, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { mockTauriCore } from '@/test-utils';

const { invokeMock, factory } = mockTauriCore();
vi.mock('@tauri-apps/api/core', () => factory());

const { ThemeProvider } = await import('@/lib/theme');
const { SettingsDialog } = await import('@/components/widgets/settings-dialog');

beforeEach(() => {
  document.documentElement.classList.remove('dark');
  invokeMock.mockReset();
  invokeMock.mockResolvedValue(undefined);
});

describe('SettingsDialog', () => {
  it('drives the theme through the appearance picker', async () => {
    render(
      <ThemeProvider>
        <SettingsDialog open onOpenChange={() => {}} />
      </ThemeProvider>,
    );

    // The three appearance segments are present.
    const dark = screen.getByRole('tab', { name: 'Dark' });
    expect(screen.getByRole('tab', { name: 'System' })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Light' })).toBeInTheDocument();

    // Picking Dark applies the scheme and persists it through the host.
    act(() => dark.click());
    expect(document.documentElement.classList.contains('dark')).toBe(true);
    await waitFor(() =>
      expect(invokeMock).toHaveBeenCalledWith('update_host_file', { patch: { colorSchemePreference: 'dark' } }),
    );
  });
});
