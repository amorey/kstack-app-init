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

import { act, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it } from 'vitest';

import { SettingsDialog } from '@/components/widgets/settings-dialog';
import { ThemeProvider } from '@/lib/theme';

beforeEach(() => {
  localStorage.clear();
  document.documentElement.classList.remove('dark');
});

describe('SettingsDialog', () => {
  it('drives the theme through the appearance picker', () => {
    render(
      <ThemeProvider>
        <SettingsDialog open onOpenChange={() => {}} />
      </ThemeProvider>,
    );

    // The three appearance segments are present.
    const dark = screen.getByRole('tab', { name: 'Dark' });
    expect(screen.getByRole('tab', { name: 'System' })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Light' })).toBeInTheDocument();

    // Picking Dark applies the scheme and persists it.
    act(() => dark.click());
    expect(document.documentElement.classList.contains('dark')).toBe(true);
    expect(localStorage.getItem('kstack:color-scheme-preference')).toBe('dark');
  });
});
