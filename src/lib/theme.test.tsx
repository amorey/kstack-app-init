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

import { ThemeProvider, applyPreference, readStoredPreference, useColorScheme } from '@/lib/theme';
import { mockMatchMedia } from '@/test-utils';

const STORAGE_KEY = 'kstack:color-scheme-preference';

beforeEach(() => {
  localStorage.clear();
  document.documentElement.classList.remove('dark');
});

describe('readStoredPreference', () => {
  it('defaults to system when unset', () => {
    expect(readStoredPreference()).toBe('system');
  });

  it('returns a stored valid preference', () => {
    localStorage.setItem(STORAGE_KEY, 'dark');
    expect(readStoredPreference()).toBe('dark');
  });

  it('falls back to system for a corrupt value', () => {
    localStorage.setItem(STORAGE_KEY, 'chartreuse');
    expect(readStoredPreference()).toBe('system');
  });
});

describe('applyPreference', () => {
  it('adds the dark class for the dark preference', () => {
    mockMatchMedia(false);
    applyPreference('dark');
    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });

  it('removes the dark class for the light preference', () => {
    mockMatchMedia(true);
    document.documentElement.classList.add('dark');
    applyPreference('light');
    expect(document.documentElement.classList.contains('dark')).toBe(false);
  });

  it('follows the OS for the system preference', () => {
    mockMatchMedia(true);
    applyPreference('system');
    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });
});

function Harness() {
  const { preference, setPreference } = useColorScheme();
  return (
    <div>
      <span data-testid="preference">{preference}</span>
      <button type="button" onClick={() => setPreference('dark')}>
        dark
      </button>
      <button type="button" onClick={() => setPreference('system')}>
        system
      </button>
    </div>
  );
}

describe('ThemeProvider', () => {
  it('applies the stored preference on mount', () => {
    mockMatchMedia(false);
    localStorage.setItem(STORAGE_KEY, 'dark');
    render(
      <ThemeProvider>
        <Harness />
      </ThemeProvider>,
    );
    expect(document.documentElement.classList.contains('dark')).toBe(true);
    expect(screen.getByTestId('preference').textContent).toBe('dark');
  });

  it('applies and persists an explicit choice', () => {
    mockMatchMedia(false);
    render(
      <ThemeProvider>
        <Harness />
      </ThemeProvider>,
    );
    act(() => screen.getByText('dark').click());
    expect(document.documentElement.classList.contains('dark')).toBe(true);
    expect(localStorage.getItem(STORAGE_KEY)).toBe('dark');
    expect(screen.getByTestId('preference').textContent).toBe('dark');
  });

  it('re-applies when the OS scheme flips while on system', () => {
    const setDark = mockMatchMedia(false);
    render(
      <ThemeProvider>
        <Harness />
      </ThemeProvider>,
    );
    // Starts on system + light OS → no dark class.
    expect(document.documentElement.classList.contains('dark')).toBe(false);
    // OS flips to dark → the subscribed provider re-applies.
    setDark(true);
    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });

  it('ignores OS changes once an explicit choice is made', () => {
    const setDark = mockMatchMedia(false);
    render(
      <ThemeProvider>
        <Harness />
      </ThemeProvider>,
    );
    // Choose Dark explicitly, then flip the OS to light: the explicit choice wins.
    act(() => screen.getByText('dark').click());
    setDark(false);
    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });
});
