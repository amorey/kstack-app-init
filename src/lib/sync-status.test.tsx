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

import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

const { useSyncStatus, formatSyncFreshness } = await import('./sync-status');

describe('formatSyncFreshness', () => {
  const base = 1_000_000_000_000;

  it('reports never-synced for 0', () => {
    expect(formatSyncFreshness(0, base)).toMatch(/never synced/i);
  });

  it('buckets recent syncs into a relative label', () => {
    expect(formatSyncFreshness(base - 2_000, base)).toMatch(/just now/i);
    expect(formatSyncFreshness(base - 42_000, base)).toMatch(/42s/);
    expect(formatSyncFreshness(base - 5 * 60_000, base)).toMatch(/5m/);
    expect(formatSyncFreshness(base - 3 * 3_600_000, base)).toMatch(/3h/);
  });

  it('handles the exact bucket boundaries (strict <)', () => {
    expect(formatSyncFreshness(base - 5_000, base)).toMatch(/5s/); // not "just now"
    expect(formatSyncFreshness(base - 60_000, base)).toMatch(/1m/); // not "60s"
    expect(formatSyncFreshness(base - 3_600_000, base)).toMatch(/1h/); // not "60m"
  });

  it('clamps a future lastSyncedAt to "just now" (clock skew)', () => {
    expect(formatSyncFreshness(base + 10_000, base)).toMatch(/just now/i);
  });
});

describe('useSyncStatus', () => {
  it('throws outside the provider', () => {
    function Bare() {
      useSyncStatus();
      return null;
    }
    expect(() => render(<Bare />)).toThrow(/SyncStatusProvider/);
  });
});
