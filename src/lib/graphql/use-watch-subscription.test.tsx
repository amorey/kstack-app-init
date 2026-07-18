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

import { createElement } from 'react';
import { act, renderHook, waitFor } from '@testing-library/react';
import { Client, gql, Provider } from 'urql';
import type { ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { mockTauriCore } from '@/test-utils';

// End-to-end wiring: a real urql Client running the real subscribe-exchange over
// a mocked Tauri channel, so the hook, the exchange, and the transport-status
// registry all share one module graph. (The exchange's own unit test mocks the
// registry; this test exercises the integration the mock stands in for.)
const { invokeMock, channels, liveChannel, factory } = mockTauriCore();
vi.mock('@tauri-apps/api/core', () => factory());
vi.mock('../error-bus', () => ({
  reportError: () => {},
  errorMessage: (err: unknown) => String(err),
}));

const { tauriSubscriptionExchange } = await import('./subscribe-exchange');
const { useWatchSubscription, watchPhase } = await import('./use-watch-subscription');

const TICK = gql`
  subscription Tick {
    tick
  }
`;

const NEXT = (n: number) => JSON.stringify({ type: 'next', payload: { data: { tick: n } } });
const OPEN = JSON.stringify({ type: 'open' });

// Append each tick — a stand-in for a delta reducer whose accumulation must be
// thrown away and rebuilt from scratch on a reconnect.
const appendTick = (prev: number[] | undefined, data: { tick: number }) => [...(prev ?? []), data.tick];

// Drive the live channel, wrapped in act so React flushes the resulting renders.
const emit = (raw: string) => act(() => liveChannel().onmessage!(raw));

// A fresh urql Client (running the real exchange) wrapped as a renderHook provider.
function makeWrapper() {
  const client = new Client({ url: 'tauri://graphql', exchanges: [tauriSubscriptionExchange] });
  return ({ children }: { children: ReactNode }) => createElement(Provider, { value: client }, children);
}

function renderWatch() {
  return renderHook(
    () => useWatchSubscription<{ tick: number }, number[]>({ query: TICK, variables: {} }, appendTick),
    { wrapper: makeWrapper() },
  );
}

// The first tuple element of the hook result.
const state = (r: { result: { current: [{ data?: number[] | undefined; connected: boolean }, unknown] } }) =>
  r.result.current[0];

describe('useWatchSubscription', () => {
  beforeEach(() => {
    invokeMock.mockReset();
    channels.length = 0;
    let id = 0;
    invokeMock.mockImplementation(async (cmd: string) => {
      if (cmd === 'graphql_subscribe') {
        id += 1;
        return id;
      }
      return undefined;
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('is disconnected with no data before the connection opens', async () => {
    const r = renderWatch();
    await waitFor(() => expect(channels.length).toBeGreaterThan(0)); // the exchange dialed
    expect(state(r).data).toBeUndefined();
    expect(state(r).connected).toBe(false);
  });

  it('reports connected with undefined data on an empty snapshot (gap 1)', async () => {
    const r = renderWatch();
    await waitFor(() => expect(channels.length).toBeGreaterThan(0));

    await emit(OPEN); // connection up, but the snapshot replays nothing
    expect(state(r).connected).toBe(true);
    expect(state(r).data).toBeUndefined();
  });

  it('accumulates delta frames once connected', async () => {
    const r = renderWatch();
    await waitFor(() => expect(channels.length).toBeGreaterThan(0));

    await emit(OPEN);
    await emit(NEXT(1));
    await emit(NEXT(2));
    expect(state(r).data).toEqual([1, 2]);
    expect(state(r).connected).toBe(true);
  });

  it('keeps last-known data but flips connected false on a drop (gap 2)', async () => {
    vi.useFakeTimers();
    const r = renderWatch();
    await vi.waitFor(() => expect(channels.length).toBeGreaterThan(0));

    await emit(OPEN);
    await emit(NEXT(1));
    await emit(NEXT(2));

    await emit(JSON.stringify({ type: 'complete' })); // transport drops
    expect(state(r).connected).toBe(false);
    expect(state(r).data).toEqual([1, 2]); // still shown through the outage
  });

  it('rebuilds from scratch on reconnect — prior-connection state cannot linger', async () => {
    vi.useFakeTimers();
    const r = renderWatch();
    await vi.waitFor(() => expect(channels.length).toBeGreaterThan(0));

    await emit(OPEN);
    await emit(NEXT(1));
    await emit(NEXT(2));
    expect(state(r).data).toEqual([1, 2]);

    await emit(JSON.stringify({ type: 'complete' }));
    await vi.advanceTimersByTimeAsync(1_000); // backoff → reconnect dials
    await vi.waitFor(() => expect(channels.length).toBe(2));

    await emit(OPEN); // reconnect opens: masks stale data until the replay folds
    expect(state(r).connected).toBe(true);
    expect(state(r).data).toBeUndefined();

    await emit(NEXT(7)); // replayed snapshot has only object 7
    expect(state(r).data).toEqual([7]); // [1,2] are gone, not [1,2,7]
  });

  it('resets to empty on a reconnect whose replayed snapshot is empty', async () => {
    vi.useFakeTimers();
    const r = renderWatch();
    await vi.waitFor(() => expect(channels.length).toBeGreaterThan(0));

    await emit(OPEN);
    await emit(NEXT(1));
    expect(state(r).data).toEqual([1]);

    await emit(JSON.stringify({ type: 'complete' }));
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.waitFor(() => expect(channels.length).toBe(2));

    await emit(OPEN); // everything was deleted during the outage
    expect(state(r).connected).toBe(true);
    expect(state(r).data).toBeUndefined();
  });

  // urql carries a useSubscription accumulator across a variables change (new op
  // key). A per-key generation would restart at 1 and alias the old tag, folding
  // the next operation's first frame onto the previous one's data — e.g. chat,
  // where each request changes `variables`, would append a later response's first
  // chunk to the previous response. The globally-monotonic serial prevents it.
  it("does not carry a prior operation's accumulator when variables change (P1)", async () => {
    const r = renderHook(
      ({ n }: { n: number }) =>
        useWatchSubscription<{ tick: number }, number[]>({ query: TICK, variables: { n } }, appendTick),
      { wrapper: makeWrapper(), initialProps: { n: 1 } },
    );
    await waitFor(() => expect(channels.length).toBeGreaterThan(0));
    await emit(OPEN);
    await emit(NEXT(1));
    await emit(NEXT(2));
    expect(state(r).data).toEqual([1, 2]);

    act(() => r.rerender({ n: 2 })); // new variables ⇒ urql starts a new operation
    await waitFor(() => expect(channels.length).toBe(2));

    await emit(OPEN); // the new operation's connection opens
    expect(state(r).connected).toBe(true);
    expect(state(r).data).toBeUndefined(); // the old [1,2] must not show through

    await emit(NEXT(9));
    expect(state(r).data).toEqual([9]); // fresh — not [1, 2, 9]
  });

  // urql also retains the accumulator when the *same* operation pauses and
  // re-executes. Teardown clears the key's status, so a per-key counter would
  // restart at 1 and match the retained tag; the monotonic serial does not.
  it('does not reuse a retained accumulator when the same operation pauses and reopens (P2)', async () => {
    const r = renderHook(
      ({ paused }: { paused: boolean }) =>
        useWatchSubscription<{ tick: number }, number[]>({ query: TICK, variables: {}, pause: paused }, appendTick),
      { wrapper: makeWrapper(), initialProps: { paused: false } },
    );
    await waitFor(() => expect(channels.length).toBeGreaterThan(0));
    await emit(OPEN);
    await emit(NEXT(1));
    await emit(NEXT(2));
    expect(state(r).data).toEqual([1, 2]);

    act(() => r.rerender({ paused: true })); // teardown (urql retains the accumulator)
    act(() => r.rerender({ paused: false })); // re-execute the same op key
    await waitFor(() => expect(channels.length).toBe(2));

    await emit(OPEN);
    expect(state(r).connected).toBe(true);
    expect(state(r).data).toBeUndefined(); // the retained [1,2] must not survive

    await emit(NEXT(9));
    expect(state(r).data).toEqual([9]); // fresh — not [1, 2, 9]
  });
});

describe('watchPhase', () => {
  it('reads no-data-yet as connecting when the transport is down', () => {
    expect(watchPhase(false, false)).toBe('connecting');
  });

  it('reads no-data-yet as empty when the transport is up (an empty snapshot)', () => {
    expect(watchPhase(false, true)).toBe('empty');
  });

  it('reads held data through an outage as reconnecting', () => {
    expect(watchPhase(true, false)).toBe('reconnecting');
  });

  it('reads data on a live connection as live', () => {
    expect(watchPhase(true, true)).toBe('live');
  });
});
