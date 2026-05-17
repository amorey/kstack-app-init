import { pipe, subscribe } from 'wonka';
import { Client, gql } from 'urql';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { mockTauriCore } from '@/test-utils';

// Mocks ---------------------------------------------------------------

const { invokeMock, channels, liveChannel, factory } = mockTauriCore();
vi.mock('@tauri-apps/api/core', () => factory());

const reportErrorMock = vi.fn();
vi.mock('../error-bus', () => ({
  reportError: (...args: unknown[]) => reportErrorMock(...args),
}));

const { tauriSubscriptionExchange } = await import('./subscribe-exchange');

// Helpers -------------------------------------------------------------

function makeClient() {
  return new Client({
    url: 'tauri://graphql',
    exchanges: [tauriSubscriptionExchange],
  });
}

const TICK = gql`
  subscription Tick {
    tick
  }
`;

const NEXT = (n: number) => JSON.stringify({ type: 'next', payload: { data: { tick: n } } });

// Flush the pending microtask queue (the async invoke('graphql_subscribe')
// chain) without advancing fake timers.
const flush = async () => {
  await Promise.resolve();
  await Promise.resolve();
};

// Subscribe and collect every delivered `tick`.
function start() {
  const client = makeClient();
  const seen: number[] = [];
  const sub = pipe(
    client.subscription(TICK, {}),
    subscribe((r) => {
      if (r.data) seen.push((r.data as { tick: number }).tick);
    }),
  );
  return { seen, unsubscribe: sub.unsubscribe };
}

describe('tauriSubscriptionExchange', () => {
  beforeEach(() => {
    invokeMock.mockReset();
    reportErrorMock.mockReset();
    channels.length = 0;
    // graphql_subscribe hands back a unique, increasing op id per connect.
    let id = 0;
    invokeMock.mockImplementation(async (cmd: string) => {
      if (cmd === 'graphql_subscribe') {
        id += 1;
        return id;
      }
      if (cmd === 'graphql_unsubscribe') return undefined;
      throw new Error(`unexpected ${cmd}`);
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  const subscribeCalls = () => invokeMock.mock.calls.filter((c) => c[0] === 'graphql_subscribe').length;

  it('forwards `next` payloads to the urql sink and unsubscribes on teardown', async () => {
    const { seen, unsubscribe } = start();
    await flush();

    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_subscribe',
      expect.objectContaining({ query: expect.stringContaining('tick'), variables: {}, channel: expect.any(Object) }),
    );

    liveChannel().onmessage!(NEXT(1));
    liveChannel().onmessage!(NEXT(2));
    expect(seen).toEqual([1, 2]);

    unsubscribe();
    await flush();
    expect(invokeMock).toHaveBeenCalledWith('graphql_unsubscribe', { id: 1 });
  });

  it('reconnects (with backoff) when the transport completes while still subscribed', async () => {
    vi.useFakeTimers();
    const { seen, unsubscribe } = start();
    await flush();
    expect(subscribeCalls()).toBe(1);

    // A transport-level `complete` must NOT tear the urql subscription down;
    // it triggers a backoff reconnect instead. The user sees the banner.
    liveChannel().onmessage!(JSON.stringify({ type: 'complete' }));
    expect(reportErrorMock).toHaveBeenCalledWith(expect.objectContaining({ source: 'subscription' }));
    expect(subscribeCalls()).toBe(1); // not immediate

    await vi.advanceTimersByTimeAsync(1_000); // first backoff step
    expect(subscribeCalls()).toBe(2);

    // The reconnected channel delivers data again.
    liveChannel().onmessage!(NEXT(7));
    expect(seen).toEqual([7]);

    unsubscribe();
  });

  it('reconnects on transport `error`, reports it, and stays silent once healthy', async () => {
    vi.useFakeTimers();
    const { seen, unsubscribe } = start();
    await flush();

    liveChannel().onmessage!(JSON.stringify({ type: 'error', payload: 'boom' }));
    expect(reportErrorMock).toHaveBeenCalledWith(expect.objectContaining({ source: 'subscription' }));

    await vi.advanceTimersByTimeAsync(1_000);
    expect(subscribeCalls()).toBe(2);

    // Healthy frame ⇒ backoff resets and no further errors are reported
    // (the banner auto-dismisses, so "recovered" == "we go quiet").
    reportErrorMock.mockClear();
    liveChannel().onmessage!(NEXT(1));
    expect(seen).toEqual([1]);
    expect(reportErrorMock).not.toHaveBeenCalled();

    // Backoff was reset by the healthy frame: the next drop reconnects
    // again after the *base* delay, not a grown one.
    liveChannel().onmessage!(JSON.stringify({ type: 'complete' }));
    await vi.advanceTimersByTimeAsync(1_000);
    expect(subscribeCalls()).toBe(3);

    unsubscribe();
  });

  it('uses capped exponential backoff across repeated failures', async () => {
    vi.useFakeTimers();
    const { unsubscribe } = start();
    await flush();

    // Drop the live connection, then assert the reconnect lands exactly at
    // `delay` (not a tick before) — proving the backoff curve and its cap.
    const dropThenReconnectAt = async (delay: number, priorCalls: number) => {
      liveChannel().onmessage!(JSON.stringify({ type: 'complete' }));
      await vi.advanceTimersByTimeAsync(delay - 1);
      expect(subscribeCalls()).toBe(priorCalls); // not yet
      await vi.advanceTimersByTimeAsync(1);
      expect(subscribeCalls()).toBe(priorCalls + 1); // reconnected
    };

    // Each new connection immediately dies ⇒ delay doubles: 1s, 2s, 4s …
    // capped at 30s and held there.
    await dropThenReconnectAt(1_000, 1);
    await dropThenReconnectAt(2_000, 2);
    await dropThenReconnectAt(4_000, 3);
    await dropThenReconnectAt(8_000, 4);
    await dropThenReconnectAt(16_000, 5);
    await dropThenReconnectAt(30_000, 6);
    await dropThenReconnectAt(30_000, 7);

    unsubscribe();
  });

  it('does not reconnect after the consumer unsubscribes', async () => {
    vi.useFakeTimers();
    const { unsubscribe } = start();
    await flush();
    expect(subscribeCalls()).toBe(1);

    unsubscribe();
    await flush();
    expect(invokeMock).toHaveBeenCalledWith('graphql_unsubscribe', { id: 1 });

    // A late transport `complete` on the dead channel is inert.
    liveChannel().onmessage!(JSON.stringify({ type: 'complete' }));
    await vi.advanceTimersByTimeAsync(60_000);
    expect(subscribeCalls()).toBe(1);
  });

  it('reports the drop once per outage, not on every backoff retry', async () => {
    vi.useFakeTimers();
    const { unsubscribe } = start();
    await flush();

    liveChannel().onmessage!(JSON.stringify({ type: 'complete' }));
    expect(reportErrorMock).toHaveBeenCalledTimes(1);

    // Still down: each capped retry reconnects but must NOT re-report
    // (otherwise the 5s-auto-dismiss banner flickers forever).
    await vi.advanceTimersByTimeAsync(1_000);
    liveChannel().onmessage!(JSON.stringify({ type: 'complete' }));
    await vi.advanceTimersByTimeAsync(2_000);
    liveChannel().onmessage!(JSON.stringify({ type: 'complete' }));
    expect(reportErrorMock).toHaveBeenCalledTimes(1);

    // Recover, then drop again ⇒ a fresh outage reports once more.
    await vi.advanceTimersByTimeAsync(4_000);
    liveChannel().onmessage!(NEXT(1));
    liveChannel().onmessage!(JSON.stringify({ type: 'complete' }));
    expect(reportErrorMock).toHaveBeenCalledTimes(2);

    unsubscribe();
  });

  it('reconnects on a malformed transport frame', async () => {
    vi.useFakeTimers();
    const { seen, unsubscribe } = start();
    await flush();

    liveChannel().onmessage!('}{ not json');
    expect(reportErrorMock).toHaveBeenCalledWith(
      expect.objectContaining({ source: 'subscription', message: expect.stringContaining('malformed') }),
    );

    await vi.advanceTimersByTimeAsync(1_000);
    expect(subscribeCalls()).toBe(2);
    liveChannel().onmessage!(NEXT(5));
    expect(seen).toEqual([5]);

    unsubscribe();
  });

  it('cancels a pending backoff reconnect if the consumer unsubscribes first', async () => {
    vi.useFakeTimers();
    const { unsubscribe } = start();
    await flush();

    liveChannel().onmessage!(JSON.stringify({ type: 'complete' })); // schedules reconnect
    unsubscribe(); // before the backoff elapses
    await vi.advanceTimersByTimeAsync(60_000);
    expect(subscribeCalls()).toBe(1);
  });
});
