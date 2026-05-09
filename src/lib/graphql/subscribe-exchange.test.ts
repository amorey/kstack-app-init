import { pipe, subscribe } from 'wonka';
import { Client, gql } from 'urql';
import { beforeEach, describe, expect, it, vi } from 'vitest';

// Mocks ---------------------------------------------------------------

// Capture every Channel constructed so the test can drive its onmessage.
type FakeChannel = { onmessage?: (raw: string) => void };
let lastChannel: FakeChannel | undefined;

const invokeMock = vi.fn();
vi.mock('@tauri-apps/api/core', () => ({
  invoke: (...args: unknown[]) => invokeMock(...args),
  Channel: function FakeChannelCtor(this: FakeChannel) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    lastChannel = this;
  },
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

describe('tauriSubscriptionExchange', () => {
  beforeEach(() => {
    invokeMock.mockReset();
    lastChannel = undefined;
  });

  it('forwards `next` payloads to the urql sink and completes', async () => {
    invokeMock.mockImplementation(async (cmd: string) => {
      if (cmd === 'graphql_subscribe') return 7;
      if (cmd === 'graphql_unsubscribe') return undefined;
      throw new Error(`unexpected ${cmd}`);
    });

    const client = makeClient();
    const seen: unknown[] = [];

    pipe(
      client.subscription(TICK, {}),
      subscribe((result) => {
        if (result.data) seen.push(result.data);
      }),
    );

    // Wait a tick so the async invoke('graphql_subscribe') resolves and
    // wires up the channel before we drive it.
    await Promise.resolve();
    await Promise.resolve();

    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_subscribe',
      expect.objectContaining({
        query: expect.stringContaining('tick'),
        variables: {},
        channel: expect.any(Object),
      }),
    );
    expect(lastChannel).toBeDefined();

    lastChannel!.onmessage!(JSON.stringify({ type: 'next', payload: { data: { tick: 1 } } }));
    lastChannel!.onmessage!(JSON.stringify({ type: 'next', payload: { data: { tick: 2 } } }));
    lastChannel!.onmessage!(JSON.stringify({ type: 'complete' }));
    // Post-complete frames must be ignored — proves sink.complete() ran.
    lastChannel!.onmessage!(JSON.stringify({ type: 'next', payload: { data: { tick: 99 } } }));

    expect(seen).toEqual([{ tick: 1 }, { tick: 2 }]);
  });

  it('calls graphql_unsubscribe on teardown', async () => {
    invokeMock.mockImplementation(async (cmd: string) => {
      if (cmd === 'graphql_subscribe') return 42;
      if (cmd === 'graphql_unsubscribe') return undefined;
      throw new Error(`unexpected ${cmd}`);
    });

    const client = makeClient();
    const { unsubscribe } = pipe(
      client.subscription(TICK, {}),
      subscribe(() => {}),
    );

    await Promise.resolve();
    await Promise.resolve();

    unsubscribe();
    // Allow the deferred unsubscribe to run.
    await Promise.resolve();

    expect(invokeMock).toHaveBeenCalledWith('graphql_unsubscribe', { id: 42 });
  });
});
