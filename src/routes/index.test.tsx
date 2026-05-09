import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

type FakeChannel = { onmessage?: (raw: string) => void };
const invokeMock = vi.fn();
let lastChannel: FakeChannel | undefined;

vi.mock('@tauri-apps/api/core', () => ({
  invoke: (...args: unknown[]) => invokeMock(...args),
  Channel: function FakeChannelCtor(this: FakeChannel) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    lastChannel = this;
  },
}));

const { routeTree } = await import('@/routeTree');
const { renderWithRouter } = await import('@/test-utils');

describe('index route', () => {
  beforeEach(() => {
    invokeMock.mockReset();
    lastChannel = undefined;
    // Default no-op handlers — individual tests override with mockImplementationOnce
    // when they need specific responses. Tick subscribes on mount, so even tests
    // that don't care about it must satisfy `graphql_subscribe`.
    invokeMock.mockImplementation(async (cmd: string) => {
      if (cmd === 'graphql_subscribe') return 0;
      if (cmd === 'graphql_unsubscribe') return undefined;
      return '';
    });
  });

  it('renders the home page', async () => {
    await renderWithRouter(routeTree, '/');
    expect(screen.getByText(/kstack/i)).toBeInTheDocument();
  });

  it('renders incoming tick subscription values', async () => {
    await renderWithRouter(routeTree, '/');
    // Wait for graphql_subscribe to resolve and wire up the channel.
    await new Promise<void>((r) => {
      setTimeout(r, 0);
    });
    expect(lastChannel).toBeDefined();

    lastChannel!.onmessage!(JSON.stringify({ type: 'next', payload: { data: { tick: 3 } } }));
    expect(await screen.findByText(/Tick: 3/)).toBeInTheDocument();
  });

  it('shows the sidecar pong response after clicking Ping', async () => {
    invokeMock.mockImplementation(async (cmd: string) => {
      if (cmd === 'graphql_query') return '{"data":{"ping":"pong"}}';
      if (cmd === 'graphql_subscribe') return 0;
      if (cmd === 'graphql_unsubscribe') return undefined;
      throw new Error(`unexpected ${cmd}`);
    });
    await renderWithRouter(routeTree, '/');

    await userEvent.click(screen.getByRole('button', { name: /ping sidecar/i }));

    expect(await screen.findByText(/pong/)).toBeInTheDocument();
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_query',
      expect.objectContaining({
        body: expect.stringContaining('ping'),
      }),
    );
  });
});
