import { act, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

type FakeChannel = { onmessage?: (raw: string) => void };
type InvokeArgs = [cmd: string, payload?: { body?: string }];

const invokeMock = vi.fn<(...args: InvokeArgs) => Promise<unknown>>();
// Both subscriptions on the page (tick + settingsWatch) construct a Channel;
// we keep them all so individual tests can pick the one they care about.
const channels: FakeChannel[] = [];

vi.mock('@tauri-apps/api/core', () => ({
  invoke: (...args: InvokeArgs) => invokeMock(...args),
  Channel: function FakeChannelCtor(this: FakeChannel) {
    channels.push(this);
  },
}));

// AuthProvider's listen() must resolve, otherwise it throws and the
// provider falls back to UNAUTH — which would hide the cloud-sync demo
// behind the auth gate.
vi.mock('@tauri-apps/api/event', () => ({
  listen: async () => () => {},
}));

const { routeTree } = await import('@/routeTree');
const { renderWithRouter } = await import('@/test-utils');

// `auth_status` must report authenticated so CloudSyncGate mounts the demo.
function installDefaultInvokes() {
  invokeMock.mockImplementation(async (cmd, payload) => {
    if (cmd === 'auth_status') {
      return { authenticated: true, email: 'test@example.com', name: null, sub: 'sub-1' };
    }
    if (cmd === 'graphql_subscribe') return channels.length;
    if (cmd === 'graphql_unsubscribe') return undefined;
    if (cmd === 'graphql_query') {
      const body = payload?.body ?? '';
      if (body.includes('updateSettings')) {
        return '{"data":{"updateSettings":{"placeholder":"updated"}}}';
      }
      if (body.includes('settings')) {
        return '{"data":{"settings":{"placeholder":"initial"}}}';
      }
      return '{"data":{"ping":"pong"}}';
    }
    return '';
  });
}

// settingsWatch subscribes after Tick on initial render, so its channel
// is the last one constructed.
async function settingsChannel(): Promise<FakeChannel> {
  await new Promise<void>((r) => {
    setTimeout(r, 0);
  });
  return channels[channels.length - 1];
}

describe('index route', () => {
  beforeEach(() => {
    invokeMock.mockReset();
    channels.length = 0;
    installDefaultInvokes();
  });

  it('renders the home page', async () => {
    await renderWithRouter(routeTree, '/');
    expect(screen.getByText(/kstack/i)).toBeInTheDocument();
  });

  it('renders incoming tick subscription values', async () => {
    await renderWithRouter(routeTree, '/');
    await new Promise<void>((r) => {
      setTimeout(r, 0);
    });
    // Tick subscribes first.
    const tick = channels[0];
    await act(async () => {
      tick.onmessage!(JSON.stringify({ type: 'next', payload: { data: { tick: 3 } } }));
    });
    expect(await screen.findByText(/Tick: 3/)).toBeInTheDocument();
  });

  it('shows the sidecar pong response after clicking Ping', async () => {
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

  it('renders the initial placeholder from the sidecar', async () => {
    await renderWithRouter(routeTree, '/');
    expect(await screen.findByDisplayValue('initial')).toBeInTheDocument();
  });

  it('saves an edited placeholder through updateSettings', async () => {
    await renderWithRouter(routeTree, '/');
    const input = await screen.findByDisplayValue('initial');
    await userEvent.clear(input);
    await userEvent.type(input, 'changed');
    await userEvent.click(screen.getByRole('button', { name: /save/i }));

    const mutateCall = invokeMock.mock.calls.find(
      ([cmd, payload]) => cmd === 'graphql_query' && (payload?.body ?? '').includes('updateSettings'),
    );
    expect(mutateCall, 'updateSettings should have been invoked').toBeDefined();
    expect(mutateCall![1]!.body).toContain('changed');
  });

  it('updates the displayed value when settingsWatch pushes a new event', async () => {
    await renderWithRouter(routeTree, '/');
    await screen.findByDisplayValue('initial');
    const settings = await settingsChannel();
    await act(async () => {
      settings.onmessage!(
        JSON.stringify({
          type: 'next',
          payload: { data: { settingsWatch: { placeholder: 'pushed' } } },
        }),
      );
    });
    expect(await screen.findByDisplayValue('pushed')).toBeInTheDocument();
  });
});
