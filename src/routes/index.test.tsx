import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const invokeMock = vi.fn();
vi.mock('@tauri-apps/api/core', () => ({
  invoke: (...args: unknown[]) => invokeMock(...args),
}));

const { routeTree } = await import('@/routeTree');
const { renderWithRouter } = await import('@/test-utils');

describe('index route', () => {
  beforeEach(() => {
    invokeMock.mockReset();
  });

  it('renders the home page', async () => {
    await renderWithRouter(routeTree, '/');
    expect(screen.getByText(/kstack/i)).toBeInTheDocument();
  });

  it('shows the sidecar pong response after clicking Ping', async () => {
    invokeMock.mockResolvedValueOnce('{"data":{"ping":"pong"}}');
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
