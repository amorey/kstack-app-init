import { beforeEach, describe, expect, it, vi } from 'vitest';

// Mock the Tauri invoke surface; the adapter is the only thing that should
// touch it, so the mock fully simulates the host bridge for these tests.
const invokeMock = vi.fn();
vi.mock('@tauri-apps/api/core', () => ({
  invoke: (...args: unknown[]) => invokeMock(...args),
}));

// Imported after vi.mock so the module under test sees the mock.
const { invokeFetch } = await import('./invoke-fetch');

describe('invokeFetch', () => {
  beforeEach(() => {
    invokeMock.mockReset();
  });

  it('forwards the request body to graphql_query and wraps the response', async () => {
    invokeMock.mockResolvedValueOnce('{"data":{"ping":"pong"}}');

    const res = await invokeFetch('tauri://graphql', {
      method: 'POST',
      body: '{"query":"{ ping }"}',
    });

    expect(invokeMock).toHaveBeenCalledWith('graphql_query', { body: '{"query":"{ ping }"}' });
    expect(res.ok).toBe(true);
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual({ data: { ping: 'pong' } });
  });

  it('translates an invoke rejection into a 500 response with errors[]', async () => {
    invokeMock.mockRejectedValueOnce('boom');

    const res = await invokeFetch('tauri://graphql', { method: 'POST', body: '{}' });

    expect(res.ok).toBe(false);
    expect(res.status).toBe(500);
    await expect(res.json()).resolves.toEqual({ errors: [{ message: 'boom' }] });
  });
});
