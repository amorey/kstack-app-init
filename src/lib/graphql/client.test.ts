import { beforeEach, describe, expect, it, vi } from 'vitest';

const invokeMock = vi.fn();
vi.mock('@tauri-apps/api/core', () => ({
  invoke: (...args: unknown[]) => invokeMock(...args),
}));

const { createGraphqlClient } = await import('./client');

describe('createGraphqlClient', () => {
  beforeEach(() => {
    invokeMock.mockReset();
  });

  it('routes a query through invokeFetch and returns parsed data', async () => {
    invokeMock.mockResolvedValueOnce('{"data":{"ping":"pong"}}');

    const client = createGraphqlClient();
    const result = await client.query('{ ping }', {}).toPromise();

    expect(result.error).toBeUndefined();
    expect(result.data).toEqual({ ping: 'pong' });
    expect(invokeMock).toHaveBeenCalledTimes(1);
    expect(invokeMock).toHaveBeenCalledWith(
      'graphql_query',
      expect.objectContaining({
        body: expect.stringContaining('ping'),
      }),
    );
  });
});
