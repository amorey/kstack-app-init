// Copyright 2026 The Kstack Authors
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

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const invokeMock = vi.fn();
vi.mock('@tauri-apps/api/core', () => ({
  invoke: (...args: unknown[]) => invokeMock(...args),
}));

const { createGraphqlClient } = await import('./client');

// Worst-case retry window: maxNumberAttempts(3) over a backoff capped at
// maxDelayMs(5_000) is < 15s; this comfortably drains every scheduled
// retry timer (advanceTimersByTimeAsync also flushes the microtasks
// between them) without depending on the exact jittered delays.
const RETRY_DRAIN_MS = 20_000;

describe('createGraphqlClient', () => {
  beforeEach(() => {
    invokeMock.mockReset();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('routes a query through invokeFetch and returns parsed data', async () => {
    invokeMock.mockResolvedValueOnce({ status: 200, body: '{"data":{"ping":"pong"}}' });

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

  it('retries a query that fails with a network error, then succeeds', async () => {
    vi.useFakeTimers();
    invokeMock
      .mockRejectedValueOnce('sidecar momentarily unreachable')
      .mockResolvedValueOnce({ status: 200, body: '{"data":{"ping":"pong"}}' });

    const client = createGraphqlClient();
    const pending = client.query('{ ping }', {}).toPromise();

    await vi.advanceTimersByTimeAsync(RETRY_DRAIN_MS);
    const result = await pending;

    expect(result.error).toBeUndefined();
    expect(result.data).toEqual({ ping: 'pong' });
    expect(invokeMock).toHaveBeenCalledTimes(2);
  });

  it('gives up with a network error after the bounded number of attempts', async () => {
    vi.useFakeTimers();
    invokeMock.mockRejectedValue('sidecar down');

    const client = createGraphqlClient();
    const pending = client.query('{ ping }', {}).toPromise();

    await vi.advanceTimersByTimeAsync(RETRY_DRAIN_MS);
    const result = await pending;

    // 1 initial attempt + 2 retries = maxNumberAttempts, then it fails
    // fast with a terminal networkError rather than retrying forever.
    expect(invokeMock).toHaveBeenCalledTimes(3);
    expect(result.error?.networkError).toBeDefined();
  });

  it('does not retry a GraphQL error (deterministic — only transport failures retry)', async () => {
    vi.useFakeTimers();
    invokeMock.mockResolvedValue({ status: 200, body: '{"errors":[{"message":"bad field"}]}' });

    const client = createGraphqlClient();
    const pending = client.query('{ ping }', {}).toPromise();

    await vi.advanceTimersByTimeAsync(RETRY_DRAIN_MS);
    const result = await pending;

    expect(result.error?.graphQLErrors[0]?.message).toBe('bad field');
    expect(invokeMock).toHaveBeenCalledTimes(1);
  });
});
