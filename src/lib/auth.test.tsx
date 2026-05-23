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

import { act, cleanup, render, renderHook, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

type Status = { authenticated: boolean; email: string | null; name: string | null; sub: string | null };
type EventCb = (e: { payload: Status }) => void;

const SESSION_RESOLVED_EVENT = 'auth:session-resolved';
const SESSION_CHANGED_EVENT = 'auth:session-changed';

const invokeMock = vi.fn<(cmd: string, payload?: unknown) => Promise<unknown>>();

// `listen()` resolves with an unlisten fn. We keep every registered
// handler keyed by event name so tests can fire the host's restore /
// session-changed events by hand.
const unlistenMock = vi.fn();
const listenHandlers = new Map<string, EventCb>();
const listenMock = vi.fn(async (event: string, cb: EventCb) => {
  listenHandlers.set(event, cb);
  return unlistenMock;
});

function fireEvent(event: string, payload: Status) {
  const cb = listenHandlers.get(event);
  if (!cb) throw new Error(`no listener registered for ${event}`);
  cb({ payload });
}

vi.mock('@tauri-apps/api/core', () => ({
  invoke: (cmd: string, payload?: unknown) => invokeMock(cmd, payload),
}));

vi.mock('@tauri-apps/api/event', () => ({
  listen: (event: string, cb: EventCb) => listenMock(event, cb),
}));

// Real error bus — exercising the integration is cheap and confirms the
// `source: 'auth'` contract that `ConnectionStatus` filters on.
const { onError } = await import('@/lib/error-bus');
const { SessionProvider, useSession } = await import('./auth');

const AUTHED: Status = { authenticated: true, email: 'a@example.com', name: 'Ada', sub: 'sub-1' };
const ANON: Status = { authenticated: false, email: null, name: null, sub: null };

function renderSession() {
  return renderHook(() => useSession(), { wrapper: SessionProvider });
}

describe('SessionProvider / useSession', () => {
  beforeEach(() => {
    invokeMock.mockReset();
    listenMock.mockClear();
    unlistenMock.mockClear();
    listenHandlers.clear();
  });

  afterEach(cleanup);

  it('starts loading with an anonymous session', () => {
    invokeMock.mockReturnValue(new Promise<never>(() => {})); // never settles
    const { result } = renderSession();
    expect(result.current.loading).toBe(true);
    expect(result.current.session).toEqual(ANON);
  });

  it('settles the session from auth_status', async () => {
    invokeMock.mockResolvedValue(AUTHED);
    const { result } = renderSession();
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.session).toEqual(AUTHED);
    expect(invokeMock).toHaveBeenCalledWith('auth_status', undefined);
  });

  it('subscribes to the restore event before fetching status', async () => {
    invokeMock.mockResolvedValue(ANON);
    renderSession();
    await waitFor(() => expect(listenMock).toHaveBeenCalled());
    expect(listenHandlers.has(SESSION_RESOLVED_EVENT)).toBe(true);
  });

  it('settles from the restore event when it arrives first', async () => {
    invokeMock.mockReturnValue(new Promise<never>(() => {})); // auth_status hangs
    const { result } = renderSession();
    await waitFor(() => expect(listenHandlers.has(SESSION_RESOLVED_EVENT)).toBe(true));
    await act(async () => {
      fireEvent(SESSION_RESOLVED_EVENT, AUTHED);
    });
    expect(result.current.loading).toBe(false);
    expect(result.current.session).toEqual(AUTHED);
  });

  it('subscribes to the session-changed event', async () => {
    invokeMock.mockResolvedValue(ANON);
    renderSession();
    await waitFor(() => expect(listenHandlers.has(SESSION_CHANGED_EVENT)).toBe(true));
  });

  it('syncs to anonymous when another window logs out (session-changed)', async () => {
    invokeMock.mockResolvedValue(AUTHED); // this window is signed in
    const { result } = renderSession();
    await waitFor(() => expect(result.current.session).toEqual(AUTHED));

    // The host broadcasts the post-logout status from the other window.
    await act(async () => {
      fireEvent(SESSION_CHANGED_EVENT, ANON);
    });
    expect(result.current.session).toEqual(ANON);
  });

  it('syncs to authenticated when another window logs in (session-changed)', async () => {
    invokeMock.mockResolvedValue(ANON);
    const { result } = renderSession();
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      fireEvent(SESSION_CHANGED_EVENT, AUTHED);
    });
    expect(result.current.session).toEqual(AUTHED);
  });

  it('login() invokes auth_login and adopts the returned session', async () => {
    invokeMock.mockResolvedValueOnce(ANON); // initial auth_status
    const { result } = renderSession();
    await waitFor(() => expect(result.current.loading).toBe(false));

    invokeMock.mockResolvedValueOnce(AUTHED); // auth_login
    await act(async () => {
      await result.current.login();
    });
    expect(invokeMock).toHaveBeenCalledWith('auth_login', undefined);
    expect(result.current.session).toEqual(AUTHED);
  });

  it('logout() invokes auth_logout and resets to an anonymous session', async () => {
    invokeMock.mockResolvedValueOnce(AUTHED); // initial auth_status
    const { result } = renderSession();
    await waitFor(() => expect(result.current.session).toEqual(AUTHED));

    invokeMock.mockResolvedValueOnce(undefined); // auth_logout
    await act(async () => {
      await result.current.logout();
    });
    expect(invokeMock).toHaveBeenCalledWith('auth_logout', undefined);
    expect(result.current.session).toEqual(ANON);
  });

  it('reports an auth-source error and stays anonymous when init fails', async () => {
    const errors: { source: string; message: string }[] = [];
    const off = onError((e) => errors.push(e));
    invokeMock.mockRejectedValue(new Error('keychain locked'));

    const { result } = renderSession();
    await waitFor(() => expect(result.current.loading).toBe(false));
    off();

    expect(result.current.session).toEqual(ANON);
    expect(errors).toHaveLength(1);
    expect(errors[0].source).toBe('auth');
    expect(errors[0].message).toContain('init: keychain locked');
  });

  it('reports and rethrows when login fails', async () => {
    invokeMock.mockResolvedValueOnce(ANON); // initial auth_status
    const { result } = renderSession();
    await waitFor(() => expect(result.current.loading).toBe(false));

    const errors: { source: string; message: string }[] = [];
    const off = onError((e) => errors.push(e));
    invokeMock.mockRejectedValueOnce(new Error('user cancelled'));

    await expect(result.current.login()).rejects.toThrow('user cancelled');
    off();

    expect(errors).toHaveLength(1);
    expect(errors[0].source).toBe('auth');
    expect(errors[0].message).toContain('login: user cancelled');
    expect(result.current.session).toEqual(ANON); // unchanged on failure
  });

  it('unsubscribes from every event listener on unmount', async () => {
    invokeMock.mockResolvedValue(ANON);
    const { unmount } = renderSession();
    await waitFor(() => expect(listenHandlers.has(SESSION_CHANGED_EVENT)).toBe(true));
    unmount();
    // One unlisten per registered listener (restore + session-changed).
    expect(unlistenMock).toHaveBeenCalledTimes(2);
  });

  it('throws when useSession is used outside a provider', () => {
    const spy = vi.spyOn(console, 'error').mockImplementation(() => {});
    function Bare() {
      useSession();
      return null;
    }
    expect(() => render(<Bare />)).toThrow(/useSession must be used inside <SessionProvider>/);
    spy.mockRestore();
  });
});
