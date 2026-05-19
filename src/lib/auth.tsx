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

// React-side session state. The renderer never holds tokens — it asks
// the Rust host for the current session and triggers login/logout via
// Tauri commands. The access token is fetched on demand by
// request-issuing code (e.g. the urql fetch adapter), so it never
// lingers in component state.
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';

import { errorMessage, reportError } from '@/lib/error-bus';

export type Session = {
  authenticated: boolean;
  email: string | null;
  name: string | null;
  sub: string | null;
};

const ANON_SESSION: Session = { authenticated: false, email: null, name: null, sub: null };

// Mirror of `auth::RESTORE_EVENT` in src-tauri. The host emits exactly
// once at startup after `try_restore` resolves; the payload is the
// fresh session, so the renderer skips a follow-up `auth_status` call.
const RESTORE_EVENT = 'auth:restore-complete';

// Mirror of `auth::SESSION_EVENT` in src-tauri. The host broadcasts the
// fresh session on every post-startup auth change (login / logout /
// refresh) to *all* windows, so a logout in one window updates the
// others instead of leaving them on a stale authenticated UI.
const SESSION_EVENT = 'auth:session-changed';

type SessionContextValue = {
  session: Session;
  loading: boolean;
  login: () => Promise<void>;
  logout: () => Promise<void>;
};

const SessionContext = createContext<SessionContextValue | null>(null);

function reportSessionError(action: string, e: unknown): void {
  reportError({ source: 'auth', message: `${action}: ${errorMessage(e)}`, cause: e });
}

export function SessionProvider({ children }: { children: React.ReactNode }) {
  const [session, setSession] = useState<Session>(ANON_SESSION);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    const unlisteners: (() => void)[] = [];

    const settle = (s: Session) => {
      if (cancelled) return;
      setSession(s);
      setLoading(false);
    };

    // Tauri events are not buffered, so an event that fires between mount
    // and `listen()` resolving would be lost. Order matters: register the
    // subscriptions first, *then* fetch — guarantees we either see the
    // restore event or read the post-restore session directly. The
    // session-changed listener stays mounted for the provider's whole
    // life: unlike the one-shot restore, auth changes keep arriving.
    (async () => {
      try {
        const offRestore = await listen<Session>(RESTORE_EVENT, (e) => settle(e.payload));
        const offSession = await listen<Session>(SESSION_EVENT, (e) => settle(e.payload));
        if (cancelled) {
          offRestore();
          offSession();
          return;
        }
        unlisteners.push(offRestore, offSession);
        const s = await invoke<Session>('auth_status');
        settle(s);
      } catch (e) {
        reportSessionError('init', e);
        settle(ANON_SESSION);
      }
    })();

    return () => {
      cancelled = true;
      unlisteners.forEach((off) => off());
    };
  }, []);

  const login = useCallback(async () => {
    try {
      const s = await invoke<Session>('auth_login');
      setSession(s);
    } catch (e) {
      reportSessionError('login', e);
      throw e;
    }
  }, []);

  const logout = useCallback(async () => {
    try {
      await invoke('auth_logout');
      setSession(ANON_SESSION);
    } catch (e) {
      reportSessionError('logout', e);
      throw e;
    }
  }, []);

  const value = useMemo(() => ({ session, loading, login, logout }), [session, loading, login, logout]);

  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>;
}

export function useSession(): SessionContextValue {
  const ctx = useContext(SessionContext);
  if (!ctx) throw new Error('useSession must be used inside <SessionProvider>');
  return ctx;
}
