// React-side auth state. The renderer never holds tokens — it asks the
// Rust host for status and triggers login/logout via Tauri commands. The
// access token is fetched on demand by request-issuing code (e.g. the
// urql fetch adapter), so it never lingers in component state.
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';

import { errorMessage, reportError } from '@/lib/error-bus';

export type AuthStatus = {
  authenticated: boolean;
  email: string | null;
  name: string | null;
  sub: string | null;
};

const UNAUTH: AuthStatus = { authenticated: false, email: null, name: null, sub: null };

// Mirror of `auth::RESTORE_EVENT` in src-tauri. The host emits exactly
// once at startup after `try_restore` resolves; the payload is the
// fresh `Status`, so the renderer skips a follow-up `auth_status` call.
const RESTORE_EVENT = 'auth:restore-complete';

type AuthContextValue = {
  status: AuthStatus;
  /** True until the host's startup silent-restore has reported back. */
  loading: boolean;
  login: () => Promise<void>;
  logout: () => Promise<void>;
};

const AuthContext = createContext<AuthContextValue | null>(null);

function reportAuthError(action: string, e: unknown): void {
  reportError({ source: 'auth', message: `${action}: ${errorMessage(e)}`, cause: e });
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [status, setStatus] = useState<AuthStatus>(UNAUTH);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    let unlisten: (() => void) | null = null;

    const settle = (s: AuthStatus) => {
      if (cancelled) return;
      setStatus(s);
      setLoading(false);
    };

    // Tauri events are not buffered, so an event that fires between mount
    // and `listen()` resolving would be lost. Order matters: register the
    // subscription first, *then* fetch — guarantees we either see the
    // event or read the post-restore state directly.
    (async () => {
      try {
        unlisten = await listen<AuthStatus>(RESTORE_EVENT, (e) => settle(e.payload));
        if (cancelled) {
          unlisten();
          return;
        }
        const s = await invoke<AuthStatus>('auth_status');
        settle(s);
      } catch (e) {
        reportAuthError('init', e);
        settle(UNAUTH);
      }
    })();

    return () => {
      cancelled = true;
      if (unlisten) unlisten();
    };
  }, []);

  const login = useCallback(async () => {
    try {
      const s = await invoke<AuthStatus>('auth_login');
      setStatus(s);
    } catch (e) {
      reportAuthError('login', e);
      throw e;
    }
  }, []);

  const logout = useCallback(async () => {
    try {
      await invoke('auth_logout');
      setStatus(UNAUTH);
    } catch (e) {
      reportAuthError('logout', e);
      throw e;
    }
  }, []);

  const value = useMemo(() => ({ status, loading, login, logout }), [status, loading, login, logout]);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used inside <AuthProvider>');
  return ctx;
}
