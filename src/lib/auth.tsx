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

// React-side auth state, sourced entirely from the sidecar over GraphQL.
// The renderer never holds tokens: the sidecar owns the OAuth flow + OS
// keychain, and publishes the current auth state on the `authStateWatch`
// subscription (current snapshot first, then deltas on sign-in / sign-out /
// refresh). `login`/`logout` are thin `login`/`logout` mutations — the
// resulting state change arrives back over the same subscription, so there's
// one source of truth. Sign-in is non-blocking: the mutation returns as soon
// as the sidecar opens the browser; the signed-in state lands later via the
// subscription (or not at all if the user abandons the browser flow).
import { createContext, useCallback, useContext, useMemo } from 'react';
import { useMutation, useSubscription } from 'urql';

import { graphql } from '@/gql';
import type { AuthStateWatchSubscription } from '@/gql/graphql';

// AuthState mirrors the sidecar's GraphQL shape: `authenticated` is the explicit
// sign-in signal; `identity` carries the verified claims and is non-null only
// when authenticated.
export type Identity = {
  sub: string;
  email: string;
  name: string;
};

export type AuthState = {
  authenticated: boolean;
  identity: Identity | null;
};

const SIGNED_OUT: AuthState = { authenticated: false, identity: null };

const AuthStateWatchSubscription = graphql(`
  subscription AuthStateWatch {
    authStateWatch {
      authenticated
      identity {
        sub
        email
        name
      }
    }
  }
`);

const StartLoginMutation = graphql(`
  mutation StartLogin {
    startLogin
  }
`);

const LogoutMutation = graphql(`
  mutation Logout {
    logout
  }
`);

type AuthStateContextValue = {
  authState: AuthState;
  loading: boolean;
  login: () => Promise<void>;
  logout: () => Promise<void>;
};

const AuthStateContext = createContext<AuthStateContextValue | null>(null);

// toAuthState adapts the GraphQL shape to the renderer's AuthState, mirroring its
// nesting. Identity is null while signed out, so an absent identity reads as
// anonymous regardless of the boolean.
function toAuthState(s: AuthStateWatchSubscription['authStateWatch']): AuthState {
  return {
    authenticated: s.authenticated,
    identity: s.identity ? { sub: s.identity.sub, email: s.identity.email, name: s.identity.name } : null,
  };
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [{ data }] = useSubscription({ query: AuthStateWatchSubscription });
  const [, runStartLogin] = useMutation(StartLoginMutation);
  const [, runLogout] = useMutation(LogoutMutation);

  // No frame yet → still loading, anonymous. Once the first snapshot lands the
  // state reflects it (the sidecar always emits a current snapshot first).
  const authState = data ? toAuthState(data.authStateWatch) : SIGNED_OUT;
  const loading = data === undefined;

  // login/logout just fire the mutations; the resulting state arrives over the
  // subscription. Mutation/transport errors are reported to the error bus by the
  // GraphQL error exchange — here we only surface them to callers (ProfileMenu
  // swallows them). The eventual outcome of the async browser sign-in is not a
  // mutation error; it shows up as an auth-state change (or stays signed-out).
  const login = useCallback(async () => {
    const res = await runStartLogin({});
    if (res.error) throw res.error;
  }, [runStartLogin]);

  const logout = useCallback(async () => {
    const res = await runLogout({});
    if (res.error) throw res.error;
  }, [runLogout]);

  const value = useMemo(() => ({ authState, loading, login, logout }), [authState, loading, login, logout]);

  return <AuthStateContext.Provider value={value}>{children}</AuthStateContext.Provider>;
}

export function useAuthState(): AuthStateContextValue {
  const ctx = useContext(AuthStateContext);
  if (!ctx) throw new Error('useAuthState must be used inside <AuthProvider>');
  return ctx;
}
