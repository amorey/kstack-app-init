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

import { cleanup, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

type Identity = { sub: string; email: string; name: string };
type AuthState = { authenticated: boolean; identity: Identity | null };
type AuthStateValue = {
  authState: AuthState;
  loading: boolean;
  login: () => Promise<void>;
  logout: () => Promise<void>;
};

const authStateValue = vi.fn<() => AuthStateValue>();
vi.mock('@/lib/auth', () => ({ useAuthState: () => authStateValue() }));

// The account menu opens the overlay dialogs through the `useDialog` controller
// (it no longer renders them). Stub the controller so we can assert the menu
// requests the right dialog on select, without a DialogProvider ancestor.
const openDialog = vi.fn<(id: string) => void>();
vi.mock('@/lib/dialog', () => ({
  useDialog: () => ({ activeDialog: null, openDialog, closeDialog: vi.fn() }),
}));

const { AccountMenu } = await import('./account-menu');

const login = vi.fn<() => Promise<void>>();
const logout = vi.fn<() => Promise<void>>();

const ANON: AuthState = { authenticated: false, identity: null };

function setAuthState(over: Partial<AuthStateValue> = {}) {
  authStateValue.mockReturnValue({ authState: ANON, loading: false, login, logout, ...over });
}

describe('AccountMenu', () => {
  // base-ui's menu relies on pointer-capture / scroll APIs jsdom lacks.
  beforeAll(() => {
    Element.prototype.hasPointerCapture = vi.fn();
    Element.prototype.releasePointerCapture = vi.fn();
    Element.prototype.scrollIntoView = vi.fn();
  });

  beforeEach(() => {
    login.mockReset().mockResolvedValue(undefined);
    logout.mockReset().mockResolvedValue(undefined);
    openDialog.mockReset();
    authStateValue.mockReset();
  });

  afterEach(cleanup);

  it('signed out: the button reads "Guest" and the menu offers sign-in', async () => {
    setAuthState();
    const user = userEvent.setup();
    render(<AccountMenu />);

    const trigger = screen.getByRole('button', { name: 'Account: Guest' });
    expect(within(trigger).getByText('Guest')).toBeInTheDocument();
    await user.click(trigger);

    const item = await screen.findByRole('menuitem', { name: /sign in/i });
    await user.click(item);
    expect(login).toHaveBeenCalledTimes(1);
    expect(logout).not.toHaveBeenCalled();
  });

  it('signed out + loading: the sign-in item is disabled and does not log in', async () => {
    setAuthState({ loading: true });
    const user = userEvent.setup();
    render(<AccountMenu />);

    await user.click(screen.getByRole('button', { name: 'Account: Guest' }));
    const item = await screen.findByRole('menuitem', { name: /sign in/i });
    expect(item).toHaveAttribute('aria-disabled', 'true');

    await user.click(item);
    expect(login).not.toHaveBeenCalled();
  });

  it('signed in: the button shows the email and the menu signs out', async () => {
    setAuthState({
      authState: { authenticated: true, identity: { sub: 's1', email: 'andres.morey@example.com', name: '' } },
    });
    const user = userEvent.setup();
    render(<AccountMenu />);

    const trigger = screen.getByRole('button', { name: 'Account: andres.morey@example.com' });
    expect(within(trigger).getByText('andres.morey@example.com')).toBeInTheDocument();

    await user.click(trigger);
    const signOut = await screen.findByRole('menuitem', { name: /sign out/i });
    // Signed out is not offered while signed in.
    expect(screen.queryByRole('menuitem', { name: /^sign in$/i })).not.toBeInTheDocument();
    await user.click(signOut);
    expect(logout).toHaveBeenCalledTimes(1);
    expect(login).not.toHaveBeenCalled();
  });

  it('signed in with a name: shows the name and email, with initials in the avatar', async () => {
    setAuthState({
      authState: { authenticated: true, identity: { sub: 's2', email: 'ada@example.io', name: 'Ada Lovelace' } },
    });
    render(<AccountMenu />);

    const trigger = screen.getByRole('button', { name: 'Account: ada@example.io' });
    expect(within(trigger).getByText('Ada Lovelace')).toBeInTheDocument();
    expect(within(trigger).getByText('ada@example.io')).toBeInTheDocument();
    expect(within(trigger).getByText('AL')).toBeInTheDocument();
  });

  it('opens the cluster-sync panel from the Clusters item', async () => {
    setAuthState();
    const user = userEvent.setup();
    render(<AccountMenu />);

    await user.click(screen.getByRole('button', { name: 'Account: Guest' }));
    await user.click(await screen.findByRole('menuitem', { name: /clusters/i }));

    expect(openDialog).toHaveBeenCalledWith('clusters');
  });

  it('offers a Settings entry point in both states', async () => {
    setAuthState();
    const user = userEvent.setup();
    render(<AccountMenu />);

    await user.click(screen.getByRole('button', { name: 'Account: Guest' }));
    expect(await screen.findByRole('menuitem', { name: /settings/i })).toBeInTheDocument();
  });
});
