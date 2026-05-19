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

import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

type Session = { authenticated: boolean; email: string | null; name: string | null; sub: string | null };
type SessionValue = {
  session: Session;
  loading: boolean;
  login: () => Promise<void>;
  logout: () => Promise<void>;
};

const sessionValue = vi.fn<() => SessionValue>();
vi.mock('@/lib/auth', () => ({ useSession: () => sessionValue() }));

const { ProfileMenu } = await import('./profile-menu');

const login = vi.fn<() => Promise<void>>();
const logout = vi.fn<() => Promise<void>>();

const ANON: Session = { authenticated: false, email: null, name: null, sub: null };

function setSession(over: Partial<SessionValue> = {}) {
  sessionValue.mockReturnValue({ session: ANON, loading: false, login, logout, ...over });
}

describe('ProfileMenu', () => {
  // Radix's dropdown relies on pointer-capture / scroll APIs jsdom lacks.
  beforeAll(() => {
    Element.prototype.hasPointerCapture = vi.fn();
    Element.prototype.releasePointerCapture = vi.fn();
    Element.prototype.scrollIntoView = vi.fn();
  });

  beforeEach(() => {
    login.mockReset().mockResolvedValue(undefined);
    logout.mockReset().mockResolvedValue(undefined);
    sessionValue.mockReset();
  });

  afterEach(cleanup);

  it('signed out: trigger reads "Sign in" and the menu offers sign-in', async () => {
    setSession();
    const user = userEvent.setup();
    render(<ProfileMenu />);

    const trigger = screen.getByRole('button', { name: 'Sign in' });
    await user.click(trigger);

    const item = await screen.findByRole('menuitem', { name: /sign in/i });
    await user.click(item);
    expect(login).toHaveBeenCalledTimes(1);
    expect(logout).not.toHaveBeenCalled();
  });

  it('signed out + loading: the sign-in item is disabled and does not log in', async () => {
    setSession({ loading: true });
    const user = userEvent.setup();
    render(<ProfileMenu />);

    await user.click(screen.getByRole('button', { name: 'Sign in' }));
    const item = await screen.findByRole('menuitem', { name: /sign in/i });
    expect(item).toHaveAttribute('aria-disabled', 'true');

    await user.click(item);
    expect(login).not.toHaveBeenCalled();
  });

  it('signed in: shows initials, an account label, and signs out', async () => {
    setSession({
      session: { authenticated: true, email: 'andres.morey@example.com', name: null, sub: 's1' },
    });
    const user = userEvent.setup();
    render(<ProfileMenu />);

    const trigger = screen.getByRole('button', { name: 'Account: andres.morey@example.com' });
    expect(await screen.findByText('AM')).toBeInTheDocument();

    await user.click(trigger);
    expect(await screen.findByText('andres.morey@example.com')).toBeInTheDocument();

    await user.click(screen.getByRole('menuitem', { name: /sign out/i }));
    expect(logout).toHaveBeenCalledTimes(1);
    expect(login).not.toHaveBeenCalled();
  });

  it('signed in with a name: shows the name as primary and email as secondary', async () => {
    setSession({
      session: { authenticated: true, email: 'ada@example.io', name: 'Ada Lovelace', sub: 's2' },
    });
    const user = userEvent.setup();
    render(<ProfileMenu />);

    await user.click(screen.getByRole('button', { name: 'Account: ada@example.io' }));

    expect(await screen.findByText('Ada Lovelace')).toBeInTheDocument();
    expect(screen.getByText('ada@example.io')).toBeInTheDocument();
  });

  it('falls back to a generic label when signed in without email or name', () => {
    setSession({
      session: { authenticated: true, email: null, name: null, sub: 's3' },
    });
    render(<ProfileMenu />);
    expect(screen.getByRole('button', { name: 'Account: signed in' })).toBeInTheDocument();
  });
});
