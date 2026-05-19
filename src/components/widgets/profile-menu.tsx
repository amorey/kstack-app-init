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

// Profile affordance in the top-right. A session is an add-on, not a
// gate — the app loads regardless, and the user can sign in / out at
// will from here. The trigger is a circular icon button so it doesn't
// take much space; the menu shows the user's email when signed in,
// otherwise a single "Sign in" item.
import { LogIn, LogOut, User } from 'lucide-react';

import { Avatar, AvatarFallback } from '@kubetail/ui/elements/avatar';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@kubetail/ui/elements/dropdown-menu';

import { useSession } from '@/lib/auth';

function initials(s: string): string {
  // Two letters max. Splits on whitespace and common separators so an
  // email like `andres.morey@…` yields `AM`, not `AN`.
  const trimmed = s.trim();
  if (!trimmed) return '?';
  const parts = trimmed.split(/[\s._-]+/).filter(Boolean);
  if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
  return trimmed.slice(0, 2).toUpperCase();
}

export function ProfileMenu() {
  const { session, loading, login, logout } = useSession();
  const label = session.email ?? session.name ?? null;

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        aria-label={session.authenticated ? `Account: ${label ?? 'signed in'}` : 'Sign in'}
        className="rounded-full outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <Avatar size="sm">
          <AvatarFallback>
            {session.authenticated && label ? initials(label) : <User className="size-4" aria-hidden />}
          </AvatarFallback>
        </Avatar>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" sideOffset={6} className="min-w-48">
        <DropdownMenuGroup>
          {session.authenticated ? (
            <>
              <DropdownMenuLabel className="flex flex-col font-normal">
                <span className="text-sm font-medium">{session.name ?? session.email ?? 'Signed in'}</span>
                {session.name && session.email ? (
                  <span className="text-xs text-muted-foreground">{session.email}</span>
                ) : null}
              </DropdownMenuLabel>
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={() => logout().catch(() => {})}>
                <LogOut className="size-4" aria-hidden />
                Sign out
              </DropdownMenuItem>
            </>
          ) : (
            <DropdownMenuItem disabled={loading} onClick={() => login().catch(() => {})}>
              <LogIn className="size-4" aria-hidden />
              Sign in
            </DropdownMenuItem>
          )}
        </DropdownMenuGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
