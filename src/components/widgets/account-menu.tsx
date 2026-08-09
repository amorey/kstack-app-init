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

// Account affordance at the foot of the sidebar. A session is an add-on, not a
// gate — the app loads signed out. Dialogs are only *requested* here via
// `useDialog`; they render in `AppDialogs` so they outlive the sidebar card
// unmounting on auto-collapse.
import { ChevronUp, Database, LogOut, Settings, User } from 'lucide-react';

import { Avatar, AvatarFallback } from '@kubetail/ui/elements/avatar';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@kubetail/ui/elements/dropdown-menu';

import { useAuthState } from '@/lib/auth';
import { useDialog } from '@/lib/dialog';

function initials(s: string): string {
  // Split on separators too, so `andres.morey@…` yields `AM`, not `AN`.
  const trimmed = s.trim();
  if (!trimmed) return '?';
  const parts = trimmed.split(/[\s._-]+/).filter(Boolean);
  if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
  return trimmed.slice(0, 2).toUpperCase();
}

export function AccountMenu() {
  const { authState, loading, login, logout } = useAuthState();
  const { authenticated, identity } = authState;
  // email/name are non-null strings (empty = absent), so `||`, not `??`.
  const email = identity?.email || null;
  const name = identity?.name || null;
  const account = authenticated ? email || name || 'Signed in' : 'Guest';

  const { openDialog } = useDialog();

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        aria-label={`Account: ${account}`}
        className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left outline-none hover:bg-muted focus-visible:ring-2 focus-visible:ring-ring"
      >
        <Avatar size="sm">
          <AvatarFallback>
            {authenticated && (name || email) ? initials(name || email!) : <User className="size-4" aria-hidden />}
          </AvatarFallback>
        </Avatar>
        <span className="min-w-0 flex-1">
          {authenticated ? (
            <>
              <span className="block truncate text-sm font-medium">{name || email}</span>
              {name && email ? <span className="block truncate text-xs text-muted-foreground">{email}</span> : null}
            </>
          ) : (
            <span className="block truncate text-sm font-medium">Guest</span>
          )}
        </span>
        <ChevronUp className="size-4 shrink-0 text-muted-foreground" aria-hidden />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" side="top" sideOffset={6} className="min-w-56">
        {!authenticated && (
          <>
            <DropdownMenuItem
              disabled={loading}
              onClick={() => login().catch(() => {})}
              className="flex-col items-start gap-0 bg-primary text-primary-foreground focus:bg-primary/90 focus:text-primary-foreground"
            >
              <span className="text-sm font-semibold">Sign in</span>
              <span className="text-xs opacity-90">or create an account</span>
            </DropdownMenuItem>
            <DropdownMenuSeparator />
          </>
        )}
        <DropdownMenuItem onClick={() => openDialog('clusters')}>
          <Database className="size-4" aria-hidden />
          Clusters
        </DropdownMenuItem>
        <DropdownMenuItem onClick={() => openDialog('settings')}>
          <Settings className="size-4" aria-hidden />
          Settings
        </DropdownMenuItem>
        {authenticated && (
          <>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={() => logout().catch(() => {})}>
              <LogOut className="size-4" aria-hidden />
              Sign out
            </DropdownMenuItem>
          </>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
