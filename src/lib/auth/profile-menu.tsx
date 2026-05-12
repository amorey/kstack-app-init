// Profile affordance in the top-right. Auth is an add-on, not a gate —
// the app loads regardless, and the user can sign in / out at will from
// here. The trigger is a circular icon button so it doesn't take much
// space; the menu shows the user's email when signed in, otherwise a
// single "Sign in" item.
import { LogIn, LogOut, User } from 'lucide-react';

import { Avatar, AvatarFallback } from '@kubetail/ui/elements/avatar';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@kubetail/ui/elements/dropdown-menu';

import { useAuth } from './auth-context';

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
  const { status, loading, login, logout } = useAuth();
  const label = status.email ?? status.name ?? null;

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        aria-label={status.authenticated ? `Account: ${label ?? 'signed in'}` : 'Sign in'}
        className="rounded-full outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <Avatar size="sm">
          <AvatarFallback>
            {status.authenticated && label ? initials(label) : <User className="size-4" aria-hidden />}
          </AvatarFallback>
        </Avatar>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" sideOffset={6} className="min-w-[12rem]">
        {status.authenticated ? (
          <>
            <DropdownMenuLabel className="flex flex-col font-normal">
              <span className="text-sm font-medium">{status.name ?? status.email ?? 'Signed in'}</span>
              {status.name && status.email ? (
                <span className="text-xs text-muted-foreground">{status.email}</span>
              ) : null}
            </DropdownMenuLabel>
            <DropdownMenuSeparator />
            <DropdownMenuItem onSelect={() => logout().catch(() => {})}>
              <LogOut className="size-4" aria-hidden />
              Sign out
            </DropdownMenuItem>
          </>
        ) : (
          <DropdownMenuItem disabled={loading} onSelect={() => login().catch(() => {})}>
            <LogIn className="size-4" aria-hidden />
            Sign in
          </DropdownMenuItem>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
