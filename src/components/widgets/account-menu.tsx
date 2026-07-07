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

// The account affordance at the foot of the sidebar: a single full-width button
// that reads "Guest" when signed out and the user's identity (name/email) when
// signed in, opening a menu that rises above it. A session is an add-on, not a
// gate — the app loads regardless, and the user can sign in / out at will from
// here. The menu also hosts entry points to the cluster-sync panel (whose Sheet
// this component owns) and Settings.
import { useState } from 'react';
import { ChevronUp, Database, LogOut, Settings, User } from 'lucide-react';

import { Avatar, AvatarFallback } from '@kubetail/ui/elements/avatar';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@kubetail/ui/elements/dropdown-menu';

import { ClusterSyncPanel } from '@/components/widgets/cluster-sync-panel';
import { useAuthState } from '@/lib/auth';

function initials(s: string): string {
  // Two letters max. Splits on whitespace and common separators so an
  // email like `andres.morey@…` yields `AM`, not `AN`.
  const trimmed = s.trim();
  if (!trimmed) return '?';
  const parts = trimmed.split(/[\s._-]+/).filter(Boolean);
  if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
  return trimmed.slice(0, 2).toUpperCase();
}

export function AccountMenu() {
  const { authState, loading, login, logout } = useAuthState();
  const { authenticated, identity } = authState;
  // Identity.email/name are non-null strings (empty = absent), so fall back with
  // `||` rather than `??`.
  const email = identity?.email || null;
  const name = identity?.name || null;
  // The trigger's accessible label: the signed-in identity, or "Guest".
  const account = authenticated ? email || name || 'Signed in' : 'Guest';

  // The cluster-sync panel is opened from a menu item; its Sheet is rendered
  // here and its open state lives here so the item can trigger it.
  const [clustersOpen, setClustersOpen] = useState(false);

  return (
    <>
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
        {/* Rises above the button (it sits at the sidebar's foot); min-width keeps
            it from collapsing narrower than the trigger. */}
        <DropdownMenuContent align="start" side="top" sideOffset={6} className="min-w-56">
          {!authenticated && (
            <>
              {/* A prominent primary call-to-action, matching the mockup's blue
                  sign-in button. */}
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
          <DropdownMenuItem onClick={() => setClustersOpen(true)}>
            <Database className="size-4" aria-hidden />
            Clusters
          </DropdownMenuItem>
          {/* Settings has no screen yet — a placeholder entry point. */}
          <DropdownMenuItem
            onClick={() => {
              /* TODO: open settings once a settings screen exists */
            }}
          >
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
      <ClusterSyncPanel open={clustersOpen} onOpenChange={setClustersOpen} />
    </>
  );
}
