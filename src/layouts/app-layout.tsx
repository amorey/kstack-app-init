// Copyright 2026 The Kstack Authors
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

// The main window's layout: floating sidebar plus the routed page, mounted once by
// the pathless `_app` route (the page swaps in the inset). Secondary windows get
// their own sidebar-less layout alongside this one.
//
// The `main` frame owns background, full height, and the top padding that reserves
// the title-bar band (see `app-sidebar.tsx`) so no page slides under the drag strip.
import { Outlet, useLocation } from '@tanstack/react-router';

import { AccountMenu } from '@/components/widgets/account-menu';
import { AppDialogs } from '@/components/widgets/app-dialogs';
import { AppSidebar } from '@/components/widgets/app-sidebar';
import { DashboardResourceNav } from '@/components/widgets/dashboard-resource-nav';
import { KubeContextBar } from '@/components/widgets/kube-context-bar';
import { ModeNav } from '@/components/widgets/mode-nav';
import { ConnectionStatus } from '@/lib/connection-status';
import { DialogProvider } from '@/lib/dialog';

export function AppLayout() {
  // Resource nav mounts only in dashboard mode. This layout stays mounted across
  // the mode switch, so subscribe via `useLocation` (re-renders per navigation) —
  // `useMatchRoute` would read stale until a reload.
  const pathname = useLocation({ select: (location) => location.pathname });
  const onDashboard = pathname === '/dashboard' || pathname.startsWith('/dashboard/');

  return (
    // Dialogs host lives outside the sidebar so an open dialog survives the card
    // unmounting below the md breakpoint; the account menu only requests opens.
    <DialogProvider>
      <ConnectionStatus />
      <AppSidebar
        nav={
          <div className="flex flex-col gap-3">
            <ModeNav />
            {onDashboard && <DashboardResourceNav />}
          </div>
        }
        footer={<AccountMenu />}
      >
        {/* Context bar spans the content area (not the sidebar): one window-wide
            choice shared by both modes, with room for the full context name. */}
        <main className="flex min-h-(--app-min-h) flex-col bg-background pt-16">
          <KubeContextBar />
          <Outlet />
        </main>
      </AppSidebar>
      <AppDialogs />
    </DialogProvider>
  );
}
