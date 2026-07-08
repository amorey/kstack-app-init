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

// The main window's layout: the floating sidebar (Chat/Dashboard nav at the
// top, account chrome in the footer) plus the routed page beside it. Mounted as
// a pathless layout route (`src/routes/_app.tsx`) that the chat and dashboard
// routes nest under, so the shell is rendered once and the page swaps in the
// inset. Secondary windows (log tail, container exec) will get their own
// sidebar-less layout alongside this one.
//
// The `main` frame is this window's page chrome — background, full height, and
// the top padding that reserves the title-bar band (see `app-sidebar.tsx`) so no
// page slides content under the window drag strip. Pages emit only their content
// and opt into a narrow reading width with `CenteredColumn`.
import { Outlet } from '@tanstack/react-router';

import { AccountMenu } from '@/components/widgets/account-menu';
import { AppDialogs } from '@/components/widgets/app-dialogs';
import { AppSidebar } from '@/components/widgets/app-sidebar';
import { ModeNav } from '@/components/widgets/mode-nav';
import { ConnectionStatus } from '@/lib/connection-status';
import { DialogProvider } from '@/lib/dialog';

export function AppLayout() {
  return (
    // The dialogs host lives outside the sidebar so an open dialog survives the
    // card unmounting when the window auto-collapses below the md breakpoint —
    // the account menu (in the sidebar footer) only requests opens via context.
    <DialogProvider>
      <ConnectionStatus />
      <AppSidebar nav={<ModeNav />} footer={<AccountMenu />}>
        <main className="flex min-h-[var(--app-min-h)] flex-col bg-background pt-16">
          <Outlet />
        </main>
      </AppSidebar>
      <AppDialogs />
    </DialogProvider>
  );
}
