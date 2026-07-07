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

// The app's visual shell: the floating sidebar plus the page beside it. The
// navigation chrome (kube-context picker) and the account chrome (the full-width
// account menu, which also hosts the cluster-sync panel) live in the sidebar;
// the routed page renders in the inset. The Linux/Windows File menu lives in the
// title bar (see `AppSidebar`), not here. Kept separate from `__root.tsx` so the
// layout can be tested without the provider stack.
import type { ReactNode } from 'react';

import { AccountMenu } from '@/components/widgets/account-menu';
import { AppSidebar } from '@/components/widgets/app-sidebar';
import { ConnectionStatus } from '@/lib/connection-status';
import { KubeContextPicker } from '@/components/widgets/kube-context-picker';

export function AppShell({ children }: { children?: ReactNode }) {
  return (
    <>
      <ConnectionStatus />
      <AppSidebar
        nav={
          <div className="flex items-center gap-2 px-2">
            <KubeContextPicker />
          </div>
        }
        footer={<AccountMenu />}
      >
        {children}
      </AppSidebar>
    </>
  );
}
