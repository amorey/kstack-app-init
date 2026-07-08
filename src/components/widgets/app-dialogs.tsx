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

// The render site for the app's overlay dialogs. Mounted once in AppLayout,
// above the sidebar, so an open dialog survives the sidebar card unmounting when
// the window auto-collapses below the md breakpoint. Only the *mounted* dialog is
// rendered — each dialog's hooks and subscriptions cost nothing until it's opened
// — so the registry scales as we add more. The mount lifecycle (open, linger for
// the exit animation, then unmount) lives in `DialogProvider`; this host just
// renders whatever it says is mounted. Register a dialog by adding a line to
// `DIALOGS` (and a `DialogId` variant).
import type { ComponentType } from 'react';

import { ClusterSyncPanel } from '@/components/widgets/cluster-sync-panel';
import { useDialog, type AppDialogProps, type DialogId } from '@/lib/dialog';

// Each overlay dialog is a controlled component taking `AppDialogProps`. Partial
// because a `DialogId` may be reserved before its dialog is built (e.g. settings).
const DIALOGS: Partial<Record<DialogId, ComponentType<AppDialogProps>>> = {
  clusters: ClusterSyncPanel,
};

export function AppDialogs() {
  const { activeDialog, mountedDialog, closeDialog } = useDialog();
  const Mounted = mountedDialog ? DIALOGS[mountedDialog] : undefined;
  if (!Mounted) return null;

  return (
    <Mounted
      // Open while this is the active dialog; false once dismissed, which drives
      // the exit animation before the provider unmounts it (via `notifyClosed`).
      open={activeDialog === mountedDialog}
      onOpenChange={(open) => {
        if (!open) closeDialog();
      }}
    />
  );
}
