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

// App-level dialog controller. The overlay screens (Clusters today, Settings
// next) are opened from the sidebar's account menu — but the sidebar card
// unmounts when the window auto-collapses below the md breakpoint (see
// `app-sidebar.tsx`), so a dialog rendered inside it would vanish mid-view. This
// splits the two concerns across the sidebar boundary: the *trigger* (a menu
// item) stays in the sidebar and only calls `openDialog(id)` through this
// context, while the dialog *render* lives above the sidebar (the `AppDialogs`
// host in AppLayout), where it survives the card unmounting.
//
// The controller also owns the mount lifecycle so the host stays a dumb renderer
// and dialog components never handle their own teardown: `mountedDialog` trails
// `activeDialog` — opening sets both, closing clears only `activeDialog` (which
// drives the dialog's `open` to false) and leaves `mountedDialog` set so the
// dialog lingers for its exit animation. The shared `Dialog` wrapper calls
// `notifyClosed` once base-ui reports the close transition settled, which clears
// `mountedDialog` and unmounts the dialog. Only one dialog is open at a time.
import type { ReactNode } from 'react';
import { createContext, useContext, useMemo, useState } from 'react';

// The overlay screens this controller can open. Add a dialog by extending this
// union and rendering it in the `AppDialogs` host.
export type DialogId = 'clusters' | 'settings';

type DialogContextValue = {
  activeDialog: DialogId | null;
  // The dialog currently mounted; trails `activeDialog` through the exit animation.
  mountedDialog: DialogId | null;
  openDialog: (id: DialogId) => void;
  closeDialog: () => void;
  // Reports that the mounted dialog's close transition has settled, so it can be
  // unmounted. Called by the shared `Dialog` wrapper, not by dialog components.
  notifyClosed: () => void;
};

// The controlled props every overlay dialog accepts. `AppDialogs` supplies these
// and each dialog forwards them to the shared `Dialog` wrapper. Close-completion
// is handled by the wrapper via context, so dialogs never see it.
export type AppDialogProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
};

const DialogContext = createContext<DialogContextValue | null>(null);

export function DialogProvider({ children }: { children: ReactNode }) {
  const [activeDialog, setActiveDialog] = useState<DialogId | null>(null);
  const [mountedDialog, setMountedDialog] = useState<DialogId | null>(null);
  const value = useMemo<DialogContextValue>(
    () => ({
      activeDialog,
      mountedDialog,
      openDialog: (id) => {
        setMountedDialog(id);
        setActiveDialog(id);
      },
      closeDialog: () => setActiveDialog(null),
      notifyClosed: () => setMountedDialog(null),
    }),
    [activeDialog, mountedDialog],
  );
  return <DialogContext.Provider value={value}>{children}</DialogContext.Provider>;
}

export function useDialog(): DialogContextValue {
  const ctx = useContext(DialogContext);
  if (!ctx) throw new Error('useDialog must be used within a DialogProvider');
  return ctx;
}

// Non-throwing read for the shared `Dialog` wrapper, which reports its close-
// completion to the host when there is one. A `Dialog` rendered outside the host
// (no provider) still works — it simply has no host to notify.
export function useDialogHost(): DialogContextValue | null {
  return useContext(DialogContext);
}
