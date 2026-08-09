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

// App-level dialog controller. Triggers live in the sidebar but the render lives
// above it (`AppDialogs` in AppLayout) — the sidebar card unmounts when the window
// collapses below md, so a dialog rendered inside it would vanish mid-view.
// `mountedDialog` trails `activeDialog` through the exit animation: closing clears
// only `activeDialog`; the shared `Dialog` wrapper calls `notifyClosed` once the
// close settles, which unmounts. One dialog open at a time.
import type { ReactNode } from 'react';
import { createContext, useContext, useMemo, useState } from 'react';

// Add a dialog by extending this union and rendering it in the `AppDialogs` host.
export type DialogId = 'clusters' | 'settings';

type DialogContextValue = {
  activeDialog: DialogId | null;
  // Trails `activeDialog` through the exit animation.
  mountedDialog: DialogId | null;
  openDialog: (id: DialogId) => void;
  closeDialog: () => void;
  // Close transition settled → unmount. Called by the shared `Dialog` wrapper only.
  notifyClosed: () => void;
};

// Controlled props every overlay dialog forwards to the shared `Dialog` wrapper;
// close-completion is handled by the wrapper via context.
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

// Non-throwing read for the shared `Dialog` wrapper — a `Dialog` outside the
// provider still works, it just has no host to notify.
export function useDialogHost(): DialogContextValue | null {
  return useContext(DialogContext);
}
