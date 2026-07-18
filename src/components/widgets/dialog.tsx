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

// The app's reusable centered dialog: a controlled base-ui Dialog (via
// @kubetail/ui) wrapped with a standard header (title + optional description) and a
// scrollable body capped at the viewport height, so overlay screens share one
// sizing/scroll/close shell instead of hand-assembling the raw `Dialog*` primitives
// (imported here as `DialogRoot`/`DialogContent`/…). Fully controlled — the caller
// owns `open`/`onOpenChange`; there's no built-in trigger. Width defaults to a
// comfortable reading measure; pass `className` to widen — it's tailwind-merged, so
// a `sm:max-w-*` utility replaces the default rather than stacking with it. A close
// button and click-outside/Escape dismissal come from the underlying Dialog. Under
// the app's `DialogProvider` (via `AppDialogs`), it reports close-completion to the
// host so a lazily-mounted dialog unmounts only once its exit animation has played.
import type { ReactNode } from 'react';
import { createPortal } from 'react-dom';

import {
  Dialog as DialogRoot,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@kubetail/ui/elements/dialog';
import { cn } from '@kubetail/ui/lib/utils';

import { isMacOS } from '@/lib/platform';
import { useDialogHost } from '@/lib/dialog';

// True when a native event landed on the window's drag region — the title-bar
// strip carrying `data-tauri-drag-region` (see `WindowDragBand` below and the
// shell's bands in `app-sidebar.tsx`).
function isDragRegionEvent(event: Event): boolean {
  const { target } = event;
  return target instanceof Element && target.closest('[data-tauri-drag-region]') !== null;
}

// A window-drag strip painted across the top of the *dialog's* portal. While a
// dialog is open its backdrop (and, when modal, base-ui's internal backdrop)
// covers the shell's own title-bar drag band and swallows the mousedown that
// moves the window — and both live in the portal's stacking context, above the
// whole app, so a band rendered in the app tree can't reach over them. Portaling
// this band to `document.body` puts it in that same top-level context, where a
// high `z-index` lets it sit above the backdrop and start a window drag again.
//
// It spans only the title-bar band (matched to `app-sidebar.tsx`: 44px on macOS,
// 32px elsewhere), well clear of a centered dialog's body. On macOS the native
// traffic lights float above web content, so they stay clickable through it.
// A press here reads as an outside-press to base-ui; `Dialog` cancels that
// dismissal (see below) so dragging the window never closes the dialog.
function WindowDragBand() {
  return createPortal(
    <div
      data-testid="dialog-window-drag-region"
      data-tauri-drag-region
      aria-hidden
      className={`fixed inset-x-0 top-0 z-60 ${isMacOS() ? 'h-11' : 'h-8'}`}
    />,
    document.body,
  );
}

type DialogProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: ReactNode;
  description?: ReactNode;
  // Extra classes for the dialog content box — chiefly width (e.g. `sm:max-w-4xl`).
  className?: string;
  children: ReactNode;
};

export function Dialog({ open, onOpenChange, title, description, className, children }: DialogProps) {
  const host = useDialogHost();
  return (
    <DialogRoot
      open={open}
      onOpenChange={(nextOpen, eventDetails) => {
        // Grabbing the window-drag band (which overlays the backdrop) reads as an
        // outside-press to base-ui and would dismiss the dialog the instant the
        // user starts moving the window. Cancel that — dragging the window must
        // not close the dialog. Escape, the close button, and clicking the dimmed
        // backdrop still dismiss it.
        if (!nextOpen && eventDetails.reason === 'outside-press' && isDragRegionEvent(eventDetails.event)) {
          eventDetails.cancel();
          return;
        }
        onOpenChange(nextOpen);
      }}
      // Once the close transition settles, tell the host so it can unmount us.
      onOpenChangeComplete={(nextOpen) => {
        if (!nextOpen) host?.notifyClosed();
      }}
    >
      {/* Keeps the window draggable by its title bar while the dialog is open. */}
      {open && <WindowDragBand />}
      {/* Cap at the viewport and lay out as a column so the body scrolls within
          the dialog instead of pushing it off-screen. */}
      <DialogContent className={cn('flex max-h-[85vh] flex-col', className)}>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          {description ? <DialogDescription>{description}</DialogDescription> : null}
        </DialogHeader>
        {/* min-h-0 lets this flex child shrink below its content so overflow
            scrolls here rather than growing the dialog. */}
        <div className="min-h-0 flex-1 overflow-y-auto">{children}</div>
      </DialogContent>
    </DialogRoot>
  );
}
