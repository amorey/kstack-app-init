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

// The app's reusable centered dialog over @kubetail/ui's base-ui `Dialog*`
// primitives: standard header + viewport-capped scrolling body. Fully controlled
// (no built-in trigger). `className` is tailwind-merged, so a `sm:max-w-*`
// replaces the default width rather than stacking. Under `DialogProvider` it
// reports close-completion so a lazily-mounted dialog unmounts only after its
// exit animation.
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

// Did the event land on a `data-tauri-drag-region` strip?
function isDragRegionEvent(event: Event): boolean {
  const { target } = event;
  return target instanceof Element && target.closest('[data-tauri-drag-region]') !== null;
}

// Restores window dragging while a dialog is open: the backdrop covers the
// shell's title-bar drag band, so this band must be portaled to `document.body`
// (the backdrop's stacking context) to sit above it. Height must match
// `app-sidebar.tsx`'s title bar (44px macOS / 32px elsewhere). A press here
// reads as an outside-press to base-ui — cancelled below.
// See docs/adr/2026-08-09-per-platform-window-chrome.md
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
        // Dragging the window must not close the dialog; Escape, the close
        // button and the backdrop still dismiss it.
        if (!nextOpen && eventDetails.reason === 'outside-press' && isDragRegionEvent(eventDetails.event)) {
          eventDetails.cancel();
          return;
        }
        onOpenChange(nextOpen);
      }}
      // Close transition settled — the host may now unmount us.
      onOpenChangeComplete={(nextOpen) => {
        if (!nextOpen) host?.notifyClosed();
      }}
    >
      {open && <WindowDragBand />}
      {/* Column + viewport cap so the body scrolls inside instead of pushing the
          dialog off-screen. */}
      <DialogContent className={cn('flex max-h-[85vh] flex-col', className)}>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          {description ? <DialogDescription>{description}</DialogDescription> : null}
        </DialogHeader>
        {/* min-h-0: let this flex child shrink below its content so overflow
            scrolls here rather than growing the dialog. */}
        <div className="min-h-0 flex-1 overflow-y-auto">{children}</div>
      </DialogContent>
    </DialogRoot>
  );
}
