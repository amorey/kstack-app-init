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
// @kubetail/ui) wrapped with a standard header (title + optional description) and
// a scrollable body capped at the viewport height, so overlay screens (Clusters
// today, Settings next) share one sizing/scroll/close shell. This is the opinionated
// composition on top of the raw `Dialog*` primitives (imported here as
// `DialogRoot`/`DialogContent`/…); consumers use this instead of hand-assembling
// the parts. Fully controlled — the caller owns `open`/`onOpenChange`; there's no
// built-in trigger. Width defaults to a comfortable reading measure; pass
// `className` to widen (e.g. a wide data table) — it's tailwind-merged onto the
// dialog content, so a `sm:max-w-*` utility replaces the built-in default rather
// than stacking with it. A close button (top-right) and click-outside/Escape
// dismissal come from the underlying Dialog. When rendered under the app's
// `DialogProvider` (the usual case, via `AppDialogs`), it reports its close-
// completion to the host so a lazily-mounted dialog is unmounted only once its
// exit animation has played — dialog components need do nothing for this.
import type { ReactNode } from 'react';

import {
  Dialog as DialogRoot,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@kubetail/ui/elements/dialog';
import { cn } from '@kubetail/ui/lib/utils';

import { useDialogHost } from '@/lib/dialog';

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
      onOpenChange={onOpenChange}
      // Once the close transition settles, tell the host so it can unmount us.
      onOpenChangeComplete={(nextOpen) => {
        if (!nextOpen) host?.notifyClosed();
      }}
    >
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
