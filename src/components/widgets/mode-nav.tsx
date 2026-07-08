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

// Chat/Dashboard switch at the top of the sidebar, styled as a segmented
// control. These are real router links (not local state), so each mode is a
// route (`/chat`, `/dashboard`) — deep-linkable and addressable as its own
// window later. The active link is highlighted via the router's `data-status`.
import { Link } from '@tanstack/react-router';

// Segmented-control pill styling. The container reads as one control (muted
// track); the active link floats as a raised background chip.
const TRACK = 'inline-flex w-full items-center rounded-lg bg-muted p-[3px] text-muted-foreground';
const ITEM =
  'flex-1 rounded-md px-2 py-1 text-center text-sm font-medium transition-colors hover:text-foreground ' +
  'data-[status=active]:bg-background data-[status=active]:text-foreground data-[status=active]:shadow-sm';

export function ModeNav() {
  return (
    <nav className={TRACK} aria-label="View">
      <Link to="/chat" className={ITEM}>
        Chat
      </Link>
      <Link to="/dashboard" className={ITEM}>
        Dashboard
      </Link>
    </nav>
  );
}
