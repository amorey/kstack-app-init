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

// The webview's client for `host.json` — the host's persisted settings file and
// their source of truth (see `src-tauri/src/host_file.rs`). The webview keeps no
// copy of its own; this module owns the whole protocol so a setting's own module
// (e.g. `theme.tsx`) only has to pick its field out of the file:
//
//   - Boot: the host injects the file into every window as `window.__KSTACK_HOST__`
//     before any page script runs, so it reads synchronously — no flash, no async
//     reconcile. `readInjectedHostFile` is that read.
//   - Change: `updateHostFile` sends a partial patch through the host's general
//     `update_host_file` command. Best-effort — see below.
//   - Sync: after every write the host broadcasts the merged file to all windows;
//     `subscribeHostFile` delivers it, so a change made in any window lands in the
//     others (and, redundantly but idempotently, in the writer's).
//
// Adding a setting means adding a field here and to the host's `HostFile` /
// `HostFilePatch` — not a new command or a new sync path.
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';

// The contents of `host.json`, as the host serializes it (see
// `host_file::init_script`). Every setting is optional: the host omits unset
// ones, and the file itself is absent outside Tauri (plain-browser dev). Values
// are typed as `string` rather than their narrow unions because the file is
// external input — each setting's module validates its own field, so a newer
// host format can't yank a value out from under an older webview.
export type HostFile = {
  schemaVersion?: number;
  colorSchemePreference?: string;
};

// A partial update: only the fields present are written; the host merges the
// rest. Mirrors the host's `HostFilePatch`.
export type HostFilePatch = Partial<Omit<HostFile, 'schemaVersion'>>;

declare global {
  interface Window {
    __KSTACK_HOST__?: HostFile;
  }
}

// Event the host broadcasts to every window after `host.json` changes, carrying
// the merged file. Hand-mirrors `host_file::UPDATED_EVENT` in the host.
const UPDATED_EVENT = 'host-file-updated';

// The host-injected snapshot of `host.json`, or an empty file when the global is
// absent (plain-browser dev). Synchronous — safe to read during first paint.
export function readInjectedHostFile(): HostFile {
  return window.__KSTACK_HOST__ ?? {};
}

// Merge a patch into `host.json`. Fire-and-forget: callers apply their change
// optimistically, and a failed write (or an absent bridge) costs durability
// across restart, not the current session.
export function updateHostFile(patch: HostFilePatch): void {
  invoke('update_host_file', { patch }).catch(() => {});
}

// Subscribe to the host's post-write broadcast of the merged file. Returns an
// unsubscribe that is safe to call before `listen` has resolved — the caller
// (typically a `useEffect` cleanup) never has to await registration.
export function subscribeHostFile(onUpdate: (file: HostFile) => void): () => void {
  let unlisten: (() => void) | undefined;
  let cancelled = false;

  listen<HostFile>(UPDATED_EVENT, (event) => onUpdate(event.payload ?? {}))
    .then((fn) => {
      // Unsubscribed while `listen` was still in flight.
      if (cancelled) fn();
      else unlisten = fn;
    })
    // Absent bridge (plain-browser dev) — no cross-window sync to track.
    .catch(() => {});

  return () => {
    cancelled = true;
    unlisten?.();
  };
}
