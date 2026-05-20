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

//! Single place where every long-lived background task is spawned, so the
//! event topology of the host is grep-able from one file.
//!
//! ## Event flows
//!
//! | Trigger              | Producer              | Channel                       | Consumer(s)                                | Effect                          |
//! |----------------------|-----------------------|-------------------------------|--------------------------------------------|---------------------------------|
//! | login / refresh      | `auth::AUTH`          | `Auth::watch_credentials`     | `sidecar::credentials` (pusher)            | POST `/control/credentials`     |
//! | login / refresh      | `auth::AUTH`          | `Auth::watch_credentials`     | `auth::broadcast` (session broadcaster)    | emit app event to every window  |
//! | TTL timer / OS wake  | `auth::refresher`     | calls `access_token_…`        | `auth::AUTH` (`bump_creds` on success)     | drives the two flows above      |
//! | OS power / network   | `wake::Waker`         | `watch<u64>` (subscribe)      | `auth::refresher`                          | re-arm + refresh check          |
//! | OS power / network   | `wake::Waker`         | `watch<u64>` (subscribe)      | `sidecar::wake_poster`                     | POST `/control/wake`            |
//! | sidecar `READY` line | `sidecar::lifecycle`  | stdout parse                  | host (sets UDS path used by transport)     | unblocks GraphQL bridge         |
//!
//! ## Two-phase startup
//!
//! [`spawn_pre_restore`] runs synchronously inside `setup` so it's subscribed
//! before the keychain restore can mutate auth state. [`spawn_post_restore`]
//! runs inside the restore task after the `auth:session-resolved` event
//! fires — that way the refresher doesn't race a refresh attempt against
//! the restore itself.

use tauri::{AppHandle, Manager, Runtime};

use crate::{auth, sidecar, wake};

/// Tasks that must be live *before* the keychain restore can mutate auth
/// state. Today: the session broadcaster, which fans creds changes out to
/// every window — if it weren't subscribed first, a restore that bumps
/// `creds_gen` would be missed.
pub fn spawn_pre_restore<R: Runtime>(app: &AppHandle<R>) {
    auth::broadcast::spawn_session_broadcaster(app);
}

/// Tasks that depend on the restored auth state. Spawns the wake signal,
/// its subscribers (refresher + wake_poster), and the credential pusher.
/// Hands the `Waker` to `app.manage` so the watch channel stays alive for
/// the process — see [`wake::spawn_wake`] for why.
pub fn spawn_post_restore<R: Runtime>(app: &AppHandle<R>) {
    let waker = wake::spawn_wake();
    auth::refresher::spawn_auth_refresher(waker.subscribe());
    sidecar::spawn_wake_poster(app, waker.subscribe());
    app.manage(waker);
    sidecar::spawn_credential_pusher(app);
}
