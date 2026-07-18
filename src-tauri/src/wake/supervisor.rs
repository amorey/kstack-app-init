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

//! Tauri glue for the wake/network-return → `Poke` driver. Owns the event
//! channel: native sources push [`RawEvent`](super::core::RawEvent)s in, and
//! [`run_coalescer`] fires one [`SidecarService::poke`] per debounced burst.

use std::time::Duration;

use tauri::{AppHandle, Manager};
use tokio::sync::mpsc;

use super::{core, platform};
use crate::state::AppState;

/// Trailing-edge debounce: the poke fires this long after the last trigger in a
/// burst, so a wake quickly followed by network-return collapses to one poke
/// while staying well under the sidecar's ~30s wall-clock backstop.
const DEBOUNCE_WINDOW: Duration = Duration::from_secs(3);

/// Depth of the source→coalescer channel. Sources emit only a handful of events
/// per transition; a saturated channel drops the excess, which the coalescer
/// would collapse anyway.
const CHANNEL_DEPTH: usize = 16;

/// Starts the host-side wake/network-return supervisor for the app's lifetime.
///
/// Spawns the platform sources, then runs the coalescer until app Quit cancels
/// [`AppState::shutdown`] (or every source drops). Poke is best-effort — a
/// failure is logged and swallowed, since the sidecar's wall-clock detector still
/// covers sleep. Mirrors `tray::spawn_authstate_subscription`.
pub fn spawn_wake_poke_supervisor(app: &AppHandle) {
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        let shutdown = app.state::<AppState>().shutdown.clone();
        let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);

        // Each source pushes RawEvents into `tx` and owns its own teardown
        // (Linux/Windows honor `shutdown`; macOS leaks its passive observers).
        // The app handle lets macOS's NSWorkspace source register on the main thread.
        platform::spawn_sources(&app, tx, shutdown.clone());

        // Drain + debounce, poking once per burst; `app` is cloned per poke.
        core::run_coalescer(rx, DEBOUNCE_WINDOW, shutdown, move || {
            let app = app.clone();
            async move {
                if let Err(err) = app.state::<AppState>().sidecar.poke().await {
                    // Best-effort — the wall-clock detector is the backstop.
                    tracing::warn!(%err, "wake poke failed");
                }
            }
        })
        .await;
    });
}
