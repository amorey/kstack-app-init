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

//! Tauri glue for the wake/network-return → `Poke` driver.
//!
//! Owns the single event channel: the per-platform native sources push
//! [`RawEvent`](super::core::RawEvent)s into the sender, and [`run_coalescer`]
//! drains them, firing one [`SidecarService::poke`] per debounced burst.

use std::time::Duration;

use tauri::{AppHandle, Manager};
use tokio::sync::mpsc;

use super::{core, platform};
use crate::state::AppState;

/// Trailing-edge debounce window. After a triggering edge, the poke fires this
/// long after the *last* trigger in the burst — long enough that a wake quickly
/// followed by network-return collapses to a single poke, short enough that the
/// resync still feels prompt (well under the sidecar's ~30s wall-clock backstop).
const DEBOUNCE_WINDOW: Duration = Duration::from_secs(3);

/// Depth of the source→coalescer channel. Sources only ever emit a handful of
/// events per real-world transition, so a small buffer is plenty; a saturated
/// channel simply drops the excess (the coalescer would collapse them anyway).
const CHANNEL_DEPTH: usize = 16;

/// Starts the host-side wake/network-return supervisor for the app's lifetime.
///
/// Spawns the `#[cfg]`-selected platform sources, then runs the coalescer until
/// app Quit cancels [`AppState::shutdown`] (or every source drops). Poke is
/// best-effort: a failure is logged and swallowed, since the sidecar's own
/// wall-clock detector still covers sleep. Mirrors the shape of the tray
/// supervisors (`tray::spawn_kubeconfig_subscription`).
pub fn spawn_wake_poke_supervisor(app: &AppHandle) {
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        let shutdown = app.state::<AppState>().shutdown.clone();
        let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);

        // Bring up the native event sources; each pushes RawEvents into `tx`.
        // Teardown is the source's own concern (Linux/Windows honor `shutdown`;
        // macOS leaks its passive observers for the process lifetime). The app
        // handle lets a source register on the main thread (macOS NSWorkspace).
        platform::spawn_sources(&app, tx, shutdown.clone());

        // Drain + debounce, poking the sidecar once per burst. `app` is moved in
        // and cloned per poke (the spawned future outlives the closure call).
        core::run_coalescer(rx, DEBOUNCE_WINDOW, shutdown, move || {
            let app = app.clone();
            async move {
                if let Err(err) = app.state::<AppState>().sidecar.poke().await {
                    // Best-effort: the sidecar's wall-clock detector is the
                    // backstop, so we don't retry.
                    tracing::warn!(%err, "wake poke failed");
                }
            }
        })
        .await;
    });
}
