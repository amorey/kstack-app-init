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

//! Tauri glue for the wake → `Poke` driver: owns the event channel, spawns the
//! platform sources, runs the coalescer.

use std::time::Duration;

use tauri::{AppHandle, Manager};
use tokio::sync::mpsc;

use super::{core, platform};
use crate::state::AppState;

/// Trailing-edge debounce: one poke per burst (wake + network-return collapse),
/// well under the sidecar's ~30s wall-clock backstop.
const DEBOUNCE_WINDOW: Duration = Duration::from_secs(3);

/// Source→coalescer channel depth. A saturated channel drops the excess, which
/// the coalescer would collapse anyway.
const CHANNEL_DEPTH: usize = 16;

/// Starts the wake/network-return supervisor for the app's lifetime; ends when
/// [`AppState::shutdown`] cancels or every source drops. Poke is best-effort —
/// failures logged and swallowed (the wall-clock detector covers sleep).
pub fn spawn_wake_poke_supervisor(app: &AppHandle) {
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        let shutdown = app.state::<AppState>().shutdown.clone();
        let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);

        // Sources own their teardown (Linux/Windows honor `shutdown`; macOS
        // leaks its passive observers). `app` lets macOS register on the main thread.
        platform::spawn_sources(&app, tx, shutdown.clone());

        core::run_coalescer(rx, DEBOUNCE_WINDOW, shutdown, move || {
            let app = app.clone();
            async move {
                if let Err(err) = app.state::<AppState>().sidecar.poke().await {
                    tracing::warn!(%err, "wake poke failed");
                }
            }
        })
        .await;
    });
}
