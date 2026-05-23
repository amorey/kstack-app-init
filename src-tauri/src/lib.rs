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

//! The Tauri host crate.
//!
//! This crate is the desktop app's native host: it owns windows, the tray and
//! menus, and the Go sidecar's lifecycle, and it bridges GraphQL operations
//! from the webview to the sidecar over an AF_UNIX socket.
//!
//! [`run`] is the single entry point — `main.rs` (and the mobile entry point)
//! call it to build, configure, and run the Tauri application. The module
//! layout:
//!
//! - [`commands`] — `#[tauri::command]` handlers invoked from the webview.
//! - `dock_menu` — custom macOS Dock menu (macOS only).
//! - [`error`] — the host-wide [`AppError`](error::AppError) type.
//! - [`helpers`] — menu and tray construction.
//! - [`services`] — long-lived services: the [`SidecarService`] and the
//!   OAuth [`AuthService`].
//! - [`state`] — the Tauri-managed [`AppState`].
//! - [`window_manager`] — window creation and focus.

#![warn(clippy::unwrap_used)]

mod commands;
#[cfg(target_os = "macos")]
mod dock_menu;
mod error;
mod helpers;
mod services;
mod state;
mod window_manager;

use std::time::Duration;

use tauri::{Manager, RunEvent};

use crate::services::auth::{AuthConfig, AuthService};
use crate::services::sidecar::SidecarService;
use crate::state::AppState;
use crate::window_manager::WindowManager;

/// How long the quit path waits for the sidecar to exit cleanly before the
/// `RunEvent::Exit` handler force-kills it. The sidecar's own shutdown is
/// bounded at ~5s (3s HTTP drain + 2s sync-engine join — see sidecar
/// `main.go`); this allows for that plus a margin.
const SIDECAR_SHUTDOWN_GRACE: Duration = Duration::from_secs(6);

/// Process-global setup: logging and the OS keychain store.
///
/// Run once at the very start of [`run`], before the Tauri builder. Kept here
/// (rather than in `main`) so it also covers the mobile entry point.
fn init_process() {
    // Initialize logging as early as possible.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("KSTACK_LOG_LEVEL")
                .or_else(|_| tracing_subscriber::EnvFilter::try_from_default_env())
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    // Register the OS keychain as keyring's default store (OAuth tokens are
    // persisted there). Non-fatal on failure: auth still works in-memory for
    // the session, it just will not survive a restart.
    if let Err(err) = keyring::use_native_store(true) {
        tracing::warn!(%err, "failed to initialize the OS keychain");
    }
}

/// Builds, configures, and runs the Tauri application.
///
/// This is the host's entry point. It initializes tracing, registers plugins
/// (single-instance first — see the note below), wires the webview command
/// handlers, runs the `setup` hook to spawn the sidecar and build the menu and
/// tray, then enters the event loop.
///
/// The event loop intercepts shutdown: closing the last window does **not**
/// exit the process (the app stays alive in the background); only an explicit
/// Quit triggers a real exit, at which point the sidecar is shut down. OS
/// termination signals are funneled into that same path — see
/// [`spawn_signal_handler`].
///
/// `tauri_plugin_single_instance` must be registered before all other plugins
/// so a second launch is forwarded into the original process rather than
/// starting a new one.
#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    init_process();

    // Initialize builder
    let mut builder = tauri::Builder::default();

    // Register app as single-instance (must come first)
    #[cfg(desktop)]
    {
        builder = builder.plugin(tauri_plugin_single_instance::init(|app, _argv, _cwd| {
            let state = app.state::<AppState>();

            // Bring the main window forward or create it if it was closed
            if let Err(err) = state.window_manager.show_main_window(app) {
                tracing::error!(%err, "failed to show main window on second-instance launch")
            }
        }));
    }

    // Initialize app
    let app = builder
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_shell::init())
        .invoke_handler(tauri::generate_handler![
            commands::auth_login,
            commands::auth_logout,
            commands::auth_status,
            commands::ready,
            commands::graphql_query,
            commands::graphql_subscribe,
            commands::graphql_unsubscribe,
        ])
        .setup(|app| {
            // Initialize dependencies
            let sidecar = SidecarService::spawn(app.handle())?;
            let window_manager = WindowManager::new();
            let auth = AuthService::new(AuthConfig::from_env());

            app.manage(AppState {
                sidecar,
                window_manager,
                auth,
            });

            // Restore any persisted OAuth session in the background.
            AuthService::spawn_restore(app.handle());

            // Build menu and tray
            helpers::build_app_menu(app.handle())?;
            helpers::build_tray(app.handle())?;

            // Custom macOS Dock menu (no Tauri API — see dock_menu module).
            #[cfg(target_os = "macos")]
            dock_menu::install(app.handle());

            // Route Unix termination signals into the graceful-exit path.
            // (Windows session-end arrives as WM_ENDSESSION, which tao already
            // routes to RunEvent::Exit — see helpers::spawn_signal_handler.)
            #[cfg(all(desktop, unix))]
            helpers::spawn_signal_handler(app.handle());

            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while running tauri application");

    // Run with an event handler so we can intercept shutdown
    app.run(|app_handle, event| {
        match event {
            RunEvent::Ready => {
                // TODO: app is ready
            }
            RunEvent::ExitRequested { api, code, .. } => {
                match code {
                    // Keep app open in background when the last window is closing
                    None => api.prevent_exit(),

                    // Start graceful shutdown if user has requested Quit
                    Some(code) => {
                        tracing::info!(code, "exit requested; initiating graceful shutdown");

                        let state = app_handle.state::<AppState>();
                        if !state.sidecar.graceful_shutdown(SIDECAR_SHUTDOWN_GRACE) {
                            tracing::warn!("sidecar did not exit within grace period");
                        }
                    }
                }
            }
            RunEvent::Exit => {
                // Force-kill the sidecar in case the graceful path timed out.
                let state = app_handle.state::<AppState>();
                state.sidecar.kill();

                tracing::info!("shutdown complete");
            }
            _ => {}
        }
    });
}
