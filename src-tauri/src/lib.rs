// Copyright 2026 The Kstack Authors
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

//! The Tauri host crate: windows, tray/menus, the Go sidecar's lifecycle, and
//! the webview→sidecar GraphQL bridge over an AF_UNIX socket.
//!
//! [`run`] is the single entry point — `main.rs` (and the mobile entry point)
//! call it. Module map: [`app_menu`], [`commands`], `dock_menu` (macOS),
//! [`error`], [`services`] ([`SidecarService`]), [`state`] ([`AppState`]),
//! [`tray`], [`window_manager`].

#![warn(clippy::unwrap_used)]
// `unwrap()` is idiomatic in tests.
#![cfg_attr(test, allow(clippy::unwrap_used))]

mod app_menu;
mod commands;
#[cfg(target_os = "macos")]
mod dock_menu;
mod error;
mod host_file;
// Opaque platforms only; Linux windows are transparent (see `window_manager`).
#[cfg(not(target_os = "linux"))]
mod os_theme;
mod services;
mod state;
mod tray;
mod wake;
mod window_manager;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tauri::{Manager, RunEvent};
use tokio_util::sync::CancellationToken;

use crate::services::sidecar::SidecarService;
use crate::state::AppState;
use crate::window_manager::WindowManager;

/// Quit-path wait before `RunEvent::Exit` force-kills the sidecar. Sidecar's
/// own shutdown is bounded at ~5s (see sidecar `main.go`); this adds margin.
const SIDECAR_SHUTDOWN_GRACE: Duration = Duration::from_secs(6);

/// Process-global setup: logging. Lives here (not `main`) so it also covers
/// the mobile entry point.
fn init_process() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("KSTACK_LOG_LEVEL")
                .or_else(|_| tracing_subscriber::EnvFilter::try_from_default_env())
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
}

/// Routes Unix termination signals (`SIGTERM`/`SIGINT`/`SIGHUP`) through
/// [`AppHandle::exit`] — the same graceful path as menu/tray "Quit". Tao
/// installs no signal handlers, so without this a signal skips the `RunEvent`
/// shutdown hooks.
///
/// Unix-only by design: Windows session-end arrives as `WM_ENDSESSION`, which
/// tao already routes to `RunEvent::Exit`; macOS system shutdown is caught in
/// `applicationShouldTerminate:` (see `dock_menu`).
#[cfg(all(desktop, unix))]
fn spawn_signal_handler(app: &tauri::AppHandle) {
    use tokio::signal::unix::{signal, SignalKind};

    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        // A Quit via another path cancels `shutdown`, ending this task.
        let shutdown = app.state::<AppState>().shutdown.clone();

        // A failed registration abandons signal handling entirely rather than
        // leaving a partial set wired up.
        let mut streams = match (
            signal(SignalKind::terminate()),
            signal(SignalKind::interrupt()),
            signal(SignalKind::hangup()),
        ) {
            (Ok(term), Ok(int), Ok(hup)) => (term, int, hup),
            (term, int, hup) => {
                let err = term.err().or(int.err()).or(hup.err());
                tracing::error!(?err, "failed to install termination signal handlers");
                return;
            }
        };

        let received = tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = streams.0.recv() => "SIGTERM",
            _ = streams.1.recv() => "SIGINT",
            _ = streams.2.recv() => "SIGHUP",
        };

        tracing::info!(signal = received, "received termination signal; exiting");
        app.exit(0);
    });
}

/// Builds, configures, and runs the Tauri application (the host's entry
/// point).
///
/// The event loop intercepts shutdown: closing the last window keeps the app
/// alive in the background; only an explicit Quit (or a Unix signal — see
/// [`spawn_signal_handler`]) exits and tears down the sidecar.
#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    init_process();

    let mut builder = tauri::Builder::default();

    // Single-instance must be registered before all other plugins so a second
    // launch forwards into this process.
    #[cfg(desktop)]
    {
        builder = builder.plugin(tauri_plugin_single_instance::init(|app, _argv, _cwd| {
            // Show/recreate the main window. Must be `spawn_blocking`, not the
            // main thread (WebView2 main-thread build deadlocks — see
            // `commands::new_window`) and not `spawn` (the build parks its
            // thread until the window exists).
            let app = app.clone();
            tauri::async_runtime::spawn_blocking(move || {
                if let Err(err) = app
                    .state::<AppState>()
                    .window_manager
                    .show_main_window(&app)
                {
                    tracing::error!(%err, "failed to show main window on second-instance launch")
                }
            });
        }));
    }

    let app = builder
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_shell::init())
        .invoke_handler(tauri::generate_handler![
            commands::ready,
            commands::graphql_query,
            commands::graphql_subscribe,
            commands::graphql_unsubscribe,
            commands::new_window,
            commands::update_host_file,
            commands::quit,
        ])
        .setup(|app| {
            let sidecar = SidecarService::spawn(app.handle())?;
            let window_manager = WindowManager::new();

            // Create the main window. All windows are built in code
            // (`tauri.conf.json` declares none); `build_window` owns chrome and
            // pre-paint theming — visible from creation, no reveal step. See
            // docs/adr/2026-08-09-first-paint-theming.md.
            window_manager.show_main_window(app.handle())?;

            app.manage(AppState {
                sidecar,
                window_manager,
                tray: Arc::new(Mutex::new(tray::TraySnapshots::default())),
                shutdown: CancellationToken::new(),
            });

            // Build menu and tray
            app_menu::build_app_menu(app.handle())?;
            tray::build_tray(app.handle())?;

            // Keep the tray's account section live off the sidecar's
            // AuthStateWatch stream.
            tray::spawn_authstate_subscription(app.handle());

            // Poke the sidecar on OS wake / network-return; see
            // docs/adr/2026-08-09-poke-resync-fanout.md.
            wake::spawn_wake_poke_supervisor(app.handle());

            // Custom macOS Dock menu (no Tauri API — see dock_menu module).
            #[cfg(target_os = "macos")]
            dock_menu::install(app.handle());

            #[cfg(all(desktop, unix))]
            spawn_signal_handler(app.handle());

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
            #[cfg(target_os = "macos")]
            RunEvent::Reopen {
                has_visible_windows,
                ..
            } => {
                if !has_visible_windows {
                    let state = app_handle.state::<AppState>();
                    if let Err(err) = state.window_manager.show_main_window(app_handle) {
                        tracing::error!(%err, "failed to show main window on dock reopen");
                    }
                }
            }
            RunEvent::ExitRequested { api, code, .. } => {
                match code {
                    // Keep app open in background when the last window is closing
                    None => api.prevent_exit(),

                    // Start graceful shutdown if user has requested Quit
                    Some(code) => {
                        tracing::info!(code, "exit requested; initiating graceful shutdown");

                        let state = app_handle.state::<AppState>();
                        // Cancel app-lifetime background tasks first so they
                        // don't retry against a dying sidecar. Idempotent.
                        state.shutdown.cancel();
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
