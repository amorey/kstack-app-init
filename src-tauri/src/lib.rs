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
//! - [`app_menu`] — the application menu bar.
//! - [`commands`] — `#[tauri::command]` handlers invoked from the webview.
//! - `dock_menu` — custom macOS Dock menu (macOS only).
//! - [`error`] — the host-wide [`AppError`](error::AppError) type.
//! - [`services`] — long-lived services: the [`SidecarService`].
//! - [`state`] — the Tauri-managed [`AppState`].
//! - [`tray`] — the system tray icon, menu, and live context subscription.
//! - [`window_manager`] — window creation and focus.

#![warn(clippy::unwrap_used)]
// `unwrap()` is idiomatic in tests; the lint above exists to keep
// production code unwrap-free, so silence it under `cfg(test)`.
#![cfg_attr(test, allow(clippy::unwrap_used))]

mod app_menu;
mod commands;
#[cfg(target_os = "macos")]
mod dock_menu;
mod error;
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

/// How long the quit path waits for the sidecar to exit cleanly before the
/// `RunEvent::Exit` handler force-kills it. The sidecar's own shutdown is
/// bounded at ~5s (3s HTTP drain + 2s sync-engine join — see sidecar
/// `main.go`); this allows for that plus a margin.
const SIDECAR_SHUTDOWN_GRACE: Duration = Duration::from_secs(6);

/// Process-global setup: logging.
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
}

/// Routes Unix termination signals into the same graceful-exit path as an
/// explicit "Quit".
///
/// Tao installs no signal handlers, so without this a `SIGTERM` / `SIGINT` /
/// `SIGHUP` would kill the process outright — bypassing the `RunEvent`
/// shutdown hooks entirely. Funneling through [`AppHandle::exit`] produces the
/// same sequence as the menu/tray "Quit": `ExitRequested { code: Some(0) }`
/// followed by `Exit`. `SIGKILL` remains uncatchable by design.
///
/// Spawned onto Tauri's async runtime; the task lives until a signal arrives
/// or the process exits.
///
/// Unix-only, and deliberately so:
/// - **Windows** has no POSIX signals. Its real session-end (logout / restart
///   / shutdown) arrives as a `WM_ENDSESSION` window message, which tao already
///   handles by firing `RunEvent::Exit` — so it lands in the event loop's
///   shutdown arm with no handler needed here.
/// - **macOS** system shutdown is caught earlier, in `applicationShouldTerminate:`
///   (see the `dock_menu` module).
#[cfg(all(desktop, unix))]
fn spawn_signal_handler(app: &tauri::AppHandle) {
    use tokio::signal::unix::{signal, SignalKind};

    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        // If Quit happens through another path (menu/tray), the shutdown
        // signal fires and this task exits instead of lingering for a signal
        // that will never come.
        let shutdown = app.state::<AppState>().shutdown.clone();

        // A failed registration is logged and abandons signal handling
        // entirely rather than leaving a partial set wired up.
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
            commands::ready,
            commands::graphql_query,
            commands::graphql_subscribe,
            commands::graphql_unsubscribe,
            commands::new_window,
            commands::quit,
        ])
        .setup(|app| {
            // Initialize dependencies
            let sidecar = SidecarService::spawn(app.handle())?;
            let window_manager = WindowManager::new();

            // The floating sidebar is the window's title bar; drop native
            // decorations on Linux/Windows so the webview draws its own chrome
            // (macOS keeps its Overlay title bar from tauri.macos.conf.json).
            window_manager.apply_main_window_chrome(app.handle())?;

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
            // AuthStateWatch stream (populates on the first frame).
            tray::spawn_authstate_subscription(app.handle());

            // Fire the sidecar's Poke on OS wake / network-return so long-lived
            // connections resync promptly (accelerates the sidecar's wall-clock
            // detector and covers network-return without sleep).
            wake::spawn_wake_poke_supervisor(app.handle());

            // Custom macOS Dock menu (no Tauri API — see dock_menu module).
            #[cfg(target_os = "macos")]
            dock_menu::install(app.handle());

            // Route Unix termination signals into the graceful-exit path.
            // (Windows session-end arrives as WM_ENDSESSION, which tao already
            // routes to RunEvent::Exit — see spawn_signal_handler.)
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
                        // Stop the app-lifetime background tasks first (tray
                        // supervisor, signal handler) so they don't reconnect
                        // or sit in a backoff sleep while we tear the sidecar
                        // down. Cancelling is idempotent.
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
