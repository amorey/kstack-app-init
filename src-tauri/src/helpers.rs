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

//! UI scaffolding for the Tauri host: the application menu bar and the
//! system tray icon.
//!
//! Both builders are called once during app setup (see `lib.rs`). Their menu
//! event handlers route user actions — opening or focusing windows, quitting —
//! through the shared [`AppState`] and its `window_manager`.

use tauri::menu::{Menu, MenuBuilder, MenuItem, PredefinedMenuItem, SubmenuBuilder};
use tauri::tray::TrayIconBuilder;
use tauri::AppHandle;
use tauri::Manager;

use crate::error::Result;
use crate::state::AppState;

/// Builds and installs the application menu bar.
///
/// Constructs a single "File" submenu with a "New Window" item (`CmdOrCtrl+N`)
/// and a "Quit" item (`CmdOrCtrl+Q`), sets it as the app menu, and registers a
/// menu-event handler.
///
/// "Quit" is a custom item rather than [`SubmenuBuilder::quit`] on purpose: the
/// predefined quit terminates the process natively (macOS `terminate:`), which
/// skips Tauri's `RunEvent::ExitRequested` graceful-shutdown hook. Routing it
/// through [`AppHandle::exit`] takes the same path as the tray's "Quit".
///
/// The handler responds to `new_window` by creating a window via the
/// [`AppState`] `window_manager` (a failure is logged rather than propagated,
/// since menu callbacks cannot return errors) and to `quit` via
/// [`AppHandle::exit`].
///
/// # Errors
///
/// Returns an error if any menu item or submenu fails to build, or if setting
/// the menu on the app fails.
pub fn build_app_menu(app: &AppHandle) -> Result<()> {
    let new_window = MenuItem::with_id(app, "new_window", "New Window", true, Some("CmdOrCtrl+N"))?;
    let quit = MenuItem::with_id(app, "quit", "Quit", true, Some("CmdOrCtrl+Q"))?;

    let file_menu = SubmenuBuilder::new(app, "File")
        .item(&new_window)
        .separator()
        .item(&quit)
        .build()?;

    let menu = MenuBuilder::new(app).item(&file_menu).build()?;
    app.set_menu(menu)?;

    app.on_menu_event(|app, event| match event.id().as_ref() {
        "new_window" => {
            let state = app.state::<AppState>();
            if let Err(err) = state.window_manager.new_window(app) {
                tracing::error!(%err, "failed to open window from app menu");
            }
        }
        "quit" => app.exit(0),
        _ => {}
    });

    Ok(())
}

/// Builds and installs the system tray icon and its context menu.
///
/// The tray menu offers "New Window", "Show Main Window", and "Quit", using
/// the app's default window icon. The menu-event handler routes each item
/// through the [`AppState`] `window_manager`: `tray_new_window` creates a new
/// window, `tray_show_main` shows the main window, and `tray_quit` exits the
/// process via [`AppHandle::exit`]. Window-manager failures are logged rather
/// than propagated, since menu callbacks cannot return errors.
///
/// # Errors
///
/// Returns an error if any menu item fails to build or if the tray icon fails
/// to build.
///
/// # Panics
///
/// Panics if the app has no default window icon.
pub fn build_tray(app: &AppHandle) -> Result<()> {
    let new_window = MenuItem::with_id(app, "tray_new_window", "New Window", true, None::<&str>)?;
    let show_main = MenuItem::with_id(
        app,
        "tray_show_main",
        "Show Main Window",
        true,
        None::<&str>,
    )?;
    let separator = PredefinedMenuItem::separator(app)?;
    let quit = MenuItem::with_id(app, "tray_quit", "Quit", true, None::<&str>)?;

    let menu: Menu<_> = Menu::with_items(app, &[&new_window, &show_main, &separator, &quit])?;

    TrayIconBuilder::new()
        .menu(&menu)
        .icon(
            app.default_window_icon()
                .expect("missing default icon")
                .clone(),
        )
        .on_menu_event(|app, event| {
            let state = app.state::<AppState>();
            match event.id.as_ref() {
                "tray_new_window" => {
                    if let Err(err) = state.window_manager.new_window(app) {
                        tracing::error!(%err, "failed to open window from tray");
                    }
                }
                "tray_show_main" => {
                    if let Err(err) = state.window_manager.show_main_window(app) {
                        tracing::error!(%err, "failed to show main window");
                    }
                }
                "tray_quit" => app.exit(0),
                _ => {}
            }
        })
        .build(app)?;

    Ok(())
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
pub fn spawn_signal_handler(app: &AppHandle) {
    use tokio::signal::unix::{signal, SignalKind};

    let app = app.clone();
    tauri::async_runtime::spawn(async move {
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
            _ = streams.0.recv() => "SIGTERM",
            _ = streams.1.recv() => "SIGINT",
            _ = streams.2.recv() => "SIGHUP",
        };

        tracing::info!(signal = received, "received termination signal; exiting");
        app.exit(0);
    });
}
