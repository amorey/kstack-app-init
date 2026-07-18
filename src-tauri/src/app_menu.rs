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

//! The application menu bar. Built once during app setup (see `lib.rs`); its
//! menu-event handler routes user actions through the shared [`AppState`] and
//! its `window_manager`.

use tauri::AppHandle;

use crate::error::Result;

#[cfg(target_os = "macos")]
use crate::state::AppState;
#[cfg(target_os = "macos")]
use tauri::menu::{MenuBuilder, MenuItem, SubmenuBuilder};
#[cfg(target_os = "macos")]
use tauri::Manager;

/// Builds and installs the application menu bar (macOS only).
///
/// On macOS this builds a "File" submenu with "New Window" (`CmdOrCtrl+N`) and
/// "Quit" (`CmdOrCtrl+Q`), sets it as the global menu bar, and registers a
/// menu-event handler.
///
/// On Linux/Windows this is a no-op: the native menu would render as a bar
/// *inside* each window, so the webview's `AppMenu` provides the same New
/// Window / Quit affordances via the `new_window` / `quit` commands instead.
///
/// "Quit" is a custom item rather than [`SubmenuBuilder::quit`] on purpose: the
/// predefined quit terminates the process natively (macOS `terminate:`), which
/// skips Tauri's `RunEvent::ExitRequested` graceful-shutdown hook. Routing it
/// through [`AppHandle::exit`] takes the same path as the tray's "Quit".
///
/// # Errors
///
/// Returns an error if any menu item or submenu fails to build, or if setting
/// the menu fails.
pub fn build_app_menu(app: &AppHandle) -> Result<()> {
    // Linux/Windows: nothing to install; the webview's `AppMenu` stands in.
    #[cfg(not(target_os = "macos"))]
    {
        let _ = app;
        Ok(())
    }

    #[cfg(target_os = "macos")]
    {
        build_macos_menu(app)
    }
}

#[cfg(target_os = "macos")]
fn build_macos_menu(app: &AppHandle) -> Result<()> {
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
