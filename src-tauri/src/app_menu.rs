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

//! The application menu bar (macOS-only), built once during app setup.

use tauri::AppHandle;

use crate::error::Result;

#[cfg(target_os = "macos")]
use crate::state::AppState;
#[cfg(target_os = "macos")]
use tauri::menu::{MenuBuilder, MenuItem, SubmenuBuilder};
#[cfg(target_os = "macos")]
use tauri::Manager;

/// Builds and installs the macOS menu bar (File → New Window / Quit). No-op on
/// Linux/Windows, where a native menu would render inside each window — the
/// webview's `AppMenu` provides the same actions via `new_window` / `quit`.
///
/// "Quit" must stay a custom item routed through [`AppHandle::exit`]:
/// [`SubmenuBuilder::quit`] terminates natively (`terminate:`) and skips the
/// `RunEvent::ExitRequested` graceful-shutdown hook.
pub fn build_app_menu(app: &AppHandle) -> Result<()> {
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
