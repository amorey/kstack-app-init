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

//! System tray icon + context menu + the gRPC auth-state watch that keeps the
//! account section live. Tauri wiring lives here; the pure account-section
//! logic lives in [`account_menu`]. [`build_tray`] and
//! [`spawn_authstate_subscription`] each run once during setup (`lib.rs`).

pub mod account_menu;

use std::time::Duration;

use tauri::menu::{Menu, MenuItem, PredefinedMenuItem, SubmenuBuilder};
use tauri::tray::{TrayIconBuilder, TrayIconId};
use tauri::{AppHandle, Manager, Wry};

use crate::error::Result;
use crate::state::AppState;
use account_menu::{
    account_snapshot_from, build_account_descriptor, AccountMenuDescriptor, ACCOUNT_LOGIN_ID,
    ACCOUNT_LOGOUT_ID, ACCOUNT_SETTINGS_ID,
};

/// Latest account-watch snapshot; `None` renders as SignedOut until the stream
/// delivers. Plain `std::sync::Mutex` — no `.await` is ever held under the
/// lock.
#[derive(Default)]
pub(crate) struct TraySnapshots {
    account: Option<account_menu::AccountSnapshot>,
}

/// Base URL for the kstack cloud dashboard.
const KSTACK_CLOUD_DASHBOARD_URL: &str = "https://app.kstack.sh";

/// Stable tray id so the watch supervisor can swap the menu via
/// [`Manager::tray_by_id`] without owning the handle.
const TRAY_ID: &str = "main";

/// Builds and installs the system tray icon and menu. Menu callbacks can't
/// return errors, so failures are logged. Panics if the app has no default
/// window icon.
pub fn build_tray(app: &AppHandle) -> Result<()> {
    // Start signed-out; the account watch supervisor populates on first frame.
    let menu = build_tray_menu(app, AccountMenuDescriptor::SignedOut)?;

    TrayIconBuilder::with_id(TrayIconId::new(TRAY_ID))
        .menu(&menu)
        .icon(
            app.default_window_icon()
                .expect("missing default icon")
                .clone(),
        )
        .on_menu_event(|app, event| {
            let id = event.id.as_ref();

            // Auth actions: fire-and-forget RPCs; the result comes back through
            // the watch stream and rebuilds the menu.
            match id {
                ACCOUNT_LOGIN_ID => {
                    let app = app.clone();
                    tauri::async_runtime::spawn(async move {
                        let state = app.state::<AppState>();
                        if let Err(err) = state.sidecar.start_login().await {
                            tracing::error!(%err, "failed to start login from tray");
                        }
                    });
                    return;
                }
                ACCOUNT_LOGOUT_ID => {
                    let app = app.clone();
                    tauri::async_runtime::spawn(async move {
                        let state = app.state::<AppState>();
                        if let Err(err) = state.sidecar.logout().await {
                            tracing::error!(%err, "failed to logout from tray");
                        }
                    });
                    return;
                }
                ACCOUNT_SETTINGS_ID => {
                    let app = app.clone();
                    tauri::async_runtime::spawn(async move {
                        use tauri_plugin_opener::OpenerExt;
                        if let Err(err) = app.opener().open_url(
                            format!("{KSTACK_CLOUD_DASHBOARD_URL}/account"),
                            None::<&str>,
                        ) {
                            tracing::error!(%err, "failed to open account settings URL");
                        }
                    });
                    return;
                }
                _ => {}
            }

            match id {
                // Tray events fire on the main thread; window builds must go to
                // the blocking pool — never the main thread (WebView2
                // deadlock) or an async worker (see `commands::new_window`).
                "tray_new_window" => {
                    let app = app.clone();
                    tauri::async_runtime::spawn_blocking(move || {
                        if let Err(err) = app.state::<AppState>().window_manager.new_window(&app) {
                            tracing::error!(%err, "failed to open window from tray");
                        }
                    });
                }
                "tray_show_main" => {
                    let app = app.clone();
                    tauri::async_runtime::spawn_blocking(move || {
                        if let Err(err) = app
                            .state::<AppState>()
                            .window_manager
                            .show_main_window(&app)
                        {
                            tracing::error!(%err, "failed to show main window");
                        }
                    });
                }
                "tray_quit" => app.exit(0),
                _ => {}
            }
        })
        .build(app)?;

    Ok(())
}

/// Builds the full tray menu (account section + static items) — used for the
/// initial build and every watch-triggered rebuild.
fn build_tray_menu(app: &AppHandle, account: AccountMenuDescriptor) -> tauri::Result<Menu<Wry>> {
    let new_window = MenuItem::with_id(app, "tray_new_window", "New Window", true, None::<&str>)?;
    let show_main = MenuItem::with_id(
        app,
        "tray_show_main",
        "Show Main Window",
        true,
        None::<&str>,
    )?;
    let quit = MenuItem::with_id(app, "tray_quit", "Quit", true, None::<&str>)?;

    // Account section: "Sign in / Sign up" when signed out, or a submenu
    // titled with the user's name containing "Account Settings" + "Sign out".
    let acct_menu_item: Box<dyn tauri::menu::IsMenuItem<Wry>> = match account {
        AccountMenuDescriptor::SignedOut => {
            let item = MenuItem::with_id(
                app,
                ACCOUNT_LOGIN_ID,
                "Sign in / Sign up",
                true,
                None::<&str>,
            )?;
            Box::new(item)
        }
        AccountMenuDescriptor::SignedIn { title } => {
            let settings = MenuItem::with_id(
                app,
                ACCOUNT_SETTINGS_ID,
                "Account Settings",
                true,
                None::<&str>,
            )?;
            let sign_out =
                MenuItem::with_id(app, ACCOUNT_LOGOUT_ID, "Sign out", true, None::<&str>)?;
            let submenu = SubmenuBuilder::new(app, title)
                .item(&settings)
                .item(&sign_out)
                .build()?;
            Box::new(submenu)
        }
    };

    Menu::with_items(
        app,
        &[
            acct_menu_item.as_ref(),
            &PredefinedMenuItem::separator(app)?,
            &new_window,
            &show_main,
            &PredefinedMenuItem::separator(app)?,
            &quit,
        ],
    )
}

/// Rebuilds the tray menu from the snapshot in [`AppState::tray`]. muda
/// requires menu mutation on the main thread, hence
/// [`AppHandle::run_on_main_thread`].
fn rebuild_tray_menu(app: &AppHandle) {
    // Snapshot under the lock, release before the main-thread dispatch.
    let account_snap = {
        let state = app.state::<AppState>();
        let guard = state.tray.lock().unwrap_or_else(|p| p.into_inner());
        guard
            .account
            .clone()
            .unwrap_or_else(account_menu::AccountSnapshot::signed_out)
    };

    let menu_app = app.clone();
    if let Err(err) = app.run_on_main_thread(move || {
        let Some(tray) = menu_app.tray_by_id(TRAY_ID) else {
            return;
        };
        let account = build_account_descriptor(&account_snap);
        match build_tray_menu(&menu_app, account) {
            Ok(menu) => {
                if let Err(err) = tray.set_menu(Some(menu)) {
                    tracing::error!(%err, "failed to set tray menu");
                }
            }
            Err(err) => tracing::error!(%err, "failed to build tray menu"),
        }
    }) {
        tracing::error!(%err, "failed to schedule tray menu rebuild");
    }
}

/// Supervises the gRPC auth-state watch for the app's lifetime. First frame is
/// a full snapshot; failed opens retry with capped backoff; a stream that ends
/// after delivering data reconnects promptly, one that ends without data backs
/// off (no busy-loop).
pub fn spawn_authstate_subscription(app: &AppHandle) {
    /// First retry delay; doubles each failure up to `MAX_BACKOFF`.
    const BASE_BACKOFF: Duration = Duration::from_millis(500);
    const MAX_BACKOFF: Duration = Duration::from_secs(10);

    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        let shutdown = app.state::<AppState>().shutdown.clone();
        let mut backoff = BASE_BACKOFF;
        loop {
            let state = app.state::<AppState>();
            let open = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                open = state.sidecar.watch_auth_state() => open,
            };

            let delay = match open {
                Ok(mut stream) => {
                    let mut saw_snapshot = false;
                    loop {
                        let msg = tokio::select! {
                            biased;
                            _ = shutdown.cancelled() => return,
                            msg = stream.message() => msg,
                        };
                        match msg {
                            Ok(Some(auth_state)) => {
                                saw_snapshot = true;
                                {
                                    let app_state = app.state::<AppState>();
                                    app_state
                                        .tray
                                        .lock()
                                        .unwrap_or_else(|p| p.into_inner())
                                        .account = Some(account_snapshot_from(&auth_state));
                                }
                                rebuild_tray_menu(&app);
                            }
                            Ok(None) => break,
                            Err(err) => {
                                tracing::warn!(%err, "auth-state tray watch stream error");
                                break;
                            }
                        }
                    }
                    if saw_snapshot {
                        backoff = BASE_BACKOFF;
                        tracing::info!("auth-state tray watch ended; reconnecting");
                        BASE_BACKOFF
                    } else {
                        tracing::warn!(
                            ?backoff,
                            "auth-state watch ended before any snapshot; backing off"
                        );
                        let delay = backoff;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        delay
                    }
                }
                Err(err) => {
                    tracing::warn!(%err, ?backoff, "auth-state tray watch failed; retrying");
                    let delay = backoff;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    delay
                }
            };

            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
        }
    });
}
