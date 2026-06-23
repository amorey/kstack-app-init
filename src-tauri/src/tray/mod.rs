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

//! The system tray icon, its context menu, and the host-internal gRPC
//! auth-state watch that keeps the account section live.
//!
//! The Tauri wiring lives here; the pure, unit-tested domain logic behind the
//! account section (menu-id constants, descriptor building) lives in
//! [`account_menu`]. [`build_tray`] and [`spawn_authstate_subscription`] are each
//! called once during app setup (see `lib.rs`). The menu event handler routes
//! user actions — opening or focusing windows, signing in/out, quitting —
//! through the shared [`AppState`].

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

/// Holds the latest gRPC snapshot for the tray's account watch stream so each
/// rebuild sees the most-recent state. Starts as `None` and renders as its safe
/// default (SignedOut) until the stream delivers. Protected by a plain
/// `std::sync::Mutex`: the critical section is tiny and synchronous — no `.await`
/// is ever held under the lock — so poisoning uses
/// `unwrap_or_else(|p| p.into_inner())`.
#[derive(Default)]
pub(crate) struct TraySnapshots {
    account: Option<account_menu::AccountSnapshot>,
}

/// Base URL for the kstack cloud dashboard.
const KSTACK_CLOUD_DASHBOARD_URL: &str = "https://app.kstack.sh";

/// Stable id for the system tray icon, so the watch supervisor can look it up
/// via [`Manager::tray_by_id`] and swap its menu without owning the handle.
const TRAY_ID: &str = "main";

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
    // Start signed-out; the account watch supervisor populates it on its first
    // frame (see spawn_authstate_subscription below).
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

            // Account auth actions — async RPCs, fire-and-forget; the result
            // comes back through the auth-state watch stream and rebuilds the
            // menu automatically.
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

            let state = app.state::<AppState>();
            match id {
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

/// Builds the full system-tray menu, including the account section. Used both
/// for the initial tray build and for every rebuild triggered by the account
/// watch stream, so the static items (New Window / Show Main Window / Quit) stay
/// defined in one place.
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

/// Rebuilds the full tray menu from the latest account snapshot stored in
/// [`AppState::tray`]. The account watch supervisor calls this whenever its
/// stream delivers a new frame.
///
/// muda requires menu mutation on the main thread, so the work is queued via
/// [`AppHandle::run_on_main_thread`] rather than run on the stream-reader task.
fn rebuild_tray_menu(app: &AppHandle) {
    // Extract the account snapshot under the lock, release before the main-thread
    // dispatch — no .await is ever held under this lock.
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

/// Starts the host-internal gRPC auth-state watch that keeps the tray's account
/// section live, and supervises it for the app's lifetime.
///
/// The first frame (a full snapshot) populates the account section; subsequent
/// frames track session changes (sign-in, sign-out, token refresh carrying the
/// same `authenticated` bit). Resilience mirrors the renderer's GraphQL
/// reconnect, which this host-internal stream doesn't get for free:
/// failed opens retry with capped backoff; a stream that ends after delivering
/// data reconnects promptly; one that ends without data backs off to avoid
/// busy-looping.
///
/// The task only ends when the app exits.
pub fn spawn_authstate_subscription(app: &AppHandle) {
    /// First retry delay; doubles each failure up to `MAX_BACKOFF`.
    const BASE_BACKOFF: Duration = Duration::from_millis(500);
    /// Ceiling for the backoff so a sidecar that never comes up doesn't
    /// stretch the retry interval unboundedly.
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
                                // Write the new auth snapshot into the shared
                                // holder, then rebuild from both latest states.
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
