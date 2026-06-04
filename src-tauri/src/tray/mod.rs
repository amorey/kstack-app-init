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
//! kube-context watch that keeps the "Default Context" submenu live.
//!
//! The Tauri wiring lives here; the pure, unit-tested domain logic behind the
//! "Default Context" submenu (menu-id formatting, descriptor building) lives in
//! [`default_context_picker`]. [`build_tray`] and [`spawn_kubeconfig_subscription`]
//! are each called once during app setup (see `lib.rs`). The menu event handler
//! routes user actions — opening or focusing windows, switching context,
//! quitting — through the shared [`AppState`].

pub mod account_menu;
mod default_context_picker;

use std::time::Duration;

use tauri::menu::{CheckMenuItem, Menu, MenuItem, PredefinedMenuItem, SubmenuBuilder};
use tauri::tray::{TrayIconBuilder, TrayIconId};
use tauri::{AppHandle, Manager, Wry};

use crate::error::Result;
use crate::services::sidecar::KubeContextState;
use crate::state::AppState;
use account_menu::{
    account_snapshot_from, build_account_descriptor, AccountMenuDescriptor, ACCOUNT_LOGIN_ID,
    ACCOUNT_LOGOUT_ID, ACCOUNT_SETTINGS_ID,
};
use default_context_picker::{
    build_context_descriptors, context_menu_id, parse_context_menu_id, ContextItem,
};

/// Holds the latest gRPC snapshots for both tray watch streams so each rebuild
/// sees the most-recent state from both. Updated by whichever stream fires;
/// the other field starts as `None` and renders as its safe default (Loading /
/// SignedOut) until its stream delivers. Protected by a plain `std::sync::Mutex`:
/// the critical section is tiny and synchronous — no `.await` is ever held
/// under the lock — so poisoning uses `unwrap_or_else(|p| p.into_inner())`.
#[derive(Default)]
pub(crate) struct TraySnapshots {
    kube: Option<KubeContextState>,
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
    // Start in the loading state for both sections; the watch supervisors
    // populate them on their first frames (see spawn_kubeconfig_subscription /
    // spawn_authstate_subscription below).
    let menu = build_tray_menu(
        app,
        ContextMenuState::Loading,
        AccountMenuDescriptor::SignedOut,
    )?;

    TrayIconBuilder::with_id(TrayIconId::new(TRAY_ID))
        .menu(&menu)
        .icon(
            app.default_window_icon()
                .expect("missing default icon")
                .clone(),
        )
        .on_menu_event(|app, event| {
            let id = event.id.as_ref();

            // A `kube_ctx::<name>` click switches the default context. The RPC
            // is async and the callback is sync, so fire-and-forget on the
            // runtime; the resulting config change comes back through the watch
            // stream and re-checks the menu.
            if let Some(name) = parse_context_menu_id(id) {
                let app = app.clone();
                let name = name.to_string();
                tauri::async_runtime::spawn(async move {
                    let state = app.state::<AppState>();
                    if let Err(err) = state.sidecar.set_current_context(name).await {
                        tracing::error!(%err, "failed to set kube-context from tray");
                    }
                });
                return;
            }

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

/// What the "Default Context" submenu should render. Distinguishes "we don't
/// know the contexts yet" from "we know, and there are none" — the same on-disk
/// emptiness and a still-connecting/unavailable watcher would otherwise look
/// identical to the user.
enum ContextMenuState<'a> {
    /// No `kubeConfigWatch` snapshot has arrived yet (connecting, retrying, or
    /// the watcher is unavailable). Rendered as a disabled "Loading…".
    Loading,
    /// A snapshot arrived; render these contexts, or a disabled
    /// "No contexts found" when the slice is empty.
    Loaded(&'a [ContextItem]),
}

/// Builds the full system-tray menu, including the dynamic "Default Context"
/// submenu and the account section. Used both for the initial tray build and
/// for every rebuild triggered by either watch stream, so the static items
/// (New Window / Show Main Window / Quit) stay defined in one place.
fn build_tray_menu(
    app: &AppHandle,
    ctx_state: ContextMenuState,
    account: AccountMenuDescriptor,
) -> tauri::Result<Menu<Wry>> {
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

    let mut ctx_builder = SubmenuBuilder::new(app, "Default Context");
    match ctx_state {
        // Disabled placeholders carry distinct ids so they never match the
        // `kube_ctx::` prefix the menu-event handler routes on.
        ContextMenuState::Loading => {
            let item = MenuItem::with_id(app, "kube_ctx_loading", "Loading…", false, None::<&str>)?;
            ctx_builder = ctx_builder.item(&item);
        }
        ContextMenuState::Loaded([]) => {
            let item = MenuItem::with_id(
                app,
                "kube_ctx_none",
                "No contexts found",
                false,
                None::<&str>,
            )?;
            ctx_builder = ctx_builder.item(&item);
        }
        ContextMenuState::Loaded(descriptors) => {
            for d in descriptors {
                // A checked radio-style item per context; the click is routed
                // by its `kube_ctx::<name>` id in the tray menu-event handler.
                let item = CheckMenuItem::with_id(
                    app,
                    context_menu_id(&d.label),
                    &d.label,
                    true,
                    d.checked,
                    None::<&str>,
                )?;
                ctx_builder = ctx_builder.item(&item);
            }
        }
    }
    let ctx_submenu = ctx_builder.build()?;

    Menu::with_items(
        app,
        &[
            acct_menu_item.as_ref(),
            &PredefinedMenuItem::separator(app)?,
            &new_window,
            &show_main,
            &PredefinedMenuItem::separator(app)?,
            &ctx_submenu,
            &PredefinedMenuItem::separator(app)?,
            &quit,
        ],
    )
}

/// Rebuilds the full tray menu from the combined latest kube-context and auth
/// snapshots stored in [`AppState::tray`]. Both watch supervisors call this
/// whenever their stream delivers a new frame.
///
/// muda requires menu mutation on the main thread, so the work is queued via
/// [`AppHandle::run_on_main_thread`] rather than run on the stream-reader task.
fn rebuild_tray_menu(app: &AppHandle) {
    // Extract both states under the lock, release before the main-thread
    // dispatch — no .await is ever held under this lock.
    let (kube_data, account_snap) = {
        let state = app.state::<AppState>();
        let guard = state.tray.lock().unwrap_or_else(|p| p.into_inner());
        // Project kube to (Vec<String>, String); KubeContextState isn't Clone.
        let kube_data = guard.kube.as_ref().map(|k| {
            (
                k.contexts
                    .iter()
                    .map(|c| c.name.clone())
                    .collect::<Vec<_>>(),
                k.current_context.clone(),
            )
        });
        let account_snap = guard
            .account
            .clone()
            .unwrap_or_else(account_menu::AccountSnapshot::signed_out);
        (kube_data, account_snap)
    };

    let menu_app = app.clone();
    if let Err(err) = app.run_on_main_thread(move || {
        let Some(tray) = menu_app.tray_by_id(TRAY_ID) else {
            return;
        };
        // Build account descriptor inside the closure — it's cheap and avoids
        // moving a non-Copy enum across both match arms.
        let account = build_account_descriptor(&account_snap);
        let menu_result = match kube_data.as_ref() {
            None => build_tray_menu(&menu_app, ContextMenuState::Loading, account),
            Some((contexts, current)) => {
                let descriptors = build_context_descriptors(contexts, current);
                build_tray_menu(&menu_app, ContextMenuState::Loaded(&descriptors), account)
            }
        };
        match menu_result {
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

/// Starts the host-internal gRPC kube-context watch that keeps the tray's
/// context list live, and supervises it for the app's lifetime.
///
/// The first frame (a full snapshot) populates the menu; subsequent frames
/// track on-disk changes (including those this app makes via the
/// `set_current_context` RPC on a tray click).
///
/// Resilience mirrors the renderer's GraphQL reconnect, which this
/// host-internal stream doesn't get for free:
///   * **Startup** — on a cold/slow start the sidecar may still be binding
///     past the connect budget; a single attempt would fail and leave the tray
///     stuck on "Loading…". Failed opens retry with capped backoff.
///   * **Reconnect** — when the stream drops (e.g. a sidecar restart) the loop
///     re-opens it so the menu resumes tracking changes instead of freezing on
///     the last-seen state.
///   * **No data** — a stream that ends without ever delivering a snapshot
///     would otherwise reconnect on a tight loop, so that case takes the capped
///     backoff. The tray stays on "Loading…" until a snapshot lands.
///
/// The task only ends when the app exits.
pub fn spawn_kubeconfig_subscription(app: &AppHandle) {
    /// First retry delay; doubles each failure up to `MAX_BACKOFF`.
    const BASE_BACKOFF: Duration = Duration::from_millis(500);
    /// Ceiling for the backoff so a sidecar that never comes up doesn't
    /// stretch the retry interval unboundedly.
    const MAX_BACKOFF: Duration = Duration::from_secs(10);

    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        // App Quit cancels this; we then stop reconnecting and abandon any
        // pending backoff sleep rather than churning against a sidecar that's
        // being torn down (see AppState::shutdown / lib.rs ExitRequested).
        let shutdown = app.state::<AppState>().shutdown.clone();
        let mut backoff = BASE_BACKOFF;
        loop {
            // Open the watch stream, bailing out immediately on Quit. Bind the
            // State guard so it outlives the await inside the select.
            let state = app.state::<AppState>();
            let open = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                open = state.sidecar.watch_kube_context() => open,
            };

            // Decide how long to wait before the next attempt. A shutdown at
            // any await below ends the supervisor outright.
            let delay = match open {
                Ok(mut stream) => {
                    // Pump snapshots until the stream ends or errors. Each
                    // message is a full snapshot; rebuild the menu from it.
                    let mut saw_snapshot = false;
                    loop {
                        let msg = tokio::select! {
                            biased;
                            _ = shutdown.cancelled() => return,
                            msg = stream.message() => msg,
                        };
                        match msg {
                            Ok(Some(kube)) => {
                                saw_snapshot = true;
                                // Write the new kube snapshot into the shared
                                // holder, then rebuild from both latest states.
                                {
                                    let app_state = app.state::<AppState>();
                                    app_state
                                        .tray
                                        .lock()
                                        .unwrap_or_else(|p| p.into_inner())
                                        .kube = Some(kube);
                                }
                                rebuild_tray_menu(&app);
                            }
                            // Clean end-of-stream or a transport error: leave
                            // the loop and reconnect (the inner select already
                            // covers Quit).
                            Ok(None) => break,
                            Err(err) => {
                                tracing::warn!(%err, "kube-context tray watch stream error");
                                break;
                            }
                        }
                    }
                    if saw_snapshot {
                        // Ended after delivering data — a genuine drop.
                        // Reconnect promptly with a fresh backoff budget.
                        backoff = BASE_BACKOFF;
                        tracing::info!("kube-context tray watch ended; reconnecting");
                        BASE_BACKOFF
                    } else {
                        // Opened but ended before any snapshot (watcher
                        // unavailable). Back off so we don't busy-loop.
                        tracing::warn!(
                            ?backoff,
                            "kube-context watch ended before any snapshot; backing off"
                        );
                        let delay = backoff;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        delay
                    }
                }
                Err(err) => {
                    tracing::warn!(%err, ?backoff, "kube-context tray watch failed; retrying");
                    let delay = backoff;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    delay
                }
            };

            // Cancellable backoff: Quit shouldn't wait out a full sleep
            // (up to MAX_BACKOFF) before the task exits.
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
        }
    });
}

/// Starts the host-internal gRPC auth-state watch that keeps the tray's account
/// section live, and supervises it for the app's lifetime.
///
/// The first frame (a full snapshot) populates the account section; subsequent
/// frames track session changes (sign-in, sign-out, token refresh carrying the
/// same `authenticated` bit). Resilience mirrors [`spawn_kubeconfig_subscription`]:
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
