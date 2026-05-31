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
//! [`default_context_picker`]. [`build_tray`] and [`spawn_tray_subscription`]
//! are each called once during app setup (see `lib.rs`). The menu event handler
//! routes user actions — opening or focusing windows, switching context,
//! quitting — through the shared [`AppState`].

mod default_context_picker;

use std::time::Duration;

use tauri::menu::{CheckMenuItem, Menu, MenuItem, PredefinedMenuItem, SubmenuBuilder};
use tauri::tray::{TrayIconBuilder, TrayIconId};
use tauri::{AppHandle, Manager, Wry};

use crate::error::Result;
use crate::services::sidecar::KubeContextState;
use crate::state::AppState;
use default_context_picker::{
    build_context_descriptors, context_menu_id, parse_context_menu_id, ContextItem,
};

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
    // Start in the loading state; the host-internal kubeConfigWatch
    // subscription's first frame populates the real contexts (see below).
    let menu = build_tray_menu(app, ContextMenuState::Loading)?;

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
/// submenu. Used both for the initial tray build (with [`ContextMenuState::Loading`])
/// and for every rebuild [`KubeContextTraySink`] performs on a config change,
/// so the static items (New Window / Show Main Window / Quit) stay defined in
/// one place.
fn build_tray_menu(app: &AppHandle, state: ContextMenuState) -> tauri::Result<Menu<Wry>> {
    let new_window = MenuItem::with_id(app, "tray_new_window", "New Window", true, None::<&str>)?;
    let show_main = MenuItem::with_id(
        app,
        "tray_show_main",
        "Show Main Window",
        true,
        None::<&str>,
    )?;
    let quit = MenuItem::with_id(app, "tray_quit", "Quit", true, None::<&str>)?;

    let mut ctx_builder = SubmenuBuilder::new(app, "Default Context");
    match state {
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
            &new_window,
            &show_main,
            &PredefinedMenuItem::separator(app)?,
            &ctx_submenu,
            &PredefinedMenuItem::separator(app)?,
            &quit,
        ],
    )
}

/// Rebuilds the tray's "Default Context" submenu from a gRPC kube-context
/// snapshot. muda requires menu mutation on the main thread, so the work is
/// queued there via [`AppHandle::run_on_main_thread`] rather than run on the
/// stream-reader task.
fn rebuild_tray_menu(app: &AppHandle, state: &KubeContextState) {
    let contexts: Vec<String> = state.contexts.iter().map(|c| c.name.clone()).collect();
    let current = state.current_context.clone();
    let menu_app = app.clone();
    if let Err(err) = app.run_on_main_thread(move || {
        let Some(tray) = menu_app.tray_by_id(TRAY_ID) else {
            return;
        };
        let descriptors = build_context_descriptors(&contexts, &current);
        match build_tray_menu(&menu_app, ContextMenuState::Loaded(&descriptors)) {
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
pub fn spawn_tray_subscription(app: &AppHandle) {
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
                            Ok(Some(state)) => {
                                saw_snapshot = true;
                                rebuild_tray_menu(&app, &state);
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
