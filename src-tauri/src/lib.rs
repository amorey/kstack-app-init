// Surface accidental `unwrap()` in runtime code as a lint warning. Tests opt
// out individually via `#[allow(clippy::unwrap_used)]` on the `mod tests`.
#![warn(clippy::unwrap_used)]

use tauri::{
    menu::{MenuBuilder, MenuItemBuilder},
    tray::TrayIconBuilder,
    Emitter, Manager, RunEvent,
};
use tauri_plugin_log::{Target, TargetKind};

pub mod auth;
pub mod deep_link;
pub mod gpu;
pub mod sidecar;
pub mod windows;

/// Single env var controls verbosity for both the Rust host and the Go
/// sidecar (mirrored in `sidecar/internal/logging.ParseLevel`).
pub(crate) const KSTACK_LOG_ENV: &str = "KSTACK_LOG";

/// Base URL of the kstack cloud (e.g. `https://api.kstack.sh`).
/// Forwarded through to the sidecar (which is the only thing that talks
/// to the cloud); the sidecar appends `/graphql` itself. Unset means
/// "fall back to the compiled-in default" — see `sidecar/main.go`.
pub(crate) const KSTACK_CLOUD_URL_ENV: &str = "KSTACK_CLOUD_URL";

fn host_log_level() -> log::LevelFilter {
    match std::env::var(KSTACK_LOG_ENV)
        .unwrap_or_default()
        .to_lowercase()
        .as_str()
    {
        "debug" => log::LevelFilter::Debug,
        "warn" | "warning" => log::LevelFilter::Warn,
        "error" => log::LevelFilter::Error,
        _ => log::LevelFilter::Info,
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // Must run before the builder creates the WebKitGTK window: on
    // Linux, decide whether to force software rendering so a missing /
    // broken EGL display degrades gracefully instead of aborting the
    // process. No-op on macOS/Windows.
    gpu::apply_webkit_compositing_fallback();

    let mut builder = tauri::Builder::default();

    // Single-instance must be registered first: when a second launch happens
    // its process exits inside this plugin's init before any other plugin
    // (notably the sidecar) gets a chance to bind shared resources. The
    // callback fires in the *original* process with the new launch's argv,
    // which is also where deep-link URLs will arrive once that plugin lands.
    #[cfg(desktop)]
    {
        builder = builder.plugin(tauri_plugin_single_instance::init(|app, _argv, _cwd| {
            windows::show_or_open(app);
        }));
    }

    builder
        // Persists per-window size/position/maximized state across launches,
        // keyed by window label. Auto-attaches save hooks to every window
        // (including dynamically created `window-N` ones); restoration of
        // dynamic windows is explicit — see `windows::open_new`.
        .plugin(tauri_plugin_window_state::Builder::default().build())
        .plugin(tauri_plugin_deep_link::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_notification::init())
        .plugin(
            tauri_plugin_log::Builder::new()
                .targets([
                    Target::new(TargetKind::LogDir {
                        file_name: Some("kstack".into()),
                    }),
                    Target::new(TargetKind::Stdout),
                ])
                .level(host_log_level())
                .build(),
        )
        .menu(windows::build_menu)
        .on_menu_event(windows::on_menu_event)
        .invoke_handler(tauri::generate_handler![
            sidecar::command::graphql_query,
            sidecar::command::graphql_subscribe,
            sidecar::command::graphql_unsubscribe,
            auth::commands::auth_login,
            auth::commands::auth_logout,
            auth::commands::auth_status,
            auth::commands::auth_access_token,
        ])
        .setup(|app| {
            app.manage(sidecar::command::Operations::default());
            sidecar::spawn(app)?;
            deep_link::init(app.handle())?;

            // Fan auth session changes out to every window so a logout
            // (or login) in one window updates the others. Subscribed
            // here, before the restore task below can bump credentials,
            // so no change is missed.
            auth::broadcast::spawn_session_broadcaster(app.handle());

            // Silent restore from the keychain runs off the critical path.
            // The event payload carries the resolved `Status` so the
            // renderer doesn't need a follow-up `auth_status` round-trip.
            let restore_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                if let Err(e) = auth::AUTH.try_restore().await {
                    log::warn!("auth: restore failed: {e}");
                }
                let status = auth::AUTH.status().await;
                if let Err(e) = restore_handle.emit(auth::SESSION_RESOLVED_EVENT, status) {
                    log::warn!("auth: emit restore event: {e}");
                }
                // Host wake (OS power-resume / network-change): drives a
                // /control/resync poster and feeds the credential pusher
                // so a restored session — and recovery after suspend —
                // reaches the sidecar promptly.
                let wake_rx = sidecar::spawn_wake(&restore_handle);
                sidecar::spawn_credential_pusher(&restore_handle, wake_rx);
            });

            // Tray icon with Show / Quit menu. The icon is reused from the
            // bundled app icon so we don't need to ship a separate asset.
            let show = MenuItemBuilder::with_id("show", "Show kstack").build(app)?;
            let quit = MenuItemBuilder::with_id("quit", "Quit kstack").build(app)?;
            let menu = MenuBuilder::new(app).items(&[&show, &quit]).build()?;

            TrayIconBuilder::with_id("main")
                .icon(
                    app.default_window_icon().cloned().ok_or(
                        "missing default window icon — set bundle.icon in tauri.conf.json",
                    )?,
                )
                .menu(&menu)
                .show_menu_on_left_click(true)
                .on_menu_event(|app, event| match event.id().as_ref() {
                    "show" => windows::show_or_open(app),
                    "quit" => app.exit(0),
                    _ => {}
                })
                .build(app)?;

            Ok(())
        })
        .on_window_event(windows::on_close_requested)
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app, event| match event {
            // Keep the process alive after the last window closes — exit
            // only happens via the tray's Quit item (or Cmd+Q on macOS),
            // which calls `app.exit(code)` with an explicit code.
            RunEvent::ExitRequested { api, code, .. } if code.is_none() => {
                api.prevent_exit();
            }
            // macOS: clicking the dock icon when no window is visible
            // should reopen — or open — a window.
            #[cfg(target_os = "macos")]
            RunEvent::Reopen {
                has_visible_windows: false,
                ..
            } => windows::show_or_open(app),
            RunEvent::Exit => sidecar::shutdown(app),
            _ => {}
        });
}
