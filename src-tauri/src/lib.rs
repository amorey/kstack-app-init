// Surface accidental `unwrap()` in runtime code as a lint warning. Tests opt
// out individually via `#[allow(clippy::unwrap_used)]` on the `mod tests`.
#![warn(clippy::unwrap_used)]

use tauri::{
    menu::{MenuBuilder, MenuItemBuilder},
    tray::TrayIconBuilder,
    Manager, RunEvent, WindowEvent,
};
#[cfg(target_os = "windows")]
use tauri_plugin_notification::NotificationExt;
use tauri_plugin_log::{Target, TargetKind};

pub mod sidecar;

/// Single env var controls verbosity for both the Rust host and the Go
/// sidecar (mirrored in `sidecar/internal/logging.ParseLevel`).
pub(crate) const KSTACK_LOG_ENV: &str = "KSTACK_LOG";

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
    tauri::Builder::default()
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
        .invoke_handler(tauri::generate_handler![
            sidecar::command::graphql_query,
            sidecar::command::graphql_subscribe,
            sidecar::command::graphql_unsubscribe,
        ])
        .setup(|app| {
            app.manage(sidecar::command::Operations::default());
            sidecar::spawn(app)?;

            // Tray icon with Show / Quit menu. The icon is reused from the
            // bundled app icon so we don't need to ship a separate asset.
            let show = MenuItemBuilder::with_id("show", "Show kstack").build(app)?;
            let quit = MenuItemBuilder::with_id("quit", "Quit kstack").build(app)?;
            let menu = MenuBuilder::new(app).items(&[&show, &quit]).build()?;

            TrayIconBuilder::with_id("main")
                .icon(app.default_window_icon().cloned().ok_or(
                    "missing default window icon — set bundle.icon in tauri.conf.json",
                )?)
                .menu(&menu)
                .show_menu_on_left_click(true)
                .on_menu_event(|app, event| match event.id().as_ref() {
                    "show" => show_main_window(app),
                    "quit" => app.exit(0),
                    _ => {}
                })
                .build(app)?;

            Ok(())
        })
        .on_window_event(|window, event| {
            // Closing the window should not quit the app — hide it instead
            // so the process keeps running until the user picks Quit from
            // the tray (or Cmd+Q on macOS). Same behavior on all platforms.
            if let WindowEvent::CloseRequested { api, .. } = event {
                let _ = window.hide();
                api.prevent_close();
                #[cfg(target_os = "windows")]
                notify_first_close(window.app_handle());
            }
        })
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
            // should reopen the main window.
            #[cfg(target_os = "macos")]
            RunEvent::Reopen {
                has_visible_windows: false,
                ..
            } => show_main_window(app),
            RunEvent::Exit => sidecar::shutdown(app),
            _ => {}
        });
}

/// On Windows, show a one-time toast the first time the user closes the
/// window so they understand the app is still running in the tray. The
/// marker is a zero-byte file in the app config dir; deleting it re-arms
/// the notification (handy for QA).
#[cfg(target_os = "windows")]
fn notify_first_close<R: tauri::Runtime>(app: &tauri::AppHandle<R>) {
    let Ok(config_dir) = app.path().app_config_dir() else {
        return;
    };
    let marker = config_dir.join(".close-to-tray-notified");
    if marker.exists() {
        return;
    }
    if let Err(err) = std::fs::create_dir_all(&config_dir) {
        log::warn!("close-to-tray: could not create config dir: {err}");
        return;
    }
    if let Err(err) = std::fs::write(&marker, b"") {
        // If we can't persist the marker we'd notify on every close, which
        // is worse than skipping — bail.
        log::warn!("close-to-tray: could not write marker: {err}");
        return;
    }
    if let Err(err) = app
        .notification()
        .builder()
        .title("kstack is still running")
        .body("Use the tray icon to reopen or quit.")
        .show()
    {
        log::warn!("close-to-tray: could not show notification: {err}");
    }
}

/// Show, unminimize, and focus the main window. Used by the tray menu and
/// the macOS dock-reopen handler.
fn show_main_window<R: tauri::Runtime>(app: &tauri::AppHandle<R>) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.unminimize();
        let _ = window.show();
        let _ = window.set_focus();
    }
}
