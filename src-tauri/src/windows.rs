//! Window lifecycle: spawning additional windows, the app menu that drives
//! them, and the close-to-tray policy that keeps the process alive when the
//! last visible window closes.
//!
//! UX model (browser-like, "option A"):
//! * `Cmd/Ctrl+N` (or File → New Window) opens an additional window.
//! * Closing one of many windows destroys it.
//! * Closing the last *visible* window hides it to the tray instead — the
//!   process keeps running so the sidecar (and eventually the agent loop)
//!   stays alive in the background.

use std::sync::atomic::{AtomicU32, Ordering};

use tauri::{
    menu::{Menu, MenuBuilder, MenuEvent, MenuItemBuilder, PredefinedMenuItem, SubmenuBuilder},
    AppHandle, Manager, Runtime, WebviewUrl, WebviewWindow, WebviewWindowBuilder, Window,
    WindowEvent,
};
#[cfg(target_os = "windows")]
use tauri_plugin_notification::NotificationExt;

/// Label of the window declared in `tauri.conf.json` — created automatically
/// at startup. Additional windows use `window-N` from `WINDOW_COUNTER`.
pub const MAIN_WINDOW_LABEL: &str = "main";

/// Monotonic per-process counter for additional window labels. Starts at 2
/// (1 is conceptually "main") and never reuses values within a session, so
/// a closed window's saved geometry stays parked under its old label and a
/// freshly opened window gets a clean slate.
static WINDOW_COUNTER: AtomicU32 = AtomicU32::new(2);

const MENU_ID_NEW_WINDOW: &str = "new-window";

/// Open a new window pointing at the same frontend entry as the main window.
/// Geometry mirrors the `tauri.conf.json` defaults so first-open feels
/// consistent; later this is what `tauri-plugin-window-state` will override
/// on a per-label basis.
pub fn open_new<R: Runtime>(app: &AppHandle<R>) -> tauri::Result<WebviewWindow<R>> {
    let n = WINDOW_COUNTER.fetch_add(1, Ordering::SeqCst);
    let label = format!("window-{n}");
    WebviewWindowBuilder::new(app, &label, WebviewUrl::App("index.html".into()))
        .title("kstack-app")
        .inner_size(800.0, 600.0)
        .build()
}

/// Bring an existing window forward, or open one if none exist. Used by the
/// tray's "Show kstack" item, the macOS dock-reopen event, and the
/// single-instance callback when a second launch is folded into this one.
///
/// Preference order: focus `main` if it's still alive (the most common case);
/// otherwise focus any other surviving window; otherwise open a fresh one.
pub fn show_or_open<R: Runtime>(app: &AppHandle<R>) {
    if let Some(w) = app.get_webview_window(MAIN_WINDOW_LABEL) {
        focus(&w);
        return;
    }
    if let Some(w) = app.webview_windows().into_values().next() {
        focus(&w);
        return;
    }
    if let Err(err) = open_new(app) {
        log::warn!("show_or_open: could not open a new window: {err}");
    }
}

fn focus<R: Runtime>(window: &WebviewWindow<R>) {
    let _ = window.unminimize();
    let _ = window.show();
    let _ = window.set_focus();
}

/// Intercept window close: if it's the last visible window, hide it instead
/// of destroying it (close-to-tray). Otherwise let the close proceed so
/// closing one of many windows behaves like a browser tab.
pub fn on_close_requested<R: Runtime>(window: &Window<R>, event: &WindowEvent) {
    let WindowEvent::CloseRequested { api, .. } = event else {
        return;
    };

    let app = window.app_handle();
    // The window being closed still reports `is_visible() == true` here, so
    // a count of 1 means *this* is the last visible window.
    let visible_count = app
        .webview_windows()
        .values()
        .filter(|w| w.is_visible().unwrap_or(false))
        .count();

    if visible_count > 1 {
        // One of many — let it close normally.
        return;
    }

    let _ = window.hide();
    api.prevent_close();
    #[cfg(target_os = "windows")]
    notify_first_close(app);
}

/// Build the application menu: a File submenu carrying the "New Window"
/// accelerator (Cmd/Ctrl+N) plus an Edit submenu that wires up the standard
/// clipboard predefined items — without those, Cmd+C/V/X don't work in text
/// inputs on macOS because Tauri 2 doesn't install them by default. On
/// macOS we additionally prepend the conventional app submenu (About, Hide,
/// Quit). On Windows/Linux the menu is attached per-window automatically.
pub fn build_menu<R: Runtime>(app: &AppHandle<R>) -> tauri::Result<Menu<R>> {
    let new_window = MenuItemBuilder::with_id(MENU_ID_NEW_WINDOW, "New Window")
        .accelerator("CmdOrCtrl+N")
        .build(app)?;

    let file = SubmenuBuilder::new(app, "File")
        .item(&new_window)
        .separator()
        .item(&PredefinedMenuItem::close_window(app, None)?)
        .build()?;

    let edit = SubmenuBuilder::new(app, "Edit")
        .item(&PredefinedMenuItem::undo(app, None)?)
        .item(&PredefinedMenuItem::redo(app, None)?)
        .separator()
        .item(&PredefinedMenuItem::cut(app, None)?)
        .item(&PredefinedMenuItem::copy(app, None)?)
        .item(&PredefinedMenuItem::paste(app, None)?)
        .item(&PredefinedMenuItem::select_all(app, None)?)
        .build()?;

    let mut builder = MenuBuilder::new(app);

    #[cfg(target_os = "macos")]
    {
        let app_submenu = SubmenuBuilder::new(app, "kstack")
            .item(&PredefinedMenuItem::about(app, None, None)?)
            .separator()
            .item(&PredefinedMenuItem::services(app, None)?)
            .separator()
            .item(&PredefinedMenuItem::hide(app, None)?)
            .item(&PredefinedMenuItem::hide_others(app, None)?)
            .item(&PredefinedMenuItem::show_all(app, None)?)
            .separator()
            .item(&PredefinedMenuItem::quit(app, None)?)
            .build()?;
        builder = builder.item(&app_submenu);
    }

    builder.item(&file).item(&edit).build()
}

/// Dispatch a menu event from the app menu. Tray menu events are routed
/// separately by `TrayIconBuilder::on_menu_event` and don't reach here.
pub fn on_menu_event<R: Runtime>(app: &AppHandle<R>, event: MenuEvent) {
    if event.id().as_ref() == MENU_ID_NEW_WINDOW {
        if let Err(err) = open_new(app) {
            log::warn!("menu: could not open new window: {err}");
        }
    }
}

/// Windows-only: show a one-time toast the first time the user closes the
/// last visible window so they understand the app is still running in the
/// tray. The marker is a zero-byte file in the app config dir; deleting it
/// re-arms the notification (handy for QA).
#[cfg(target_os = "windows")]
fn notify_first_close<R: Runtime>(app: &AppHandle<R>) {
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
