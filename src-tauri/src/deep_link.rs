//! Deep-link URL handling. Scheme `kstack://` is declared under
//! `plugins.deep-link.desktop.schemes` in `tauri.conf.json` (which feeds the
//! macOS Info.plist and the Linux `.desktop` MimeType at bundle time).
//!
//! Three delivery paths all converge on this module's `on_open_url` listener:
//! 1. **Cold start with a URL** — the OS launches us with the URL; the
//!    plugin replays it as an `on_open_url` event after `setup`.
//! 2. **macOS hot delivery** — the OS sends an Apple Event to the running
//!    process, which the plugin turns into the same event.
//! 3. **Windows/Linux hot delivery** — the OS launches a fresh process; the
//!    single-instance plugin's `deep-link` feature forwards the URL into
//!    the running process's event stream rather than into the argv callback.
//!
//! Routing logic (e.g. `kstack://oauth/callback?...`) is intentionally
//! stubbed — TODO 9 (OAuth to kstack-cloud) is the first real consumer.

use tauri::{AppHandle, Runtime};
use tauri_plugin_deep_link::DeepLinkExt;

use crate::windows;

pub fn init<R: Runtime>(app: &AppHandle<R>) -> tauri::Result<()> {
    // Runtime scheme registration. On Windows the installer writes the
    // registry keys, but in `tauri dev` no installer ran — so we register
    // here. On Linux scheme association via `.desktop` is fragile; calling
    // `register` writes a per-user `.desktop` file that survives. On macOS
    // registration is purely via `Info.plist`, and `register` is a no-op.
    #[cfg(any(target_os = "linux", windows))]
    if let Err(err) = app.deep_link().register("kstack") {
        // Non-fatal — deep links may still work if a previous run (or the
        // installer) already registered the scheme. Worth logging though,
        // because if this fails repeatedly the OS has no path to us.
        log::warn!("deep-link: could not register `kstack` scheme: {err}");
    }

    let handle = app.clone();
    app.deep_link().on_open_url(move |event| {
        for url in event.urls() {
            // Stub: bring a window forward and log. Real routing (OAuth
            // callback, future cluster-share links, etc.) lands here.
            log::info!("deep-link received: {url}");
            windows::show_or_open(&handle);
        }
    });

    Ok(())
}
