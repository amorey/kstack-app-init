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

//! Window lifecycle for the Tauri host.
//!
//! The `"main"` window is declared statically in `tauri.conf.json`, so Tauri
//! creates it at startup. After that, [`WindowManager`] owns all window
//! operations: recreating `"main"` if the user closed it, and creating
//! additional windows with unique labels.
//!
//! Closing the last window does not exit the process (see `lib.rs`), so the
//! main window may be absent while the app is still running — every entry point
//! here is written to recreate it on demand.

use std::sync::atomic::{AtomicU64, Ordering};

use tauri::{AppHandle, Manager, WebviewUrl, WebviewWindow, WebviewWindowBuilder};

use crate::error::Result;

/// Label of the statically-declared primary window.
const MAIN_LABEL: &str = "main";

/// Title shown on every window the host creates.
const WINDOW_TITLE: &str = "kstack";

// The traffic-light geometry below is only *used* on macOS (via `build_window`),
// but is compiled and unit-tested on every platform — only the native builder
// call is `#[cfg]`-gated — so the arithmetic stays covered on CI. `allow(dead_code)`
// silences the off-macOS "unused" warning without dropping the items.

/// Logical-pixel gap the webview's floating sidebar leaves between its card and
/// the window edge (Tailwind `p-2`), i.e. the card's uniform offset from both
/// the top and left window edges. Kept in sync with the sidebar's spacing in
/// `src/components/widgets/app-sidebar.tsx`.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
const SIDEBAR_GAP: f64 = 8.0;

/// Height (logical px) of the sidebar's title-bar header row (`h-11`), the strip
/// the macOS traffic lights sit in. Kept in sync with `app-sidebar.tsx`.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
const TITLE_BAR_HEIGHT: f64 = 44.0;

/// Horizontal inset (logical px) from the sidebar card's left edge to the first
/// traffic-light button. The webview derives `MAC_TOGGLE_LEFT` (in
/// `app-sidebar.tsx`) from this and `SIDEBAR_GAP` to place the sidebar toggle
/// just past the lights — bump this and check that offset still clears them.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
const TRAFFIC_LIGHT_LEFT_INSET: f64 = 12.0;

/// Logical `(x, y)` position for the macOS traffic lights so they sit inside the
/// sidebar's title-bar header instead of the window's default top-left corner.
///
/// `gap` is the sidebar card's uniform offset from the window edges and
/// `header_height` is the title-bar row height. The lights are inset from the
/// card's left edge and centered vertically in the header. This is the pure
/// arithmetic behind the `traffic_light_position` builder call in
/// [`build_window`]; the exact insets track the webview sizing in
/// `app-sidebar.tsx` and may want visual tuning on macOS.
///
/// [`build_window`]: WindowManager::build_window
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn traffic_light_position(gap: f64, header_height: f64) -> (f64, f64) {
    /// macOS traffic-light button diameter (logical px).
    const BUTTON_DIAMETER: f64 = 12.0;
    let x = gap + TRAFFIC_LIGHT_LEFT_INSET;
    let y = gap + (header_height - BUTTON_DIAMETER) / 2.0;
    (x, y)
}

/// Owns window creation and focus for the host.
#[derive(Default)]
pub struct WindowManager {
    /// Monotonic counter feeding unique labels for windows created via
    /// [`new_window`].
    ///
    /// [`new_window`]: WindowManager::new_window
    next_id: AtomicU64,
}

impl WindowManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a fresh window with a unique label (`window-1`, `window-2`, …).
    ///
    /// Always creates a new window — it never reuses an existing one. The
    /// unique label keeps Tauri's window registry unambiguous; the OS-visible
    /// title is the same [`WINDOW_TITLE`] for every window.
    pub fn new_window(&self, app: &AppHandle) -> Result<WebviewWindow> {
        let label = self.next_label();
        self.build_window(app, &label, WINDOW_TITLE)
    }

    /// Produce the next unique window label (`window-1`, `window-2`, …).
    ///
    /// Each call advances a monotonic counter, so a label is never reused for
    /// the lifetime of the process, even across concurrent callers.
    fn next_label(&self) -> String {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("window-{id}")
    }

    /// Apply platform title-bar chrome to the statically-declared `"main"`
    /// window.
    ///
    /// Windows created via [`build_window`] get their chrome from the builder,
    /// but Tauri builds the initial `"main"` window from the static config
    /// before the setup hook runs. On macOS `tauri.macos.conf.json` gives it the
    /// Overlay title bar + traffic-light position, which already match; the base
    /// `tauri.conf.json` (Linux) and `tauri.windows.conf.json` (Windows) both
    /// leave native decorations on, so they're dropped here to make the first
    /// window frameless like the rest, letting the webview's own controls take
    /// over.
    ///
    /// [`build_window`]: WindowManager::build_window
    pub fn apply_main_window_chrome(&self, app: &AppHandle) -> Result<()> {
        #[cfg(not(target_os = "macos"))]
        if let Some(window) = app.get_webview_window(MAIN_LABEL) {
            window.set_decorations(false)?;
        }
        // On macOS the static config carries the Overlay chrome — nothing to do.
        #[cfg(target_os = "macos")]
        let _ = app;
        Ok(())
    }

    /// Reveal the main window: recreate it if the user closed it, unminimize it
    /// if minimized, then show and focus it.
    pub fn show_main_window(&self, app: &AppHandle) -> Result<()> {
        let window = match app.get_webview_window(MAIN_LABEL) {
            Some(window) => window,
            None => self.build_window(app, MAIN_LABEL, WINDOW_TITLE)?,
        };

        // A minimized window won't surface on show()/set_focus() alone.
        if window.is_minimized().unwrap_or(false) {
            window.unminimize()?;
        }
        window.show()?;
        window.set_focus()?;

        Ok(())
    }

    /// Build a webview window pointing at the app's frontend entry point.
    ///
    /// The webview's floating sidebar doubles as the window title bar. On macOS
    /// that means an Overlay title bar (native traffic lights kept, title text
    /// hidden) with the lights repositioned into the sidebar header; on
    /// Linux/Windows the window is frameless and the webview draws its own
    /// controls (`WindowControls`). `traffic_light_position` requires the
    /// Overlay style with decorations left on, so macOS keeps decorations.
    ///
    /// Linux additionally makes the window transparent so the webview's
    /// `WindowFrame` can paint its own border + shadow into a gutter; Windows
    /// stays opaque and lets DWM draw the borderless window's native shadow.
    fn build_window(&self, app: &AppHandle, label: &str, title: &str) -> Result<WebviewWindow> {
        let mut builder = WebviewWindowBuilder::new(app, label, WebviewUrl::default())
            .title(title)
            .inner_size(800.0, 600.0)
            // Floor the window so the layout never renders below its narrowest design.
            .min_inner_size(600.0, 400.0);

        #[cfg(target_os = "macos")]
        {
            let (x, y) = traffic_light_position(SIDEBAR_GAP, TITLE_BAR_HEIGHT);
            builder = builder
                .title_bar_style(tauri::TitleBarStyle::Overlay)
                .hidden_title(true)
                .traffic_light_position(tauri::LogicalPosition::new(x, y));
        }

        // Both non-macOS platforms are frameless — the webview's sidebar is the
        // title bar and draws its own `WindowControls` (mirrors the same
        // `not(macos)` decision in `apply_main_window_chrome`).
        #[cfg(not(target_os = "macos"))]
        {
            builder = builder.decorations(false);
        }

        // Linux additionally goes transparent so the webview's `WindowFrame` can
        // paint its own rounded border + outer shadow into a gutter at the window
        // edge (the OS draws neither for a decoration-less window). Windows stays
        // opaque: DWM already draws a borderless window's own shadow (and rounds
        // the corners on Win11), so a second custom one would double-stack.
        #[cfg(target_os = "linux")]
        {
            builder = builder.transparent(true);
        }

        let window = builder.build()?;

        // The `debug-prod` feature compiles the inspector into a release build
        // so production behavior (e.g. the bundled CSP) can be examined. Never
        // enabled in shipped builds — see the feature note in `Cargo.toml`.
        #[cfg(feature = "debug-prod")]
        window.open_devtools();

        Ok(window)
    }
}

#[cfg(test)]
mod tests {
    use super::WindowManager;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn labels_start_at_one() {
        let wm = WindowManager::new();
        assert_eq!(wm.next_label(), "window-1");
    }

    #[test]
    fn traffic_lights_center_vertically_in_the_header() {
        // The lights sit `TRAFFIC_LIGHT_LEFT_INSET` in from the card's left edge
        // and centered in the header: (header - 12px button) / 2 below its top.
        let (x, y) = super::traffic_light_position(super::SIDEBAR_GAP, super::TITLE_BAR_HEIGHT);
        assert_eq!(x, super::SIDEBAR_GAP + super::TRAFFIC_LIGHT_LEFT_INSET);
        assert_eq!(
            y,
            super::SIDEBAR_GAP + (super::TITLE_BAR_HEIGHT - 12.0) / 2.0
        );
    }

    #[test]
    fn config_traffic_light_position_matches_computed_value() {
        // The statically-declared `main` window carries the traffic-light
        // position as a JSON literal (baked in before Rust runs, so it can't
        // read the constants). Pin the literal to the computed value so the two
        // can't silently drift when the sidebar geometry is retuned. The
        // literal lives in the macOS config overlay (traffic lights are
        // macOS-only; the base config is the transparent Linux/Windows window).
        let (x, y) = super::traffic_light_position(super::SIDEBAR_GAP, super::TITLE_BAR_HEIGHT);
        let config: serde_json::Value =
            serde_json::from_str(include_str!("../tauri.macos.conf.json"))
                .expect("valid config JSON");
        let pos = &config["app"]["windows"][0]["trafficLightPosition"];
        assert_eq!(pos["x"].as_f64(), Some(x));
        assert_eq!(pos["y"].as_f64(), Some(y));
    }

    #[test]
    fn labels_increase_monotonically() {
        let wm = WindowManager::new();
        assert_eq!(wm.next_label(), "window-1");
        assert_eq!(wm.next_label(), "window-2");
        assert_eq!(wm.next_label(), "window-3");
    }

    #[test]
    fn each_manager_has_its_own_counter() {
        let a = WindowManager::new();
        let b = WindowManager::new();
        assert_eq!(a.next_label(), "window-1");
        assert_eq!(a.next_label(), "window-2");
        // `b` is independent — it is not advanced by calls on `a`.
        assert_eq!(b.next_label(), "window-1");
    }

    #[test]
    fn concurrent_labels_are_unique() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 250;

        let wm = Arc::new(WindowManager::new());
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let wm = Arc::clone(&wm);
                thread::spawn(move || (0..PER_THREAD).map(|_| wm.next_label()).collect::<Vec<_>>())
            })
            .collect();

        let mut seen = HashSet::new();
        for handle in handles {
            for label in handle.join().expect("worker thread panicked") {
                assert!(
                    seen.insert(label.clone()),
                    "label {label} was handed out twice"
                );
            }
        }
        assert_eq!(seen.len(), THREADS * PER_THREAD);
    }
}
