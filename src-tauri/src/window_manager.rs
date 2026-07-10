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
//! Every window — including the first `"main"` window, created in `lib.rs`'s
//! `setup` hook — is built in code by [`WindowManager`], so all window chrome
//! lives in one place (`build_window`); `tauri.conf.json` declares no windows.
//! Beyond creation, [`WindowManager`] recreates `"main"` if the user closed it
//! and creates additional windows with unique labels.
//!
//! **Windows are visible from creation and painted with the app's color scheme
//! at t0** — the native surface carries the resolved `--background` color (see
//! [`background_color_for`]) before the webview has drawn anything, so the very
//! first frame is themed. Nothing is deferred and the webview plays no part:
//! there is no reveal command and no hidden-then-shown dance.
//!
//! What makes that work is `tauri/macos-private-api`, which enables
//! `wry/transparent` and so clears `WKWebView`'s opaque white backing (the
//! private `drawsBackground` key). Without it the webview paints white over the
//! window background until its first frame — the launch flash — no matter what
//! the native layer is set to. Because that backing is cleared, the *document*
//! must be opaque: `index.css` paints `html` from the same `--background` token
//! on the opaque platforms, and `index.html`'s inline script applies the color
//! scheme before any page script runs, so the webview's first paint lands on
//! the same color the native surface is already showing.
//!
//! Closing the last window does not exit the process (see `lib.rs`), so the
//! main window may be absent while the app is still running — every entry point
//! here is written to recreate it on demand.

use std::sync::atomic::{AtomicU64, Ordering};

use tauri::window::Color;
use tauri::{AppHandle, Manager, WebviewUrl, WebviewWindow, WebviewWindowBuilder};

use crate::error::Result;
use crate::host_file::{self, ColorSchemePreference};

/// Label of the primary window (created at startup by `lib.rs`'s `setup`).
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

/// The `--background` token color for each resolved scheme, as `@kubetail/ui`
/// defines it (`color-white` / `color-zinc-950`) and `index.css` paints `html`
/// with. Nothing pins these to that package's tokens — the values live in its
/// shipped CSS, not in this repo — but the webview paints the exact token over
/// this the moment it draws, so they only ever need to match to the eye.
#[cfg_attr(target_os = "linux", allow(dead_code))]
const LIGHT_BACKGROUND: Color = Color(255, 255, 255, 255);
#[cfg_attr(target_os = "linux", allow(dead_code))]
const DARK_BACKGROUND: Color = Color(9, 9, 11, 255);

/// The native window background for a persisted color-scheme preference.
///
/// This is what prevents the launch flash: it is the color on screen from the
/// moment the window appears until the webview's first paint, and the color the
/// native surface shows whenever it is visible on its own thereafter (chiefly
/// live resize, where the OS stretches the window ahead of a repaint).
///
/// Always a concrete color, never "leave it to the OS". `WebviewWindowBuilder::
/// background_color` drives *two* layers — the window and the webview — and wry
/// only clears the webview's opaque white backing when a color is set. Passing
/// nothing would restore the very flash this exists to prevent, so `system`
/// resolves against `os_prefers_dark` (from `os_theme`) rather than opting out.
/// The cost is that a scheme change while the app is closed is picked up at the
/// next launch, which is exactly when this is read.
///
/// Only *called* on the opaque platforms ([`build_window`] skips it on
/// transparent Linux), but compiled and unit-tested everywhere so the mapping
/// stays covered on CI — same pattern as the traffic-light geometry above.
///
/// [`build_window`]: WindowManager::build_window
#[cfg_attr(target_os = "linux", allow(dead_code))]
fn background_color_for(preference: Option<ColorSchemePreference>, os_prefers_dark: bool) -> Color {
    let dark = match preference {
        Some(ColorSchemePreference::Light) => false,
        Some(ColorSchemePreference::Dark) => true,
        // `system` — and a file that predates the setting — follow the OS.
        Some(ColorSchemePreference::System) | None => os_prefers_dark,
    };
    if dark {
        DARK_BACKGROUND
    } else {
        LIGHT_BACKGROUND
    }
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
        self.build_window(app, &label)
    }

    /// Produce the next unique window label (`window-1`, `window-2`, …).
    ///
    /// Each call advances a monotonic counter, so a label is never reused for
    /// the lifetime of the process, even across concurrent callers.
    fn next_label(&self) -> String {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("window-{id}")
    }

    /// Surface the main window: recreate it if the user closed it, unminimize it
    /// if minimized, then show and focus it.
    pub fn show_main_window(&self, app: &AppHandle) -> Result<()> {
        let window = match app.get_webview_window(MAIN_LABEL) {
            Some(window) => window,
            None => self.build_window(app, MAIN_LABEL)?,
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
    ///
    /// On the opaque platforms the window is painted with the app's resolved
    /// color scheme at creation (see [`background_color_for`]), so its first
    /// frame is themed without ever being hidden.
    fn build_window(&self, app: &AppHandle, label: &str) -> Result<WebviewWindow> {
        let mut builder = WebviewWindowBuilder::new(app, label, WebviewUrl::default())
            .title(WINDOW_TITLE)
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
        // title bar and draws its own `WindowControls`.
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

        // `host.json` (the persisted host settings, source of truth for the
        // color-scheme preference) feeds the window's pre-paint state two ways.
        // A failed config-dir lookup degrades to defaults rather than failing
        // window creation — worst case is the one-frame flash this prevents.
        let host_file = host_file::path(app)
            .map(|path| host_file::read(&path))
            .unwrap_or_default();

        // 1. Expose the file to the webview (`window.__KSTACK_HOST__`) before
        //    any page script runs, so the first-paint inline script in
        //    `index.html` reads the preference synchronously from the same
        //    source the native background below is painted from.
        builder = builder.initialization_script(host_file::init_script(&host_file));

        // 2. On the opaque platforms, paint the native surface with the resolved
        //    scheme so the window is themed from its very first frame. This also
        //    clears the webview's opaque white backing (see the module docs), so
        //    it must be set unconditionally — `system` resolves against the OS
        //    rather than opting out. Linux is transparent, so its background
        //    must stay unset.
        #[cfg(not(target_os = "linux"))]
        {
            let color = background_color_for(
                host_file.color_scheme_preference,
                crate::os_theme::prefers_dark(),
            );
            builder = builder.background_color(color);
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
    use super::{background_color_for, WindowManager};
    use crate::host_file::ColorSchemePreference;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn explicit_preference_ignores_the_os_scheme() {
        // An explicit choice wins over the OS in both directions — that's what
        // makes the app's scheme, not the OS's, the one on screen at t0.
        for os_prefers_dark in [false, true] {
            assert_eq!(
                background_color_for(Some(ColorSchemePreference::Light), os_prefers_dark),
                super::LIGHT_BACKGROUND
            );
            assert_eq!(
                background_color_for(Some(ColorSchemePreference::Dark), os_prefers_dark),
                super::DARK_BACKGROUND
            );
        }
    }

    #[test]
    fn system_preference_follows_the_os_scheme() {
        // `system`, and a file predating the setting, defer to the OS. Never
        // `None`: wry only clears the webview's white backing when a color is
        // set, so opting out here would reinstate the launch flash.
        for preference in [Some(ColorSchemePreference::System), None] {
            assert_eq!(
                background_color_for(preference, true),
                super::DARK_BACKGROUND
            );
            assert_eq!(
                background_color_for(preference, false),
                super::LIGHT_BACKGROUND
            );
        }
    }

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
    fn config_declares_no_windows() {
        // Every window is created in code (`build_window`) so the chrome — and
        // the pre-paint background/init-script it will carry — is defined in
        // exactly one place. A window declared in the config would bypass all
        // of it, so pin the config to declaring none.
        let config: serde_json::Value =
            serde_json::from_str(include_str!("../tauri.conf.json")).expect("valid config JSON");
        assert_eq!(
            config["app"]["windows"].as_array().map(Vec::len),
            Some(0),
            "windows must be created via build_window, not declared in tauri.conf.json"
        );
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
