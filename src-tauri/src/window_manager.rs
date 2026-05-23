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
    fn build_window(&self, app: &AppHandle, label: &str, title: &str) -> Result<WebviewWindow> {
        let window = WebviewWindowBuilder::new(app, label, WebviewUrl::default())
            .title(title)
            .inner_size(800.0, 600.0)
            .build()?;

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
