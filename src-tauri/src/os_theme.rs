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

//! Reads the OS's current color scheme, to resolve the `system` color-scheme
//! preference (see [`crate::host_file::ColorSchemePreference`]) into a concrete
//! light/dark background *before* the first window exists — Tauri only exposes a
//! theme on an already-built window. Compiled only on the opaque platforms (Linux
//! windows are transparent). Every failure path degrades to light: a wrong answer
//! costs one frame of the wrong — but never white — background.

/// Whether the OS is currently in dark mode.
///
/// The global-domain `AppleInterfaceStyle` default is absent in light mode and
/// `"Dark"` in dark mode. Read via `NSUserDefaults` rather than `NSAppearance` so
/// it works before `NSApp` exists (when the first window is built). A `None` from
/// `stringForKey` — key absent, domain unreadable, or non-string — reads as light.
#[cfg(target_os = "macos")]
pub fn prefers_dark() -> bool {
    use objc2_foundation::{NSString, NSUserDefaults};

    let defaults = NSUserDefaults::standardUserDefaults();
    let key = NSString::from_str("AppleInterfaceStyle");
    defaults
        .stringForKey(&key)
        .is_some_and(|style| style.to_string().eq_ignore_ascii_case("dark"))
}

/// Whether the OS is currently in dark mode.
///
/// `AppsUseLightTheme` is the per-user app-theme flag: `0` dark, `1` light. A
/// missing value (older Windows, or a profile that never set it) reads as light.
#[cfg(target_os = "windows")]
pub fn prefers_dark() -> bool {
    use std::ffi::c_void;

    use windows_sys::Win32::Foundation::ERROR_SUCCESS;
    use windows_sys::Win32::System::Registry::{RegGetValueW, HKEY_CURRENT_USER, RRF_RT_REG_DWORD};

    /// NUL-terminated UTF-16, as the `W` registry APIs expect.
    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    let subkey = wide(r"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize");
    let value = wide("AppsUseLightTheme");
    let mut data: u32 = 0;
    let mut size = std::mem::size_of::<u32>() as u32;

    // SAFETY: `subkey`/`value` are NUL-terminated and outlive the call;
    // `RRF_RT_REG_DWORD` constrains the write to `pvdata` to a `DWORD`, which
    // `size` matches; null `pdwtype` skips the type report.
    let status = unsafe {
        RegGetValueW(
            HKEY_CURRENT_USER,
            subkey.as_ptr(),
            value.as_ptr(),
            RRF_RT_REG_DWORD,
            std::ptr::null_mut(),
            std::ptr::from_mut(&mut data).cast::<c_void>(),
            &mut size,
        )
    };

    status == ERROR_SUCCESS && data == 0
}
