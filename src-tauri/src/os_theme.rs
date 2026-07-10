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

//! Reads the OS's current color scheme.
//!
//! `window_manager` needs a concrete light/dark answer *before* the first
//! window exists, to paint its native background at creation. Tauri only
//! exposes a theme on an already-built window, so the OS is queried directly.
//!
//! This resolves the `system` color-scheme preference (see
//! [`crate::host_file::ColorSchemePreference`]); an explicit `light`/`dark`
//! preference never consults it. Compiled only on the opaque platforms — Linux
//! windows are transparent and take no native background at all.
//!
//! A wrong answer costs one frame of the wrong (but never white) background,
//! so every failure path here degrades to light rather than propagating.

/// Whether the OS is currently in dark mode.
///
/// The global-domain `AppleInterfaceStyle` default is absent in light mode and
/// the string `"Dark"` in dark mode — the flag the OS itself keys on. Read via
/// `NSUserDefaults` rather than `NSAppearance` so it works before `NSApp` is
/// initialized, which is when the first window is built.
///
/// `stringForKey` returns `None` both when the key is absent (ordinary light
/// mode) and when the defaults domain is unreadable or the value isn't a
/// string — all of those deliberately read as light, per the module policy.
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
/// `AppsUseLightTheme` is the per-user app-theme flag Windows itself keys on:
/// `0` means dark, `1` means light. A missing value (older Windows, or a
/// profile that never set it) means light.
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
    // `size` matches. A null `pdwtype` means "don't report the type".
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
