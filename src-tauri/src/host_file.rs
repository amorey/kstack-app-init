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

//! `host.json` (`app_config_dir()/host.json`) — the persisted-settings source
//! of truth; the webview keeps no copy. See
//! docs/adr/2026-08-09-host-json-settings.md.
//!
//! Reaches the webview two ways, both fed from one read in
//! `window_manager::build_window`: [`init_script`] (`window.__KSTACK_HOST__`,
//! injected before any page script) and the [`UPDATED_EVENT`] broadcast after
//! every write. Writes come through the `update_host_file` command.
//!
//! Versioned JSON, all-`Option` fields, partial patches ([`HostFilePatch`]) —
//! a new setting is a new `Option` field, not a new command. Defensive reads
//! (missing/corrupt → defaults), atomic writes (temp + rename).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tauri::Manager;

/// Schema version stamped into the file on every write, for future migrations.
const CURRENT_SCHEMA_VERSION: u32 = 1;

/// Broadcast to every window after `host.json` changes, payload = merged
/// [`HostFile`]. The string is hand-mirrored in the webview
/// (`src/lib/host-file.ts`).
pub const UPDATED_EVENT: &str = "host-file-updated";

/// Absolute path of `host.json` (dir created on demand by [`update`]).
pub fn path(app: &tauri::AppHandle) -> tauri::Result<PathBuf> {
    Ok(app.path().app_config_dir()?.join("host.json"))
}

/// The user's color-scheme choice; serialized lowercase to match the webview's
/// `ColorSchemePreference` strings (`theme.tsx`).
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum ColorSchemePreference {
    System,
    Light,
    Dark,
}

/// Parsed `host.json`. Settings must stay `Option` so absent fields (older
/// files, first run) default at the point of use rather than failing the parse.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HostFile {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color_scheme_preference: Option<ColorSchemePreference>,
}

impl Default for HostFile {
    fn default() -> Self {
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            color_scheme_preference: None,
        }
    }
}

/// Partial update to [`HostFile`]: `Some` overwrites, `None` leaves untouched.
/// Argument shape of `update_host_file` (webview camelCase JSON).
#[derive(Deserialize, Clone, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HostFilePatch {
    pub color_scheme_preference: Option<ColorSchemePreference>,
}

/// Read `host.json`; missing or corrupt → defaults (a corrupt file must never
/// take the host down — the next write replaces it wholesale).
pub fn read(path: &Path) -> HostFile {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|contents| serde_json::from_str(&contents).ok())
        .unwrap_or_default()
}

/// Merge `patch` onto the current contents, stamp [`CURRENT_SCHEMA_VERSION`],
/// persist atomically (unique temp + rename; concurrent windows can't tear the
/// file, last writer wins), and return the merged result. Creates the parent
/// dir on first run.
pub fn update(path: &Path, patch: HostFilePatch) -> std::io::Result<HostFile> {
    let mut file = read(path);
    if let Some(preference) = patch.color_scheme_preference {
        file.color_scheme_preference = Some(preference);
    }
    file.schema_version = CURRENT_SCHEMA_VERSION;

    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("host file path has no parent directory"))?;
    std::fs::create_dir_all(parent)?;

    let contents = serde_json::to_string_pretty(&file).map_err(std::io::Error::other)?;

    // Unique temp name so concurrent writers never share a partial file; the
    // rename is atomic.
    static WRITE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        WRITE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::write(&tmp, contents)?;
    std::fs::rename(&tmp, path)?;

    Ok(file)
}

/// Initialization script exposing the file as `window.__KSTACK_HOST__`.
/// `build_window` injects it into every window; Tauri runs it before any page
/// script, so the webview's first-paint code reads it synchronously.
pub fn init_script(file: &HostFile) -> String {
    // Fallback rather than unwrap; the inline script defaults missing fields.
    let json = serde_json::to_string(file).unwrap_or_else(|_| "{}".into());
    format!("window.__KSTACK_HOST__ = {json};")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A unique path inside the OS temp dir, in a directory that does not yet
    /// exist (exercising `update`'s create-parent-dirs behavior).
    fn temp_path() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir()
            .join(format!("kstack-host-file-test-{}-{id}", std::process::id()))
            .join("host.json")
    }

    #[test]
    fn read_missing_file_returns_defaults() {
        let file = read(&temp_path());
        assert_eq!(file, HostFile::default());
        assert_eq!(file.schema_version, CURRENT_SCHEMA_VERSION);
        assert_eq!(file.color_scheme_preference, None);
    }

    #[test]
    fn read_parses_existing_file() {
        let path = temp_path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            r#"{"schemaVersion":1,"colorSchemePreference":"dark"}"#,
        )
        .unwrap();
        let file = read(&path);
        assert_eq!(
            file.color_scheme_preference,
            Some(ColorSchemePreference::Dark)
        );
    }

    #[test]
    fn read_corrupt_file_returns_defaults() {
        let path = temp_path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "{not json").unwrap();
        assert_eq!(read(&path), HostFile::default());
    }

    #[test]
    fn update_writes_and_reloads() {
        let path = temp_path();
        // Parent dir deliberately absent — update must create it (first run).
        let updated = update(
            &path,
            HostFilePatch {
                color_scheme_preference: Some(ColorSchemePreference::Dark),
            },
        )
        .unwrap();
        assert_eq!(
            updated.color_scheme_preference,
            Some(ColorSchemePreference::Dark)
        );
        // A fresh read round-trips the persisted value.
        assert_eq!(read(&path), updated);
    }

    #[test]
    fn update_with_empty_patch_preserves_existing_values() {
        let path = temp_path();
        update(
            &path,
            HostFilePatch {
                color_scheme_preference: Some(ColorSchemePreference::Light),
            },
        )
        .unwrap();
        let updated = update(&path, HostFilePatch::default()).unwrap();
        assert_eq!(
            updated.color_scheme_preference,
            Some(ColorSchemePreference::Light)
        );
    }

    #[test]
    fn update_stamps_current_version() {
        let path = temp_path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            r#"{"schemaVersion":0,"colorSchemePreference":"light"}"#,
        )
        .unwrap();
        let updated = update(&path, HostFilePatch::default()).unwrap();
        assert_eq!(updated.schema_version, CURRENT_SCHEMA_VERSION);
        assert_eq!(read(&path).schema_version, CURRENT_SCHEMA_VERSION);
    }

    #[test]
    fn patch_deserializes_webview_camel_case_json() {
        let patch: HostFilePatch = serde_json::from_str(r#"{"colorSchemePreference":"system"}"#)
            .expect("patch should parse");
        assert_eq!(
            patch.color_scheme_preference,
            Some(ColorSchemePreference::System)
        );
    }

    #[test]
    fn init_script_exposes_the_file_as_a_global() {
        let file = HostFile {
            schema_version: 1,
            color_scheme_preference: Some(ColorSchemePreference::Dark),
        };
        assert_eq!(
            init_script(&file),
            r#"window.__KSTACK_HOST__ = {"schemaVersion":1,"colorSchemePreference":"dark"};"#
        );
    }

    #[test]
    fn init_script_omits_absent_fields() {
        // An unset preference must be absent (webview reads `undefined`), not
        // `null` — the inline script treats both as "system", but absence keeps
        // the contract identical to the file on disk.
        assert_eq!(
            init_script(&HostFile::default()),
            r#"window.__KSTACK_HOST__ = {"schemaVersion":1};"#
        );
    }
}
