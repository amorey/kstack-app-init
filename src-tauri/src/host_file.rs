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

//! `host.json` — the host's persisted settings file.
//!
//! Holds the settings the Rust host needs (chiefly at startup, before any
//! webview exists), under `app_config_dir()/host.json`. It is the durable
//! source of truth for those settings — the webview keeps no copy of its own.
//! It reaches the webview two ways, both fed from one read in
//! `window_manager::build_window`: [`init_script`] exposes the file as the
//! `window.__KSTACK_HOST__` global before any page script runs (synchronous
//! first-paint reads), and the [`UPDATED_EVENT`] broadcast after every write
//! keeps already-open windows in step. The webview writes back through the
//! general `update_host_file` command (see `commands.rs`).
//!
//! Today it carries one value: the color-scheme preference, which
//! `window_manager` turns into a native window background so a freshly created
//! window's first frame matches the app's scheme.
//!
//! The format is a single versioned JSON object with optional fields; updates
//! are partial ([`HostFilePatch`]) and merge onto the current contents, so
//! adding a setting is a new `Option` field on [`HostFile`]/[`HostFilePatch`],
//! not a new command. Reads are defensive (missing or corrupt file → defaults)
//! and writes are atomic (unique temp file + rename).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tauri::Manager;

/// Schema version stamped into the file on every write, for future migrations.
const CURRENT_SCHEMA_VERSION: u32 = 1;

/// Event broadcast to every window after `host.json` changes, carrying the
/// merged [`HostFile`] as its payload (same camelCase JSON as the file and the
/// injected `window.__KSTACK_HOST__` global). This is how already-open windows
/// track the source of truth live — the webview's `ThemeProvider` listens for
/// it; the string is hand-mirrored there.
pub const UPDATED_EVENT: &str = "host-file-updated";

/// Absolute path of `host.json`: the app's config directory (created on demand
/// by [`update`]) joined with the fixed filename.
pub fn path(app: &tauri::AppHandle) -> tauri::Result<PathBuf> {
    Ok(app.path().app_config_dir()?.join("host.json"))
}

/// The user's color-scheme choice, as the webview's `theme.tsx` defines it:
/// `system` follows the OS; `light`/`dark` are explicit overrides. Serialized
/// lowercase to match the webview's `ColorSchemePreference` strings.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum ColorSchemePreference {
    System,
    Light,
    Dark,
}

/// The parsed contents of `host.json`. All settings are `Option` so absent
/// fields (older files, first run) fall back to their defaults at the point of
/// use rather than failing the parse.
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

/// A partial update to [`HostFile`]: `Some` fields overwrite, `None` fields
/// leave the current value untouched. This is the argument shape of the
/// `update_host_file` command, so it deserializes from the webview's camelCase
/// JSON (e.g. `{ "colorSchemePreference": "dark" }`).
#[derive(Deserialize, Clone, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HostFilePatch {
    pub color_scheme_preference: Option<ColorSchemePreference>,
}

/// Read and parse `host.json`, falling back to defaults when the file is
/// missing or unparseable — a corrupt file must never take the host down; the
/// next write replaces it wholesale.
pub fn read(path: &Path) -> HostFile {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|contents| serde_json::from_str(&contents).ok())
        .unwrap_or_default()
}

/// Merge `patch` onto the current file contents, stamp [`CURRENT_SCHEMA_VERSION`],
/// and persist. The write is atomic (unique temp file + rename) so concurrent
/// writers — e.g. two windows saving settings — can't interleave into a torn
/// file; last writer wins. Creates the parent directory if needed (the app
/// config dir may not exist on first run). Returns the merged result.
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

    // Serialization can't fail for this plain data struct, but route the error
    // instead of unwrapping (crate-wide `clippy::unwrap_used`).
    let contents = serde_json::to_string_pretty(&file).map_err(std::io::Error::other)?;

    // Unique temp name so concurrent writers never share a partially-written
    // file; the rename is atomic, so readers see the old or the new contents.
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

/// The initialization script exposing the file to a window's webview, as
/// `window.__KSTACK_HOST__`. `build_window` injects it into every window it
/// creates; Tauri runs it before any page script, so the webview's first-paint
/// code (the inline script in `index.html`) reads the preference synchronously
/// from the same source of truth the host painted the native background from.
pub fn init_script(file: &HostFile) -> String {
    // Serializing this plain data struct can't fail; fall back to an empty
    // object rather than unwrapping (crate-wide `clippy::unwrap_used`) — the
    // inline script treats missing fields as defaults anyway.
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
