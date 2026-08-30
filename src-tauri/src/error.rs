// Copyright 2026 The Kstack Authors
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

//! The host's unified error type: [`AppError`] wraps dependency errors behind
//! `#[from]` so `?` works crate-wide, and serializes as its `Display` string
//! across the Tauri command boundary.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, AppError>;

/// The host-wide error type.
#[derive(Debug, Error)]
pub enum AppError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("tauri error: {0}")]
    Tauri(#[from] tauri::Error),

    /// e.g. spawning the sidecar binary.
    #[error("shell error: {0}")]
    Shell(#[from] tauri_plugin_shell::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Serializes as the error's `Display` string — the Tauri-boundary contract, so
/// a command's `Err` reaches the webview as the human-readable message.
impl serde::Serialize for AppError {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{AppError, Result};
    use std::io::{Error as IoError, ErrorKind};

    #[test]
    fn io_variant_display_is_transparent() {
        // `#[error(transparent)]` means the wrapped error's message shows
        // through unchanged — no `"io error: "` prefix.
        let err: AppError = IoError::new(ErrorKind::NotFound, "missing file").into();
        assert_eq!(err.to_string(), "missing file");
    }

    #[test]
    fn serializes_as_its_display_string() {
        // This is the Tauri-boundary contract: a command error reaches the
        // webview as a bare JSON string equal to the error's `Display`.
        let err: AppError = IoError::new(ErrorKind::PermissionDenied, "denied").into();
        let json = serde_json::to_string(&err).expect("AppError should serialize");
        assert_eq!(json, format!("\"{err}\""));
        assert_eq!(json, "\"denied\"");
    }

    #[test]
    fn from_conversion_enables_question_mark() {
        // The `#[from]` impl is what lets `?` promote a foreign error into
        // `AppError` across module boundaries.
        fn read_missing() -> Result<String> {
            let contents = std::fs::read_to_string("/nonexistent/kstack/test/path")?;
            Ok(contents)
        }

        let err = read_missing().expect_err("reading a missing path should fail");
        assert!(matches!(err, AppError::Io(_)));
    }
}
