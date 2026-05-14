//! Token storage. The refresh token is the only durable secret — it lives
//! in the OS keychain in release builds, or in a 0600 file under the app
//! data dir in debug builds.
//!
//! Why two backends: in `cargo build` / `tauri dev`, the binary's signing
//! identity changes every rebuild. macOS Keychain ACLs are pinned to that
//! identity, so each rebuild triggers an "Allow / Always Allow" prompt
//! the user can't escape. We cap that pain to debug builds only — the
//! file backend is gated by `cfg(debug_assertions)`, which is always
//! `false` for `cargo build --release` (i.e. real distributions).
//!
//! Access + ID tokens stay in process memory because they expire fast
//! (Hydra's default `ttl.access_token` is 1h) and persisting them only
//! widens the blast radius of a leak.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// In-memory token set. Constructed from an `openidconnect` token response
/// at the call site; everything that needs "is this expired?" goes through
/// [`Tokens::is_expired`].
#[derive(Debug, Clone, Serialize)]
pub struct Tokens {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub id_token: Option<String>,
    /// Absolute Unix timestamp; computed once at issuance.
    pub expires_at: u64,
    pub email: Option<String>,
    pub name: Option<String>,
    pub sub: Option<String>,
}

impl Tokens {
    /// Treat the token as expired 30s before it actually is, so a request
    /// that picks up the token on its way out won't fail mid-flight when
    /// the network is slow.
    pub fn is_expired(&self) -> bool {
        now() + 30 >= self.expires_at
    }
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn save_refresh_token(rt: Option<&str>) -> Result<(), String> {
    backend::save(rt)
}

/// Returns `Ok(None)` when there is no stored token (first-launch case),
/// not an error.
pub fn load_refresh_token() -> Result<Option<String>, String> {
    backend::load()
}

#[cfg(not(debug_assertions))]
mod backend {
    const SERVICE: &str = "sh.kstack.app";
    const USER: &str = "oauth-refresh";

    pub fn save(rt: Option<&str>) -> Result<(), String> {
        let entry = keyring::Entry::new(SERVICE, USER).map_err(|e| format!("keyring open: {e}"))?;
        match rt {
            Some(s) => entry
                .set_password(s)
                .map_err(|e| format!("keyring set: {e}")),
            None => match entry.delete_credential() {
                Ok(()) => Ok(()),
                Err(keyring::Error::NoEntry) => Ok(()),
                Err(e) => Err(format!("keyring delete: {e}")),
            },
        }
    }

    pub fn load() -> Result<Option<String>, String> {
        let entry = keyring::Entry::new(SERVICE, USER).map_err(|e| format!("keyring open: {e}"))?;
        match entry.get_password() {
            Ok(p) => Ok(Some(p)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(format!("keyring get: {e}")),
        }
    }
}

#[cfg(debug_assertions)]
mod backend {
    //! Debug-only file backend. Writes are atomic: a temp sibling is
    //! created with mode 0600 (Unix), filled, then `rename`d into place.
    //! That keeps a torn write — or a concurrent reader during write —
    //! from ever seeing a half-written or empty file. Plain `truncate(true)
    //! + write_all` exposes both windows.

    use std::fs::{self, OpenOptions};
    use std::io::{ErrorKind, Write};
    use std::path::{Path, PathBuf};

    fn token_path() -> Result<PathBuf, String> {
        let base = dirs::data_dir().ok_or("no per-user data dir")?;
        Ok(base.join("sh.kstack.app").join("oauth-refresh.dev"))
    }

    fn write_atomic(path: &Path, contents: &str) -> Result<(), String> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("mkdir: {e}"))?;
        }
        let tmp = path.with_extension("dev.tmp");
        let mut opts = OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut f = opts.open(&tmp).map_err(|e| format!("open tmp: {e}"))?;
        f.write_all(contents.as_bytes())
            .map_err(|e| format!("write tmp: {e}"))?;
        f.sync_all().map_err(|e| format!("sync tmp: {e}"))?;
        // On Windows `rename` returns an error if the destination exists,
        // so explicitly remove first. POSIX `rename` is atomic and replaces.
        #[cfg(windows)]
        let _ = fs::remove_file(path);
        fs::rename(&tmp, path).map_err(|e| format!("rename: {e}"))
    }

    pub fn save(rt: Option<&str>) -> Result<(), String> {
        let path = token_path()?;
        match rt {
            Some(s) => write_atomic(&path, s),
            None => match fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                Err(e) => Err(format!("delete: {e}")),
            },
        }
    }

    pub fn load() -> Result<Option<String>, String> {
        let path = token_path()?;
        match fs::read_to_string(&path) {
            // Treat empty as missing — defends against a torn write from
            // an older non-atomic version of this code or an external rm.
            Ok(s) if s.is_empty() => Ok(None),
            Ok(s) => Ok(Some(s)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("read: {e}")),
        }
    }
}

#[cfg(all(test, unix, debug_assertions))]
mod tests {
    #![allow(clippy::unwrap_used)]
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    /// Sanity: in debug builds, the saved file ends up mode 0600 and the
    /// roundtrip preserves the value. We poke the real `dirs::data_dir()`
    /// path so this also flushes any stored value at the end.
    #[test]
    fn dev_file_is_user_only_and_roundtrips() {
        save_refresh_token(Some("rt-test-value")).unwrap();
        let path = dirs::data_dir()
            .unwrap()
            .join("sh.kstack.app")
            .join("oauth-refresh.dev");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "dev token file must be user-only");
        assert_eq!(
            load_refresh_token().unwrap().as_deref(),
            Some("rt-test-value")
        );
        save_refresh_token(None).unwrap();
        assert!(load_refresh_token().unwrap().is_none());
    }
}
