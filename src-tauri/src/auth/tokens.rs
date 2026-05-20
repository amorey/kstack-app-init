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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// In-memory token set. Constructed from an `openidconnect` token response
/// at the call site; everything that needs "is this expired?" goes through
/// [`Tokens::is_expired`].
#[derive(Debug, Clone, Serialize)]
pub struct Tokens {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub id_token: Option<String>,
    /// Absolute Unix timestamps; both computed once at issuance.
    pub issued_at: u64,
    pub expires_at: u64,
    pub email: Option<String>,
    pub name: Option<String>,
    pub sub: Option<String>,
}

/// Minimum headroom before `expires_at` to treat the token as expired —
/// covers the refresh round-trip plus a retry for short-lived tokens
/// where 25% of lifetime would be too small.
const REFRESH_FLOOR: Duration = Duration::from_secs(60);

impl Tokens {
    /// Refresh once 75% of the token's lifetime has elapsed (i.e. 25%
    /// remaining), with a floor of [`REFRESH_FLOOR`] so very short-lived
    /// tokens still get a sane margin. Matches the credential pusher's
    /// scheduling so the request path and the background pusher agree on
    /// "stale enough to refresh".
    pub fn is_expired(&self) -> bool {
        self.is_expired_at(now())
    }

    fn is_expired_at(&self, now: u64) -> bool {
        let lifetime = self.expires_at.saturating_sub(self.issued_at);
        let margin = (lifetime / 4).max(REFRESH_FLOOR.as_secs());
        now.saturating_add(margin) >= self.expires_at
    }
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Durable session state. Carries the refresh token (the only secret we
/// strictly need to keep) alongside the most recent ID token, which lets
/// the cold-start path recover identity claims (`email`, `name`, `sub`)
/// before the refresh-token grant resolves — and gives the refresh path
/// something to carry forward when the IdP doesn't reissue an ID token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Persisted {
    pub refresh_token: String,
    #[serde(default)]
    pub id_token: Option<String>,
}

pub fn save_persisted(p: Option<&Persisted>) -> Result<(), String> {
    let blob = match p {
        Some(p) => Some(serde_json::to_string(p).map_err(|e| format!("serialize: {e}"))?),
        None => None,
    };
    backend::save(blob.as_deref())
}

/// Returns `Ok(None)` when there is no stored session (first-launch case),
/// not an error.
pub fn load_persisted() -> Result<Option<Persisted>, String> {
    let Some(raw) = backend::load()? else {
        return Ok(None);
    };
    let p = serde_json::from_str::<Persisted>(&raw).map_err(|e| format!("deserialize: {e}"))?;
    Ok(Some(p))
}

#[cfg(all(not(debug_assertions), not(windows)))]
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

#[cfg(all(not(debug_assertions), windows))]
mod backend {
    //! Windows Credential Manager caps one credential blob at
    //! `CRED_MAX_CREDENTIAL_BLOB_SIZE` = 2560 bytes (the keyring crate
    //! measures the UTF-16 encoding). Our persisted blob is a JSON object
    //! carrying the refresh token *and* the most recent ID token — a JWT
    //! that routinely pushes it past that ceiling. macOS Keychain has no
    //! comparable small limit, so only Windows splits.
    //!
    //! Layout: a manifest entry at `oauth-refresh` holds the decimal chunk
    //! count; data lives in `oauth-refresh.0..N`. The manifest is written
    //! *last* on save and read *first* on load, so a torn write is
    //! detectable (a chunk missing under a committed count) and resolves to
    //! "no session" — a clean forced re-login, not a hard error.
    //!
    //! Backward compat: an older build could have left a single short blob
    //! directly under `oauth-refresh`. If the manifest doesn't parse as an
    //! integer we treat it as that legacy whole blob.

    const SERVICE: &str = "sh.kstack.app";
    const USER: &str = "oauth-refresh";

    /// UTF-16 byte cap is 2560; 1000 BMP chars = 2000 bytes, leaving margin.
    /// Tokens are ASCII base64url in practice, so this is conservative.
    const CHUNK_CHARS: usize = 1000;

    /// Upper bound on stale-chunk scans, so a corrupt manifest can't spin
    /// us forever.
    const MAX_CHUNKS: usize = 256;

    fn entry(user: &str) -> Result<keyring::Entry, String> {
        keyring::Entry::new(SERVICE, user).map_err(|e| format!("keyring open: {e}"))
    }

    fn chunk_user(i: usize) -> String {
        format!("{USER}.{i}")
    }

    /// `Ok(true)` if a credential existed and was removed, `Ok(false)` if
    /// it was already absent.
    fn delete(user: &str) -> Result<bool, String> {
        match entry(user)?.delete_credential() {
            Ok(()) => Ok(true),
            Err(keyring::Error::NoEntry) => Ok(false),
            Err(e) => Err(format!("keyring delete: {e}")),
        }
    }

    /// Delete the dense chunk run starting at `start`, stopping at the
    /// first gap. Chunks are always written contiguously, so a gap means
    /// no higher chunk survives.
    fn delete_chunks_from(start: usize) -> Result<(), String> {
        for i in start..MAX_CHUNKS {
            if !delete(&chunk_user(i))? {
                break;
            }
        }
        Ok(())
    }

    /// Delete every data chunk plus the manifest. Used by logout and to
    /// scrub a partial write.
    fn purge() -> Result<(), String> {
        delete_chunks_from(0)?;
        delete(USER)?;
        Ok(())
    }

    pub fn save(rt: Option<&str>) -> Result<(), String> {
        let Some(s) = rt else {
            return purge();
        };
        let chars: Vec<char> = s.chars().collect();
        let count = chars.len().div_ceil(CHUNK_CHARS).max(1);
        // Write data first; the manifest is what commits the write.
        for i in 0..count {
            let start = i * CHUNK_CHARS;
            let end = ((i + 1) * CHUNK_CHARS).min(chars.len());
            let blob: String = chars[start..end].iter().collect();
            entry(&chunk_user(i))?
                .set_password(&blob)
                .map_err(|e| format!("keyring set: {e}"))?;
        }
        entry(USER)?
            .set_password(&count.to_string())
            .map_err(|e| format!("keyring set: {e}"))?;
        // Drop chunks left behind by a previously longer value.
        delete_chunks_from(count)
    }

    pub fn load() -> Result<Option<String>, String> {
        let manifest = match entry(USER)?.get_password() {
            Ok(m) => m,
            Err(keyring::Error::NoEntry) => return Ok(None),
            Err(e) => return Err(format!("keyring get: {e}")),
        };
        let Ok(count) = manifest.parse::<usize>() else {
            // Legacy single-entry blob from a pre-chunking build.
            return Ok(Some(manifest));
        };
        let mut out = String::new();
        for i in 0..count {
            match entry(&chunk_user(i))?.get_password() {
                Ok(part) => out.push_str(&part),
                // Committed count but a chunk is gone: torn write. Scrub and
                // report "no session" so the app forces a clean login.
                Err(keyring::Error::NoEntry) => {
                    let _ = purge();
                    return Ok(None);
                }
                Err(e) => return Err(format!("keyring get: {e}")),
            }
        }
        Ok(Some(out))
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

#[cfg(test)]
mod expiry_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn tk(issued_at: u64, expires_at: u64) -> Tokens {
        Tokens {
            access_token: String::new(),
            refresh_token: None,
            id_token: None,
            issued_at,
            expires_at,
            email: None,
            name: None,
            sub: None,
        }
    }

    /// With a 1h token, refresh should trip at exactly 75% elapsed
    /// (i.e. 25% = 900s remaining).
    #[test]
    fn proportional_margin_at_75_percent_of_lifetime() {
        let t = tk(1_000, 4_600); // lifetime = 3600
        assert!(!t.is_expired_at(1_000), "just issued");
        assert!(!t.is_expired_at(3_699), "74.97% elapsed, still fresh");
        assert!(t.is_expired_at(3_700), "75% elapsed, refresh now");
        assert!(t.is_expired_at(4_600), "at expiry");
        assert!(t.is_expired_at(9_999), "past expiry");
    }

    /// Short-lived tokens hit the floor instead of 25%. lifetime=120 →
    /// 25%=30s, but the 60s floor wins, so refresh at remaining ≤ 60s.
    #[test]
    fn floor_dominates_for_short_lifetimes() {
        let t = tk(1_000, 1_120); // lifetime = 120
        assert!(!t.is_expired_at(1_059), "61s remaining > 60s floor");
        assert!(t.is_expired_at(1_060), "60s remaining hits floor");
    }

    /// Degenerate token (issued_at == expires_at, lifetime = 0): floor
    /// applies, and any non-future `now` is expired.
    #[test]
    fn zero_lifetime_is_always_expired() {
        let t = tk(0, 0);
        assert!(t.is_expired_at(0));
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
        let p = Persisted {
            refresh_token: "rt-test-value".into(),
            id_token: Some("idt-test-value".into()),
        };
        save_persisted(Some(&p)).unwrap();
        let path = dirs::data_dir()
            .unwrap()
            .join("sh.kstack.app")
            .join("oauth-refresh.dev");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "dev token file must be user-only");
        let loaded = load_persisted().unwrap().expect("persisted present");
        assert_eq!(loaded.refresh_token, "rt-test-value");
        assert_eq!(loaded.id_token.as_deref(), Some("idt-test-value"));
        save_persisted(None).unwrap();
        assert!(load_persisted().unwrap().is_none());
    }
}
