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

//! Host↔sidecar IPC endpoint. Two responsibilities:
//!   1. **Addressing.** [`Endpoint::pick`] — a per-instance UDS path (Unix) or
//!      `\\.\pipe\` name (Windows), picked *before* spawn and passed via
//!      `--socket`, so the sidecar never negotiates where to listen. Pure,
//!      I/O-free value.
//!   2. **Dialing.** [`connect`] / [`connect_with_budget`] — capped-backoff
//!      retry, returning a [`Stream`] hyper consumes identically on both
//!      platforms.
//!
//! "ipc", not "socket" — the latter would exclude Windows named pipes.

use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

// tokio's clock (virtual under tokio::time::pause) so the budget/backoff is
// testable without real sleeps.
use tokio::time::Instant;

use crate::error::{AppError, Result};

/// Apple's `sun_path` cap (Linux allows 108); the tighter bound so a path that
/// fits on one Unix fits on all.
#[cfg(unix)]
const UNIX_SUN_PATH_MAX: usize = 104;

/// Per-process counter so two `pick` calls never collide on filename
/// (parallel test runs).
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Default connect-retry budget: covers sidecar startup with margin without
/// dragging out launch when it's never coming up.
pub const DEFAULT_CONNECT_BUDGET: Duration = Duration::from_secs(5);

/// Initial retry delay, doubling up to [`MAX_RETRY_DELAY`].
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(50);
const MAX_RETRY_DELAY: Duration = Duration::from_millis(500);

/// UDS stream on Unix, named-pipe stream on Windows — one
/// `AsyncRead + AsyncWrite + Unpin` surface via [`interprocess`].
pub type Stream = interprocess::local_socket::tokio::Stream;

/// The address the sidecar listens on and the host dials.
#[derive(Clone, Debug)]
pub struct Endpoint(String);

impl Endpoint {
    /// Returns the address as the string the sidecar's `--socket` flag expects.
    pub fn as_arg(&self) -> &str {
        &self.0
    }

    /// Picks an unused per-instance address under `base` (ignored on Windows —
    /// pipes live in a flat `\\.\pipe\` namespace).
    #[cfg(unix)]
    pub fn pick(base: &Path) -> Result<Endpoint> {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        // `kstack-sidecar-` matches the sidecar's own default name
        // (sidecar/listen_unix.go).
        let filename = format!("kstack-sidecar-{pid}-{n}.sock");
        let path: PathBuf = base.join(&filename);

        let s = path
            .to_str()
            .ok_or_else(|| {
                AppError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "ipc endpoint path is not valid UTF-8",
                ))
            })?
            .to_owned();

        if s.len() >= UNIX_SUN_PATH_MAX {
            return Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "ipc endpoint path is {} bytes; kernel limit is {} including the trailing NUL",
                    s.len(),
                    UNIX_SUN_PATH_MAX
                ),
            )));
        }

        Ok(Endpoint(s))
    }

    #[cfg(windows)]
    pub fn pick(_base: &Path) -> Result<Endpoint> {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        // Aligns with sidecar/listen_windows.go (`kstack-sidecar-<pid>`).
        Ok(Endpoint(format!(r"\\.\pipe\kstack-sidecar-{pid}-{n}")))
    }
}

/// Dials `endpoint` with the default budget. The sidecar takes a few ms to bind
/// after spawn, so expect at least one retry (see [`connect_with_budget`]).
pub async fn connect(endpoint: &Endpoint) -> Result<Stream> {
    connect_with_budget(endpoint, DEFAULT_CONNECT_BUDGET).await
}

/// Retries `Stream::connect` with capped exponential backoff until the
/// endpoint accepts or `budget` is exhausted.
///
/// Retryable ("not ready yet"): `NotFound` (sidecar hasn't bound) and
/// `WouldBlock`/`ERROR_PIPE_BUSY` (all pipe instances busy). Anything else
/// (e.g. access denied) surfaces immediately — it won't self-correct. On
/// budget exhaustion the most recent error is returned verbatim so logs show
/// *why*.
pub async fn connect_with_budget(endpoint: &Endpoint, budget: Duration) -> Result<Stream> {
    /// Windows `ERROR_PIPE_BUSY` (see tokio's named-pipe docs).
    const ERROR_PIPE_BUSY: i32 = 231;

    use interprocess::local_socket::{traits::tokio::Stream as _, GenericFilePath, ToFsName};

    // `GenericFilePath` accepts both Unix paths and `\\.\pipe\…` strings —
    // no cfg-branch.
    let name = endpoint
        .as_arg()
        .to_fs_name::<GenericFilePath>()
        .map_err(AppError::Io)?;

    let deadline = Instant::now() + budget;
    let mut delay = INITIAL_RETRY_DELAY;

    let last_err = loop {
        match Stream::connect(name.clone()).await {
            Ok(stream) => return Ok(stream),
            Err(err) => {
                let retryable = matches!(
                    err.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::WouldBlock,
                ) || err.raw_os_error() == Some(ERROR_PIPE_BUSY);
                if !retryable || Instant::now() >= deadline {
                    break err;
                }
                // Cap each sleep at the remaining budget so we never overshoot.
                let remaining = deadline.saturating_duration_since(Instant::now());
                let sleep_for = delay.min(remaining);
                tokio::time::sleep(sleep_for).await;
                delay = (delay * 2).min(MAX_RETRY_DELAY);
            }
        }
    };

    Err(AppError::Io(last_err))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// On Unix, the picked path must live inside the directory we asked for —
    /// the host expects to be able to control runtime-directory placement
    /// (and its perms) and the sidecar must find the file there.
    #[cfg(unix)]
    #[test]
    fn pick_returns_path_under_base_dir_on_unix() {
        let base = std::env::temp_dir();
        let path = Endpoint::pick(&base).expect("pick should succeed under tmpdir");

        let chosen = Path::new(path.as_arg());
        assert!(
            chosen.starts_with(&base),
            "{} should start with {}",
            chosen.display(),
            base.display()
        );
    }

    /// Two picks must never produce the same address — otherwise a second
    /// app instance (or a parallel test) could collide on the socket.
    #[cfg(unix)]
    #[test]
    fn pick_returns_unique_paths() {
        let base = std::env::temp_dir();
        let a = Endpoint::pick(&base).unwrap();
        let b = Endpoint::pick(&base).unwrap();
        assert_ne!(a.as_arg(), b.as_arg());
    }

    /// The picked name should describe what the endpoint serves (the
    /// sidecar's IPC) rather than just "kstack-something", so anyone
    /// inspecting `/tmp` (or `\\.\pipe\`) can identify it at a glance.
    /// Also aligns with the sidecar's own default socket name
    /// (`kstack-sidecar-<pid>`) so the two halves of the contract look
    /// related on disk.
    #[test]
    fn pick_uses_sidecar_prefix() {
        let base = std::env::temp_dir();
        let path = Endpoint::pick(&base).expect("pick");
        let arg = path.as_arg();
        // The check is platform-agnostic: both `/tmp/kstack-sidecar-…`
        // and `\\.\pipe\kstack-sidecar-…` contain the prefix verbatim.
        assert!(
            arg.contains("kstack-sidecar-"),
            "expected `kstack-sidecar-` in {arg}"
        );
    }

    /// macOS caps `sun_path` at 104 bytes; a path that overflows must be
    /// rejected at construction rather than producing an addr the kernel will
    /// truncate or reject at bind/connect time.
    #[cfg(target_os = "macos")]
    #[test]
    fn pick_rejects_paths_exceeding_sun_path_on_macos() {
        // Build a base that, combined with the filename, exceeds 104 bytes.
        // The filename alone is ~25–35 chars, so any base ≥ 80 bytes triggers
        // the guard regardless of the appended PID/counter.
        let long_base: PathBuf = PathBuf::from("/").join("a".repeat(120));
        let err = Endpoint::pick(&long_base).expect_err("overflowing path should be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("kernel limit"),
            "error should mention the kernel limit, got: {msg}"
        );
    }

    /// On Windows the address must be a `\\.\pipe\…` name — that is the only
    /// namespace the named-pipe APIs accept.
    #[cfg(windows)]
    #[test]
    fn pick_returns_pipe_path_on_windows() {
        let path = Endpoint::pick(Path::new("ignored")).expect("pick should succeed on windows");
        assert!(
            path.as_arg().starts_with(r"\\.\pipe\"),
            "got: {}",
            path.as_arg()
        );
    }

    /// `as_arg` is the contract with the sidecar CLI: whatever bytes we
    /// constructed must come back out unmodified for the child process to
    /// bind on.
    #[test]
    fn as_arg_round_trips() {
        let base = std::env::temp_dir();
        let path = Endpoint::pick(&base).expect("pick should succeed");
        let a = path.as_arg().to_owned();
        // Calling twice yields the same string (no internal mutation).
        assert_eq!(path.as_arg(), a);
    }

    /// Binds an [`interprocess`] listener at `path`. Used by the connect
    /// tests below so a single fixture works on both Unix (UDS) and
    /// Windows (named pipe) without cfg branches.
    fn bind_listener(path: &str) -> interprocess::local_socket::tokio::Listener {
        use interprocess::local_socket::{GenericFilePath, ListenerOptions, ToFsName};
        let name = path.to_fs_name::<GenericFilePath>().expect("to_fs_name");
        ListenerOptions::new()
            .name(name)
            .create_tokio()
            .expect("bind listener")
    }

    /// Removes a UDS socket file post-test. No-op on Windows where named
    /// pipes are kernel objects (not files) that vanish on handle close.
    fn cleanup_path(path: &str) {
        #[cfg(unix)]
        let _ = std::fs::remove_file(path);
        #[cfg(windows)]
        let _ = path; // silence unused-arg warning
    }

    /// Happy path: if the sidecar is already listening when the host
    /// dials, `connect` returns a usable stream on the first attempt.
    #[tokio::test]
    async fn connect_succeeds_against_a_listening_endpoint() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let _listener = bind_listener(path.as_arg());
        connect_with_budget(&path, Duration::from_secs(1))
            .await
            .expect("connect should succeed");
        cleanup_path(path.as_arg());
    }

    /// The sidecar may not be ready the instant `connect` is first polled
    /// — it spawns concurrently. `connect` must keep retrying until the
    /// listener comes up, within its budget. Driven on a paused clock: the
    /// endpoint appears at virtual 150ms while the dial retries, and tokio
    /// auto-advances virtual time between the parked timers — no real sleep.
    #[tokio::test(start_paused = true)]
    async fn connect_retries_until_endpoint_appears() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let path_arg = path.as_arg().to_owned();
        let dial = connect_with_budget(&path, Duration::from_secs(2));

        let bind_later = async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            bind_listener(&path_arg)
        };

        let (stream, _listener) = tokio::join!(dial, bind_later);
        assert!(
            stream.is_ok(),
            "expected connect to succeed; got {stream:?}"
        );
        cleanup_path(path.as_arg());
    }

    /// If nothing ever binds, `connect` must surface a transport error
    /// within the budget — not hang forever (would deadlock app startup).
    #[tokio::test]
    async fn connect_gives_up_after_budget() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let started = std::time::Instant::now();
        let err = connect_with_budget(&path, Duration::from_millis(300))
            .await
            .expect_err("no listener: connect should fail");
        let elapsed = started.elapsed();

        // Bounded above by budget + one extra backoff slice so we don't
        // tie the assertion to the exact retry cadence.
        assert!(
            elapsed < Duration::from_secs(2),
            "connect should fail near the budget, took {elapsed:?}"
        );
        assert!(matches!(err, AppError::Io(_)), "got: {err}");
    }
}
