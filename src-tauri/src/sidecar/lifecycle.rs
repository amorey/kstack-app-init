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

//! Sidecar process lifecycle: spawn the externalBin, watch its stdout
//! for the READY line, gracefully shut it down on app exit.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tauri::{App, AppHandle, Manager};
use tauri_plugin_shell::process::{CommandChild, CommandEvent};
use tauri_plugin_shell::ShellExt;
use tokio::sync::watch;

use super::transport::READY_PREFIX;
use crate::{KSTACK_CLOUD_URL_ENV, KSTACK_LOG_LEVEL_ENV};

/// Sidecar normally prints READY within ~50ms.
const READY_TIMEOUT: Duration = Duration::from_secs(5);

/// Budget for stdin-EOF graceful exit (single-digit ms typically) plus the
/// host's stderr-reader draining final log lines through tauri-plugin-log
/// before SIGKILL fallback.
const GRACE_PERIOD: Duration = Duration::from_millis(250);
const GRACE_POLL: Duration = Duration::from_millis(10);

fn sidecar_env_overrides<F: Fn(&str) -> Option<String>>(getenv: F) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for key in [KSTACK_LOG_LEVEL_ENV, KSTACK_CLOUD_URL_ENV] {
        if let Some(v) = getenv(key) {
            out.push((key.to_string(), v));
        }
    }
    out
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ShutdownOutcome {
    Graceful,
    Killed,
}

/// Drop `handle` (closes stdin → sidecar's EOF watcher runs its graceful
/// exit branch in `sidecar/main.go`), then poll `is_alive` until it
/// reports exit or `grace` elapses; on timeout call `force_kill`.
fn graceful_kill<H, P, K>(handle: H, grace: Duration, is_alive: P, force_kill: K) -> ShutdownOutcome
where
    P: Fn() -> bool,
    K: FnOnce(),
{
    drop(handle);
    let deadline = Instant::now() + grace;
    while Instant::now() < deadline {
        if !is_alive() {
            return ShutdownOutcome::Graceful;
        }
        std::thread::sleep(GRACE_POLL);
    }
    if !is_alive() {
        return ShutdownOutcome::Graceful;
    }
    force_kill();
    ShutdownOutcome::Killed
}

/// Strip a single trailing `\n` from a sidecar log line; lossy-decode so a
/// malformed byte never panics the lifecycle task.
fn format_sidecar_line(line: &[u8]) -> String {
    String::from_utf8_lossy(line)
        .trim_end_matches('\n')
        .to_owned()
}

pub(crate) struct SidecarState {
    /// `None` until the stdout reader sees the READY line.
    socket: watch::Receiver<Option<PathBuf>>,
    /// Held for the lifetime of the app so `shutdown()` can stop it.
    child: Mutex<Option<CommandChild>>,
}

impl SidecarState {
    pub(crate) fn socket_rx(&self) -> watch::Receiver<Option<PathBuf>> {
        self.socket.clone()
    }
}

pub(super) async fn wait_for_socket(
    mut rx: watch::Receiver<Option<PathBuf>>,
) -> Result<PathBuf, String> {
    if let Some(p) = rx.borrow().clone() {
        return Ok(p);
    }
    let wait = async {
        while rx.changed().await.is_ok() {
            if let Some(p) = rx.borrow().clone() {
                return Ok(p);
            }
        }
        Err("sidecar exited before READY".to_string())
    };
    tokio::time::timeout(READY_TIMEOUT, wait)
        .await
        .map_err(|_| "timed out waiting for sidecar READY".to_string())?
}

/// Resolve the sidecar's UDS path, waiting up to `READY_TIMEOUT` for the
/// READY line if the sidecar hasn't announced yet. Shared by every
/// host-only POST helper (credentials, resync, …).
pub(super) async fn resolve_socket<R: tauri::Runtime>(
    app: &AppHandle<R>,
) -> Result<PathBuf, String> {
    let state = app
        .try_state::<SidecarState>()
        .ok_or("sidecar state not managed")?;
    wait_for_socket(state.socket_rx()).await
}

/// Start the sidecar binary and watch its stdout for the READY line; the
/// `SidecarState` is filled asynchronously.
pub fn spawn(app: &App) -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = app.shell().sidecar("kstack-sidecar")?;
    for (k, v) in sidecar_env_overrides(|k| std::env::var(k).ok()) {
        cmd = cmd.env(k, v);
    }
    let (mut rx, child) = cmd.spawn()?;

    let (socket_tx, socket_rx) = watch::channel::<Option<PathBuf>>(None);
    app.manage(SidecarState {
        socket: socket_rx,
        child: Mutex::new(Some(child)),
    });

    tauri::async_runtime::spawn(async move {
        while let Some(ev) = rx.recv().await {
            match ev {
                CommandEvent::Stdout(line) => {
                    let s = String::from_utf8_lossy(&line);
                    if socket_tx.borrow().is_none() {
                        if let Some(rest) = s.strip_prefix(READY_PREFIX) {
                            let _ = socket_tx.send(Some(PathBuf::from(rest.trim())));
                            continue;
                        }
                    }
                    log::info!(target: "sidecar", "{}", format_sidecar_line(&line));
                }
                CommandEvent::Stderr(line) => {
                    log::info!(target: "sidecar", "{}", format_sidecar_line(&line));
                }
                CommandEvent::Error(e) => {
                    log::error!(target: "sidecar", "command event error: {e}");
                }
                CommandEvent::Terminated(p) => {
                    log::info!(target: "sidecar", "terminated: {p:?}");
                    break;
                }
                _ => {}
            }
        }
    });

    Ok(())
}

/// Drop the `CommandChild` so the sidecar sees stdin EOF and runs its
/// graceful shutdown branch (see `sidecar/main.go`); SIGKILL fallback if
/// it doesn't exit within `GRACE_PERIOD`. Safe to call multiple times.
pub fn shutdown(app: &AppHandle) {
    if let Some(state) = app.try_state::<SidecarState>() {
        // Recover even on poison: a panicked previous holder shouldn't
        // block our last-ditch attempt to stop the child.
        let mut guard = state
            .child
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(child) = guard.take() {
            let pid = child.pid();
            let outcome = graceful_kill(
                child,
                GRACE_PERIOD,
                || process_alive(pid),
                || {
                    log::warn!(
                        target: "sidecar",
                        "sidecar didn't exit within {GRACE_PERIOD:?}, sending SIGKILL"
                    );
                    force_kill(pid);
                },
            );
            log::info!(target: "sidecar", "shutdown outcome: {outcome:?}");
        }
    }
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    // SAFETY: `kill` with sig=0 only checks delivery permission; no signal sent.
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    use windows::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
    use windows::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    // SAFETY: OpenProcess returns either a valid handle or an error; we close
    // it on every path. GetExitCodeProcess writes to a stack u32 we own.
    let Ok(handle) = (unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, pid) }) else {
        return false;
    };
    let mut code: u32 = 0;
    let alive =
        unsafe { GetExitCodeProcess(handle, &mut code) }.is_ok() && code == STILL_ACTIVE.0 as u32;
    let _ = unsafe { CloseHandle(handle) };
    alive
}

#[cfg(unix)]
fn force_kill(pid: u32) {
    // SAFETY: PID came from a CommandChild we just dropped; reuse in the
    // sub-second window between drop and probe is not a real risk on the
    // platforms we ship to.
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
}

#[cfg(windows)]
fn force_kill(pid: u32) {
    use windows::Win32::Foundation::CloseHandle;
    use windows::Win32::System::Threading::{OpenProcess, TerminateProcess, PROCESS_TERMINATE};

    // SAFETY: PID came from a CommandChild we just dropped; reuse in the
    // sub-second window between drop and probe is not a real risk on the
    // platforms we ship to. Handle is closed on every path.
    if let Ok(handle) = unsafe { OpenProcess(PROCESS_TERMINATE, false, pid) } {
        let _ = unsafe { TerminateProcess(handle, 1) };
        let _ = unsafe { CloseHandle(handle) };
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn format_sidecar_line_strips_trailing_newline() {
        assert_eq!(format_sidecar_line(b"hello\n"), "hello");
    }

    #[test]
    fn format_sidecar_line_handles_no_trailing_newline() {
        assert_eq!(format_sidecar_line(b"hello"), "hello");
    }

    #[test]
    fn format_sidecar_line_preserves_internal_newlines() {
        assert_eq!(format_sidecar_line(b"line1\nline2\n"), "line1\nline2");
    }

    #[test]
    fn format_sidecar_line_lossy_decodes_invalid_utf8() {
        assert_eq!(format_sidecar_line(&[0xff]), "\u{FFFD}");
    }

    #[test]
    fn sidecar_env_overrides_passes_through_kstack_log() {
        let env = sidecar_env_overrides(|k| match k {
            "KSTACK_LOG_LEVEL" => Some("debug".into()),
            _ => None,
        });
        assert_eq!(
            env,
            vec![("KSTACK_LOG_LEVEL".to_string(), "debug".to_string())]
        );
    }

    #[test]
    fn sidecar_env_overrides_is_empty_when_unset() {
        let env = sidecar_env_overrides(|_| None);
        assert!(env.is_empty(), "got: {env:?}");
    }

    #[test]
    fn sidecar_env_overrides_passes_through_kstack_cloud_url() {
        let env = sidecar_env_overrides(|k| match k {
            "KSTACK_CLOUD_URL" => Some("https://api.example".into()),
            _ => None,
        });
        assert_eq!(
            env,
            vec![(
                "KSTACK_CLOUD_URL".to_string(),
                "https://api.example".to_string()
            )]
        );
    }

    /// Stand-in for `CommandChild` in tests — flips the cell on drop, so
    /// tests can prove `graceful_kill` closes stdin before waiting.
    struct DropProbe<'a>(&'a std::cell::Cell<bool>);
    impl Drop for DropProbe<'_> {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    #[test]
    fn graceful_kill_drops_handle_first_and_skips_force_kill_on_clean_exit() {
        let dropped = std::cell::Cell::new(false);
        let probe = DropProbe(&dropped);
        let force_kill_calls = std::cell::Cell::new(0u32);

        let outcome = graceful_kill(
            probe,
            Duration::from_millis(50),
            || false,
            || force_kill_calls.set(force_kill_calls.get() + 1),
        );

        assert!(dropped.get(), "handle must be dropped before the wait");
        assert_eq!(force_kill_calls.get(), 0);
        assert_eq!(outcome, ShutdownOutcome::Graceful);
    }

    #[test]
    fn graceful_kill_force_kills_when_process_outlives_grace_period() {
        let dropped = std::cell::Cell::new(false);
        let probe = DropProbe(&dropped);
        let force_kill_calls = std::cell::Cell::new(0u32);

        let outcome = graceful_kill(
            probe,
            Duration::from_millis(20),
            || true,
            || force_kill_calls.set(force_kill_calls.get() + 1),
        );

        assert!(dropped.get());
        assert_eq!(force_kill_calls.get(), 1);
        assert_eq!(outcome, ShutdownOutcome::Killed);
    }

    #[tokio::test]
    async fn returns_immediately_if_already_set() {
        let (_tx, rx) = watch::channel(Some(PathBuf::from("/tmp/already.sock")));
        let result = wait_for_socket(rx).await.unwrap();
        assert_eq!(result, PathBuf::from("/tmp/already.sock"));
    }

    #[tokio::test]
    async fn awaits_then_resolves_when_sender_publishes() {
        let (tx, rx) = watch::channel::<Option<PathBuf>>(None);
        let task = tokio::spawn(wait_for_socket(rx));
        tokio::task::yield_now().await;
        tx.send(Some(PathBuf::from("/tmp/late.sock"))).unwrap();
        let result = task.await.unwrap().unwrap();
        assert_eq!(result, PathBuf::from("/tmp/late.sock"));
    }

    #[tokio::test]
    async fn errors_when_sender_dropped_before_ready() {
        let (tx, rx) = watch::channel::<Option<PathBuf>>(None);
        let task = tokio::spawn(wait_for_socket(rx));
        tokio::task::yield_now().await;
        drop(tx);
        let err = task.await.unwrap().unwrap_err();
        assert!(err.contains("exited before READY"), "got: {err}");
    }

    #[tokio::test(start_paused = true)]
    async fn times_out_if_ready_never_arrives() {
        let (_tx, rx) = watch::channel::<Option<PathBuf>>(None);
        let task = tokio::spawn(wait_for_socket(rx));
        tokio::time::advance(READY_TIMEOUT + Duration::from_secs(1)).await;
        let err = task.await.unwrap().unwrap_err();
        assert!(err.contains("timed out"), "got: {err}");
    }

    #[cfg(unix)]
    #[test]
    fn process_alive_returns_false_for_unused_pid() {
        // i32::MAX is above the default pid_max on every Unix we ship to,
        // so kill(sig=0) returns ESRCH and the probe reports dead.
        assert!(!process_alive(i32::MAX as u32));
    }

    #[cfg(unix)]
    #[test]
    fn process_alive_tracks_real_child_and_force_kill_terminates_it() {
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();

        assert!(
            process_alive(pid),
            "child should be alive immediately after spawn"
        );

        force_kill(pid);
        let _ = child.wait();

        assert!(
            !process_alive(pid),
            "child should report dead after force_kill + wait"
        );
    }

    #[cfg(windows)]
    #[test]
    fn process_alive_returns_false_for_unused_pid() {
        // PID 0 is the System Idle Process on Windows; OpenProcess refuses
        // it with ERROR_INVALID_PARAMETER, so the probe must report dead
        // rather than panicking or returning true.
        assert!(!process_alive(0));
    }

    #[cfg(windows)]
    #[test]
    fn process_alive_tracks_real_child_and_force_kill_terminates_it() {
        // `ping -n 30 127.0.0.1` runs ~29s — long enough that the assertions
        // below are not racing the child's natural exit.
        let mut child = std::process::Command::new("cmd")
            .args(["/c", "ping -n 30 127.0.0.1 > nul"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn cmd");
        let pid = child.id();

        assert!(
            process_alive(pid),
            "child should be alive immediately after spawn"
        );

        force_kill(pid);
        let _ = child.wait();

        // After wait() the kernel object is signaled; OpenProcess may still
        // succeed briefly on a zombie, but GetExitCodeProcess will report a
        // non-STILL_ACTIVE code.
        assert!(
            !process_alive(pid),
            "child should report dead after force_kill + wait"
        );
    }
}
