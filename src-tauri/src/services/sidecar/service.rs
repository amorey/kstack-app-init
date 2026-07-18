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

use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tauri::ipc::Channel;
use tauri::{AppHandle, Manager, Runtime};
use tauri_plugin_shell::process::{CommandChild, CommandEvent};
use tauri_plugin_shell::ShellExt;
use tokio::sync::watch;

use crate::error::{AppError, Result};

use super::graphql::{FrameSink, GraphqlResponse, QueryClient, SubscriptionClient};
use super::grpc::{AuthStateStream, GrpcClient};
use super::ipc::{Endpoint, DEFAULT_CONNECT_BUDGET};
use super::logs::{forward_sidecar_line, Severity};

/// First-token marker the sidecar prints on stdout once `net.Listen` returns and
/// `Serve` is running (`sidecar/main.go:116`). The drain task matches it to flip
/// the readiness watch; the rest of the line is the socket path the host already
/// knows.
const READY_MARKER: &[u8] = b"READY ";

/// Mutable lifecycle state of the sidecar child, guarded by a single mutex.
///
/// [`SidecarService::graceful_shutdown`] *takes* `child` (dropping it to close
/// stdin) but records `pid` first, so a later [`SidecarService::kill`] can still
/// force-terminate a process that ignored the graceful request. `exited` (set by
/// the drain task on termination) makes `kill` skip the pid fallback once the
/// process is gone — the number may have been reused.
struct State {
    /// The running child, or `None` once graceful_shutdown/kill has run.
    child: Option<CommandChild>,
    /// The child's pid, retained after `child` is dropped so a forced kill
    /// is still possible. `None` once the process has been force-killed.
    pid: Option<u32>,
    /// `true` once the process has been observed to exit. Once set, the
    /// pid is no longer safe to kill (it may have been reused).
    exited: bool,
}

/// Owns the bundled sidecar child process and the IPC bridges that talk to it
/// (query client, SSE reader, gRPC channel), all built around the same
/// [`Endpoint`] picked at spawn time. Construct via [`SidecarService::spawn`].
///
/// Lifecycle state is held behind a [`Mutex`] shared with the drain task via
/// [`Arc`], so shutdown can be driven from any thread.
pub struct SidecarService {
    state: Arc<Mutex<State>>,
    exit_rx: Mutex<Receiver<()>>,
    /// Cloned per call by [`SidecarService::ready`] so concurrent callers don't
    /// contend. The drain task's sender flips it to `true` on [`READY_MARKER`];
    /// on process exit the sender drops, surfacing to any in-flight `wait_for` as
    /// a recv error — the "exited before ready" signal.
    ready_rx: watch::Receiver<bool>,
    query_client: QueryClient,
    subscription_client: SubscriptionClient,
    /// gRPC channel to the sidecar's host-internal control surface (auth-state
    /// watch + login/logout, resync poke), multiplexed over the same socket as
    /// GraphQL via h2c.
    grpc_client: GrpcClient,
}

impl SidecarService {
    /// Spawns the `kstack-sidecar` binary and begins forwarding its output.
    ///
    /// A background task drains the process's event stream, re-emitting each
    /// sidecar log line through `tracing` (see [`forward_sidecar_line`]) and
    /// reporting termination. The returned [`SidecarService`] retains
    /// ownership of the child so it can be shut down later.
    pub fn spawn<R: Runtime>(app: &AppHandle<R>) -> Result<Self> {
        let endpoint = Endpoint::pick(&std::env::temp_dir())?;

        // Per-machine, non-roaming app data dir for the per-cluster SQLite cache
        // and cloud settings/queue. We join a human-readable name onto the OS base
        // (`local_data_dir`) rather than Tauri's `app_local_data_dir`, which
        // derives the leaf from the reverse-DNS bundle id (`sh.kstack.app`); the
        // convention under ~/Library/Application Support is a display name. The
        // base maps to ~/Library/Application Support (macOS), %LOCALAPPDATA%
        // (Windows), ~/.local/share (Linux).
        //
        // A debug build (`tauri dev`) uses a separate "Kstack-dev" sibling so its
        // cache/queue can't collide with an installed release's "Kstack". Created
        // up front so the sidecar can mkdir subdirs under it.
        let dir_name = if cfg!(debug_assertions) {
            "Kstack-dev"
        } else {
            "Kstack"
        };
        let data_dir = app
            .path()
            .local_data_dir()
            .map_err(|e| {
                AppError::Io(std::io::Error::other(format!(
                    "resolve local_data_dir: {e}"
                )))
            })?
            .join(dir_name);
        std::fs::create_dir_all(&data_dir).map_err(AppError::Io)?;

        let mut command = app
            .shell()
            .sidecar("kstack-sidecar")?
            .args(cmd_args(&endpoint, &data_dir));
        // Point the sidecar's keychain at a dev-specific service name in debug
        // builds so a dev run's stored sign-in doesn't share (and clobber) the
        // installed release's entry.
        if cfg!(debug_assertions) {
            command = command.env("KSTACK_KEYCHAIN_SERVICE", dir_name);
        }
        let (mut rx, child) = command.spawn()?;
        let pid = child.pid();

        let state = Arc::new(Mutex::new(State {
            child: Some(child),
            pid: Some(pid),
            exited: false,
        }));

        // Capacity 1 so the drain task never blocks delivering the exit: the slot
        // holds the message even if no one is waiting yet.
        let (exit_tx, exit_rx) = sync_channel::<()>(1);

        // `false` = not ready yet; the drain task flips it to `true` on the READY
        // stdout line.
        let (ready_tx, ready_rx) = watch::channel(false);

        let drain_state = Arc::clone(&state);
        tauri::async_runtime::spawn(async move {
            while let Some(ev) = rx.recv().await {
                match ev {
                    CommandEvent::Stdout(line) => {
                        // Detect readiness before forwarding, so the line still
                        // appears as a normal sidecar info entry in the host logs.
                        if line.starts_with(READY_MARKER) {
                            // Err only if every receiver dropped — nothing to notify.
                            let _ = ready_tx.send(true);
                        }
                        forward_sidecar_line(&line, Severity::Info);
                    }
                    CommandEvent::Stderr(line) => forward_sidecar_line(&line, Severity::Warn),
                    CommandEvent::Terminated(payload) => {
                        tracing::info!(?payload, "sidecar exited");

                        // Record the exit so `kill` skips the pid fallback (the
                        // pid may already be reused).
                        drain_state
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .exited = true;

                        // Wake any thread blocked in graceful_shutdown. try_send:
                        // a full slot or absent receiver is fine.
                        let _ = exit_tx.try_send(());
                    }
                    _ => {}
                }
            }
        });

        let query_client = QueryClient::new(endpoint.clone());
        let subscription_client = SubscriptionClient::new(endpoint.clone());
        let grpc_client = GrpcClient::new(endpoint);

        Ok(Self {
            state,
            exit_rx: Mutex::new(exit_rx),
            ready_rx,
            query_client,
            subscription_client,
            grpc_client,
        })
    }

    /// Waits for the sidecar to announce readiness on stdout ([`READY_MARKER`]).
    /// Resolves the moment the marker arrives, or errors if the sidecar exits
    /// first / the budget elapses.
    ///
    /// A proactive startup gate: without it the frontend's first GraphQL call
    /// absorbs the lazy-dial retry budget while the sidecar is still binding, and
    /// the user perceives it as latency. Event-driven rather than dial-and-retry,
    /// so it resolves in about one tracing hop. Errors:
    /// - `TimedOut` if the marker doesn't arrive within [`DEFAULT_CONNECT_BUDGET`].
    /// - generic `Io` if the drain task ended (process exited) before sending —
    ///   the dropped watch sender surfaces as a recv error.
    pub async fn ready(&self) -> Result<()> {
        let mut rx = self.ready_rx.clone();
        // Fast path: marker already arrived, skip the timeout future.
        if *rx.borrow() {
            return Ok(());
        }
        // `.map(|_| ())` drops the `watch::Ref<'_, bool>` before the match arm —
        // its borrow on `rx` would otherwise trip the borrow checker.
        let waited = tokio::time::timeout(DEFAULT_CONNECT_BUDGET, rx.wait_for(|ready| *ready))
            .await
            .map(|res| res.map(|_| ()));
        match waited {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(AppError::Io(std::io::Error::other(
                "sidecar exited before announcing readiness",
            ))),
            Err(_) => Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "sidecar did not announce readiness within budget",
            ))),
        }
    }

    /// Forwards a GraphQL query/mutation to the sidecar over its UDS HTTP
    /// endpoint. Stateless per-call; see [`QueryClient`].
    pub async fn query(&self, body: String) -> Result<GraphqlResponse> {
        self.query_client.query(body).await
    }

    /// Registers a new GraphQL subscription, opening a dedicated SSE
    /// connection to the sidecar for it. Forwards every inbound envelope to
    /// `channel`. Returns the host-side op id to pass to
    /// [`SidecarService::unsubscribe`].
    pub async fn subscribe(
        &self,
        query: String,
        variables: serde_json::Value,
        channel: Channel<String>,
    ) -> Result<u64> {
        // `subscribe` takes an `Arc<dyn FrameSink>`, keeping the seam open for any
        // host-internal consumer; the webview wraps its `Channel` in `TauriChannelSink`.
        self.subscription_client
            .subscribe(query, variables, Arc::new(TauriChannelSink(channel)))
            .await
    }

    /// Cancels a previously registered subscription. Tolerant of unknown
    /// ids — see [`SubscriptionClient::unsubscribe`].
    pub async fn unsubscribe(&self, id: u64) {
        self.subscription_client.unsubscribe(id).await;
    }

    /// Runs the synchronous login setup phase on the sidecar (loopback bind +
    /// browser open). Returns once setup succeeds or fails; the resulting session
    /// change is delivered via [`Self::watch_auth_state`].
    pub async fn start_login(&self) -> Result<()> {
        self.grpc_client.start_login().await
    }

    /// Clears local credentials and revokes the refresh token (fire-and-forget
    /// revocation on the sidecar).
    pub async fn logout(&self) -> Result<()> {
        self.grpc_client.logout().await
    }

    /// Opens the host-internal auth-state watch stream over gRPC: the current
    /// snapshot first (latest-value), then a fresh snapshot on every session
    /// change. Drives the tray's account submenu. The caller pulls frames with
    /// `stream.message().await`; an error / end-of-stream means the connection
    /// dropped and the supervisor should re-open it.
    pub async fn watch_auth_state(&self) -> Result<AuthStateStream> {
        self.grpc_client.watch_auth_state().await
    }

    /// Nudges the sidecar to resync over gRPC (best-effort). Driven by the
    /// host's wake / network-return supervisor ([`crate::wake`]); the sidecar
    /// broadcasts a `SourceHost` resync to its cluster-sync and settings-sync
    /// engines. Failures are the caller's to swallow — the sidecar's wall-clock
    /// detector is the backstop.
    pub async fn poke(&self) -> Result<()> {
        self.grpc_client.poke().await
    }

    /// Asks the sidecar to shut down gracefully, blocking until it exits or
    /// `timeout` elapses.
    ///
    /// The sidecar treats stdin EOF as a "parent gone" signal and runs its own
    /// clean shutdown (HTTP drain, SSE completion, sync-engine join). We trigger
    /// the EOF by dropping the [`CommandChild`], which drops the stdin pipe —
    /// working identically on every platform, unlike POSIX signals Windows lacks.
    ///
    /// Synchronous: returns only once the process actually exited (per the drain
    /// task) or `timeout` runs out, so it's safe to call from a blocking context
    /// like the `RunEvent` handler.
    ///
    /// Returns `true` on a clean exit within `timeout`, `false` if the grace
    /// period lapsed first — then the caller should follow up with
    /// [`SidecarService::kill`] (the pid is retained so that fallback works). A
    /// no-op returning `true` if already shut down.
    pub fn graceful_shutdown(&self, timeout: Duration) -> bool {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.exited {
                return true;
            }

            // Dropping the CommandChild drops its stdin PipeWriter, delivering
            // EOF to the sidecar. `pid` is left intact. The guard is released
            // here, before blocking below, so the drain task can set `exited`.
            drop(state.child.take());
        }

        // Block until the drain task reports the exit, or the grace period ends.
        // recv_timeout returns Err on both timeout and a dropped sender; either
        // way the process is not known to have exited.
        let exit_rx = self
            .exit_rx
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        exit_rx.recv_timeout(timeout).is_ok()
    }

    /// Force-terminates the sidecar process if it is still running.
    ///
    /// If the live child handle is still held this kills through it directly.
    /// Otherwise — e.g. after [`SidecarService::graceful_shutdown`] dropped the
    /// handle — it falls back to terminating by pid via the platform API, but
    /// *only* if the process has not already been observed to exit: once it
    /// has, the pid may belong to an unrelated process and must not be killed.
    ///
    /// Best-effort: failures are ignored (a stale pid simply means the process
    /// already exited), and the call is a no-op once it has run. Safe to call
    /// more than once. This is immediate; for a clean exit call
    /// [`SidecarService::graceful_shutdown`] first.
    pub fn kill(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Prefer the live handle: it reaps the child and avoids a pid-reuse race.
        // `pid` is cleared regardless so a repeat call is a no-op.
        if let Some(child) = state.child.take() {
            let _ = child.kill();
            state.pid = None;
            return;
        }

        // Handle gone (graceful_shutdown ran): fall back to the pid, but skip it
        // if the process already exited (the kernel may have reused the number).
        if state.exited {
            state.pid = None;
            return;
        }

        if let Some(pid) = state.pid.take() {
            force_kill_by_pid(pid);
        }
    }
}

/// Returns the CLI flags the sidecar binary expects at spawn time. Factored out
/// so the argument contract is unit-testable without the Tauri runtime.
fn cmd_args(socket: &Endpoint, data_dir: &std::path::Path) -> Vec<String> {
    vec![
        "--socket".to_string(),
        socket.as_arg().to_owned(),
        "--data-dir".to_string(),
        data_dir.to_string_lossy().into_owned(),
    ]
}

/// Terminates a process by pid using the platform's native API.
///
/// Best-effort: a stale pid (process already gone) is ignored on both
/// platforms. Used only as the fallback once the [`CommandChild`] handle has
/// been dropped by [`SidecarService::graceful_shutdown`].
#[cfg(unix)]
fn force_kill_by_pid(pid: u32) {
    // SAFETY: `kill(2)` with a valid pid and signal has no memory effects;
    // a stale pid simply returns ESRCH, which we ignore.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGKILL);
    }
}

#[cfg(windows)]
fn force_kill_by_pid(pid: u32) {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{OpenProcess, TerminateProcess, PROCESS_TERMINATE};

    // SAFETY: OpenProcess returns null on failure (e.g. the process already
    // exited), which we check before use; TerminateProcess and CloseHandle
    // are sound on the handle OpenProcess hands back.
    unsafe {
        let handle = OpenProcess(PROCESS_TERMINATE, 0, pid);
        if handle.is_null() {
            return;
        }
        TerminateProcess(handle, 1);
        CloseHandle(handle);
    }
}

/// Adapts Tauri's `Channel<String>` to the [`FrameSink`] trait so `subscribe`
/// needn't know about the Tauri runtime. `Channel::send` errors only if the
/// webview is gone, in which case the frame is safely dropped.
struct TauriChannelSink(Channel<String>);

impl FrameSink for TauriChannelSink {
    fn send_frame(&self, frame: String) {
        let _ = self.0.send(frame);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Locks the host↔sidecar CLI contract: the host hands the sidecar the
    /// listen address via `--socket <path>` and the app data dir via
    /// `--data-dir <path>`. Changing this shape without updating the sidecar
    /// would silently leave the cluster cache pointed at the wrong location (or
    /// the sockets binding to different paths).
    #[test]
    fn cmd_args_passes_socket_and_data_dir() {
        let base = std::env::temp_dir();
        let path = Endpoint::pick(&base).expect("pick should succeed");
        let data_dir = std::path::PathBuf::from("/some/app/data");
        let args = cmd_args(&path, &data_dir);
        assert_eq!(
            args,
            vec![
                "--socket".to_string(),
                path.as_arg().to_owned(),
                "--data-dir".to_string(),
                data_dir.to_string_lossy().into_owned(),
            ]
        );
    }
}
