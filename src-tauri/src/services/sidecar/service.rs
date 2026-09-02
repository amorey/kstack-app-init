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

/// First-token marker the sidecar prints on stdout once its listener is up
/// (`sidecar/main.go`); the drain task matches it to flip the readiness watch.
const READY_MARKER: &[u8] = b"READY ";

/// Sidecar child lifecycle state. `graceful_shutdown` takes `child` (dropping
/// it closes stdin) but keeps `pid` so `kill` can still force-terminate;
/// `exited` makes `kill` skip the pid fallback — the number may be reused.
struct State {
    /// The running child, or `None` once graceful_shutdown/kill has run.
    child: Option<CommandChild>,
    /// Retained after `child` is dropped; `None` once force-killed.
    pid: Option<u32>,
    /// Set by the drain task; once true the pid is not safe to kill.
    exited: bool,
}

/// Owns the bundled sidecar child process and its IPC bridges (query client,
/// SSE reader, gRPC channel), all on the [`Endpoint`] picked at spawn time.
/// Construct via [`SidecarService::spawn`]. Shutdown can be driven from any
/// thread (state behind `Arc<Mutex>`).
pub struct SidecarService {
    state: Arc<Mutex<State>>,
    exit_rx: Mutex<Receiver<()>>,
    /// Flipped true on [`READY_MARKER`]; on process exit the sender drops,
    /// surfacing to `wait_for` as a recv error ("exited before ready").
    ready_rx: watch::Receiver<bool>,
    query_client: QueryClient,
    subscription_client: SubscriptionClient,
    /// Host-internal control surface (auth watch/login/logout, poke), h2c over
    /// the same socket as GraphQL. See docs/adr/2026-08-09-single-socket-h2c.md.
    grpc_client: GrpcClient,
}

impl SidecarService {
    /// Spawns the `kstack-sidecar` binary; a background task drains its event
    /// stream (log forwarding + termination reporting).
    pub fn spawn<R: Runtime>(app: &AppHandle<R>) -> Result<Self> {
        let endpoint = Endpoint::pick(&std::env::temp_dir())?;

        // Per-machine app data dir for the sidecar's SQLite cache and cloud
        // settings/queue. Human-readable leaf on `local_data_dir`, not Tauri's
        // bundle-id `app_local_data_dir` (Application Support convention is a
        // display name). Debug builds use a "Kstack-dev" sibling so dev can't
        // collide with an installed release. Created up front so the sidecar
        // can mkdir subdirs.
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
        // Dev-specific keychain service name so a dev run's stored sign-in
        // doesn't clobber the installed release's entry.
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

        // Capacity 1 so the drain task never blocks delivering the exit.
        let (exit_tx, exit_rx) = sync_channel::<()>(1);

        let (ready_tx, ready_rx) = watch::channel(false);

        let drain_state = Arc::clone(&state);
        tauri::async_runtime::spawn(async move {
            while let Some(ev) = rx.recv().await {
                match ev {
                    CommandEvent::Stdout(line) => {
                        // Detect readiness, then still forward the line as a
                        // normal log entry.
                        if line.starts_with(READY_MARKER) {
                            let _ = ready_tx.send(true);
                        }
                        forward_sidecar_line(&line, Severity::Info);
                    }
                    CommandEvent::Stderr(line) => forward_sidecar_line(&line, Severity::Warn),
                    CommandEvent::Terminated(payload) => {
                        tracing::info!(?payload, "sidecar exited");

                        // `kill` must skip the pid fallback after this (pid may
                        // be reused).
                        drain_state
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .exited = true;

                        // Wake any thread blocked in graceful_shutdown.
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

    /// Waits for the sidecar's [`READY_MARKER`]; errors if the sidecar exits
    /// first (`Io`) or [`DEFAULT_CONNECT_BUDGET`] elapses (`TimedOut`). The
    /// frontend's startup gate — without it the first GraphQL call silently
    /// absorbs the bind-wait latency.
    pub async fn ready(&self) -> Result<()> {
        let mut rx = self.ready_rx.clone();
        if *rx.borrow() {
            return Ok(());
        }
        // `.map(|_| ())` drops the `watch::Ref` before the match arm — its
        // borrow on `rx` would otherwise trip the borrow checker.
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

    /// Forwards a GraphQL query/mutation over UDS HTTP; see [`QueryClient`].
    pub async fn query(&self, body: String) -> Result<GraphqlResponse> {
        self.query_client.query(body).await
    }

    /// Registers a GraphQL subscription on its own SSE connection, forwarding
    /// envelopes to `channel`. Returns the op id for
    /// [`SidecarService::unsubscribe`]. `webview` is the label of the webview
    /// that owns the channel, so [`SidecarService::cancel_webview`] can reach
    /// it.
    pub async fn subscribe(
        &self,
        query: String,
        variables: serde_json::Value,
        channel: Channel<String>,
        webview: String,
    ) -> Result<u64> {
        // `FrameSink` is the delivery seam; the webview's `Channel` is wrapped
        // in `TauriChannelSink`.
        self.subscription_client
            .subscribe(
                query,
                variables,
                Arc::new(TauriChannelSink(channel)),
                webview,
            )
            .await
    }

    /// Cancels a subscription; tolerant of unknown ids — see
    /// [`SubscriptionClient::unsubscribe`].
    pub async fn unsubscribe(&self, id: u64) {
        self.subscription_client.unsubscribe(id).await;
    }

    /// Cancels every subscription a webview opened — see
    /// [`SubscriptionClient::cancel_webview`].
    pub fn cancel_webview(&self, webview: &str) {
        self.subscription_client.cancel_webview(webview);
    }

    /// Runs the sidecar's synchronous login setup (loopback bind + browser
    /// open); the resulting session change arrives via
    /// [`Self::watch_auth_state`].
    pub async fn start_login(&self) -> Result<()> {
        self.grpc_client.start_login().await
    }

    /// Clears local credentials and revokes the refresh token.
    pub async fn logout(&self) -> Result<()> {
        self.grpc_client.logout().await
    }

    /// Opens the auth-state watch stream (current snapshot first, then one per
    /// session change). Error / end-of-stream means the connection dropped —
    /// the supervisor re-opens it.
    pub async fn watch_auth_state(&self) -> Result<AuthStateStream> {
        self.grpc_client.watch_auth_state().await
    }

    /// Best-effort resync nudge, driven by [`crate::wake`]; failures are the
    /// caller's to swallow. See docs/adr/2026-08-09-poke-resync-fanout.md.
    pub async fn poke(&self) -> Result<()> {
        self.grpc_client.poke().await
    }

    /// Graceful shutdown: dropping the [`CommandChild`] closes stdin, and the
    /// sidecar treats stdin EOF as "parent gone" (cross-platform, unlike POSIX
    /// signals). Blocks until the process exits or `timeout` lapses — safe from
    /// the `RunEvent` handler. Returns `false` on timeout (follow up with
    /// [`SidecarService::kill`]; the pid is retained for that). No-op → `true`
    /// if already shut down.
    pub fn graceful_shutdown(&self, timeout: Duration) -> bool {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.exited {
                return true;
            }

            // Drops stdin → EOF. `pid` stays. Guard released before blocking so
            // the drain task can set `exited`.
            drop(state.child.take());
        }

        // recv_timeout errs on both timeout and a dropped sender; either way
        // the process is not known to have exited.
        let exit_rx = self
            .exit_rx
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        exit_rx.recv_timeout(timeout).is_ok()
    }

    /// Force-terminates the sidecar. Kills through the live child handle if
    /// held, else by retained pid — but only if the process hasn't been
    /// observed to exit (a reused pid must not be killed). Best-effort and
    /// idempotent; for a clean exit call
    /// [`SidecarService::graceful_shutdown`] first.
    pub fn kill(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Prefer the live handle: reaps the child, no pid-reuse race.
        if let Some(child) = state.child.take() {
            let _ = child.kill();
            state.pid = None;
            return;
        }

        // Handle gone: pid fallback, unless the process already exited (the
        // kernel may have reused the number).
        if state.exited {
            state.pid = None;
            return;
        }

        if let Some(pid) = state.pid.take() {
            force_kill_by_pid(pid);
        }
    }
}

/// CLI flags for the sidecar; a free function so the contract is unit-testable
/// without the Tauri runtime.
///
/// `--host-pid` is the only process the sidecar's endpoint will serve. We are
/// its sole client — the webview reaches it through us — so this closes the
/// endpoint to every other process running as the user.
fn cmd_args(socket: &Endpoint, data_dir: &std::path::Path) -> Vec<String> {
    vec![
        "--socket".to_string(),
        socket.as_arg().to_owned(),
        "--data-dir".to_string(),
        data_dir.to_string_lossy().into_owned(),
        "--host-pid".to_string(),
        std::process::id().to_string(),
    ]
}

/// Best-effort kill by pid — the fallback once [`SidecarService::graceful_shutdown`]
/// dropped the child handle. Stale pids are ignored.
#[cfg(unix)]
fn force_kill_by_pid(pid: u32) {
    // SAFETY: kill(2) has no memory effects; a stale pid returns ESRCH.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGKILL);
    }
}

#[cfg(windows)]
fn force_kill_by_pid(pid: u32) {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{OpenProcess, TerminateProcess, PROCESS_TERMINATE};

    // SAFETY: OpenProcess returns null on failure (checked); TerminateProcess
    // and CloseHandle are sound on the handle it returns.
    unsafe {
        let handle = OpenProcess(PROCESS_TERMINATE, 0, pid);
        if handle.is_null() {
            return;
        }
        TerminateProcess(handle, 1);
        CloseHandle(handle);
    }
}

/// Adapts Tauri's `Channel<String>` to [`FrameSink`]. `send` errors only if
/// the webview is gone; the frame is safely dropped.
struct TauriChannelSink(Channel<String>);

impl FrameSink for TauriChannelSink {
    fn send_frame(&self, frame: String) {
        let _ = self.0.send(frame);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the host↔sidecar CLI contract (`--socket`, `--data-dir`,
    /// `--host-pid`); changing it without updating the sidecar silently
    /// misroutes the cache or the socket, or drops the peer check.
    #[test]
    fn cmd_args_passes_socket_data_dir_and_host_pid() {
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
                "--host-pid".to_string(),
                std::process::id().to_string(),
            ]
        );
    }
}
