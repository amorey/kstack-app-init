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
use super::ipc::{Endpoint, Target, DEFAULT_CONNECT_BUDGET};
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
        let endpoint = Endpoint::pick(&runtime_dir()?)?;

        // Per-machine app data dir for the sidecar's SQLite cache and cloud
        // settings/queue. Human-readable leaf on `local_data_dir`, not Tauri's
        // bundle-id `app_local_data_dir` (Application Support convention is a
        // display name). Created up front so the sidecar can mkdir subdirs.
        let data_dir = app
            .path()
            .local_data_dir()
            .map_err(|e| {
                AppError::Io(std::io::Error::other(format!(
                    "resolve local_data_dir: {e}"
                )))
            })?
            .join(APP_DIR_NAME);
        ensure_data_dir(&data_dir)?;

        let (mut rx, child) = app
            .shell()
            .sidecar("kstack-sidecar")?
            .args(cmd_args(&endpoint, &data_dir))
            .spawn()?;
        let pid = child.pid();

        let state = Arc::new(Mutex::new(State {
            child: Some(child),
            pid: Some(pid),
            exited: false,
        }));

        // What the dialers check every peer against. `State.pid` cannot serve:
        // it outlives the child so `kill` can still reach it, and the kernel
        // may have reassigned the number by then.
        let (expect_tx, expect_rx) = watch::channel(Some(pid));

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

                        // No sidecar, so no peer is legitimate any more.
                        let _ = expect_tx.send(None);

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

        let target = Target::new(endpoint, expect_rx);
        let query_client = QueryClient::new(target.clone());
        let subscription_client = SubscriptionClient::new(target.clone());
        let grpc_client = GrpcClient::new(target);

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

/// The endpoints the app signs in against. Constants, never read from this
/// process's environment: the sidecar inherits that environment, so reading
/// them here would move the redirection risk rather than close it. A dev run
/// overrides them inside the sidecar's own debug build.
const CLOUD_URL: &str = "https://api.kstack.sh";
const OAUTH_ISSUER: &str = "https://oauth.kstack.sh";
const OAUTH_CLIENT_ID: &str = "kstack-desktop";

/// The data-dir leaf under `local_data_dir`. Debug builds use a `-dev` sibling
/// so a dev run never collides with an installed release.
const APP_DIR_NAME: &str = if cfg!(debug_assertions) {
    "Kstack-dev"
} else {
    "Kstack"
};

/// The OS-keychain service the sidecar stores sign-in under: the product name,
/// as the entry is user-visible in Keychain Access and Credential Manager.
/// Renaming it orphans every stored sign-in, so it is its own constant even
/// though it matches the data-dir leaf today.
const KEYCHAIN_SERVICE: &str = if cfg!(debug_assertions) {
    "Kstack-dev"
} else {
    "Kstack"
};

/// CLI flags for the sidecar; a free function so the contract is unit-testable
/// without the Tauri runtime.
///
/// `--host-pid` is the only process the sidecar's endpoint will serve. We are
/// its sole client — the webview reaches it through us — so this closes the
/// endpoint to every other process running as the user.
fn cmd_args(socket: &Endpoint, data_dir: &std::path::Path) -> Vec<String> {
    [
        ("--socket", socket.as_arg().to_owned()),
        ("--data-dir", data_dir.to_string_lossy().into_owned()),
        ("--host-pid", std::process::id().to_string()),
        ("--cloud-url", CLOUD_URL.to_owned()),
        ("--oauth-issuer", OAUTH_ISSUER.to_owned()),
        ("--oauth-client-id", OAUTH_CLIENT_ID.to_owned()),
        ("--keychain-service", KEYCHAIN_SERVICE.to_owned()),
    ]
    .into_iter()
    .flat_map(|(flag, value)| [flag.to_owned(), value])
    .collect()
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

/// The directory the IPC endpoint is created in, owner-only.
///
/// The gain is on Linux, where the fallback would be the shared, sticky-bit
/// `/tmp`: `$XDG_RUNTIME_DIR` is a per-user directory the session manager
/// already creates `0700`. On macOS `$TMPDIR` is per-user and `0700` too, so
/// there the subdirectory is tidiness rather than a fix. Windows never gets
/// here — its pipe namespace is flat and has no directory to make private, so
/// the DACL and the peer check are the whole policy.
///
/// A directory left behind by a crashed run is adopted, not refused. Nothing
/// sweeps it, because a second window of a running app shares it and a
/// concurrent copy owns its own.
#[cfg(unix)]
fn runtime_dir() -> Result<std::path::PathBuf> {
    // SAFETY: `geteuid` reads process state and cannot fail.
    let uid = unsafe { libc::geteuid() };
    let xdg = std::env::var_os("XDG_RUNTIME_DIR")
        .map(std::path::PathBuf::from)
        .filter(|p| p.is_dir());
    let dir = runtime_dir_path(xdg.as_deref(), uid);
    ensure_private_dir(&dir, uid)?;
    Ok(dir)
}

/// Creates `dir` `0700`, or adopts one an earlier run left behind — but only
/// once this user is proved to own it, and never through a symlink.
///
/// The order is the whole protection: tightening first would let another user
/// point `/tmp/kstack-<uid>` at a directory of ours and have the app chmod it
/// on their behalf. Past the check there is no swap to lose to — `/tmp` is
/// sticky, so only the owner can replace the entry.
#[cfg(unix)]
fn ensure_private_dir(dir: &std::path::Path, uid: u32) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};

    match std::fs::DirBuilder::new().mode(0o700).create(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            // A name is not ownership, and `symlink_metadata` is what refuses a
            // link: whoever planted it owns where it leads.
            let meta = std::fs::symlink_metadata(dir).map_err(AppError::Io)?;
            if !meta.is_dir() || meta.uid() != uid {
                return Err(AppError::Io(std::io::Error::other(format!(
                    "{} is not a directory owned by this user",
                    dir.display()
                ))));
            }
        }
        Err(e) => return Err(AppError::Io(e)),
    }
    // mkdir's mode is masked by the umask, and an adopted directory carries
    // whatever mode the earlier run left it with.
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).map_err(AppError::Io)
}

#[cfg(not(unix))]
fn runtime_dir() -> Result<std::path::PathBuf> {
    Ok(std::env::temp_dir())
}

/// Where [`runtime_dir`] puts the directory: under `$XDG_RUNTIME_DIR` when the
/// session manager gave us one, else under the temp dir.
///
/// The temp dir is shared between users on Linux, so the uid goes in the name:
/// one directory for all of them means the first user to run owns it `0700` and
/// every later user's chmod fails with EPERM.
#[cfg(unix)]
fn runtime_dir_path(xdg: Option<&std::path::Path>, uid: u32) -> std::path::PathBuf {
    match xdg {
        Some(base) => base.join(RUNTIME_DIR_NAME),
        None => std::env::temp_dir().join(format!("{RUNTIME_DIR_NAME}-{uid}")),
    }
}

/// Leaf for [`runtime_dir`]. A `-dev` sibling like [`APP_DIR_NAME`], so a dev
/// run and an installed release never share a socket directory.
#[cfg(unix)]
const RUNTIME_DIR_NAME: &str = if cfg!(debug_assertions) {
    "kstack-dev"
} else {
    "kstack"
};

/// Creates the sidecar's data directory owner-only, and tightens one an earlier build
/// already created under the umask. On Windows the per-user `%LOCALAPPDATA%` ACL
/// already restricts it, so the plain create is enough.
fn ensure_data_dir(path: &std::path::Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
            .map_err(AppError::Io)?;
        // A mode on DirBuilder applies only to directories it creates.
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(AppError::Io)?;
    }
    #[cfg(not(unix))]
    std::fs::create_dir_all(path).map_err(AppError::Io)?;
    Ok(())
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
    /// `--host-pid`, and the endpoints); changing it without updating the
    /// sidecar silently misroutes the cache or the socket, drops the peer
    /// check, or leaves sign-in on an inherited environment variable.
    #[test]
    fn cmd_args_passes_socket_data_dir_host_pid_and_endpoints() {
        let base = std::env::temp_dir();
        let path = Endpoint::pick(&base).expect("pick should succeed");
        let data_dir = std::path::PathBuf::from("/some/app/data");
        assert_eq!(
            cmd_args(&path, &data_dir),
            [
                "--socket",
                path.as_arg(),
                "--data-dir",
                &data_dir.to_string_lossy(),
                "--host-pid",
                &std::process::id().to_string(),
                "--cloud-url",
                CLOUD_URL,
                "--oauth-issuer",
                OAUTH_ISSUER,
                "--oauth-client-id",
                OAUTH_CLIENT_ID,
                "--keychain-service",
                KEYCHAIN_SERVICE,
            ]
        );
    }

    /// The endpoint's directory is the wall around the socket. `/tmp` is shared
    /// and sticky-bit only, so anything that can enter the directory can reach
    /// the address the host is about to dial.
    #[cfg(unix)]
    #[test]
    fn runtime_dir_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = runtime_dir().expect("runtime dir");
        assert_eq!(
            std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700,
            "{} must be enterable only by this user",
            dir.display()
        );
    }

    /// Without `$XDG_RUNTIME_DIR` the fallback lands in a temp dir every user
    /// shares, so two users must not name the same directory: the first to run
    /// owns it `0700` and the second cannot chmod it.
    #[cfg(unix)]
    #[test]
    fn runtime_dir_path_is_per_user_outside_xdg() {
        let mine = runtime_dir_path(None, 1000);
        let theirs = runtime_dir_path(None, 1001);
        assert_ne!(mine, theirs);

        // $XDG_RUNTIME_DIR is already per-user, so the uid stays out of the name.
        let xdg = std::path::Path::new("/run/user/1000");
        assert_eq!(
            runtime_dir_path(Some(xdg), 1000),
            xdg.join(RUNTIME_DIR_NAME)
        );
    }

    /// Another user can plant the fallback name as a symlink into a directory of
    /// ours. Refusing it is half the fix; the other half is refusing it *before*
    /// the chmod, or the app tightens the target on the attacker's behalf.
    #[cfg(unix)]
    #[test]
    fn ensure_private_dir_refuses_a_symlink_without_touching_its_target() {
        use std::os::unix::fs::PermissionsExt;

        let base = std::env::temp_dir().join(format!("kstack-symlink-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let target = base.join("victim");
        std::fs::create_dir_all(&target).expect("create victim");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755))
            .expect("loosen victim");

        let planted = base.join("planted");
        std::os::unix::fs::symlink(&target, &planted).expect("plant symlink");

        // SAFETY: `geteuid` reads process state and cannot fail.
        let uid = unsafe { libc::geteuid() };
        assert!(ensure_private_dir(&planted, uid).is_err());
        assert_eq!(
            std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o755,
            "the symlink's target must be left alone"
        );

        let _ = std::fs::remove_dir_all(&base);
    }

    /// The data directory is the outer wall around the sidecar's caches, and the
    /// only one Windows has. Unix only — Windows has no POSIX mode bits.
    #[cfg(unix)]
    #[test]
    fn ensure_data_dir_creates_and_tightens_to_0700() {
        use std::os::unix::fs::PermissionsExt;

        let base = std::env::temp_dir().join(format!("kstack-ensure-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);

        let fresh = base.join("fresh");
        ensure_data_dir(&fresh).expect("create");
        assert_eq!(
            std::fs::metadata(&fresh).unwrap().permissions().mode() & 0o777,
            0o700
        );

        // Every install before this landed has a 0755 directory, and a mode on
        // DirBuilder applies only to directories it creates.
        let existing = base.join("existing");
        std::fs::create_dir(&existing).unwrap();
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o755)).unwrap();
        ensure_data_dir(&existing).expect("tighten");
        assert_eq!(
            std::fs::metadata(&existing).unwrap().permissions().mode() & 0o777,
            0o700
        );

        std::fs::remove_dir_all(&base).unwrap();
    }
}
