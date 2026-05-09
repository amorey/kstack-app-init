//! Sidecar process lifecycle: spawn the externalBin, watch its stdout for
//! the READY line, and kill it on app exit.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use tauri::{App, AppHandle, Manager};
use tauri_plugin_shell::process::{CommandChild, CommandEvent};
use tauri_plugin_shell::ShellExt;
use tokio::sync::watch;

use super::transport::READY_PREFIX;

/// Time `graphql_query` will wait for the sidecar to publish its socket
/// path before failing. Sidecar normally prints READY within ~50ms.
const READY_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct SidecarState {
    /// `None` until the stdout reader sees `READY <prefix><path>`.
    /// `watch` so commands can `await` readiness instead of polling.
    socket: watch::Receiver<Option<PathBuf>>,
    /// Held for the lifetime of the app so we can `kill()` on Exit.
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

/// Start the sidecar binary, install `SidecarState`, and watch its stdout.
/// Returns once the child is spawned; the READY line is filled asynchronously.
pub fn spawn(app: &App) -> Result<(), Box<dyn std::error::Error>> {
    let (mut rx, child) = app.shell().sidecar("kstack-sidecar")?.spawn()?;

    let (socket_tx, socket_rx) = watch::channel::<Option<PathBuf>>(None);
    app.manage(SidecarState {
        socket: socket_rx,
        child: Mutex::new(Some(child)),
    });

    tauri::async_runtime::spawn(async move {
        let mut ready_seen = false;
        while let Some(ev) = rx.recv().await {
            match ev {
                CommandEvent::Stdout(line) => {
                    let s = String::from_utf8_lossy(&line);
                    if !ready_seen {
                        if let Some(rest) = s.strip_prefix(READY_PREFIX) {
                            let _ = socket_tx.send(Some(PathBuf::from(rest.trim())));
                            ready_seen = true;
                            continue;
                        }
                    }
                    eprintln!("[sidecar:stdout] {}", s.trim_end());
                }
                CommandEvent::Stderr(line) => {
                    eprintln!(
                        "[sidecar:stderr] {}",
                        String::from_utf8_lossy(&line).trim_end()
                    );
                }
                CommandEvent::Terminated(p) => {
                    eprintln!("[sidecar] terminated: {p:?}");
                    break;
                }
                _ => {}
            }
        }
    });

    Ok(())
}

/// Kill the sidecar child. Safe to call multiple times; later calls no-op.
pub fn shutdown(app: &AppHandle) {
    if let Some(state) = app.try_state::<SidecarState>() {
        // Recover the inner guard even on poison: a panicked previous holder
        // shouldn't block our last-ditch attempt to kill the child.
        let mut guard = state
            .child
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(child) = guard.take() {
            let _ = child.kill();
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

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
        // Yield so the task gets to its first `changed().await`.
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
}
