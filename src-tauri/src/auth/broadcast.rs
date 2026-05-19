//! Broadcasts post-startup auth session changes to every webview window.
//!
//! [`RESTORE_EVENT`](super::RESTORE_EVENT) is the one-shot startup signal.
//! After that, a login / logout / refresh in *any* window (or the
//! background refresh) must reach *every* window — otherwise a logout in
//! one window leaves the others rendering a stale authenticated UI. The
//! Rust host is the single source of truth (`AUTH`); this task turns each
//! of its credential-set changes into an app-wide Tauri event carrying the
//! fresh [`Status`].
//!
//! Event-driven, not polled: it reuses `Auth`'s existing `creds_gen` watch
//! (the same signal the credential pusher waits on, bumped on
//! login/restore/refresh/logout), so no new mutation hook is needed.

use std::future::Future;

use tauri::{AppHandle, Emitter, Runtime};
use tokio::sync::watch;

use super::flow::Status;
use super::SESSION_EVENT;

/// Emit a fresh [`Status`] on every credentials change until `shutdown`
/// resolves. Generic over the status source / sink / shutdown so it is
/// unit-testable without Tauri or real time. The initial watch value is
/// *not* emitted (cold start is `RESTORE_EVENT`'s job) — only post-
/// subscribe changes are.
pub(crate) async fn run_session_broadcaster<SF, SFut, EF, S>(
    status_fn: SF,
    emit_fn: EF,
    mut creds_rx: watch::Receiver<u64>,
    shutdown: S,
) where
    SF: Fn() -> SFut,
    SFut: Future<Output = Status>,
    EF: Fn(Status),
    S: Future<Output = ()>,
{
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            changed = creds_rx.changed() => {
                // Err = every sender dropped: the process is shutting
                // down, so there is nothing left to broadcast to.
                if changed.is_err() {
                    return;
                }
                emit_fn(status_fn().await);
            }
        }
    }
}

/// Spawn the long-lived broadcaster. Lives for the process; the runtime
/// drops it on exit (the windows die with the host anyway, so no explicit
/// shutdown signal is needed).
pub fn spawn_session_broadcaster<R: Runtime>(app: &AppHandle<R>) {
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        run_session_broadcaster(
            || crate::auth::AUTH.status(),
            move |status| {
                if let Err(e) = app.emit(SESSION_EVENT, status) {
                    log::warn!("auth: emit session event: {e}");
                }
            },
            crate::auth::AUTH.watch_credentials(),
            std::future::pending::<()>(),
        )
        .await;
    });
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn st(authenticated: bool, email: &str) -> Status {
        Status {
            authenticated,
            email: Some(email.into()),
            name: None,
            sub: None,
        }
    }

    /// Each credentials change emits the *current* status; the initial
    /// watch value is not emitted (that's `RESTORE_EVENT`'s job). Channel-
    /// driven so it's deterministic, mirroring the credential pusher tests.
    #[tokio::test(start_paused = true)]
    async fn emits_current_status_on_each_change() {
        let phase = std::sync::Mutex::new(st(true, "a@x"));
        let (emit_tx, mut emit_rx) = mpsc::unbounded_channel::<Status>();
        let (creds_tx, creds_rx) = watch::channel(0u64);
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();

        let task = run_session_broadcaster(
            || async { phase.lock().unwrap().clone() },
            |s| emit_tx.send(s).unwrap(),
            creds_rx,
            async {
                let _ = shut_rx.await;
            },
        );

        let driver = async {
            // Nothing emitted until a change is signalled.
            assert!(emit_rx.try_recv().is_err(), "emitted before any change");
            creds_tx.send_modify(|v| *v += 1); // e.g. login
            assert_eq!(emit_rx.recv().await.unwrap(), st(true, "a@x"));
            *phase.lock().unwrap() = st(false, ""); // logged out elsewhere
            creds_tx.send_modify(|v| *v += 1);
            assert_eq!(emit_rx.recv().await.unwrap(), st(false, ""));
            shut_tx.send(()).unwrap();
        };

        tokio::join!(task, driver);
    }

    /// Shutdown wins even with no credentials change pending.
    #[tokio::test]
    async fn returns_on_shutdown() {
        let (emit_tx, _emit_rx) = mpsc::unbounded_channel::<Status>();
        let (_creds_tx, creds_rx) = watch::channel(0u64);
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();
        shut_tx.send(()).unwrap();

        run_session_broadcaster(
            || async { st(false, "") },
            move |s| {
                let _ = emit_tx.send(s);
            },
            creds_rx,
            async {
                let _ = shut_rx.await;
            },
        )
        .await; // returning at all is the assertion
    }

    /// Every sender gone (process shutting down) ends the loop even with a
    /// never-resolving shutdown future.
    #[tokio::test]
    async fn returns_when_all_senders_dropped() {
        let (emit_tx, _emit_rx) = mpsc::unbounded_channel::<Status>();
        let (creds_tx, creds_rx) = watch::channel(0u64);
        drop(creds_tx);

        run_session_broadcaster(
            || async { st(false, "") },
            move |s| {
                let _ = emit_tx.send(s);
            },
            creds_rx,
            std::future::pending::<()>(),
        )
        .await; // returning at all is the assertion
    }
}
