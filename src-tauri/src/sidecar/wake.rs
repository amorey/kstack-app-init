//! Host wake signal. OS power-resume / network-change events fan out to
//! two consumers so the always-on engine recovers immediately after
//! suspend instead of waiting out a timer:
//!
//!   - the credential pusher (in-process) re-derives + re-pushes the
//!     token, so a token that expired during suspend is refreshed at once;
//!   - a poster `POST`s the sidecar's host-only `/control/resync`, which
//!     pokes the engine into an immediate reconnect/resync.
//!
//! The signal is a `watch<u64>` generation (same shape as `Auth`'s
//! credentials watch); consumers `select!` on `.changed()`. The sidecar
//! engine's wall-clock backstop still covers any platform where a source
//! isn't wired yet or an event is missed.

use std::future::Future;

use tauri::{AppHandle, Manager, Runtime};
use tokio::sync::watch;

use super::lifecycle::{wait_for_socket, SidecarState};
use super::transport;

/// Broadcasts wake events. Cheap to clone; `wake()` bumps an opaque
/// generation counter (only "it changed" matters) that every subscriber
/// observes via `watch::Receiver::changed`.
#[derive(Clone)]
pub struct Waker(watch::Sender<u64>);

impl Waker {
    pub fn new() -> Self {
        // Drop the initial receiver; subscribers come via subscribe().
        // send_modify works with zero receivers (watch retains internal).
        Self(watch::channel(0u64).0)
    }

    /// Signal a wake. Idempotent/coalescing: bursts collapse to one
    /// observed change.
    pub fn wake(&self) {
        self.0.send_modify(|g| *g = g.wrapping_add(1));
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.0.subscribe()
    }
}

impl Default for Waker {
    fn default() -> Self {
        Self::new()
    }
}

/// Await a `watch` change. `true` = changed; `false` = every sender is
/// gone, which for our process-lived signals means "shut down". Names the
/// otherwise-bare `.changed().await.is_ok()` that recurs across the
/// wake/credentials `select!` arms.
pub(crate) async fn changed(rx: &mut watch::Receiver<u64>) -> bool {
    rx.changed().await.is_ok()
}

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "windows")]
mod win;

/// Spawn the per-OS wake listeners. They `waker.wake()` on power-resume /
/// network-change. Takes the `Waker` by value (it's `Clone`) so an
/// implementation can move it into OS observers / its own task. On
/// platforms without a source yet this is a no-op and the engine
/// wall-clock + credential-pusher `MAX_REARM` backstops cover
/// suspend/resume.
#[cfg(target_os = "macos")]
fn run_os_wake_sources(waker: Waker) {
    macos::spawn(waker);
}

#[cfg(target_os = "linux")]
fn run_os_wake_sources(waker: Waker) {
    // logind/NetworkManager listening is async (zbus); run it on the
    // app's tokio runtime.
    tauri::async_runtime::spawn(linux::run(waker));
}

#[cfg(target_os = "windows")]
fn run_os_wake_sources(waker: Waker) {
    win::spawn(waker);
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
fn run_os_wake_sources(_waker: Waker) {}

/// Drive a `/control/resync` push on every wake until `shutdown`. Generic
/// over the push sink so it's unit-testable without Tauri or a socket.
pub(crate) async fn run_resync_poster<PF, PFut, S>(
    mut wake_rx: watch::Receiver<u64>,
    push_fn: PF,
    shutdown: S,
) where
    PF: Fn() -> PFut,
    PFut: Future<Output = Result<(), String>>,
    S: Future<Output = ()>,
{
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            ok = changed(&mut wake_rx) => {
                if !ok { return }
                if let Err(e) = push_fn().await {
                    // Best-effort: lower severity than the credential push
                    // (which is the sidecar's only token source and
                    // retries) — the engine has its own wall-clock
                    // backstop and the next wake retries this.
                    log::debug!("resync poster: push failed: {e}");
                }
            }
        }
    }
}

/// Resolve the sidecar socket and POST /control/resync.
async fn push_resync_once<R: Runtime>(app: &AppHandle<R>) -> Result<(), String> {
    let state = app
        .try_state::<SidecarState>()
        .ok_or("sidecar state not managed")?;
    let socket = wait_for_socket(state.socket_rx()).await?;
    transport::push_resync(&socket)
        .await
        .map_err(|e| e.to_string())
}

/// Create the shared `Waker`, start the OS listeners and the resync
/// poster, and return a receiver for the credential pusher to subscribe.
///
/// The `Waker` (the sole `watch::Sender`) is **moved into the long-lived
/// poster task** so it lives for the whole process — otherwise it would
/// drop with the caller's scope and every subscriber's `changed()` would
/// immediately error, silently killing the poster *and* the credential
/// pusher.
pub fn spawn_wake<R: Runtime>(app: &AppHandle<R>) -> watch::Receiver<u64> {
    let waker = Waker::new();
    let pusher_rx = waker.subscribe();
    let poster_rx = waker.subscribe();
    run_os_wake_sources(waker.clone());

    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        // Hold the Waker for the task's (process) lifetime — this is what
        // keeps the signal alive for every subscriber.
        let _waker = waker;
        run_resync_poster(
            poster_rx,
            || push_resync_once(&app),
            std::future::pending::<()>(),
        )
        .await;
    });
    pusher_rx
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn wake_is_observed_by_subscribers_and_coalesces() {
        let w = Waker::new();
        let mut rx = w.subscribe();
        w.wake();
        rx.changed().await.unwrap(); // observed
                                     // No pending change after consuming it.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), rx.changed())
                .await
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn poster_pushes_on_wake_and_stops_on_shutdown() {
        let w = Waker::new();
        let pushes = Mutex::new(0u32);
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();

        let poster = run_resync_poster(
            w.subscribe(),
            || async {
                *pushes.lock().unwrap() += 1;
                Ok(())
            },
            async {
                let _ = shut_rx.await;
            },
        );
        let driver = async {
            w.wake();
            // Wait until the poster has handled it.
            for _ in 0..1000 {
                if *pushes.lock().unwrap() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            shut_tx.send(()).unwrap();
        };
        tokio::join!(poster, driver);
        assert_eq!(*pushes.lock().unwrap(), 1);
    }
}
