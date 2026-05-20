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

//! Pushes the host's OAuth access token to the sidecar's always-on engine.
//!
//! The sidecar is stateless re: auth on the request path, but the always-on
//! SyncEngine has no inbound request to carry a token. The host owns the
//! keychain + refresh, so this re-publishes the current access token to the
//! sidecar's host-only `/control/credentials` endpoint.
//!
//! Event-driven, not polled: after a push we sleep until ~75% of the
//! token's lifetime has elapsed (proportional margin, floored), and we also
//! wake immediately whenever `Auth` signals a credentials change
//! (login / restore / refresh / logout) — so a fresh token reaches the
//! sidecar at once, with no fixed-interval no-op wakeups.
//!
//! `tokio::time::sleep` is monotonic and pauses during system suspend, so
//! a long pre-suspend sleep would fire late on resume. The loop also
//! `select!`s on the host wake signal (`wake.rs`): an OS power/network
//! wake re-derives the token at once. `MAX_REARM` is kept as a permanent
//! defense-in-depth backstop — it only matters when a wake is missed or
//! the platform's source isn't wired (same role as the engine's
//! wall-clock backstop), and costs nothing when wakes work.

use std::future::Future;
use std::time::Duration;

use tauri::{AppHandle, Manager, Runtime};
use tokio::sync::watch;

use super::lifecycle::{wait_for_socket, SidecarState};
use super::transport;
use super::wake::changed;

/// Refresh once 25% of the token's remaining lifetime is left (i.e. at
/// ~75% elapsed), but never inside the last `MIN_MARGIN` — that headroom
/// must cover the refresh round-trip plus at least one failed retry.
const MIN_MARGIN: Duration = Duration::from_secs(60);
/// Permanent backstop: caps a single sleep so the
/// monotonic-clock-pauses-during-suspend gap stays bounded if an OS wake
/// is missed or that platform's source isn't wired. The `wake_rx` arm is
/// the fast path; this only fires when it doesn't.
const MAX_REARM: Duration = Duration::from_secs(600);
/// Backoff after a failed push before retrying within the margin.
const RETRY_BACKOFF: Duration = Duration::from_secs(5);

/// How long to sleep before the next refresh, given the wall clock and the
/// token's absolute expiry (Unix seconds). Pure so it is unit-testable.
fn refresh_delay(now: u64, expires_at: u64) -> Duration {
    let remaining = expires_at.saturating_sub(now);
    if remaining == 0 {
        return Duration::ZERO; // already expired — refresh now
    }
    // Margin = 25% of remaining, but at least MIN_MARGIN.
    let margin = (remaining / 4).max(MIN_MARGIN.as_secs());
    let sleep = remaining.saturating_sub(margin);
    Duration::from_secs(sleep).min(MAX_REARM)
}

/// Drive credential pushes until `shutdown` resolves. Generic over the
/// token source / sink / clock so it is unit-testable without Tauri, a
/// socket, or real time. Pushes only when the token changed since the last
/// *successful* push; re-pushes immediately when `creds_rx` signals a
/// change; retries a failed push after a short backoff.
pub(crate) async fn run_credential_pusher<TF, TFut, PF, PFut, NF, S>(
    token_fn: TF,
    push_fn: PF,
    mut creds_rx: watch::Receiver<u64>,
    mut wake_rx: watch::Receiver<u64>,
    now_unix: NF,
    shutdown: S,
) where
    TF: Fn() -> TFut,
    TFut: Future<Output = Result<(String, u64), String>>,
    PF: Fn(String) -> PFut,
    PFut: Future<Output = Result<(), String>>,
    NF: Fn() -> u64,
    S: Future<Output = ()>,
{
    tokio::pin!(shutdown);
    let mut last: Option<String> = None;
    loop {
        match token_fn().await {
            Err(e) => {
                // Logged out / not restored yet: wait for a credentials
                // change (or shutdown). No busy poll.
                log::debug!("credential pusher: no token: {e}");
                tokio::select! {
                    _ = &mut shutdown => return,
                    ok = changed(&mut creds_rx) => {
                        if !ok { return }
                    }
                    ok = changed(&mut wake_rx) => {
                        if !ok { return }
                    }
                }
            }
            Ok((token, expires_at)) => {
                if last.as_deref() != Some(token.as_str()) {
                    if let Err(e) = push_fn(token.clone()).await {
                        log::warn!("credential pusher: push failed, retrying: {e}");
                        tokio::select! {
                            _ = &mut shutdown => return,
                            _ = tokio::time::sleep(RETRY_BACKOFF) => {}
                            ok = changed(&mut creds_rx) => {
                                if !ok { return }
                            }
                            ok = changed(&mut wake_rx) => {
                                if !ok { return }
                            }
                        }
                        continue; // re-derive token + retry
                    }
                    last = Some(token);
                }
                let delay = refresh_delay(now_unix(), expires_at);
                tokio::select! {
                    _ = &mut shutdown => return,
                    _ = tokio::time::sleep(delay) => {}
                    ok = changed(&mut creds_rx) => {
                        if !ok { return }
                    }
                    ok = changed(&mut wake_rx) => {
                        if !ok { return }
                    }
                }
            }
        }
    }
}

/// Resolve the sidecar socket and POST the token to /control/credentials.
async fn push_once<R: Runtime>(app: &AppHandle<R>, token: &str) -> Result<(), String> {
    let state = app
        .try_state::<SidecarState>()
        .ok_or("sidecar state not managed")?;
    let socket = wait_for_socket(state.socket_rx()).await?;
    transport::push_credentials(&socket, token)
        .await
        .map_err(|e| e.to_string())
}

/// Spawn the long-lived pusher task. Lives for the process; the runtime
/// drops it on exit (the sidecar dies with the host anyway, so no explicit
/// shutdown signal is needed).
pub fn spawn_credential_pusher<R: Runtime>(app: &AppHandle<R>, wake_rx: watch::Receiver<u64>) {
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        run_credential_pusher(
            || crate::auth::AUTH.access_token_with_expiry(),
            |token| {
                let app = app.clone();
                async move { push_once(&app, &token).await }
            },
            crate::auth::AUTH.watch_credentials(),
            wake_rx,
            crate::auth::tokens::now,
            std::future::pending::<()>(),
        )
        .await;
    });
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[test]
    fn refresh_delay_is_proportional_floored_and_capped() {
        // 25% margin dominates: remaining=400 → margin=100 → sleep=300.
        assert_eq!(refresh_delay(0, 400), Duration::from_secs(300));
        // MIN_MARGIN floor: remaining=120 → 25%=30 < 60 → margin=60 →
        // sleep=60.
        assert_eq!(refresh_delay(0, 120), Duration::from_secs(60));
        // Past expiry → refresh immediately.
        assert_eq!(refresh_delay(500, 400), Duration::ZERO);
        // Long-lived token → clamped to MAX_REARM (the backstop; the
        // wake arm is the fast path, this bounds a missed/unwired wake).
        assert_eq!(refresh_delay(0, 86_400), MAX_REARM);
        // Inside the floor margin already → no sleep.
        assert_eq!(refresh_delay(0, 30), Duration::ZERO);
    }

    /// Channel-driven so it's deterministic (no reliance on paused-time
    /// auto-advance ordering): initial push, then a credentials-change
    /// signal makes it re-derive and push the new token, retrying a
    /// transient push failure within the margin.
    #[tokio::test(start_paused = true)]
    async fn pushes_then_re_pushes_on_change_and_retries() {
        // Driver flips the phase; token_fn reflects it.
        let phase = Mutex::new(("a".to_string(), 10_000u64));
        let fail_first_b = Mutex::new(true);
        let (push_tx, mut push_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (creds_tx, creds_rx) = watch::channel(0u64);
        let (_wake_tx, wake_rx) = watch::channel(0u64);
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();

        let pusher = run_credential_pusher(
            || async { Ok(phase.lock().unwrap().clone()) },
            |token| async {
                if token == "b" && std::mem::replace(&mut *fail_first_b.lock().unwrap(), false) {
                    return Err("transient".to_string());
                }
                push_tx.send(token).unwrap();
                Ok(())
            },
            creds_rx,
            wake_rx,
            || 0u64,
            async {
                let _ = shut_rx.await;
            },
        );

        let driver = async {
            assert_eq!(push_rx.recv().await.unwrap(), "a");
            *phase.lock().unwrap() = ("b".to_string(), 10_000);
            creds_tx.send_modify(|v| *v += 1);
            // "b" only arrives if the post-failure retry worked.
            assert_eq!(push_rx.recv().await.unwrap(), "b");
            shut_tx.send(()).unwrap();
        };

        tokio::join!(pusher, driver);
    }

    /// An unchanged token is not re-pushed even across repeated signals.
    /// `seen` lets the driver know the pusher actually re-derived, so the
    /// "no second push" check isn't a timing guess.
    #[tokio::test(start_paused = true)]
    async fn dedups_unchanged_token_across_signals() {
        let (push_tx, mut push_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (creds_tx, creds_rx) = watch::channel(0u64);
        let (_wake_tx, wake_rx) = watch::channel(0u64);
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();

        let pusher = run_credential_pusher(
            || {
                let seen_tx = seen_tx.clone();
                async move {
                    seen_tx.send(()).unwrap();
                    Ok(("x".to_string(), 10_000u64))
                }
            },
            |token| async {
                push_tx.send(token).unwrap();
                Ok(())
            },
            creds_rx,
            wake_rx,
            || 0u64,
            async {
                let _ = shut_rx.await;
            },
        );

        let driver = async {
            seen_rx.recv().await.unwrap(); // first derive
            assert_eq!(push_rx.recv().await.unwrap(), "x"); // pushed once
            for _ in 0..3 {
                creds_tx.send_modify(|v| *v += 1);
                seen_rx.recv().await.unwrap(); // re-derived…
                assert!(push_rx.try_recv().is_err(), "re-pushed an unchanged token");
            }
            shut_tx.send(()).unwrap();
        };

        tokio::join!(pusher, driver);
    }

    #[tokio::test(start_paused = true)]
    async fn logged_out_waits_for_signal_then_pushes() {
        let authed = Mutex::new(false);
        let (push_tx, mut push_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (creds_tx, creds_rx) = watch::channel(0u64);
        let (_wake_tx, wake_rx) = watch::channel(0u64);
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();

        let pusher = run_credential_pusher(
            || async {
                if *authed.lock().unwrap() {
                    Ok(("tok".to_string(), 10_000u64))
                } else {
                    Err("not authenticated".to_string())
                }
            },
            |token| async {
                push_tx.send(token).unwrap();
                Ok(())
            },
            creds_rx,
            wake_rx,
            || 0u64,
            async {
                let _ = shut_rx.await;
            },
        );

        let driver = async {
            *authed.lock().unwrap() = true;
            creds_tx.send_modify(|v| *v += 1); // "you logged in"
            assert_eq!(push_rx.recv().await.unwrap(), "tok");
            shut_tx.send(()).unwrap();
        };

        tokio::join!(pusher, driver);
    }

    /// A host wake (OS power-resume / network-change) re-derives the token
    /// and re-pushes — the wake-signal path that, once an OS source is
    /// wired, makes resume recovery instant.
    #[tokio::test(start_paused = true)]
    async fn wake_re_derives_and_re_pushes() {
        let phase = Mutex::new(("a".to_string(), 10_000u64));
        let (push_tx, mut push_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (_creds_tx, creds_rx) = watch::channel(0u64);
        let (wake_tx, wake_rx) = watch::channel(0u64);
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();

        let pusher = run_credential_pusher(
            || async { Ok(phase.lock().unwrap().clone()) },
            |token| async {
                push_tx.send(token).unwrap();
                Ok(())
            },
            creds_rx,
            wake_rx,
            || 0u64,
            async {
                let _ = shut_rx.await;
            },
        );

        let driver = async {
            assert_eq!(push_rx.recv().await.unwrap(), "a"); // initial push, now scheduled
            *phase.lock().unwrap() = ("b".to_string(), 10_000);
            wake_tx.send_modify(|v| *v += 1); // OS resume → re-derive
            assert_eq!(push_rx.recv().await.unwrap(), "b");
            shut_tx.send(()).unwrap();
        };

        tokio::join!(pusher, driver);
    }
}
