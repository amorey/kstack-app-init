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

//! Proactive token refresh driver. Owned by `auth` because *when to
//! refresh* is an auth-layer concern; other subsystems just observe the
//! resulting credentials-change signal.
//!
//! The loop snapshots the current refreshable token's `expires_at`,
//! sleeps until ~75% of its lifetime has elapsed (`MIN_MARGIN` floor,
//! `MAX_REARM` cap), and on wake calls `ensure_fresh` — which is just
//! `Auth::access_token_with_expiry()` with its return value dropped, so
//! the same expiry-gated refresh path used by request callers runs here
//! too. A successful refresh bumps `creds_gen`; we observe that, snapshot
//! the new expiry, and resume.
//!
//! Three event sources, all on one `select!`:
//! - **Timer** — fires at ~75% elapsed; the routine wake.
//! - **Wake** — OS power-resume / network-change (`crate::wake`).
//!   `tokio::time::sleep` is monotonic and pauses during suspend, so a
//!   long pre-suspend sleep would fire late on resume; the wake arm makes
//!   recovery immediate. `MAX_REARM` is a defense-in-depth backstop for
//!   unwired platforms / missed wakes.
//! - **Credentials change** — login/refresh/logout bumped `creds_gen`
//!   from elsewhere (initial `try_restore`, a manual login). Re-snapshot
//!   and resume; no refresh call (Hydra rotates RTs — we don't want to
//!   burn a just-issued one).

use std::future::Future;
use std::time::Duration;

use tokio::sync::watch;

use crate::wake::changed;

/// Refresh once 25% of the token's remaining lifetime is left (~75%
/// elapsed), but never inside the last `MIN_MARGIN`. The floor must
/// cover one refresh round-trip plus at least one retry, so short-lived
/// tokens don't trip a hot loop.
const MIN_MARGIN: Duration = Duration::from_secs(60);
/// Defense-in-depth cap on a single sleep. The wake arm is the fast
/// path for resume; this only fires when wakes are missed or that
/// platform's source isn't wired.
const MAX_REARM: Duration = Duration::from_secs(600);
/// Backoff after a failed refresh before retrying.
const RETRY_BACKOFF: Duration = Duration::from_secs(5);

/// Sleep length before the next refresh, given the wall clock and the
/// token's absolute expiry (Unix seconds). Pure so it is unit-testable.
fn refresh_delay(now: u64, expires_at: u64) -> Duration {
    let remaining = expires_at.saturating_sub(now);
    if remaining == 0 {
        return Duration::ZERO;
    }
    let margin = (remaining / 4).max(MIN_MARGIN.as_secs());
    let sleep = remaining.saturating_sub(margin);
    Duration::from_secs(sleep).min(MAX_REARM)
}

#[derive(Debug)]
enum Trigger {
    Timer,
    Wake,
    CredsChange,
}

/// Drive auth refreshes until `shutdown` resolves. Generic over the
/// expiry source / refresh sink / clock so it is unit-testable without
/// Tauri or real time.
///
/// - `expiry_fn`: `Some(expires_at)` if there's a refreshable token,
///   `None` if logged out (or no RT).
/// - `refresh_fn`: "ensure fresh" — typically `access_token_with_expiry`
///   with the result dropped; it gates on the auth-layer expiry check
///   so calling it when the token is still fresh is a cheap no-op.
pub(crate) async fn run_auth_refresher<EF, EFut, RF, RFut, NF, S>(
    expiry_fn: EF,
    refresh_fn: RF,
    mut creds_rx: watch::Receiver<u64>,
    mut wake_rx: watch::Receiver<u64>,
    now_unix: NF,
    shutdown: S,
) where
    EF: Fn() -> EFut,
    EFut: Future<Output = Option<u64>>,
    RF: Fn() -> RFut,
    RFut: Future<Output = Result<(), String>>,
    NF: Fn() -> u64,
    S: Future<Output = ()>,
{
    tokio::pin!(shutdown);
    loop {
        match expiry_fn().await {
            None => {
                // Logged out / no RT: idle on signals.
                tokio::select! {
                    _ = &mut shutdown => return,
                    ok = changed(&mut creds_rx) => { if !ok { return } }
                    ok = changed(&mut wake_rx) => { if !ok { return } }
                }
            }
            Some(expires_at) => {
                let delay = refresh_delay(now_unix(), expires_at);
                let trigger = tokio::select! {
                    _ = &mut shutdown => return,
                    _ = tokio::time::sleep(delay) => Trigger::Timer,
                    ok = changed(&mut creds_rx) => {
                        if !ok { return }
                        Trigger::CredsChange
                    }
                    ok = changed(&mut wake_rx) => {
                        if !ok { return }
                        Trigger::Wake
                    }
                };
                match trigger {
                    Trigger::CredsChange => {
                        // Someone else moved the token (login / external
                        // refresh / logout). Just re-snapshot.
                    }
                    Trigger::Timer | Trigger::Wake => {
                        if let Err(e) = refresh_fn().await {
                            log::warn!("auth refresher: refresh failed, retrying: {e}");
                            tokio::select! {
                                _ = &mut shutdown => return,
                                _ = tokio::time::sleep(RETRY_BACKOFF) => {}
                                ok = changed(&mut creds_rx) => { if !ok { return } }
                                ok = changed(&mut wake_rx) => { if !ok { return } }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Spawn the long-lived refresher task. Lives for the process; the
/// runtime drops it on exit.
pub fn spawn_auth_refresher(wake_rx: watch::Receiver<u64>) {
    tauri::async_runtime::spawn(async move {
        run_auth_refresher(
            || async {
                // `access_token_with_expiry` already does the expiry-gated
                // refresh dance, but here we just need the expiry to
                // schedule against; calling it would refresh eagerly.
                crate::auth::AUTH.refreshable_expiry().await
            },
            || async {
                crate::auth::AUTH
                    .access_token_with_expiry()
                    .await
                    .map(|_| ())
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
        // MIN_MARGIN floor: remaining=120 → 25%=30 < 60 → margin=60.
        assert_eq!(refresh_delay(0, 120), Duration::from_secs(60));
        // Past expiry → refresh immediately.
        assert_eq!(refresh_delay(500, 400), Duration::ZERO);
        // Long-lived → capped at MAX_REARM (defense-in-depth backstop).
        assert_eq!(refresh_delay(0, 86_400), MAX_REARM);
        // Inside the floor margin already → no sleep.
        assert_eq!(refresh_delay(0, 30), Duration::ZERO);
    }

    /// Timer fires at ~75% elapsed and triggers a refresh.
    #[tokio::test(start_paused = true)]
    async fn timer_triggers_refresh() {
        let (refresh_tx, mut refresh_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (_creds_tx, creds_rx) = watch::channel(0u64);
        let (_wake_tx, wake_rx) = watch::channel(0u64);
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();

        let refresher = run_auth_refresher(
            || async { Some(400u64) }, // expires at t=400
            || {
                let refresh_tx = refresh_tx.clone();
                async move {
                    refresh_tx.send(()).unwrap();
                    Ok(())
                }
            },
            creds_rx,
            wake_rx,
            || 0u64, // now=0 → sleep 300s, then refresh
            async {
                let _ = shut_rx.await;
            },
        );

        let driver = async {
            refresh_rx.recv().await.unwrap(); // timer fired → refresh
            shut_tx.send(()).unwrap();
        };

        tokio::join!(refresher, driver);
    }

    /// Wake signal triggers an immediate ensure-fresh (resume recovery).
    #[tokio::test(start_paused = true)]
    async fn wake_triggers_refresh() {
        let (refresh_tx, mut refresh_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (_creds_tx, creds_rx) = watch::channel(0u64);
        let (wake_tx, wake_rx) = watch::channel(0u64);
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();

        let refresher = run_auth_refresher(
            || async { Some(10_000u64) }, // far-future expiry → timer wouldn't fire soon
            || {
                let refresh_tx = refresh_tx.clone();
                async move {
                    refresh_tx.send(()).unwrap();
                    Ok(())
                }
            },
            creds_rx,
            wake_rx,
            || 0u64,
            async {
                let _ = shut_rx.await;
            },
        );

        let driver = async {
            wake_tx.send_modify(|v| *v += 1); // OS resume
            refresh_rx.recv().await.unwrap();
            shut_tx.send(()).unwrap();
        };

        tokio::join!(refresher, driver);
    }

    /// A credentials-change signal re-snapshots without calling refresh
    /// (someone else just moved the token; don't burn the new RT).
    #[tokio::test(start_paused = true)]
    async fn creds_change_resnapshots_without_refresh() {
        let (refresh_tx, mut refresh_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (creds_tx, creds_rx) = watch::channel(0u64);
        let (_wake_tx, wake_rx) = watch::channel(0u64);
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();

        let refresher = run_auth_refresher(
            || {
                let seen_tx = seen_tx.clone();
                async move {
                    seen_tx.send(()).unwrap();
                    // Far-future expiry — the timer arm won't fire
                    // during the test; only the creds signal will.
                    Some(10_000u64)
                }
            },
            || {
                let refresh_tx = refresh_tx.clone();
                async move {
                    refresh_tx.send(()).unwrap();
                    Ok(())
                }
            },
            creds_rx,
            wake_rx,
            || 0u64,
            async {
                let _ = shut_rx.await;
            },
        );

        let driver = async {
            seen_rx.recv().await.unwrap(); // first snapshot
            creds_tx.send_modify(|v| *v += 1); // someone else refreshed
            seen_rx.recv().await.unwrap(); // re-snapshot
            assert!(
                refresh_rx.try_recv().is_err(),
                "creds-change should not trigger refresh"
            );
            shut_tx.send(()).unwrap();
        };

        tokio::join!(refresher, driver);
    }

    /// Logged out → loop idles on signals; a creds bump (login) makes it
    /// proceed and schedule a refresh.
    #[tokio::test(start_paused = true)]
    async fn logged_out_idles_until_login() {
        let authed = Mutex::new(false);
        let (refresh_tx, mut refresh_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (creds_tx, creds_rx) = watch::channel(0u64);
        let (_wake_tx, wake_rx) = watch::channel(0u64);
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();

        let refresher = run_auth_refresher(
            || async {
                if *authed.lock().unwrap() {
                    Some(400u64)
                } else {
                    None
                }
            },
            || {
                let refresh_tx = refresh_tx.clone();
                async move {
                    refresh_tx.send(()).unwrap();
                    Ok(())
                }
            },
            creds_rx,
            wake_rx,
            || 0u64,
            async {
                let _ = shut_rx.await;
            },
        );

        let driver = async {
            // Refresher is parked on the None arm. Flip to logged-in.
            *authed.lock().unwrap() = true;
            creds_tx.send_modify(|v| *v += 1);
            refresh_rx.recv().await.unwrap(); // timer fires post-sleep
            shut_tx.send(()).unwrap();
        };

        tokio::join!(refresher, driver);
    }

    /// A failed refresh retries after the backoff.
    #[tokio::test(start_paused = true)]
    async fn refresh_failure_retries_with_backoff() {
        let calls = Mutex::new(0u32);
        let (refresh_tx, mut refresh_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
        let (_creds_tx, creds_rx) = watch::channel(0u64);
        let (_wake_tx, wake_rx) = watch::channel(0u64);
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();

        let refresher = run_auth_refresher(
            || async { Some(400u64) },
            || {
                let refresh_tx = refresh_tx.clone();
                let n = {
                    let mut g = calls.lock().unwrap();
                    *g += 1;
                    *g
                };
                async move {
                    refresh_tx.send(n).unwrap();
                    if n == 1 {
                        Err("transient".to_string())
                    } else {
                        Ok(())
                    }
                }
            },
            creds_rx,
            wake_rx,
            || 0u64,
            async {
                let _ = shut_rx.await;
            },
        );

        let driver = async {
            assert_eq!(refresh_rx.recv().await.unwrap(), 1); // first call fails
            assert_eq!(refresh_rx.recv().await.unwrap(), 2); // retry succeeds
            shut_tx.send(()).unwrap();
        };

        tokio::join!(refresher, driver);
    }
}
