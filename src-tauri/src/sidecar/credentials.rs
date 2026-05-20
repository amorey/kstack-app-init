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
//! keychain + refresh, so this re-publishes the current access token to
//! the sidecar's host-only `/control/credentials` endpoint.
//!
//! Pure event-driven: the loop waits on `Auth::watch_credentials()`
//! (bumped on login / refresh / logout) and pushes whenever the token
//! actually changes. The *timing* of refresh is owned by
//! [`crate::auth::refresher`] — this module is just the transport.
//! `last` dedups across signals so an idle `creds_gen` bump (e.g. a
//! logout-then-login back to the same `last` value would *not* dedup —
//! the AT is fresh) doesn't re-push the same bytes.

use std::future::Future;
use std::time::Duration;

use tauri::{AppHandle, Runtime};
use tokio::sync::watch;

use super::lifecycle::resolve_socket;
use super::transport;
use crate::wake::changed;

/// Backoff after a failed push before retrying.
const RETRY_BACKOFF: Duration = Duration::from_secs(5);

/// Drive credential pushes until `shutdown` resolves. Generic over the
/// token source / sink so it is unit-testable without Tauri or a socket.
/// Pushes only when the token changed since the last *successful* push;
/// retries a failed push after a short backoff.
pub(crate) async fn run_credential_pusher<TF, TFut, PF, PFut, S>(
    token_fn: TF,
    push_fn: PF,
    mut creds_rx: watch::Receiver<u64>,
    shutdown: S,
) where
    TF: Fn() -> TFut,
    TFut: Future<Output = Result<String, String>>,
    PF: Fn(String) -> PFut,
    PFut: Future<Output = Result<(), String>>,
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
                    ok = changed(&mut creds_rx) => { if !ok { return } }
                }
            }
            Ok(token) => {
                if last.as_deref() != Some(token.as_str()) {
                    if let Err(e) = push_fn(token.clone()).await {
                        log::warn!("credential pusher: push failed, retrying: {e}");
                        tokio::select! {
                            _ = &mut shutdown => return,
                            _ = tokio::time::sleep(RETRY_BACKOFF) => {}
                            ok = changed(&mut creds_rx) => { if !ok { return } }
                        }
                        continue; // re-derive + retry
                    }
                    last = Some(token);
                }
                // Token unchanged or just pushed: park until the next
                // credentials change (login / refresh / logout).
                tokio::select! {
                    _ = &mut shutdown => return,
                    ok = changed(&mut creds_rx) => { if !ok { return } }
                }
            }
        }
    }
}

async fn push_once<R: Runtime>(app: &AppHandle<R>, token: &str) -> Result<(), String> {
    let socket = resolve_socket(app).await?;
    transport::push_credentials(&socket, token)
        .await
        .map_err(|e| e.to_string())
}

/// Spawn the long-lived pusher task. Lives for the process; the runtime
/// drops it on exit (the sidecar dies with the host anyway, so no
/// explicit shutdown signal is needed).
pub fn spawn_credential_pusher<R: Runtime>(app: &AppHandle<R>) {
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        run_credential_pusher(
            || crate::auth::AUTH.access_token(),
            |token| {
                let app = app.clone();
                async move { push_once(&app, &token).await }
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
    use std::sync::Mutex;

    use super::*;

    /// Initial push, then a credentials-change signal makes it re-derive
    /// and push the new token, retrying a transient push failure.
    #[tokio::test(start_paused = true)]
    async fn pushes_then_re_pushes_on_change_and_retries() {
        let phase = Mutex::new("a".to_string());
        let fail_first_b = Mutex::new(true);
        let (push_tx, mut push_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (creds_tx, creds_rx) = watch::channel(0u64);
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
            async {
                let _ = shut_rx.await;
            },
        );

        let driver = async {
            assert_eq!(push_rx.recv().await.unwrap(), "a");
            *phase.lock().unwrap() = "b".to_string();
            creds_tx.send_modify(|v| *v += 1);
            assert_eq!(push_rx.recv().await.unwrap(), "b");
            shut_tx.send(()).unwrap();
        };

        tokio::join!(pusher, driver);
    }

    /// An unchanged token is not re-pushed even across repeated signals.
    #[tokio::test(start_paused = true)]
    async fn dedups_unchanged_token_across_signals() {
        let (push_tx, mut push_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (creds_tx, creds_rx) = watch::channel(0u64);
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();

        let pusher = run_credential_pusher(
            || {
                let seen_tx = seen_tx.clone();
                async move {
                    seen_tx.send(()).unwrap();
                    Ok("x".to_string())
                }
            },
            |token| async {
                push_tx.send(token).unwrap();
                Ok(())
            },
            creds_rx,
            async {
                let _ = shut_rx.await;
            },
        );

        let driver = async {
            seen_rx.recv().await.unwrap();
            assert_eq!(push_rx.recv().await.unwrap(), "x");
            for _ in 0..3 {
                creds_tx.send_modify(|v| *v += 1);
                seen_rx.recv().await.unwrap();
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
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();

        let pusher = run_credential_pusher(
            || async {
                if *authed.lock().unwrap() {
                    Ok("tok".to_string())
                } else {
                    Err("not authenticated".to_string())
                }
            },
            |token| async {
                push_tx.send(token).unwrap();
                Ok(())
            },
            creds_rx,
            async {
                let _ = shut_rx.await;
            },
        );

        let driver = async {
            *authed.lock().unwrap() = true;
            creds_tx.send_modify(|v| *v += 1);
            assert_eq!(push_rx.recv().await.unwrap(), "tok");
            shut_tx.send(()).unwrap();
        };

        tokio::join!(pusher, driver);
    }
}
