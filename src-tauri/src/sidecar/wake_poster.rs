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

//! On every host wake, `POST` the sidecar's host-only `/control/wake`
//! endpoint so the engine's `Poke` runs immediately (which today
//! triggers an upstream resync — semantics live in the engine, not on
//! the wire) instead of waiting on the wall-clock backstop.
//! Best-effort: a failed push is logged at debug; the next wake retries.

use std::future::Future;

use tauri::{AppHandle, Runtime};
use tokio::sync::watch;

use super::lifecycle::resolve_socket;
use super::transport;
use crate::wake::changed;

/// Drive a `/control/wake` push on every wake until `shutdown`. Generic
/// over the push sink so it is unit-testable without Tauri or a socket.
pub(crate) async fn run_wake_poster<PF, PFut, S>(
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
                    log::debug!("wake poster: push failed: {e}");
                }
            }
        }
    }
}

async fn post_wake_once<R: Runtime>(app: &AppHandle<R>) -> Result<(), String> {
    let socket = resolve_socket(app).await?;
    transport::post_wake(&socket)
        .await
        .map_err(|e| e.to_string())
}

/// Spawn the long-lived wake poster. Lives for the process; the
/// runtime drops it on exit.
pub fn spawn_wake_poster<R: Runtime>(app: &AppHandle<R>, wake_rx: watch::Receiver<u64>) {
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        run_wake_poster(
            wake_rx,
            || post_wake_once(&app),
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
    use crate::wake::Waker;

    #[tokio::test(start_paused = true)]
    async fn poster_pushes_on_wake_and_stops_on_shutdown() {
        let w = Waker::new();
        let pushes = Mutex::new(0u32);
        let (shut_tx, shut_rx) = tokio::sync::oneshot::channel::<()>();

        let poster = run_wake_poster(
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
