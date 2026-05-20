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

//! Linux wake sources, behind `cfg(target_os = "linux")`.
//!
//! Two system-bus D-Bus signals feed the shared [`Waker`]:
//!   - **power-resume** — logind `org.freedesktop.login1.Manager`
//!     `PrepareForSleep(bool start)`; `start == false` is "resumed";
//!   - **network-change** — NetworkManager
//!     `org.freedesktop.NetworkManager` `StateChanged`.
//!
//! Each only calls `waker.wake()` (a thread-safe `watch` bump). Anything
//! that fails (no system bus, no logind, no NetworkManager — networkd /
//! headless boxes) degrades to a warn: the engine wall-clock + the
//! credential pusher's `MAX_REARM` remain as backstops, and the upstream
//! call result is still the source of truth. Not runtime-testable in CI;
//! correctness is by review + per-OS compile in the CI matrix.

use futures_util::stream::{select_all, StreamExt};
use zbus::{proxy::SignalStream, Connection, Proxy};

use super::Waker;

/// logind signal name — used both to subscribe and to branch dispatch.
const PREPARE_FOR_SLEEP: &str = "PrepareForSleep";

/// Long-lived task: subscribe to the two signals and `wake()` on each
/// (resume only, for PrepareForSleep). Returns — ending the task — only
/// if nothing could be subscribed; the backstops then cover wake.
pub(super) async fn run(waker: Waker) {
    let conn = match Connection::system().await {
        Ok(c) => c,
        Err(e) => {
            log::warn!("linux wake: system bus unavailable: {e}");
            return;
        }
    };

    // Proxies are kept in scope for the whole task so their signal
    // subscriptions (match rules) stay registered.
    let sleep_proxy = proxy(
        &conn,
        "org.freedesktop.login1",
        "/org/freedesktop/login1",
        "org.freedesktop.login1.Manager",
    )
    .await;
    let net_proxy = proxy(
        &conn,
        "org.freedesktop.NetworkManager",
        "/org/freedesktop/NetworkManager",
        "org.freedesktop.NetworkManager",
    )
    .await;

    let mut streams = Vec::new();
    subscribe(&sleep_proxy, PREPARE_FOR_SLEEP, "logind", &mut streams).await;
    subscribe(&net_proxy, "StateChanged", "NetworkManager", &mut streams).await;
    if streams.is_empty() {
        log::warn!("linux wake: no logind/NetworkManager signals; relying on backstops");
        return;
    }

    // Merge both signal streams; branch on the member so PrepareForSleep
    // only wakes on resume (start == false) while StateChanged always
    // does (a cheap "route changed" trigger — truth is the upstream call).
    let mut merged = select_all(streams);
    while let Some(msg) = merged.next().await {
        let is_sleep_signal = msg
            .header()
            .member()
            .is_some_and(|m| m.as_str() == PREPARE_FOR_SLEEP);
        if is_sleep_signal {
            // body is `b` (bool): true = about to sleep, false = resumed.
            if msg.body().deserialize::<bool>() == Ok(false) {
                waker.wake();
            }
        } else {
            waker.wake();
        }
    }
}

/// Build a signal-only proxy. `Proxy::new` doesn't contact the bus, so
/// `None` (with a warn) means a construction error (e.g. a malformed
/// name) — an *absent* service instead simply yields no signals, also
/// covered by the backstops. Returning `None` lets the other source run.
async fn proxy(
    conn: &Connection,
    destination: &'static str,
    path: &'static str,
    interface: &'static str,
) -> Option<Proxy<'static>> {
    match Proxy::new(conn, destination, path, interface).await {
        Ok(p) => Some(p),
        Err(e) => {
            log::warn!("linux wake: proxy {destination} unavailable: {e}");
            None
        }
    }
}

/// Subscribe `proxy` to `signal`, pushing the stream onto `streams`.
/// Absent proxy or a subscribe error degrades to a warn (backstops
/// cover); the other source still runs.
async fn subscribe(
    proxy: &Option<Proxy<'static>>,
    signal: &'static str,
    label: &str,
    streams: &mut Vec<SignalStream<'static>>,
) {
    let Some(p) = proxy else { return };
    match p.receive_signal(signal).await {
        Ok(s) => streams.push(s),
        Err(e) => log::warn!("linux wake: {label} {signal} subscribe failed: {e}"),
    }
}
