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

//! The platform-agnostic heart of the wake/network-return → `Poke` driver.
//!
//! Platform sources (`macos`/`windows`/`linux`) translate native OS callbacks
//! into [`RawEvent`]s and push them down one `mpsc` channel. This module turns
//! that raw stream into *at most one* poke per burst:
//!
//! 1. [`classify`] derives the **rising edge** — a resume is already an edge, a
//!    network change fires only on offline→online (an `Option<bool>` tracks the
//!    last-known state, so an unknown→online at startup is *not* an edge).
//! 2. [`run_coalescer`] **debounces**: a triggering edge (re)arms a trailing
//!    timer, and the poke fires `window` after the *last* trigger in a burst —
//!    so a wake immediately followed by network-return collapses to one poke.
//!
//! Everything here is pure/injectable: the poke action is a closure and time is
//! driven through `tokio::time`, so the whole surface is unit-testable with a
//! fake clock and a counting sink — no Tauri, no OS, no real sockets.

use std::future::Future;
use std::time::Duration;

use tokio::sync::mpsc::Receiver;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// A raw signal from a platform source, before edge detection.
///
/// `Resumed` is already an edge (a resume only ever happens once per sleep).
/// `NetworkChanged` carries the *observed level* — the core derives the
/// offline→online edge itself so sources can stay dumb (just report state).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawEvent {
    /// The machine woke from sleep/suspend.
    Resumed,
    /// Network connectivity changed to the given state (`true` = online).
    NetworkChanged { online: bool },
}

/// Tracks the last-known network state so [`classify`] can turn level-signals
/// into rising edges. `None` until the first network observation.
#[derive(Default)]
pub struct EdgeState {
    last_online: Option<bool>,
}

/// Returns whether `ev` is a *trigger* — an edge that should (subject to
/// debounce) cause a poke. Mutates `state` to remember the latest network level.
///
/// - `Resumed` always triggers.
/// - `NetworkChanged { online: true }` triggers only on a `false → true`
///   transition. An unknown (`None`) → `true` at startup is **not** a trigger,
///   and any `→ false` transition is not a trigger.
pub fn classify(state: &mut EdgeState, ev: RawEvent) -> bool {
    match ev {
        RawEvent::Resumed => true,
        RawEvent::NetworkChanged { online } => {
            let edge = state.last_online == Some(false) && online;
            state.last_online = Some(online);
            edge
        }
    }
}

/// Maps a NetworkManager `NMState` to a simple online bool (Linux source).
///
/// Online once connectivity reaches `NM_STATE_CONNECTED_LOCAL` (50) or better —
/// the permissive "there is connectivity" reading that suits a best-effort
/// resync nudge (the core still only pokes on the offline→online *edge*). Values
/// per the NetworkManager D-Bus API (`DISCONNECTED` 20, `CONNECTING` 40,
/// `CONNECTED_LOCAL` 50, `CONNECTED_SITE` 60, `CONNECTED_GLOBAL` 70).
// Used by the Linux source + tests; dead on other platforms' lib builds.
#[allow(dead_code)]
pub fn nm_state_is_online(state: u32) -> bool {
    const NM_STATE_CONNECTED_LOCAL: u32 = 50;
    state >= NM_STATE_CONNECTED_LOCAL
}

/// Maps a Windows `NL_NETWORK_CONNECTIVITY_LEVEL_HINT` to a simple online bool
/// (Windows source).
///
/// Online at `LocalAccess` (2) or better — i.e. an interface can route. Offline
/// for `Unknown` (0) and `None` (1). Same permissive reading as
/// [`nm_state_is_online`]; the edge is derived by [`classify`].
// Used by the Windows source + tests; dead on other platforms' lib builds.
#[allow(dead_code)]
pub fn win_connectivity_is_online(level: i32) -> bool {
    const LOCAL_ACCESS: i32 = 2;
    level >= LOCAL_ACCESS
}

/// Consumes raw events from `rx`, applies edge detection + trailing-edge
/// debounce, and calls `poke` once per burst.
///
/// On a triggering edge the trailing timer is (re)armed to `window` from *now*;
/// when it elapses with no further trigger, `poke().await` runs once. Additional
/// triggers inside the window collapse into the single pending poke.
///
/// Returns when `shutdown` is cancelled (a pending poke is dropped — the
/// sidecar's wall-clock detector is the backstop) or when every sender has been
/// dropped (`rx` closed). `poke` is a closure, so the core needs no trait and no
/// `async-trait` dependency.
pub async fn run_coalescer<F, Fut>(
    mut rx: Receiver<RawEvent>,
    window: Duration,
    shutdown: CancellationToken,
    mut poke: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let mut state = EdgeState::default();
    // The trailing-edge deadline; `None` when idle (no poke pending).
    let mut deadline: Option<Instant> = None;

    loop {
        // When idle, wait forever (never-completing future) so the select only
        // wakes on a new event or shutdown; when armed, race the deadline.
        let timer = async {
            match deadline {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = timer => {
                deadline = None;
                poke().await;
            }
            ev = rx.recv() => match ev {
                Some(ev) => {
                    if classify(&mut state, ev) {
                        // (Re)arm: trailing edge measured from the last trigger.
                        deadline = Some(Instant::now() + window);
                    }
                }
                // All sources dropped: nothing more can arrive. Any pending
                // poke is abandoned (best-effort).
                None => return,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use tokio::sync::mpsc;

    use super::*;

    // ----- classify (pure, synchronous) -----

    #[test]
    fn network_offline_to_online_is_edge() {
        let mut state = EdgeState::default();
        // Observe offline first, then online: the offline→online edge triggers.
        assert!(!classify(
            &mut state,
            RawEvent::NetworkChanged { online: false }
        ));
        assert!(classify(
            &mut state,
            RawEvent::NetworkChanged { online: true }
        ));
    }

    #[test]
    fn network_online_to_online_is_not_edge() {
        let mut state = EdgeState::default();
        classify(&mut state, RawEvent::NetworkChanged { online: false });
        assert!(classify(
            &mut state,
            RawEvent::NetworkChanged { online: true }
        ));
        // Already online: a repeat online is not a fresh edge.
        assert!(!classify(
            &mut state,
            RawEvent::NetworkChanged { online: true }
        ));
    }

    #[test]
    fn unknown_to_online_is_not_edge() {
        let mut state = EdgeState::default();
        // Startup: first observation is online. Not an edge (we never saw it go
        // down), so it must not poke.
        assert!(!classify(
            &mut state,
            RawEvent::NetworkChanged { online: true }
        ));
    }

    #[test]
    fn online_to_offline_is_not_edge() {
        let mut state = EdgeState::default();
        classify(&mut state, RawEvent::NetworkChanged { online: true });
        assert!(!classify(
            &mut state,
            RawEvent::NetworkChanged { online: false }
        ));
    }

    #[test]
    fn resume_always_triggers() {
        let mut state = EdgeState::default();
        assert!(classify(&mut state, RawEvent::Resumed));
        assert!(classify(&mut state, RawEvent::Resumed));
    }

    // ----- platform connectivity-level mappings (pure) -----

    #[test]
    fn nm_state_online_at_connected_local_or_better() {
        // Offline states.
        for s in [
            0u32, /* unknown */
            10,   /* asleep */
            20,   /* disconnected */
            40,   /* connecting */
        ] {
            assert!(!nm_state_is_online(s), "state {s} should be offline");
        }
        // Connected states.
        for s in [
            50u32, /* local */
            60,    /* site */
            70,    /* global */
        ] {
            assert!(nm_state_is_online(s), "state {s} should be online");
        }
    }

    #[test]
    fn win_connectivity_online_at_local_access_or_better() {
        // Unknown (0) and None (1) are offline.
        assert!(!win_connectivity_is_online(0));
        assert!(!win_connectivity_is_online(1));
        // LocalAccess (2), InternetAccess (3), ConstrainedInternetAccess (4),
        // Hidden (5) are online.
        for level in 2..=5 {
            assert!(
                win_connectivity_is_online(level),
                "level {level} should be online"
            );
        }
    }

    // ----- run_coalescer (trailing-edge debounce, fake clock) -----

    const WINDOW: Duration = Duration::from_secs(3);

    /// Spawns the coalescer with a counting poke sink, returning the event
    /// sender, the shared counter, the shutdown token, and the task handle.
    fn spawn_coalescer() -> (
        mpsc::Sender<RawEvent>,
        Arc<AtomicUsize>,
        CancellationToken,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, rx) = mpsc::channel(16);
        let count = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let sink = count.clone();
        let handle = tokio::spawn(run_coalescer(rx, WINDOW, shutdown.clone(), move || {
            let sink = sink.clone();
            async move {
                sink.fetch_add(1, Ordering::SeqCst);
            }
        }));
        (tx, count, shutdown, handle)
    }

    /// Lets the spawned coalescer drain whatever is buffered and park on its
    /// timer/recv. Pure scheduler synchronization — it does not advance the
    /// (paused) clock, so timers stay put.
    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn single_trigger_pokes_once_after_window() {
        let (tx, count, _shutdown, _h) = spawn_coalescer();

        tx.send(RawEvent::Resumed).await.unwrap();
        settle().await;

        // Just before the window elapses: nothing has fired.
        tokio::time::advance(WINDOW - Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 0);

        // Cross the window: exactly one poke.
        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn burst_coalesces_to_one_poke() {
        let (tx, count, _shutdown, _h) = spawn_coalescer();

        // A typical laptop-open burst: wake, then network drops and returns,
        // all within the debounce window.
        tx.send(RawEvent::Resumed).await.unwrap();
        tx.send(RawEvent::NetworkChanged { online: false })
            .await
            .unwrap();
        tx.send(RawEvent::NetworkChanged { online: true })
            .await
            .unwrap();
        settle().await;

        // Still inside the window measured from the last trigger.
        tokio::time::advance(WINDOW - Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 0);

        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn non_edge_event_does_not_arm() {
        let (tx, count, _shutdown, _h) = spawn_coalescer();

        // Unknown→online at startup is not an edge: no timer is armed, so no
        // poke ever fires no matter how far time advances.
        tx.send(RawEvent::NetworkChanged { online: true })
            .await
            .unwrap();
        settle().await;
        tokio::time::advance(WINDOW * 10).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn two_separated_bursts_poke_twice() {
        let (tx, count, _shutdown, _h) = spawn_coalescer();

        tx.send(RawEvent::Resumed).await.unwrap();
        settle().await;
        tokio::time::advance(WINDOW + Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // A second, separate burst well after the first settled.
        tx.send(RawEvent::Resumed).await.unwrap();
        settle().await;
        tokio::time::advance(WINDOW + Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cancels_pending_poke() {
        let (tx, count, shutdown, handle) = spawn_coalescer();

        tx.send(RawEvent::Resumed).await.unwrap();
        settle().await;

        // Cancel before the window elapses: the pending poke is dropped and the
        // task returns.
        shutdown.cancel();
        handle.await.unwrap();

        tokio::time::advance(WINDOW * 10).await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn sources_dropped_returns() {
        let (tx, _count, _shutdown, handle) = spawn_coalescer();

        // Dropping the only sender closes the channel; the coalescer returns.
        drop(tx);
        handle.await.unwrap();
    }
}
