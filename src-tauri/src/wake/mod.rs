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

//! Host wake signal. OS power-resume / network-change events fan out to
//! every interested subscriber.
//!
//! The signal is a `watch<u64>` generation (same shape as `Auth`'s
//! credentials watch); consumers `select!` on `.changed()`.
//!
//! Note: The sidecar engine's wall-clock backstop still covers any
//! platform where a source isn't wired yet or an event is missed.

use tokio::sync::watch;

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
/// wake-aware `select!` arms (refresher, resync poster, …).
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
/// wall-clock + refresher `MAX_REARM` backstops cover suspend/resume.
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

/// Create the shared `Waker` and start the OS listeners. Returns the
/// `Waker`; the caller is responsible for keeping it alive (e.g. via
/// `app.manage`) — if every `Waker` drops, the channel closes and every
/// subscriber's `changed()` errors. Consumers subscribe via
/// [`Waker::subscribe`].
pub fn spawn_wake() -> Waker {
    let waker = Waker::new();
    run_os_wake_sources(waker.clone());
    waker
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
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
}
