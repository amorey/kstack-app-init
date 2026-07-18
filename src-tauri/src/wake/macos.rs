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

//! macOS native event sources for the wake/network-return → `Poke` driver.
//!
//! - **Wake**: `NSWorkspace` posts `NSWorkspaceDidWakeNotification` on resume
//!   from sleep. We register a small `NSObject` observer (the same `define_class`
//!   approach as [`dock_menu`](crate::dock_menu)) whose selector forwards a
//!   [`RawEvent::Resumed`]. Registration happens on the main thread (AppKit's
//!   notification center is delivered there).
//! - **Network**: `SCNetworkReachability` against the default route reports
//!   connectivity transitions. Its callback runs on a dedicated thread's
//!   `CFRunLoop` and forwards [`RawEvent::NetworkChanged`]; the core derives the
//!   offline→online edge.
//!
//! Both sources are passive once installed; we don't tear them down on shutdown
//! — the observer and run-loop thread are leaked for the process lifetime,
//! mirroring `dock_menu`'s leaked statics. Once the supervisor's receiver drops,
//! their `try_send`s simply no-op.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::OnceLock;

use core_foundation::runloop::{kCFRunLoopCommonModes, CFRunLoop};
use objc2::rc::Retained;
use objc2::runtime::{AnyObject, NSObject};
use objc2::{define_class, msg_send, sel, ClassType};
use objc2_app_kit::{NSWorkspace, NSWorkspaceDidWakeNotification};
use objc2_foundation::NSNotificationCenter;
use system_configuration::network_reachability::{ReachabilityFlags, SCNetworkReachability};
use tauri::AppHandle;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

use super::core::RawEvent;

/// Wraps a non-`Sync` value so it can live in a `static`. Sound because every
/// access is on the main thread (see [`crate::dock_menu`] for the same idiom).
struct MainThreadStatic<T>(#[allow(dead_code)] T);
// SAFETY: access is main-thread-only by construction.
unsafe impl<T> Send for MainThreadStatic<T> {}
unsafe impl<T> Sync for MainThreadStatic<T> {}

/// The channel the wake observer forwards into. A global because the
/// selector callback carries no user context (unlike the reachability closure,
/// which captures its sender directly).
static WAKE_TX: OnceLock<Sender<RawEvent>> = OnceLock::new();

/// Keeps the wake observer alive for the process lifetime — the notification
/// center holds it only weakly.
static WAKE_OBSERVER: OnceLock<MainThreadStatic<Retained<WakeObserver>>> = OnceLock::new();

define_class!(
    /// Observer target for `NSWorkspaceDidWakeNotification`. Stateless — it
    /// forwards through the [`WAKE_TX`] global.
    #[unsafe(super(NSObject))]
    #[name = "KstackWakeObserver"]
    struct WakeObserver;

    impl WakeObserver {
        #[unsafe(method(workspaceDidWake:))]
        fn workspace_did_wake(&self, _notification: Option<&AnyObject>) {
            if let Some(tx) = WAKE_TX.get() {
                // try_send: best-effort, never blocks the main thread. A full or
                // closed channel just drops the signal.
                let _ = tx.try_send(RawEvent::Resumed);
            }
        }
    }
);

/// Spawns the macOS wake + network sources.
///
/// `shutdown` is unused: both sources are passive and torn down with the process
/// (see the module docs). It is accepted to match the cross-platform
/// `spawn_sources` shape.
pub fn spawn_sources(app: &AppHandle, tx: Sender<RawEvent>, _shutdown: CancellationToken) {
    // Reachability runs on its own CFRunLoop thread; no main thread needed.
    spawn_network_source(tx.clone());

    // The wake observer must be registered on the main thread. Storing the
    // sender is the idempotency gate — a second call is a no-op.
    if WAKE_TX.set(tx).is_err() {
        return;
    }
    if let Err(err) = app.run_on_main_thread(register_wake_observer) {
        tracing::warn!(%err, "failed to schedule wake observer registration");
    }
}

/// Registers the `NSWorkspaceDidWakeNotification` observer. Must run on the main
/// thread (queued via `run_on_main_thread`).
fn register_wake_observer() {
    let observer: Retained<WakeObserver> = unsafe { msg_send![WakeObserver::class(), new] };

    // NSWorkspace notifications are posted to the workspace's *own* center, not
    // the default one.
    let workspace = NSWorkspace::sharedWorkspace();
    let center: Retained<NSNotificationCenter> = workspace.notificationCenter();
    unsafe {
        center.addObserver_selector_name_object(
            &observer,
            sel!(workspaceDidWake:),
            Some(NSWorkspaceDidWakeNotification),
            None,
        );
    }

    let _ = WAKE_OBSERVER.set(MainThreadStatic(observer));
}

/// Starts the SCNetworkReachability monitor on a dedicated CFRunLoop thread.
///
/// Primes the edge detector with the current connectivity, then forwards every
/// subsequent transition. The thread blocks in `CFRunLoop::run` for the process
/// lifetime; we never unschedule (it's idle and dies with the process).
fn spawn_network_source(tx: Sender<RawEvent>) {
    let spawned = std::thread::Builder::new()
        .name("kstack-net-reachability".into())
        .spawn(move || {
            // 0.0.0.0 — reachability of the default route ("can we reach the
            // network without user intervention").
            let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0));
            let mut reachability = SCNetworkReachability::from(addr);

            // Prime: record the baseline so a later offline→online is an edge.
            if let Ok(flags) = reachability.reachability() {
                let _ = tx.try_send(RawEvent::NetworkChanged {
                    online: is_online(flags),
                });
            }

            let cb_tx = tx.clone();
            if let Err(err) = reachability.set_callback(move |flags| {
                let _ = cb_tx.try_send(RawEvent::NetworkChanged {
                    online: is_online(flags),
                });
            }) {
                tracing::warn!(?err, "failed to set reachability callback");
                return;
            }

            // SAFETY: scheduling on the current thread's run loop with a valid,
            // non-null mode constant. The reachability ref outlives the run loop
            // (it lives on this thread's stack until the process exits).
            unsafe {
                if let Err(err) = reachability
                    .schedule_with_runloop(&CFRunLoop::get_current(), kCFRunLoopCommonModes)
                {
                    tracing::warn!(?err, "failed to schedule reachability run loop");
                    return;
                }
            }

            CFRunLoop::run_current();
        });

    if let Err(err) = spawned {
        tracing::warn!(%err, "failed to spawn reachability thread");
    }
}

/// Maps reachability flags to a simple online/offline bool: the network is
/// "online" when the target is reachable and reaching it needs no user
/// intervention (no on-demand connection / VPN dial).
fn is_online(flags: ReachabilityFlags) -> bool {
    flags.contains(ReachabilityFlags::REACHABLE)
        && !flags.contains(ReachabilityFlags::CONNECTION_REQUIRED)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reachable_without_connection_required_is_online() {
        assert!(is_online(ReachabilityFlags::REACHABLE));
    }

    #[test]
    fn not_reachable_is_offline() {
        assert!(!is_online(ReachabilityFlags::empty()));
    }

    #[test]
    fn reachable_but_connection_required_is_offline() {
        assert!(!is_online(
            ReachabilityFlags::REACHABLE | ReachabilityFlags::CONNECTION_REQUIRED
        ));
    }
}
