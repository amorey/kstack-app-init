//! macOS wake sources, behind `cfg(target_os = "macos")`.
//!
//! Two producers feed the shared [`Waker`]:
//!   - **power-resume** — an `NSWorkspace.didWakeNotification` observer;
//!   - **network-change** — an `SCNetworkReachability` callback on the
//!     general route.
//!
//! Both only ever call `waker.wake()` (a thread-safe `watch` bump), so
//! delivery thread doesn't matter. This is FFI that can't be
//! runtime-tested in CI (no way to simulate sleep/resume); correctness is
//! by review + per-OS compile/clippy. The engine wall-clock + the
//! credential pusher's `MAX_REARM` remain as backstops if a callback is
//! ever missed.

use std::net::{Ipv4Addr, SocketAddr};
use std::ptr::NonNull;
use std::time::Duration;

use block2::RcBlock;
use objc2_app_kit::{NSWorkspace, NSWorkspaceDidWakeNotification};
use objc2_foundation::NSNotification;
// Via system-configuration's re-export so CFRunLoop is the same
// core-foundation version its API expects (0.9, not the latest).
use system_configuration::core_foundation::runloop::{
    kCFRunLoopCommonModes, kCFRunLoopDefaultMode, CFRunLoop, CFRunLoopRunResult,
};
use system_configuration::network_reachability::SCNetworkReachability;

use super::Waker;

/// Install both macOS wake sources. Observers/threads are intentionally
/// process-lived (the app outlives them; nothing to unregister on exit).
pub(super) fn spawn(waker: Waker) {
    install_power_resume(waker.clone());
    spawn_reachability(waker);
}

/// Observe `NSWorkspaceDidWakeNotification`. The observer token is leaked
/// on purpose: it must live for the whole process or the notification
/// center stops delivering. `queue: None` ⇒ the block runs on the posting
/// thread, which is fine since it only does a thread-safe `wake()`.
fn install_power_resume(waker: Waker) {
    // SAFETY: standard NSWorkspace notification-center registration.
    // `addObserverForName:object:queue:usingBlock:` Block_copy's
    // (retains) the block and holds it for the observer's lifetime, so
    // dropping the stack `RcBlock` after this call is safe — the heap
    // closure outlives it. block2 0.6's `IntoBlock` imposes no `Send`
    // bound, so the binding's "block must be sendable" requirement is
    // ours to satisfy: the closure captures only a `Send + Sync +
    // 'static` Waker and runs a thread-safe `wake()`, and
    // NSNotificationCenter is documented thread-safe for add/remove.
    unsafe {
        let center = NSWorkspace::sharedWorkspace().notificationCenter();
        let block = RcBlock::new(move |_: NonNull<NSNotification>| waker.wake());
        let observer = center.addObserverForName_object_queue_usingBlock(
            Some(NSWorkspaceDidWakeNotification),
            None,
            None,
            &block,
        );
        std::mem::forget(observer);
    }
}

/// Run an `SCNetworkReachability` callback for the general route on a
/// dedicated run-loop thread. Any connectivity change pokes a resync;
/// `SCNetworkReachability` is a cheap "did the route change" trigger, not
/// a truth source — the upstream call result still decides Live/Offline.
fn spawn_reachability(waker: Waker) {
    let thread = std::thread::Builder::new()
        .name("kstack-reachability".into())
        .spawn(move || {
            // The unspecified address watches the default route, so any
            // interface up/down or network switch fires the callback.
            let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0));
            let mut reach = SCNetworkReachability::from(addr);

            if reach.set_callback(move |_flags| waker.wake()).is_err() {
                log::warn!("macos wake: SCNetworkReachability set_callback failed");
                return;
            }

            let run_loop = CFRunLoop::get_current();
            // SAFETY: kCFRunLoopCommonModes is a valid, non-null CF run
            // loop mode constant.
            // Register the source in *all* common modes so it stays
            // serviced regardless of which mode the run loop is in.
            let scheduled =
                unsafe { reach.schedule_with_runloop(&run_loop, kCFRunLoopCommonModes) };
            if scheduled.is_err() {
                log::warn!("macos wake: SCNetworkReachability schedule failed");
                return;
            }

            // Service the callback for the process lifetime. `reach` is
            // held by this closure so the registration lives.
            loop {
                // Run in a *concrete* mode. `kCFRunLoopCommonModes` is a
                // pseudo-mode valid only for scheduling sources, not for
                // CFRunLoopRunInMode — passing it is rejected outright and
                // the loop exits immediately. The default mode is a member
                // of the common-modes set, so the source scheduled above is
                // still serviced here.
                let result = CFRunLoop::run_in_mode(
                    // SAFETY: kCFRunLoopDefaultMode is a valid, non-null CF
                    // run loop mode constant.
                    unsafe { kCFRunLoopDefaultMode },
                    Duration::from_secs(24 * 60 * 60),
                    false,
                );
                // TimedOut/HandledSource ⇒ normal re-arm. Finished/
                // Stopped ⇒ the run loop has no sources (source gone);
                // exiting avoids a 100%-CPU spin — the engine wall-clock
                // + MAX_REARM backstops still cover wake.
                if matches!(
                    result,
                    CFRunLoopRunResult::Finished | CFRunLoopRunResult::Stopped
                ) {
                    log::warn!(
                        "macos wake: reachability run loop ended ({result:?}); \
                         no network-change source for the rest of this process"
                    );
                    return;
                }
            }
        });
    if let Err(e) = thread {
        log::warn!("macos wake: failed to spawn reachability thread: {e}");
    }
}
