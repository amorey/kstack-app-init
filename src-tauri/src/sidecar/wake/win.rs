//! Windows wake sources, behind `cfg(target_os = "windows")`.
//!
//! Two producers feed the shared [`Waker`]:
//!   - **power-resume** — a `PowerRegisterSuspendResumeNotification`
//!     callback (no window/message pump needed); wakes on
//!     `PBT_APMRESUMESUSPEND` / `PBT_APMRESUMEAUTOMATIC`;
//!   - **network-change** — a thread looping the blocking
//!     `NotifyAddrChange` (IP address table changed).
//!
//! Each only calls `waker.wake()` (a thread-safe `watch` bump), so the
//! callback/thread delivery context doesn't matter. The registration,
//! its context, and the thread are intentionally process-lived (the app
//! outlives them; nothing to unregister on exit).
//!
//! Module is named `win` (not `windows`) so `use windows::…` inside it
//! resolves to the crate, not this module. NOT runtime-testable from a
//! non-Windows host; correctness is by review against the vendored
//! `windows` crate + the CI matrix Windows build.

use std::ffi::c_void;

use windows::Win32::Foundation::HANDLE;
use windows::Win32::NetworkManagement::IpHelper::NotifyAddrChange;
use windows::Win32::System::Power::{
    PowerRegisterSuspendResumeNotification, DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS,
};
use windows::Win32::UI::WindowsAndMessaging::{
    DEVICE_NOTIFY_CALLBACK, PBT_APMRESUMEAUTOMATIC, PBT_APMRESUMESUSPEND,
};

use super::Waker;

/// Install both Windows wake sources.
pub(super) fn spawn(waker: Waker) {
    register_power_resume(waker.clone());
    spawn_addr_change(waker);
}

/// The OS invokes this on a system thread for power broadcasts. `context`
/// is the leaked `Waker` we registered. Wake only on the two resume
/// notifications; always return `ERROR_SUCCESS` (0).
unsafe extern "system" fn power_callback(
    context: *const c_void,
    event_type: u32,
    _setting: *const c_void,
) -> u32 {
    if event_type == PBT_APMRESUMESUSPEND || event_type == PBT_APMRESUMEAUTOMATIC {
        // SAFETY: `context` is the `*const Waker` we leaked at
        // registration; it lives for the process.
        let waker = unsafe { &*(context as *const Waker) };
        waker.wake();
    }
    0
}

fn register_power_resume(waker: Waker) {
    let waker: &'static Waker = Box::leak(Box::new(waker));
    let params = Box::leak(Box::new(DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS {
        Callback: Some(power_callback),
        Context: (waker as *const Waker) as *mut c_void,
    }));

    let mut handle: *mut c_void = std::ptr::null_mut();
    // SAFETY: standard callback registration. `recipient` is the
    // DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS pointer (callback-flavor
    // contract); params + the Waker are `Box::leak`'d so they outlive the
    // (never-issued) unregister; `handle` is an unused out-param.
    let err = unsafe {
        PowerRegisterSuspendResumeNotification(
            DEVICE_NOTIFY_CALLBACK,
            HANDLE(params as *mut _ as *mut c_void),
            &mut handle,
        )
    };
    if err.0 != 0 {
        log::warn!(
            "windows wake: PowerRegisterSuspendResumeNotification failed ({})",
            err.0
        );
    }
}

fn spawn_addr_change(waker: Waker) {
    let thread = std::thread::Builder::new()
        .name("kstack-addrchange".into())
        .spawn(move || loop {
            // SAFETY: synchronous form — null handle + null overlapped
            // blocks in-kernel until the IP address table changes, then
            // returns NO_ERROR (0). Zero CPU while parked.
            let rc = unsafe { NotifyAddrChange(std::ptr::null_mut(), std::ptr::null()) };
            if rc != 0 {
                log::warn!(
                    "windows wake: NotifyAddrChange failed ({rc}); \
                     no network-change source for the rest of this process"
                );
                return;
            }
            waker.wake();
        });
    if let Err(e) = thread {
        log::warn!("windows wake: failed to spawn addr-change thread: {e}");
    }
}
