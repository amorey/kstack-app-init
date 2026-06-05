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

//! Windows native event sources for the wake/network-return → `Poke` driver.
//!
//! Both use **callback-based** notifications, so there is no hidden window or
//! message pump to run (a message-only window would not even receive the power
//! *broadcast*):
//!
//! - **Wake**: `PowerRegisterSuspendResumeNotification` with
//!   `DEVICE_NOTIFY_CALLBACK` invokes our routine on resume; a
//!   `PBT_APMRESUMEAUTOMATIC` / `PBT_APMRESUMESUSPEND` forwards a
//!   [`RawEvent::Resumed`].
//! - **Network**: `NotifyNetworkConnectivityHintChange` (with an initial
//!   notification to prime the baseline) invokes our routine on connectivity
//!   changes; the hint's `ConnectivityLevel` is mapped by
//!   [`core::win_connectivity_is_online`] and forwarded as
//!   [`RawEvent::NetworkChanged`].
//!
//! Both callbacks fire on system threads and forward through the [`WIN_TX`]
//! global with `try_send` (best-effort, non-blocking). A shutdown task
//! unregisters both notifications on app Quit.

use std::ffi::c_void;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::OnceLock;

use tauri::AppHandle;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::NetworkManagement::IpHelper::{
    CancelMibChangeNotify2, NotifyNetworkConnectivityHintChange,
};
use windows_sys::Win32::Networking::WinSock::NL_NETWORK_CONNECTIVITY_HINT;
use windows_sys::Win32::System::Power::{
    PowerRegisterSuspendResumeNotification, PowerUnregisterSuspendResumeNotification,
    DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS,
};
use windows_sys::Win32::UI::WindowsAndMessaging::{
    DEVICE_NOTIFY_CALLBACK, PBT_APMRESUMEAUTOMATIC, PBT_APMRESUMESUSPEND,
};

use super::core::{self, RawEvent};

/// The channel both callbacks forward into. A global because the OS callbacks
/// carry only an opaque context we don't use.
static WIN_TX: OnceLock<Sender<RawEvent>> = OnceLock::new();

/// Registration handle for the suspend/resume notification (as `isize`, since
/// the raw `HPOWERNOTIFY`/`HANDLE` pointer is not `Send`). `0` = unregistered.
static POWER_HANDLE: AtomicIsize = AtomicIsize::new(0);

/// Registration handle for the connectivity-hint notification (as `isize`).
static NET_HANDLE: AtomicIsize = AtomicIsize::new(0);

/// Spawns the Windows wake + network sources.
///
/// `app` is unused on Windows (callbacks need no main-thread context); accepted
/// to match the cross-platform `spawn_sources` shape.
pub fn spawn_sources(_app: &AppHandle, tx: Sender<RawEvent>, shutdown: CancellationToken) {
    // Storing the sender is the idempotency gate — a second call is a no-op.
    if WIN_TX.set(tx).is_err() {
        return;
    }

    register_power_notification();
    register_network_notification();

    // Unregister both on app Quit.
    tauri::async_runtime::spawn(async move {
        shutdown.cancelled().await;
        unregister_all();
    });
}

/// Registers the resume-from-sleep callback (no window required).
fn register_power_notification() {
    // The OS copies Callback + Context out of this struct during the call, so a
    // local is fine — it need not outlive the registration.
    let mut params = DEVICE_NOTIFY_SUBSCRIBE_PARAMETERS {
        Callback: Some(power_callback),
        Context: std::ptr::null_mut(),
    };

    let mut handle: HANDLE = std::ptr::null_mut();
    // SAFETY: a valid callback + out-handle; DEVICE_NOTIFY_CALLBACK selects the
    // callback (not window) recipient form.
    let ret = unsafe {
        PowerRegisterSuspendResumeNotification(
            DEVICE_NOTIFY_CALLBACK,
            &mut params as *mut _ as HANDLE,
            &mut handle,
        )
    };

    if ret == 0 {
        POWER_HANDLE.store(handle as isize, Ordering::SeqCst);
    } else {
        tracing::warn!(code = ret, "PowerRegisterSuspendResumeNotification failed");
    }
}

/// Registers the connectivity-hint callback, priming the baseline via the
/// initial notification.
fn register_network_notification() {
    let mut handle: HANDLE = std::ptr::null_mut();
    // SAFETY: a valid callback + out-handle; `1` (TRUE) requests an initial
    // notification so the edge detector gets a baseline.
    let ret = unsafe {
        NotifyNetworkConnectivityHintChange(
            Some(network_callback),
            std::ptr::null(),
            1,
            &mut handle,
        )
    };

    if ret == 0 {
        NET_HANDLE.store(handle as isize, Ordering::SeqCst);
    } else {
        tracing::warn!(code = ret, "NotifyNetworkConnectivityHintChange failed");
    }
}

/// Unregisters both notifications (idempotent — each handle is taken once).
fn unregister_all() {
    let power = POWER_HANDLE.swap(0, Ordering::SeqCst);
    if power != 0 {
        // SAFETY: a handle we registered and have not yet unregistered.
        // HPOWERNOTIFY is an `isize`, so the stored value is passed directly.
        unsafe { PowerUnregisterSuspendResumeNotification(power) };
    }
    let net = NET_HANDLE.swap(0, Ordering::SeqCst);
    if net != 0 {
        // SAFETY: a handle we registered and have not yet unregistered.
        unsafe { CancelMibChangeNotify2(net as HANDLE) };
    }
}

/// `DEVICE_NOTIFY_CALLBACK` routine. `event_type` is a `PBT_*` power-broadcast
/// code; the resume codes forward a [`RawEvent::Resumed`]. Returns
/// `ERROR_SUCCESS` (`0`) as the API requires.
unsafe extern "system" fn power_callback(
    _context: *const c_void,
    event_type: u32,
    _setting: *const c_void,
) -> u32 {
    if event_type == PBT_APMRESUMEAUTOMATIC || event_type == PBT_APMRESUMESUSPEND {
        if let Some(tx) = WIN_TX.get() {
            let _ = tx.try_send(RawEvent::Resumed);
        }
    }
    0
}

/// `NotifyNetworkConnectivityHintChange` routine. Maps the hint's connectivity
/// level to online/offline and forwards a [`RawEvent::NetworkChanged`].
unsafe extern "system" fn network_callback(
    _context: *const c_void,
    hint: NL_NETWORK_CONNECTIVITY_HINT,
) {
    if let Some(tx) = WIN_TX.get() {
        let online = core::win_connectivity_is_online(hint.ConnectivityLevel);
        let _ = tx.try_send(RawEvent::NetworkChanged { online });
    }
}
