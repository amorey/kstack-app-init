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

//! Custom macOS Dock menu (shown when right-clicking the Dock icon).
//!
//! AppKit only gets the Dock menu by sending `applicationDockMenu:` to the
//! `NSApplication` delegate, and tao installs its own delegate
//! (`TaoAppDelegateParent`) without implementing it. So this module defines
//! [`DockMenuTarget`] (an `NSObject` whose actions drive the [`WindowManager`]),
//! stores its `NSMenu` and target in process-wide statics, and patches two
//! implementations onto tao's live delegate class: `applicationDockMenu:` returns
//! the stored menu, and `applicationShouldTerminate:` reroutes a user Quit through
//! Tauri's graceful-shutdown hook (a system quit is left alone — see
//! [`application_should_terminate`]).
//!
//! Runs on the main thread after tao installs the delegate — `install` is called
//! from the Tauri `setup` hook. Gated to macOS by its `#[cfg]` in `lib.rs`.

use std::ffi::CStr;
use std::sync::OnceLock;

use objc2::rc::Retained;
use objc2::runtime::{AnyClass, AnyObject, Imp, Sel};
use objc2::{define_class, msg_send, sel, ClassType};
use objc2_app_kit::{NSApplication, NSMenu, NSMenuItem};
use objc2_foundation::{MainThreadMarker, NSAppleEventManager, NSString};
use tauri::{AppHandle, Manager};

use crate::state::AppState;

/// Wraps a non-`Sync` value so it can live in a `static`. Sound because every
/// access is on the main thread — `install` writes it, AppKit's main-thread
/// callbacks read it.
struct MainThreadStatic<T>(T);

// SAFETY: see the type-level comment — access is main-thread-only by construction.
unsafe impl<T> Send for MainThreadStatic<T> {}
unsafe impl<T> Sync for MainThreadStatic<T> {}

/// Process-wide handle so the Objective-C action callbacks can reach Tauri.
static APP_HANDLE: OnceLock<AppHandle> = OnceLock::new();

/// Keeps the Dock `NSMenu` alive for the process lifetime; `applicationDockMenu:`
/// returns a borrowed pointer into it that AppKit does not retain.
static DOCK_MENU: OnceLock<MainThreadStatic<Retained<NSMenu>>> = OnceLock::new();

/// Keeps the menu's action target alive for the process lifetime — `NSMenuItem`
/// holds only a weak `target`, so it must be owned here or items fire into freed
/// memory.
static MENU_TARGET: OnceLock<MainThreadStatic<Retained<DockMenuTarget>>> = OnceLock::new();

define_class!(
    /// Action target for the Dock menu items. Stateless — callbacks reach Tauri
    /// through [`APP_HANDLE`].
    #[unsafe(super(objc2::runtime::NSObject))]
    #[name = "KstackDockMenuTarget"]
    struct DockMenuTarget;

    impl DockMenuTarget {
        #[unsafe(method(newWindow:))]
        fn new_window(&self, _sender: Option<&AnyObject>) {
            with_state(|app, state| {
                if let Err(err) = state.window_manager.new_window(app) {
                    tracing::error!(%err, "failed to open window from dock menu");
                }
            });
        }

        #[unsafe(method(showMainWindow:))]
        fn show_main_window(&self, _sender: Option<&AnyObject>) {
            with_state(|app, state| {
                if let Err(err) = state.window_manager.show_main_window(app) {
                    tracing::error!(%err, "failed to show main window from dock menu");
                }
            });
        }
    }
);

/// Run `f` with the Tauri app handle and state, or log if [`install`] never ran.
fn with_state(f: impl FnOnce(&AppHandle, &AppState)) {
    let Some(app) = APP_HANDLE.get() else {
        tracing::error!("dock menu fired before install()");
        return;
    };
    let state = app.state::<AppState>();
    f(app, &state);
}

/// Install the custom Dock menu. Must run on the main thread. Idempotent, and
/// any failure to patch tao's delegate is logged and swallowed — a missing Dock
/// menu isn't worth aborting startup over.
pub fn install(app: &AppHandle) {
    let Some(mtm) = MainThreadMarker::new() else {
        tracing::error!("dock menu install() called off the main thread");
        return;
    };

    // Storing the handle is the gate for everything else; bail if already done.
    if APP_HANDLE.set(app.clone()).is_err() {
        return;
    }

    let target: Retained<DockMenuTarget> = unsafe { msg_send![DockMenuTarget::class(), new] };
    let menu = build_menu(mtm, &target);

    let _ = MENU_TARGET.set(MainThreadStatic(target));
    let _ = DOCK_MENU.set(MainThreadStatic(menu));

    if let Err(err) = patch_delegate_class() {
        tracing::error!(
            err,
            "failed to install applicationDockMenu: on tao delegate"
        );
    }
}

/// Build the Dock `NSMenu`: New Window, Show Main Window. Quit is omitted —
/// AppKit appends its own section (including a Quit that sends `terminate:`,
/// caught by [`application_should_terminate`]) below whatever this returns.
fn build_menu(mtm: MainThreadMarker, target: &DockMenuTarget) -> Retained<NSMenu> {
    let menu = NSMenu::new(mtm);

    for (title, action) in [
        ("New Window", sel!(newWindow:)),
        ("Show Main Window", sel!(showMainWindow:)),
    ] {
        let item = NSMenuItem::new(mtm);
        item.setTitle(&NSString::from_str(title));
        unsafe {
            item.setAction(Some(action));
            item.setTarget(Some(target));
        }
        menu.addItem(&item);
    }

    menu
}

/// Add `applicationDockMenu:` and `applicationShouldTerminate:` to tao's live
/// delegate class, which (as of tao 0.35) implements neither. The former returns
/// the shared [`DOCK_MENU`], the latter reroutes the exit (see that IMP's docs).
fn patch_delegate_class() -> Result<(), &'static str> {
    let mtm = MainThreadMarker::new().ok_or("not on main thread")?;
    let app = NSApplication::sharedApplication(mtm);

    let delegate = app.delegate().ok_or("NSApplication has no delegate")?;

    // The class owns method implementations. Go through AnyObject so it works
    // regardless of the protocol-object wrapper type.
    let delegate_obj: &AnyObject = delegate.as_ref();
    let class: &AnyClass = delegate_obj.class();

    // `Imp` is `extern "C-unwind" fn()`; transmute each typed IMP to it. The type
    // encodings below spell receiver, selector, one object arg, and the return
    // (object `@` for the menu, NSUInteger `Q` for the terminate reply).
    let dock_menu_imp: Imp = unsafe {
        std::mem::transmute::<
            unsafe extern "C-unwind" fn(*mut AnyObject, Sel, *mut AnyObject) -> *mut AnyObject,
            Imp,
        >(application_dock_menu)
    };
    add_method_if_absent(class, sel!(applicationDockMenu:), dock_menu_imp, c"@@:@")?;

    let should_terminate_imp: Imp = unsafe {
        std::mem::transmute::<
            unsafe extern "C-unwind" fn(*mut AnyObject, Sel, *mut AnyObject) -> usize,
            Imp,
        >(application_should_terminate)
    };
    add_method_if_absent(
        class,
        sel!(applicationShouldTerminate:),
        should_terminate_imp,
        c"Q@:@",
    )?;

    Ok(())
}

/// Add `imp` (with Objective-C type encoding `types`) for `sel` to `class`,
/// unless the class already implements `sel` — guards against a double patch and
/// leaves any tao-provided implementation untouched.
fn add_method_if_absent(
    class: &AnyClass,
    sel: Sel,
    imp: Imp,
    types: &CStr,
) -> Result<(), &'static str> {
    if class.instance_method(sel).is_some() {
        return Ok(());
    }

    let added = unsafe {
        objc2::ffi::class_addMethod(
            (class as *const AnyClass).cast_mut(),
            sel,
            imp,
            types.as_ptr(),
        )
    };

    if added.as_bool() {
        Ok(())
    } else {
        Err("class_addMethod rejected a delegate method")
    }
}

/// `applicationDockMenu:` IMP. Returns a borrowed (non-retained) pointer to the
/// shared Dock menu; AppKit does not take ownership.
unsafe extern "C-unwind" fn application_dock_menu(
    _self: *mut AnyObject,
    _cmd: Sel,
    _sender: *mut AnyObject,
) -> *mut AnyObject {
    match DOCK_MENU.get() {
        Some(menu) => Retained::as_ptr(&menu.0).cast_mut().cast(),
        None => std::ptr::null_mut(),
    }
}

/// `applicationShouldTerminate:` IMP. AppKit sends `terminate:` for the Quit
/// items, which would skip Tauri's `RunEvent::ExitRequested` graceful-shutdown
/// hook. For a user quit we cancel it (`NSTerminateCancel`) and re-drive through
/// [`AppHandle::exit`], which emits `ExitRequested` and does not route back
/// through `terminate:` (so no recursion). A system-initiated quit (logout /
/// restart / shutdown — see [`is_system_initiated_quit`]) instead returns
/// `NSTerminateNow`, since cancelling would abort the user's logout; cleanup
/// still runs via `RunEvent::Exit`.
unsafe extern "C-unwind" fn application_should_terminate(
    _self: *mut AnyObject,
    _cmd: Sel,
    _sender: *mut AnyObject,
) -> usize {
    // NSApplicationTerminateReply discriminants.
    const NS_TERMINATE_CANCEL: usize = 0;
    const NS_TERMINATE_NOW: usize = 1;

    // A logout/restart/shutdown must run to completion — do not cancel it.
    if is_system_initiated_quit() {
        return NS_TERMINATE_NOW;
    }

    match APP_HANDLE.get() {
        Some(app) => {
            app.exit(0);
            NS_TERMINATE_CANCEL
        }
        None => {
            // Can't re-drive the exit without the handle; let AppKit terminate.
            tracing::error!("applicationShouldTerminate: fired before install()");
            NS_TERMINATE_NOW
        }
    }
}

/// Pack a four-character code (`OSType`) big-endian — the Rust equivalent of an
/// Objective-C `'abcd'` literal.
const fn four_char_code(code: [u8; 4]) -> u32 {
    u32::from_be_bytes(code)
}

/// Whether the in-flight termination came from the OS (logout / restart /
/// shutdown) rather than the user choosing Quit. macOS delivers a system quit as
/// a `kAEQuitApplication` (`'quit'`) Apple Event carrying a `kAEQuitReason`
/// (`'why?'`) parameter; a user Quit posts `terminate:` directly, so there's no
/// such event. Codes are from `<CoreServices/.../AppleEvents.h>`.
fn is_system_initiated_quit() -> bool {
    const K_AE_QUIT_APPLICATION: u32 = four_char_code(*b"quit");
    const K_AE_QUIT_REASON: u32 = four_char_code(*b"why?");

    let manager = NSAppleEventManager::sharedAppleEventManager();
    let Some(event) = manager.currentAppleEvent() else {
        return false;
    };
    event.eventID() == K_AE_QUIT_APPLICATION
        && event.paramDescriptorForKeyword(K_AE_QUIT_REASON).is_some()
}

#[cfg(test)]
mod tests {
    use super::four_char_code;

    #[test]
    fn packs_big_endian_first_byte_most_significant() {
        // Mirrors the Objective-C `'abcd'` literal: first byte in the high position.
        assert_eq!(four_char_code([0x12, 0x34, 0x56, 0x78]), 0x1234_5678);
    }

    #[test]
    fn matches_the_apple_event_codes_in_use() {
        // 'q'=0x71 'u'=0x75 'i'=0x69 't'=0x74
        assert_eq!(four_char_code(*b"quit"), 0x7175_6974);
        // 'w'=0x77 'h'=0x68 'y'=0x79 '?'=0x3f
        assert_eq!(four_char_code(*b"why?"), 0x7768_793f);
    }

    #[test]
    fn equivalent_to_u32_from_be_bytes() {
        for code in [*b"quit", *b"why?", *b"abcd", [0, 0, 0, 0], [0xff; 4]] {
            assert_eq!(four_char_code(code), u32::from_be_bytes(code));
        }
    }
}
