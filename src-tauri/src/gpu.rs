//! Headless / no-GPU guard for the Linux WebKitGTK webview.
//!
//! WebKitGTK 2.42+ made the EGL accelerated compositor effectively
//! mandatory: when it can't obtain a usable EGL display (bare QEMU VMs
//! without virtio-gpu, broken/absent Mesa, some `ssh -X` setups) it
//! prints `Could not create default EGL display: EGL_BAD_PARAMETER` and
//! **aborts the whole process** before any window appears.
//!
//! Setting `WEBKIT_DISABLE_COMPOSITING_MODE=1` makes WebKit fall back to
//! software rendering instead of aborting. Rather than always force the
//! slow path, we probe EGL ourselves at startup and only set the var
//! when the probe fails — so machines with a working GPU keep hardware
//! compositing, and "odd setups" get a working (if slower) window
//! instead of a hard crash.
//!
//! Linux-only: macOS uses WKWebView and Windows uses WebView2, neither
//! of which is affected. The non-Linux build is a no-op.

/// WebKitGTK reads this when the webview is created; we must set it
/// before Tauri builds the window (hence the call at the top of `run`).
/// Linux-only — it has no consumer on the no-op macOS/Windows path.
#[cfg(target_os = "linux")]
pub const WEBKIT_COMPOSITING_ENV: &str = "WEBKIT_DISABLE_COMPOSITING_MODE";

/// Probe EGL and, if it can't initialize, set
/// `WEBKIT_DISABLE_COMPOSITING_MODE=1` so WebKitGTK degrades to software
/// rendering rather than aborting. An explicit operator-set value (in
/// either direction) is always respected — the env var stays the escape
/// hatch. Logs via `eprintln!` because this runs before the logging
/// plugin is initialized.
#[cfg(target_os = "linux")]
pub fn apply_webkit_compositing_fallback() {
    if std::env::var_os(WEBKIT_COMPOSITING_ENV).is_some() {
        // Operator already decided; don't second-guess them.
        return;
    }

    if egl_display_initializes() {
        return;
    }

    eprintln!(
        "kstack: no usable EGL display detected; setting {WEBKIT_COMPOSITING_ENV}=1 \
         so the webview uses software rendering instead of aborting \
         (set it explicitly to override)"
    );
    // Safe on edition 2021, and correct here: single-threaded startup,
    // before the Tauri builder runs and before any webview/thread that
    // reads the environment is spawned.
    std::env::set_var(WEBKIT_COMPOSITING_ENV, "1");
}

#[cfg(not(target_os = "linux"))]
pub fn apply_webkit_compositing_fallback() {}

/// `true` iff `libEGL` loads and `eglInitialize(eglGetDisplay(
/// EGL_DEFAULT_DISPLAY))` succeeds. Any uncertainty (missing library,
/// missing symbols, no display, init failure) returns `false`: a false
/// "no GPU" only costs software compositing, whereas a false "GPU ok"
/// reintroduces the hard abort we're guarding against, so we bias to
/// the safe side.
#[cfg(target_os = "linux")]
fn egl_display_initializes() -> bool {
    use std::ffi::CString;

    let Ok(soname) = CString::new("libEGL.so.1") else {
        return false;
    };
    // SAFETY: `soname` is a valid NUL-terminated string kept alive across
    // the call; `RTLD_NOW` is a valid flag. Returns null on failure,
    // which we check before any use.
    let lib = unsafe { libc::dlopen(soname.as_ptr(), libc::RTLD_NOW) };
    if lib.is_null() {
        // No system EGL at all — WebKit can't use it either.
        return false;
    }

    let ok = egl_default_display_works(lib);

    // SAFETY: `lib` is a live handle returned by `dlopen` and not used
    // after this point.
    unsafe {
        libc::dlclose(lib);
    }
    ok
}

/// Resolve `name` from an already-`dlopen`ed handle, or `None`. `dlsym`
/// can legitimately return null, so the result is null-checked here
/// before any caller transmutes it to a callable fn pointer.
#[cfg(target_os = "linux")]
fn egl_sym(lib: *mut std::os::raw::c_void, name: &str) -> Option<*mut std::os::raw::c_void> {
    let cname = std::ffi::CString::new(name).ok()?;
    // SAFETY: `lib` is a live handle from `dlopen`; `cname` is a valid
    // NUL-terminated string alive across the call.
    let sym = unsafe { libc::dlsym(lib, cname.as_ptr()) };
    if sym.is_null() {
        None
    } else {
        Some(sym)
    }
}

/// `eglInitialize(eglGetDisplay(EGL_DEFAULT_DISPLAY))` against the open
/// `libEGL` handle. Any missing symbol, null display, or init failure
/// reports `false`. Split out from [`egl_display_initializes`] so the
/// `dlclose` there always runs regardless of which branch returns.
#[cfg(target_os = "linux")]
fn egl_default_display_works(lib: *mut std::os::raw::c_void) -> bool {
    use std::os::raw::{c_int, c_uint, c_void};
    use std::ptr;

    // `EGLBoolean` is a 32-bit unsigned int; `EGL_TRUE` == 1.
    type EglGetDisplay = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
    type EglInitialize = unsafe extern "C" fn(*mut c_void, *mut c_int, *mut c_int) -> c_uint;
    type EglTerminate = unsafe extern "C" fn(*mut c_void) -> c_uint;

    let (Some(get_display), Some(initialize)) =
        (egl_sym(lib, "eglGetDisplay"), egl_sym(lib, "eglInitialize"))
    else {
        return false;
    };
    // SAFETY: both symbols resolved non-null above; POSIX guarantees a
    // `dlsym` result is convertible to a function pointer, and the ABI
    // matches the EGL spec's declarations for these entry points.
    let get_display = unsafe { std::mem::transmute::<*mut c_void, EglGetDisplay>(get_display) };
    let initialize = unsafe { std::mem::transmute::<*mut c_void, EglInitialize>(initialize) };

    // `EGL_DEFAULT_DISPLAY` is a null native display handle.
    // SAFETY: spec-conformant call; a failed `eglGetDisplay` yields
    // `EGL_NO_DISPLAY` (null), checked next.
    let display = unsafe { get_display(ptr::null_mut()) };
    if display.is_null() {
        return false;
    }

    let mut major: c_int = 0;
    let mut minor: c_int = 0;
    // SAFETY: `display` is non-null from `eglGetDisplay`; both
    // out-pointers are valid for the duration of the call.
    let ok = unsafe { initialize(display, &mut major, &mut minor) } == 1;

    // Release the display we just brought up so WebKit starts from a
    // clean slate; best-effort, skipped if the symbol is absent.
    if ok {
        if let Some(terminate) = egl_sym(lib, "eglTerminate") {
            // SAFETY: `terminate` resolved non-null above; POSIX
            // guarantees a `dlsym` result is convertible to a function
            // pointer, and the ABI matches `eglTerminate`.
            let terminate =
                unsafe { std::mem::transmute::<*mut c_void, EglTerminate>(terminate) };
            // SAFETY: `display` is the live display brought up above.
            unsafe {
                terminate(display);
            }
        }
    }
    ok
}
