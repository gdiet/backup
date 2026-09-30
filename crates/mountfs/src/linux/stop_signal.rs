//! Recognizes that libfuse's loop ended because of SIGHUP, SIGINT or SIGTERM (DESIGN-MOUNT-026).
//!
//! `fuse_main_real` returns 8 for a signal and for a real loop failure alike, so the exit code
//! cannot tell them apart. libfuse installs its own handlers for these signals before the loop
//! starts. The `init` callback runs afterwards, on the first request from the kernel. It wraps
//! each of those handlers in [`note_signal`], which sets a flag and then calls the libfuse
//! handler unchanged.
//!
//! A signal that arrives before the first request is not recognized. It still stops the mount,
//! but is reported like a loop failure.

use std::ffi::{c_int, c_void};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::sys;

const SIGNALS: [c_int; 3] = [libc::SIGHUP, libc::SIGINT, libc::SIGTERM];

static STOPPED_BY_SIGNAL: AtomicBool = AtomicBool::new(false);
static LIBFUSE_HANDLERS: [AtomicUsize; 3] = [const { AtomicUsize::new(0) }; 3];

extern "C" fn note_signal(signal: c_int) {
    STOPPED_BY_SIGNAL.store(true, Ordering::SeqCst);
    let Some(index) = SIGNALS.iter().position(|&candidate| candidate == signal) else {
        return;
    };
    let libfuse_handler = LIBFUSE_HANDLERS[index].load(Ordering::SeqCst);
    if libfuse_handler != 0 {
        // SAFETY: the value was read from `sigaction` in `chain_libfuse_handlers` as a plain
        // (non-`SA_SIGINFO`) handler, which has exactly this signature.
        let libfuse_handler: extern "C" fn(c_int) = unsafe { std::mem::transmute(libfuse_handler) };
        libfuse_handler(signal);
    }
}

fn current_action(signal: c_int) -> Option<libc::sigaction> {
    // SAFETY: `sigaction` is plain data, and all-zero is a valid value for it.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    // SAFETY: a null new action only queries; `action` is a valid out-pointer.
    (unsafe { libc::sigaction(signal, std::ptr::null(), &mut action) } == 0).then_some(action)
}

/// Forgets the outcome of any earlier mount. Called right before `fuse_main_real`.
pub(super) fn begin() {
    STOPPED_BY_SIGNAL.store(false, Ordering::SeqCst);
    for handler in &LIBFUSE_HANDLERS {
        handler.store(0, Ordering::SeqCst);
    }
}

/// Whether SIGHUP, SIGINT or SIGTERM reached the process since [`begin`], after `init` ran.
pub(super) fn stopped_by_signal() -> bool {
    STOPPED_BY_SIGNAL.load(Ordering::SeqCst)
}

/// Puts the default disposition back for every signal whose handler is still [`note_signal`].
/// libfuse only resets a handler that is its own, so without this the wrapper would outlive the
/// mount and swallow later signals for the whole process.
pub(super) fn end() {
    for signal in SIGNALS {
        let Some(action) = current_action(signal) else {
            continue;
        };
        if action.sa_sigaction == note_signal as extern "C" fn(c_int) as usize {
            // SAFETY: `SIG_DFL` is a valid disposition, and `signal` is a catchable signal.
            unsafe { libc::signal(signal, libc::SIG_DFL) };
        }
    }
}

fn chain_libfuse_handlers() {
    let ours = note_signal as extern "C" fn(c_int) as usize;
    for (index, signal) in SIGNALS.into_iter().enumerate() {
        let Some(current) = current_action(signal) else {
            continue;
        };
        // Wrap only a plain handler that is not ours yet. Anything else is not the one libfuse
        // installs, or is already wrapped by an earlier mount in this process.
        let handler = current.sa_sigaction;
        if handler == libc::SIG_DFL
            || handler == libc::SIG_IGN
            || handler == ours
            || current.sa_flags & libc::SA_SIGINFO != 0
        {
            continue;
        }
        LIBFUSE_HANDLERS[index].store(handler, Ordering::SeqCst);
        // SAFETY: `sigaction` is plain data, and all-zero is a valid value for it.
        let mut wrapper: libc::sigaction = unsafe { std::mem::zeroed() };
        wrapper.sa_sigaction = ours;
        // SAFETY: `wrapper.sa_mask` is a valid out-pointer. `wrapper` is a complete action, and
        // `signal` is a catchable signal.
        unsafe {
            libc::sigemptyset(&mut wrapper.sa_mask);
            libc::sigaction(signal, &wrapper, std::ptr::null_mut());
        }
    }
}

/// `fuse_operations::init`. Its return value is meant to become the filesystem's private data
/// for all later callbacks, so it hands back the pointer that is already there.
///
/// Not run under `guard_callback`, because that only fits callbacks returning `c_int`. This
/// body has no operation that can panic.
pub(super) unsafe extern "C" fn dispatch_init(
    _connection: *mut c_void,
    _config: *mut c_void,
) -> *mut c_void {
    chain_libfuse_handlers();
    // SAFETY: `init` runs on libfuse's own thread with a valid context.
    unsafe { (*sys::fuse_get_context()).private_data }
}
