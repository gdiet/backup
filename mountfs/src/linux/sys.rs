//! Hand-written bindings to libfuse3's *high-level*, path-based C API
//! (`fuse3/fuse.h`) - deliberately not bindgen-generated, see
//! `docs/plans/implemented/05-cross-platform-mount-crate.md`. This is the same API surface
//! WinFSP's `cygfuse` compatibility layer emulates on Windows, which is the
//! whole point of building against it instead of `fuser`'s low-level,
//! Linux-only `/dev/fuse` protocol.
//!
//! The full read-only set plus phase 2a's structural read-write set
//! (`mkdir`/`create`/`unlink`/`rmdir`/`rename`/`utimens`/`chmod`/`chown`)
//! and phase 2b's `write`/`truncate` (see
//! `docs/plans/implemented/06-fuse-mount-readwrite.md`) are given real signatures; every
//! other `fuse_operations` slot (everything this crate has no plans for)
//! is typed as a same-size, unused function-pointer placeholder - correct
//! for `struct` layout/size purposes (all slots are pointer-sized on every
//! platform this targets) without committing to a signature before it's
//! implemented.

#![allow(non_camel_case_types)]

use std::ffi::CStr;
use std::io;
use std::sync::OnceLock;

use libc::{
    c_char, c_int, c_uint, c_void, gid_t, mode_t, off_t, pid_t, size_t, stat, statvfs, timespec,
    uid_t,
};

/// A `fuse_operations` slot this crate doesn't implement yet. Same size
/// (one pointer) and alignment as every other slot regardless of its real
/// C signature, so leaving slots typed this way doesn't affect the
/// struct's layout - only slots this crate actually assigns need their
/// real signature.
pub type Unimplemented = Option<unsafe extern "C" fn()>;

/// Passed to the `fuse_fill_dir_t` callback; `FUSE_FILL_DIR_PLUS` (bit 1) is
/// not used by this crate yet (no callers pass `stbuf`).
pub type fuse_fill_dir_flags = c_int;

/// Passed into `readdir`; `FUSE_READDIR_PLUS` (bit 0) is not requested by
/// this crate yet.
pub type fuse_readdir_flags = c_int;

pub type fuse_fill_dir_t = Option<
    unsafe extern "C" fn(
        buf: *mut c_void,
        name: *const c_char,
        stbuf: *const stat,
        off: off_t,
        flags: fuse_fill_dir_flags,
    ) -> c_int,
>;

/// Mirrors libfuse3's `struct fuse_file_info` (`fuse_common.h`). The
/// individual bitfields (`writepage`/`direct_io`/`keep_cache`/...) are
/// collapsed into a single `bits` field here since nothing in this crate
/// reads or writes them yet - only the overall struct size/layout matters
/// for now.
#[repr(C)]
pub struct fuse_file_info {
    pub flags: c_int,
    pub bits: u32,
    pub padding2: u32,
    pub fh: u64,
    pub lock_owner: u64,
    pub poll_events: u32,
}

/// Mirrors libfuse3's `struct fuse_operations` (`fuse3/fuse.h`) field for
/// field, in declaration order - the struct is a flat sequence of
/// (mostly) 8-byte function-pointer slots, so getting the *order* and
/// *count* right matters for `sizeof`/layout even for slots this crate
/// leaves null; getting each slot's exact C signature right only matters
/// for the ones it actually calls.
#[repr(C)]
pub struct fuse_operations {
    pub getattr:
        Option<unsafe extern "C" fn(*const c_char, *mut stat, *mut fuse_file_info) -> c_int>,
    pub readlink: Unimplemented,
    pub mknod: Unimplemented,
    pub mkdir: Option<unsafe extern "C" fn(*const c_char, mode_t) -> c_int>,
    pub unlink: Option<unsafe extern "C" fn(*const c_char) -> c_int>,
    pub rmdir: Option<unsafe extern "C" fn(*const c_char) -> c_int>,
    pub symlink: Unimplemented,
    pub rename: Option<unsafe extern "C" fn(*const c_char, *const c_char, c_uint) -> c_int>,
    pub link: Unimplemented,
    pub chmod: Option<unsafe extern "C" fn(*const c_char, mode_t, *mut fuse_file_info) -> c_int>,
    pub chown:
        Option<unsafe extern "C" fn(*const c_char, uid_t, gid_t, *mut fuse_file_info) -> c_int>,
    pub truncate: Option<unsafe extern "C" fn(*const c_char, off_t, *mut fuse_file_info) -> c_int>,
    pub open: Option<unsafe extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub read: Option<
        unsafe extern "C" fn(
            *const c_char,
            *mut c_char,
            size_t,
            off_t,
            *mut fuse_file_info,
        ) -> c_int,
    >,
    pub write: Option<
        unsafe extern "C" fn(
            *const c_char,
            *const c_char,
            size_t,
            off_t,
            *mut fuse_file_info,
        ) -> c_int,
    >,
    pub statfs: Option<unsafe extern "C" fn(*const c_char, *mut statvfs) -> c_int>,
    pub flush: Unimplemented,
    pub release: Option<unsafe extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub fsync: Unimplemented,
    pub setxattr: Unimplemented,
    pub getxattr: Unimplemented,
    pub listxattr: Unimplemented,
    pub removexattr: Unimplemented,
    pub opendir: Unimplemented,
    pub readdir: Option<
        unsafe extern "C" fn(
            *const c_char,
            *mut c_void,
            fuse_fill_dir_t,
            off_t,
            *mut fuse_file_info,
            fuse_readdir_flags,
        ) -> c_int,
    >,
    pub releasedir: Unimplemented,
    pub fsyncdir: Unimplemented,
    pub init: Unimplemented,
    pub destroy: Unimplemented,
    pub access: Unimplemented,
    pub create: Option<unsafe extern "C" fn(*const c_char, mode_t, *mut fuse_file_info) -> c_int>,
    pub lock: Unimplemented,
    pub utimens:
        Option<unsafe extern "C" fn(*const c_char, *const timespec, *mut fuse_file_info) -> c_int>,
    pub bmap: Unimplemented,
    pub ioctl: Unimplemented,
    pub poll: Unimplemented,
    pub write_buf: Unimplemented,
    pub read_buf: Unimplemented,
    pub flock: Unimplemented,
    pub fallocate: Unimplemented,
    pub copy_file_range: Unimplemented,
    pub lseek: Unimplemented,
}

impl Default for fuse_operations {
    /// All-null (every operation unimplemented/`ENOSYS`) - callers set only
    /// the slots they implement.
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

/// Mirrors libfuse3's `struct fuse_context` (`fuse.h`) - only
/// `private_data` is used by this crate so far (to recover the
/// `&dyn MountFilesystem` passed as `fuse_main_real`'s `private_data`
/// argument from inside a callback).
#[repr(C)]
pub struct fuse_context {
    pub fuse: *mut c_void,
    pub uid: uid_t,
    pub gid: gid_t,
    pub pid: pid_t,
    pub private_data: *mut c_void,
    pub umask: mode_t,
}

type FuseMainReal = unsafe extern "C" fn(
    argc: c_int,
    argv: *mut *mut c_char,
    op: *const fuse_operations,
    op_size: size_t,
    private_data: *mut c_void,
) -> c_int;

/// Valid only during a callback made by libfuse on the calling thread.
type FuseGetContext = unsafe extern "C" fn() -> *mut fuse_context;

struct Exports {
    main_real: FuseMainReal,
    get_context: FuseGetContext,
}

// SAFETY: these are plain code pointers into a library that stays loaded
// and unchanged for the life of the process; sharing them across threads
// is exactly what every other consumer of this API does too (same
// rationale as `windows::sys::Exports`).
unsafe impl Send for Exports {}
unsafe impl Sync for Exports {}

unsafe fn resolve<T: Copy>(handle: *mut c_void, name: &CStr) -> Option<T> {
    let addr = unsafe { libc::dlsym(handle, name.as_ptr()) };
    if addr.is_null() {
        None
    } else {
        // Same shape every FFI crate binding `dlsym` relies on: a function
        // pointer and a data pointer are both plain machine addresses.
        Some(unsafe { std::mem::transmute_copy::<*mut c_void, T>(&addr) })
    }
}

/// libfuse3 is a runtime-only dependency (unlike a system linked at build
/// time, it's genuinely possible to run this binary on a system without it
/// installed) - reported as an `io::Result` instead of panicking, same
/// rationale as `windows::sys::exports` (see `AGENTS.md`'s "Code Quality"
/// section on when a bare panic is/isn't appropriate).
fn exports() -> io::Result<&'static Exports> {
    static EXPORTS: OnceLock<Result<Exports, String>> = OnceLock::new();
    EXPORTS
        .get_or_init(|| {
            let handle = unsafe { libc::dlopen(c"libfuse3.so.3".as_ptr(), libc::RTLD_NOW) };
            if handle.is_null() {
                let reason = unsafe { CStr::from_ptr(libc::dlerror()) }
                    .to_string_lossy()
                    .into_owned();
                return Err(format!(
                    "libfuse3 not found ({reason}) - install it (Debian/Ubuntu: `apt install \
                     libfuse3-3`; Fedora: usually already present as `fuse3`)"
                ));
            }
            let main_real = unsafe { resolve(handle, c"fuse_main_real") }
                .ok_or("libfuse3.so.3 is missing fuse_main_real")?;
            let get_context = unsafe { resolve(handle, c"fuse_get_context") }
                .ok_or("libfuse3.so.3 is missing fuse_get_context")?;
            Ok(Exports {
                main_real,
                get_context,
            })
        })
        .as_ref()
        .map_err(|msg| io::Error::other(msg.clone()))
}

/// Cheap check for whether libfuse3 is actually available right now,
/// without starting a mount - see `exports`. Exposed so `linux::mount`'s
/// caller can report a clean error before announcing a mount as started,
/// rather than after (mirrors `windows::sys::check_available`).
pub fn check_available() -> io::Result<()> {
    exports().map(|_| ())
}

/// `fuse_main_real` - the non-macro function `fuse_main(argc, argv, op,
/// private_data)` expands to (`fuse_main` adds `sizeof(*op)` as `op_size`,
/// which a hand-written binding has to pass explicitly since it can't rely
/// on the C macro). Returns `Err` if libfuse3 itself couldn't be located
/// (see `exports`) - distinct from a nonzero exit code, which the caller
/// (`linux::mount`) still has to check separately.
pub unsafe fn fuse_main_real(
    argc: c_int,
    argv: *mut *mut c_char,
    op: *const fuse_operations,
    op_size: size_t,
    private_data: *mut c_void,
) -> io::Result<c_int> {
    let main_real = exports()?.main_real;
    Ok(unsafe { main_real(argc, argv, op, op_size, private_data) })
}

pub fn fuse_get_context() -> *mut fuse_context {
    // Only ever called from within a dispatch_* trampoline, i.e. from
    // libfuse's own calling thread during a callback - reachable only once
    // fuse_main_real above already resolved exports() successfully, so the
    // cached OnceLock result here is always Ok (mirrors
    // `windows::sys::fuse_get_context`).
    let get_context = exports()
        .expect("fuse_get_context called before fuse_main_real resolved libfuse3")
        .get_context;
    unsafe { get_context() }
}
