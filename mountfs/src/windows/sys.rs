//! Hand-written bindings to WinFSP's *FUSE3-compatible* API
//! (`fuse3/fuse.h`, vendored unmodified under `mountfs/vendor/winfsp/` -
//! see `mountfs/vendor/winfsp/NOTICE.md`) - the same high-level, path-based
//! shape as real libfuse3's API that `linux/sys.rs` binds directly, which
//! is the entire point of this crate (see
//! `docs/plans/cross-platform-mount-crate.md`).
//!
//! Unlike the Linux backend, nothing here is linked at build time: WinFSP's
//! *runtime* installer (all an end user needs) ships no headers or import
//! library, only `bin\winfsp-x64.dll` - so this loads that DLL and resolves
//! the handful of exports it needs (`fsp_fuse3_main_real`,
//! `fsp_fuse3_get_context`) itself, via `LoadLibraryW`/`GetProcAddress`,
//! falling back to the `HKLM\Software\WOW6432Node\WinFsp\InstallDir`
//! registry value WinFSP's installer writes if the DLL isn't already on
//! `PATH` - reimplementing WinFSP's own `FspLoad` helper
//! (`inc/winfsp/winfsp.h`) rather than vendoring that much larger header
//! just for one function.
//!
//! `getattr`/`readdir`/`open`/`read`/`release`/`statfs` (the full read-only
//! set, matching the Linux backend) are given real signatures - every
//! other `fuse3_operations` slot is typed as a same-size, unused
//! function-pointer placeholder.

#![allow(non_camel_case_types)]

use std::ffi::{CStr, c_char, c_int, c_void};
use std::sync::OnceLock;

// --- WinFSP's `fuse_*` types (Win64, non-Cygwin - `inc/fuse/winfsp_fuse.h`'s
// `#if defined(_WIN64) || defined(_WIN32)` branch) - distinct from `libc`'s
// POSIX types, since Windows has no native equivalents. ---
pub type fuse_uid_t = u32;
pub type fuse_gid_t = u32;
pub type fuse_pid_t = i32;
pub type fuse_dev_t = u32;
pub type fuse_ino_t = u64;
pub type fuse_mode_t = u32;
pub type fuse_nlink_t = u16;
pub type fuse_off_t = i64;
pub type fuse_blksize_t = i32;
pub type fuse_blkcnt_t = i64;

#[repr(C)]
pub struct fuse_timespec {
    pub tv_sec: i64,
    pub tv_nsec: i64,
}

/// Mirrors `FSP_FUSE_STAT_FIELD_DEFN` (`inc/fuse/winfsp_fuse.h`) - the
/// non-`_EX` variant (`FSP_FUSE_USE_STAT_EX` isn't defined).
#[repr(C)]
pub struct fuse_stat {
    pub st_dev: fuse_dev_t,
    pub st_ino: fuse_ino_t,
    pub st_mode: fuse_mode_t,
    pub st_nlink: fuse_nlink_t,
    pub st_uid: fuse_uid_t,
    pub st_gid: fuse_gid_t,
    pub st_rdev: fuse_dev_t,
    pub st_size: fuse_off_t,
    pub st_atim: fuse_timespec,
    pub st_mtim: fuse_timespec,
    pub st_ctim: fuse_timespec,
    pub st_blksize: fuse_blksize_t,
    pub st_blocks: fuse_blkcnt_t,
    pub st_birthtim: fuse_timespec,
}

/// Mirrors `struct fuse_statvfs` (Win64 variant - all fields are `u64`).
#[repr(C)]
pub struct fuse_statvfs {
    pub f_bsize: u64,
    pub f_frsize: u64,
    pub f_blocks: u64,
    pub f_bfree: u64,
    pub f_bavail: u64,
    pub f_files: u64,
    pub f_ffree: u64,
    pub f_favail: u64,
    pub f_fsid: u64,
    pub f_flag: u64,
    pub f_namemax: u64,
}

/// Mirrors `struct fuse3_file_info` (`fuse3/fuse_common.h`):
/// ```c
/// int flags;
/// unsigned int writepage:1, direct_io:1, keep_cache:1, flush:1,
///              nonseekable:1, flock_release:1, padding:27;
/// uint64_t fh;
/// uint64_t lock_owner;
/// uint32_t poll_events;
/// ```
/// The six named 1-bit fields plus `padding:27` add up to 33 bits, which
/// overflows a single 32-bit `unsigned int` storage unit - MSVC (like
/// GCC) starts a *second* 4-byte unit for the field that doesn't fit
/// (`padding:27`), rather than splitting a field across two units. That
/// second unit is collapsed into plain `bits2` here (nothing reads the
/// individual bits yet, same rationale as the Linux backend's equivalent
/// type) - getting this wrong shifts `fh` from offset 16 to offset 8,
/// which silently corrupted the file handle threaded through `open`/
/// `read`/`release` (confirmed by a wrong-file-content bug once `read`
/// started actually using `fh`, not just an untested theoretical
/// mismatch).
#[repr(C)]
pub struct fuse_file_info {
    pub flags: c_int,
    pub bits1: u32,
    pub bits2: u32,
    pub fh: u64,
    pub lock_owner: u64,
    pub poll_events: u32,
}

pub type fuse_fill_dir_flags = c_int;
pub type fuse_readdir_flags = c_int;

pub type fuse_fill_dir_t = Option<
    unsafe extern "C" fn(
        buf: *mut c_void,
        name: *const c_char,
        stbuf: *const fuse_stat,
        off: fuse_off_t,
        flags: fuse_fill_dir_flags,
    ) -> c_int,
>;

/// A `fuse3_operations` slot this crate doesn't implement yet - see
/// `linux::sys::Unimplemented`'s doc comment for why same-size placeholder
/// typing is fine here.
pub type Unimplemented = Option<unsafe extern "C" fn()>;

/// Mirrors `struct fuse3_operations` (`fuse3/fuse.h`) field for field, in
/// declaration order - note this is *not* identical to real libfuse3's
/// `struct fuse_operations` (`linux::sys::fuse_operations`): WinFSP's
/// version has no `lseek` or `copy_file_range` slots (40 fields here vs.
/// 42 on Linux), everything else lines up 1:1.
#[repr(C)]
pub struct fuse_operations {
    pub getattr:
        Option<unsafe extern "C" fn(*const c_char, *mut fuse_stat, *mut fuse_file_info) -> c_int>,
    pub readlink: Unimplemented,
    pub mknod: Unimplemented,
    pub mkdir: Unimplemented,
    pub unlink: Unimplemented,
    pub rmdir: Unimplemented,
    pub symlink: Unimplemented,
    pub rename: Unimplemented,
    pub link: Unimplemented,
    pub chmod: Unimplemented,
    pub chown: Unimplemented,
    pub truncate: Unimplemented,
    pub open: Option<unsafe extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub read: Option<
        unsafe extern "C" fn(
            *const c_char,
            *mut c_char,
            usize,
            fuse_off_t,
            *mut fuse_file_info,
        ) -> c_int,
    >,
    pub write: Unimplemented,
    pub statfs: Option<unsafe extern "C" fn(*const c_char, *mut fuse_statvfs) -> c_int>,
    pub flush: Option<unsafe extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub release: Option<unsafe extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub fsync: Unimplemented,
    pub setxattr: Unimplemented,
    pub getxattr: Unimplemented,
    pub listxattr: Unimplemented,
    pub removexattr: Unimplemented,
    pub opendir: Option<unsafe extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub readdir: Option<
        unsafe extern "C" fn(
            *const c_char,
            *mut c_void,
            fuse_fill_dir_t,
            fuse_off_t,
            *mut fuse_file_info,
            fuse_readdir_flags,
        ) -> c_int,
    >,
    pub releasedir: Unimplemented,
    pub fsyncdir: Unimplemented,
    pub init: Unimplemented,
    pub destroy: Unimplemented,
    pub access: Option<unsafe extern "C" fn(*const c_char, c_int) -> c_int>,
    pub create: Unimplemented,
    pub lock: Unimplemented,
    pub utimens: Unimplemented,
    pub bmap: Unimplemented,
    pub ioctl: Unimplemented,
    pub poll: Unimplemented,
    pub write_buf: Unimplemented,
    pub read_buf: Unimplemented,
    pub flock: Unimplemented,
    pub fallocate: Unimplemented,
}

impl Default for fuse_operations {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

#[repr(C)]
pub struct fuse_context {
    pub fuse: *mut c_void,
    pub uid: fuse_uid_t,
    pub gid: fuse_gid_t,
    pub pid: fuse_pid_t,
    pub private_data: *mut c_void,
    pub umask: fuse_mode_t,
}

/// Mirrors `struct fsp_fuse_env` (`inc/fuse/winfsp_fuse.h`) - the table of
/// callbacks every `fsp_fuse3_*` DLL export takes as its first argument.
/// Built once by [`fuse_env`], matching `FSP_FUSE_ENV_INIT`'s Windows
/// branch exactly (`'W'`, libc's `malloc`/`free`, and this crate's own
/// no-op `daemonize`/`set_signal_handlers` - real Windows processes never
/// fork, so there's nothing for either to do; WinFSP's own header stubs
/// them out identically on this platform).
#[repr(C)]
struct fuse_env {
    environment: u32,
    memalloc: Option<unsafe extern "C" fn(usize) -> *mut c_void>,
    memfree: Option<unsafe extern "C" fn(*mut c_void)>,
    daemonize: Option<unsafe extern "C" fn(c_int) -> c_int>,
    set_signal_handlers: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
    conv_to_win_path: Option<unsafe extern "C" fn(*const c_char) -> *mut c_char>,
    winpid_to_pid: Option<unsafe extern "C" fn(u32) -> fuse_pid_t>,
    reserved: [Option<unsafe extern "C" fn()>; 2],
}

unsafe extern "C" fn noop_daemonize(_foreground: c_int) -> c_int {
    0
}

unsafe extern "C" fn noop_set_signal_handlers(_se: *mut c_void) -> c_int {
    0
}

#[link(name = "kernel32")]
unsafe extern "system" {
    fn GetProcessHeap() -> *mut c_void;
    fn HeapAlloc(h_heap: *mut c_void, dw_flags: u32, dw_bytes: usize) -> *mut c_void;
    fn HeapFree(h_heap: *mut c_void, dw_flags: u32, lp_mem: *mut c_void) -> i32;
}

/// Uses the process's default OS heap (`GetProcessHeap`) rather than
/// `libc::malloc`/`free` - ruling out a CRT-mismatch heap corruption
/// hypothesis (winfsp-x64.dll and this Rust binary could plausibly link
/// against different CRT instances; `HeapAlloc`/`HeapFree` operate on an
/// OS-level heap handle, not a CRT-specific one, so are safe to pair
/// across a DLL boundary regardless of which CRT compiled which side).
unsafe extern "C" fn heap_alloc(size: usize) -> *mut c_void {
    unsafe { HeapAlloc(GetProcessHeap(), 0, size) }
}

unsafe extern "C" fn heap_free(ptr: *mut c_void) {
    if !ptr.is_null() {
        unsafe {
            HeapFree(GetProcessHeap(), 0, ptr);
        }
    }
}

fn fuse_env() -> &'static fuse_env {
    static ENV: fuse_env = fuse_env {
        environment: b'W' as u32,
        memalloc: Some(heap_alloc),
        memfree: Some(heap_free),
        daemonize: Some(noop_daemonize),
        set_signal_handlers: Some(noop_set_signal_handlers),
        conv_to_win_path: None,
        winpid_to_pid: None,
        reserved: [None, None],
    };
    &ENV
}

type FuseMainReal = unsafe extern "C" fn(
    env: *const fuse_env,
    argc: c_int,
    argv: *mut *mut c_char,
    ops: *const fuse_operations,
    opsize: usize,
    data: *mut c_void,
) -> c_int;

type FuseGetContext = unsafe extern "C" fn(env: *const fuse_env) -> *mut fuse_context;

#[link(name = "kernel32")]
unsafe extern "system" {
    fn LoadLibraryW(lp_lib_file_name: *const u16) -> *mut c_void;
    fn GetProcAddress(h_module: *mut c_void, lp_proc_name: *const c_char) -> *mut c_void;
}

#[link(name = "advapi32")]
unsafe extern "system" {
    fn RegGetValueW(
        hkey: *mut c_void,
        lp_sub_key: *const u16,
        lp_value: *const u16,
        dw_flags: u32,
        pdw_type: *mut u32,
        pv_data: *mut c_void,
        pcb_data: *mut u32,
    ) -> i32;
}

const HKEY_LOCAL_MACHINE: *mut c_void = 0x8000_0002_usize as *mut c_void;
const RRF_RT_REG_SZ: u32 = 0x0000_0002;

fn to_wide_null(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

/// Reimplements WinFSP's own `FspLoad` (`inc/winfsp/winfsp.h`): try
/// `LoadLibraryW("winfsp-x64.dll")` as-is first (found if it's already on
/// `PATH`), and if that fails, look up its install directory from the
/// registry key WinFSP's installer writes and load it from there.
fn load_winfsp_dll() -> Option<*mut c_void> {
    let dll_name = to_wide_null("winfsp-x64.dll");
    let module = unsafe { LoadLibraryW(dll_name.as_ptr()) };
    if !module.is_null() {
        return Some(module);
    }

    let subkey = to_wide_null("Software\\WOW6432Node\\WinFsp");
    let value = to_wide_null("InstallDir");
    let mut buf = [0u16; 260];
    let mut size = (buf.len() * std::mem::size_of::<u16>()) as u32;
    let status = unsafe {
        RegGetValueW(
            HKEY_LOCAL_MACHINE,
            subkey.as_ptr(),
            value.as_ptr(),
            RRF_RT_REG_SZ,
            std::ptr::null_mut(),
            buf.as_mut_ptr().cast(),
            &mut size,
        )
    };
    if status != 0 {
        return None;
    }
    let install_dir_len = buf.iter().position(|&c| c == 0).unwrap_or(0);
    let mut path: Vec<u16> = buf[..install_dir_len].to_vec();
    path.extend("bin\\winfsp-x64.dll".encode_utf16());
    path.push(0);

    let module = unsafe { LoadLibraryW(path.as_ptr()) };
    if module.is_null() { None } else { Some(module) }
}

unsafe fn resolve<T: Copy>(module: *mut c_void, name: &CStr) -> Option<T> {
    let addr = unsafe { GetProcAddress(module, name.as_ptr()) };
    if addr.is_null() {
        None
    } else {
        // Same shape every FFI crate binding `GetProcAddress` relies on:
        // a function pointer and a data pointer are both plain machine
        // addresses on every platform Rust supports Windows on.
        Some(unsafe { std::mem::transmute_copy::<*mut c_void, T>(&addr) })
    }
}

struct Exports {
    main_real: FuseMainReal,
    get_context: FuseGetContext,
}

// SAFETY: these are plain code pointers into a DLL that stays loaded and
// unchanged for the life of the process; sharing them across threads is
// exactly what every other consumer of this API does too.
unsafe impl Send for Exports {}
unsafe impl Sync for Exports {}

fn exports() -> &'static Exports {
    static EXPORTS: OnceLock<Exports> = OnceLock::new();
    EXPORTS.get_or_init(|| {
        let module =
            load_winfsp_dll().expect("failed to locate winfsp-x64.dll (is WinFSP installed?)");
        let main_real = unsafe { resolve(module, c"fsp_fuse3_main_real") }
            .expect("winfsp-x64.dll is missing fsp_fuse3_main_real");
        let get_context = unsafe { resolve(module, c"fsp_fuse3_get_context") }
            .expect("winfsp-x64.dll is missing fsp_fuse3_get_context");
        Exports {
            main_real,
            get_context,
        }
    })
}

/// Equivalent to real libfuse3's `fuse_main_real` (see `linux::sys`) -
/// `op_size` must be `size_of::<fuse_operations>()`, same caveat as there.
pub unsafe fn fuse_main_real(
    argc: c_int,
    argv: *mut *mut c_char,
    op: *const fuse_operations,
    op_size: usize,
    private_data: *mut c_void,
) -> c_int {
    let main_real = exports().main_real;
    unsafe { main_real(fuse_env(), argc, argv, op, op_size, private_data) }
}

/// Equivalent to real libfuse3's `fuse_get_context` (see `linux::sys`).
pub fn fuse_get_context() -> *mut fuse_context {
    let get_context = exports().get_context;
    unsafe { get_context(fuse_env()) }
}
