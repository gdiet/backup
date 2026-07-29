//! Windows backend: bindings to WinFSP's FUSE3-compatible API (see
//! `sys.rs`) - the same high-level, path-based shape the Linux backend
//! binds directly against real libfuse3. See
//! `docs/plans/cross-platform-mount-crate.md`.
//!
//! Like the Linux backend's first commit, this is currently just the
//! sequencing plan's step-4 spike: a hardcoded, read-only, single-file
//! in-memory filesystem implementing only `getattr` and `readdir`, proving
//! this crate's shared-API approach actually mounts and serves real
//! syscalls through WinFSP, not just that the bindings compile. No public
//! `MountFilesystem` dispatch yet (that's the Linux backend's second
//! commit's counterpart, not done here yet).

mod sys;

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::path::Path;
use std::sync::atomic::{AtomicPtr, Ordering};

use sys::{fuse_off_t, fuse_stat};

const HELLO_NAME: &CStr = c"hello.txt";
const HELLO_PATH: &CStr = c"/hello.txt";
const HELLO_CONTENT: &[u8] = b"hello from the mountfs Windows spike\n";

/// The `struct fuse3 *` handle for the currently-running mount, captured
/// from `fuse_get_context()` the first time any callback fires - see
/// `sys::fuse_exit`'s doc comment for why this is the only way to get it.
static ACTIVE_FUSE_HANDLE: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());

fn debug_log(msg: &str) {
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(std::env::temp_dir().join("mountfs_debug.log"))
    {
        let _ = writeln!(f, "{msg}");
        let _ = f.flush();
    }
}

fn remember_fuse_handle() {
    let ctx = sys::fuse_get_context();
    if !ctx.is_null() {
        let fuse = unsafe { (*ctx).fuse };
        if !fuse.is_null() {
            ACTIVE_FUSE_HANDLE.store(fuse, Ordering::Release);
        }
    }
}

unsafe extern "C" fn getattr(
    path: *const c_char,
    stbuf: *mut fuse_stat,
    fi: *mut sys::fuse_file_info,
) -> c_int {
    remember_fuse_handle();
    let path = unsafe { CStr::from_ptr(path) };
    debug_log(&format!("getattr enter path={path:?} fi={fi:?}"));
    unsafe { std::ptr::write_bytes(stbuf, 0, 1) };
    let result = if path.to_bytes() == b"/" {
        unsafe {
            (*stbuf).st_mode = 0o040000 | 0o755; // S_IFDIR
            (*stbuf).st_nlink = 2;
        }
        0
    } else if path == HELLO_PATH {
        unsafe {
            (*stbuf).st_mode = 0o100000 | 0o444; // S_IFREG
            (*stbuf).st_nlink = 1;
            (*stbuf).st_size = HELLO_CONTENT.len() as fuse_off_t;
        }
        0
    } else {
        -libc::ENOENT
    };
    debug_log(&format!("getattr exit result={result}"));
    result
}

unsafe extern "C" fn readdir(
    path: *const c_char,
    buf: *mut c_void,
    filler: sys::fuse_fill_dir_t,
    _offset: fuse_off_t,
    _fi: *mut sys::fuse_file_info,
    _flags: sys::fuse_readdir_flags,
) -> c_int {
    remember_fuse_handle();
    let path = unsafe { CStr::from_ptr(path) };
    debug_log(&format!("readdir enter path={path:?}"));
    if path.to_bytes() != b"/" {
        return -libc::ENOENT;
    }
    let Some(filler) = filler else {
        return -libc::EIO;
    };
    for name in [c".", c"..", HELLO_NAME] {
        unsafe { filler(buf, name.as_ptr(), std::ptr::null(), 0, 0) };
    }
    debug_log("readdir exit");
    0
}

unsafe extern "C" fn open(path: *const c_char, fi: *mut sys::fuse_file_info) -> c_int {
    let path = unsafe { CStr::from_ptr(path) };
    debug_log(&format!("open enter path={path:?} fi={fi:?}"));
    let result = if path == HELLO_PATH { 0 } else { -libc::ENOENT };
    debug_log(&format!("open exit result={result}"));
    result
}

unsafe extern "C" fn release(_path: *const c_char, _fi: *mut sys::fuse_file_info) -> c_int {
    debug_log("release enter/exit");
    0
}

unsafe extern "C" fn flush(_path: *const c_char, _fi: *mut sys::fuse_file_info) -> c_int {
    debug_log("flush enter/exit");
    0
}

unsafe extern "C" fn opendir(path: *const c_char, _fi: *mut sys::fuse_file_info) -> c_int {
    let path = unsafe { CStr::from_ptr(path) };
    debug_log(&format!("opendir enter/exit path={path:?}"));
    if path.to_bytes() == b"/" {
        0
    } else {
        -libc::ENOENT
    }
}

unsafe extern "C" fn access(path: *const c_char, mask: c_int) -> c_int {
    let path = unsafe { CStr::from_ptr(path) };
    debug_log(&format!("access enter/exit path={path:?} mask={mask}"));
    0
}

/// Runs the spike filesystem, blocking until unmounted (see
/// [`sys::fuse_exit`] - there is no Windows equivalent of
/// `fusermount3 -u` for a directory mount created this way). Returns
/// WinFSP's process exit code (0 on a clean unmount).
pub fn run(mountpoint: &Path) -> i32 {
    let ops = sys::fuse_operations {
        getattr: Some(getattr),
        readdir: Some(readdir),
        open: Some(open),
        release: Some(release),
        flush: Some(flush),
        opendir: Some(opendir),
        access: Some(access),
        ..sys::fuse_operations::default()
    };

    let program_name = CString::new("mountfs-spike").unwrap();
    // WinFSP mountpoints, like libfuse's, take a plain narrow path - UTF-16
    // round-tripped through `OsStr` only where Windows APIs need it
    // directly (`sys.rs`'s own `LoadLibraryW`/`RegGetValueW` calls), not
    // here.
    let mountpoint_str = mountpoint
        .to_str()
        .expect("mountpoint path must be valid UTF-8");
    let mountpoint_c = CString::new(mountpoint_str).unwrap();
    let mut argv: Vec<*mut c_char> = vec![
        program_name.as_ptr().cast_mut(),
        mountpoint_c.as_ptr().cast_mut(),
    ];

    unsafe {
        sys::fuse_main_real(
            argv.len() as c_int,
            argv.as_mut_ptr(),
            &ops,
            std::mem::size_of::<sys::fuse_operations>(),
            std::ptr::null_mut(),
        )
    }
}

#[allow(dead_code)] // referenced only from the test below for now
fn request_exit() {
    let handle = ACTIVE_FUSE_HANDLE.load(Ordering::Acquire);
    if !handle.is_null() {
        unsafe { sys::fuse_exit(handle) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// End-to-end: mounts the spike filesystem via real WinFSP
    /// (`fsp_fuse3_main_real`), reads it back through ordinary `std::fs`
    /// calls, and unmounts via `fuse_exit` (see its doc comment) - proving
    /// the hand-written WinFSP bindings work end-to-end on this machine's
    /// installed WinFSP runtime, not just that they compile.
    #[test]
    fn mounts_and_serves_getattr_and_readdir_via_real_winfsp() {
        let parent_dir = tempfile::tempdir().unwrap();
        let mount_path = parent_dir.path().join("mnt");

        let handle = {
            let mount_path = mount_path.clone();
            std::thread::spawn(move || run(&mount_path))
        };

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut names: Vec<String>;
        loop {
            names = std::fs::read_dir(&mount_path)
                .map(|entries| {
                    entries
                        .filter_map(|e| e.ok())
                        .map(|e| e.file_name().to_string_lossy().into_owned())
                        .collect()
                })
                .unwrap_or_default();
            if !names.is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "mount did not become ready within 10s \
                 (requires WinFSP to be installed - investigate if this fails in CI)"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        assert_eq!(names, vec!["hello.txt".to_string()]);

        let meta = std::fs::metadata(mount_path.join("hello.txt")).unwrap();
        assert!(meta.is_file());
        assert_eq!(meta.len(), HELLO_CONTENT.len() as u64);

        request_exit();
        let exit_code = handle.join().expect("mount thread panicked");
        assert_eq!(exit_code, 0, "fsp_fuse3_main_real exited with an error");
    }
}
