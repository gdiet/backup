//! Linux backend: hand-written bindings to real system libfuse3, built
//! against the *high-level* `fuse_operations` API (see `sys.rs`) instead of
//! `fuser`'s low-level `/dev/fuse` protocol - the whole point being that
//! this same API surface is also what WinFSP's `cygfuse` layer emulates on
//! Windows. See `docs/plans/cross-platform-mount-crate.md`.
//!
//! This module is currently just the sequencing plan's step-1 spike: a
//! hardcoded, read-only, single-file in-memory filesystem implementing only
//! `getattr` and `readdir`, proving the shared-API approach mounts and
//! serves real syscalls end-to-end on Linux before anything from
//! `cli/src/mount.rs` is touched. No public `MountFilesystem` trait yet.

mod sys;

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use libc::{ENOENT, S_IFDIR, S_IFREG, off_t, stat};

const HELLO_NAME: &CStr = c"hello.txt";
const HELLO_PATH: &CStr = c"/hello.txt";
const HELLO_CONTENT: &[u8] = b"hello from the mountfs Linux spike\n";

unsafe extern "C" fn getattr(
    path: *const c_char,
    stbuf: *mut stat,
    _fi: *mut sys::fuse_file_info,
) -> c_int {
    let path = unsafe { CStr::from_ptr(path) };
    unsafe { std::ptr::write_bytes(stbuf, 0, 1) };
    if path.to_bytes() == b"/" {
        unsafe {
            (*stbuf).st_mode = S_IFDIR | 0o755;
            (*stbuf).st_nlink = 2;
        }
        0
    } else if path == HELLO_PATH {
        unsafe {
            (*stbuf).st_mode = S_IFREG | 0o444;
            (*stbuf).st_nlink = 1;
            (*stbuf).st_size = HELLO_CONTENT.len() as off_t;
        }
        0
    } else {
        -ENOENT
    }
}

unsafe extern "C" fn readdir(
    path: *const c_char,
    buf: *mut c_void,
    filler: sys::fuse_fill_dir_t,
    _offset: off_t,
    _fi: *mut sys::fuse_file_info,
    _flags: sys::fuse_readdir_flags,
) -> c_int {
    let path = unsafe { CStr::from_ptr(path) };
    if path.to_bytes() != b"/" {
        return -ENOENT;
    }
    let Some(filler) = filler else {
        return -libc::EIO;
    };
    for name in [c".", c"..", HELLO_NAME] {
        unsafe { filler(buf, name.as_ptr(), std::ptr::null(), 0, 0) };
    }
    0
}

/// Runs the spike filesystem in the foreground, blocking until unmounted
/// (e.g. via `fusermount3 -u <mountpoint>`). Returns libfuse's process exit
/// code (0 on a clean unmount).
///
/// `-f` is required, not optional: without it libfuse's default behavior is
/// to daemonize (fork into the background), which is unsound to trigger
/// from a thread inside an already-multithreaded process like a Rust test
/// binary - this is exactly the kind of footgun `fuser` sidesteps by not
/// going through libfuse at all, and why every caller of this binding must
/// keep passing it.
pub fn run(mountpoint: &Path) -> i32 {
    let ops = sys::fuse_operations {
        getattr: Some(getattr),
        readdir: Some(readdir),
        ..sys::fuse_operations::default()
    };

    let program_name = CString::new("mountfs-spike").unwrap();
    let foreground_flag = CString::new("-f").unwrap();
    let mountpoint = CString::new(mountpoint.as_os_str().as_bytes()).unwrap();
    let mut argv: Vec<*mut c_char> = vec![
        program_name.as_ptr().cast_mut(),
        foreground_flag.as_ptr().cast_mut(),
        mountpoint.as_ptr().cast_mut(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// End-to-end: mounts the spike filesystem via real libfuse3
    /// (`fuse_main_real`, not `fuser`), reads it back through ordinary
    /// `std::fs` calls, and unmounts with `fusermount3 -u` (the same tool
    /// `cli mount`'s own docs point users at) - proving the hand-written
    /// high-level-API bindings work end-to-end, not just that they compile.
    #[test]
    fn mounts_and_serves_getattr_and_readdir_via_real_libfuse3() {
        let mount_dir = tempfile::tempdir().unwrap();
        let mount_path = mount_dir.path().to_path_buf();

        let handle = {
            let mount_path = mount_path.clone();
            std::thread::spawn(move || run(&mount_path))
        };

        // The mountpoint exists (and reads as empty) before the mount is
        // live, so "readdir succeeds" isn't a valid readiness signal on its
        // own - wait for it to actually start reporting our one entry.
        let deadline = Instant::now() + Duration::from_secs(5);
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
                "mount did not become ready within 5s \
                 (requires /dev/fuse access - investigate if this fails in CI)"
            );
            std::thread::sleep(Duration::from_millis(50));
        }

        assert_eq!(names, vec!["hello.txt".to_string()]);
        // Only `getattr`/`readdir` are implemented in this spike - `open`/
        // `read` aren't, so `std::fs::read` would fail with ENOSYS. `stat`
        // only needs `getattr`, which this does exercise.
        let meta = std::fs::metadata(mount_path.join("hello.txt")).unwrap();
        assert!(meta.is_file());
        assert_eq!(meta.len(), HELLO_CONTENT.len() as u64);

        let status = std::process::Command::new("fusermount3")
            .arg("-u")
            .arg(&mount_path)
            .status()
            .expect("failed to run fusermount3 -u");
        assert!(status.success(), "fusermount3 -u failed: {status}");

        let exit_code = handle.join().expect("mount thread panicked");
        assert_eq!(exit_code, 0, "fuse_main_real exited with an error");
    }
}
