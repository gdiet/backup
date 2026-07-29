//! Linux backend: hand-written bindings to real system libfuse3, built
//! against the *high-level* `fuse_operations` API (see `sys.rs`) instead of
//! `fuser`'s low-level `/dev/fuse` protocol - the whole point being that
//! this same API surface is also what WinFSP's `cygfuse` layer emulates on
//! Windows. See `docs/plans/cross-platform-mount-crate.md`.
//!
//! [`mount`] dispatches every `fuse_operations` callback to a
//! [`crate::MountFilesystem`] implementation via monomorphized `extern "C"`
//! trampolines (`dispatch_*`) - `fuse_operations` needs plain function
//! pointers with no captured environment, so each trampoline is generic
//! over `T: MountFilesystem` and recovers the caller's `&T` from
//! `fuse_get_context()->private_data` (valid for the duration of any
//! callback libfuse makes on the calling thread) rather than by closing
//! over it directly.

mod sys;

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use libc::{off_t, size_t, stat, statvfs};

use crate::{DirEntry, Errno, FileKind, Handle, MountFilesystem};

/// Recovers the `&T` passed as `mount`'s `private_data` - only sound to
/// call from within a `dispatch_*` trampoline, i.e. from libfuse's own
/// calling thread during a callback.
unsafe fn context<'a, T>() -> &'a T {
    let ctx = unsafe { sys::fuse_get_context() };
    unsafe { &*((*ctx).private_data as *const T) }
}

/// `None` on invalid UTF-8. libfuse paths are otherwise always absolute
/// (`/`-rooted, no trailing slash except the root itself).
fn path_str<'a>(path: *const c_char) -> Option<&'a str> {
    unsafe { CStr::from_ptr(path) }.to_str().ok()
}

unsafe extern "C" fn dispatch_getattr<T: MountFilesystem>(
    path: *const c_char,
    stbuf: *mut stat,
    _fi: *mut sys::fuse_file_info,
) -> c_int {
    let Some(path) = path_str(path) else {
        return -Errno::EIO.0;
    };
    let fs = unsafe { context::<T>() };
    match fs.getattr(path) {
        Ok(attr) => {
            unsafe { std::ptr::write_bytes(stbuf, 0, 1) };
            match attr.kind {
                FileKind::Directory => unsafe {
                    (*stbuf).st_mode = libc::S_IFDIR | 0o555;
                    (*stbuf).st_nlink = 2;
                },
                FileKind::File => unsafe {
                    (*stbuf).st_mode = libc::S_IFREG | 0o444;
                    (*stbuf).st_nlink = 1;
                    (*stbuf).st_size = attr.size as off_t;
                },
            }
            0
        }
        Err(errno) => -errno.0,
    }
}

unsafe extern "C" fn dispatch_readdir<T: MountFilesystem>(
    path: *const c_char,
    buf: *mut c_void,
    filler: sys::fuse_fill_dir_t,
    _offset: off_t,
    _fi: *mut sys::fuse_file_info,
    _flags: sys::fuse_readdir_flags,
) -> c_int {
    let Some(path) = path_str(path) else {
        return -Errno::EIO.0;
    };
    let Some(filler) = filler else {
        return -Errno::EIO.0;
    };
    let fs = unsafe { context::<T>() };
    match fs.readdir(path) {
        Ok(entries) => {
            let names = [".", ".."]
                .into_iter()
                .map(str::to_string)
                .chain(entries.into_iter().map(|e: DirEntry| e.name));
            for name in names {
                // A name that isn't representable as a CString (embedded
                // NUL) can't be a real path component - skip rather than
                // fail the whole listing over it.
                if let Ok(name) = CString::new(name) {
                    unsafe { filler(buf, name.as_ptr(), std::ptr::null(), 0, 0) };
                }
            }
            0
        }
        Err(errno) => -errno.0,
    }
}

unsafe extern "C" fn dispatch_open<T: MountFilesystem>(
    path: *const c_char,
    fi: *mut sys::fuse_file_info,
) -> c_int {
    let Some(path) = path_str(path) else {
        return -Errno::EIO.0;
    };
    let fs = unsafe { context::<T>() };
    let write_intent = unsafe { (*fi).flags & (libc::O_WRONLY | libc::O_RDWR) != 0 };
    match fs.open(path, write_intent) {
        Ok(handle) => {
            unsafe { (*fi).fh = handle.0 };
            0
        }
        Err(errno) => -errno.0,
    }
}

unsafe extern "C" fn dispatch_read<T: MountFilesystem>(
    _path: *const c_char,
    buf: *mut c_char,
    size: size_t,
    offset: off_t,
    fi: *mut sys::fuse_file_info,
) -> c_int {
    let fs = unsafe { context::<T>() };
    let handle = Handle(unsafe { (*fi).fh });
    match fs.read(handle, offset as u64, size as u32) {
        Ok(data) => {
            let n = data.len().min(size);
            unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf.cast::<u8>(), n) };
            n as c_int
        }
        Err(errno) => -errno.0,
    }
}

unsafe extern "C" fn dispatch_release<T: MountFilesystem>(
    _path: *const c_char,
    fi: *mut sys::fuse_file_info,
) -> c_int {
    let fs = unsafe { context::<T>() };
    fs.release(Handle(unsafe { (*fi).fh }));
    0
}

unsafe extern "C" fn dispatch_statfs<T: MountFilesystem>(
    _path: *const c_char,
    buf: *mut statvfs,
) -> c_int {
    let fs = unsafe { context::<T>() };
    match fs.statfs() {
        Ok(info) => {
            unsafe { std::ptr::write_bytes(buf, 0, 1) };
            unsafe {
                (*buf).f_bsize = info.block_size as u64;
                (*buf).f_frsize = info.block_size as u64;
                (*buf).f_blocks = info.blocks;
                (*buf).f_bfree = info.blocks_free;
                (*buf).f_bavail = info.blocks_available;
                (*buf).f_files = info.files;
                (*buf).f_ffree = info.files_free;
                (*buf).f_namemax = info.max_name_length as u64;
            }
            0
        }
        Err(errno) => -errno.0,
    }
}

/// Mounts `fs` at `mountpoint`, blocking (in the foreground - see the
/// `-f` note below) until it's unmounted (e.g. via `fusermount3 -u
/// <mountpoint>`, `umount <mountpoint>`, or process signal).
///
/// `-f` is required, not optional: without it libfuse's default behavior
/// is to daemonize (fork into the background), which is unsound to trigger
/// from a process that may have other threads running (e.g. a Rust test
/// binary, or any multi-threaded caller) - exactly the kind of footgun
/// `fuser` sidesteps by not going through libfuse at all, and why callers
/// of this binding must not be able to opt out of it.
pub fn mount<T: MountFilesystem>(fs: T, mountpoint: &Path, read_only: bool) -> io::Result<()> {
    let ops = sys::fuse_operations {
        getattr: Some(dispatch_getattr::<T>),
        readdir: Some(dispatch_readdir::<T>),
        open: Some(dispatch_open::<T>),
        read: Some(dispatch_read::<T>),
        release: Some(dispatch_release::<T>),
        statfs: Some(dispatch_statfs::<T>),
        ..sys::fuse_operations::default()
    };

    let program_name = CString::new("mountfs").unwrap();
    let foreground_flag = CString::new("-f").unwrap();
    let read_only_flag = CString::new("-oro").unwrap();
    let mountpoint_c = CString::new(mountpoint.as_os_str().as_bytes())
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

    let mut args: Vec<*mut c_char> = vec![
        program_name.as_ptr().cast_mut(),
        foreground_flag.as_ptr().cast_mut(),
    ];
    if read_only {
        args.push(read_only_flag.as_ptr().cast_mut());
    }
    args.push(mountpoint_c.as_ptr().cast_mut());

    // `fuse_main_real` doesn't take ownership of `fs` in any Rust sense -
    // it just carries the pointer through to `fuse_get_context()` for the
    // duration of the (blocking) call below, so the `Box` must outlive
    // that call and be reclaimed only after it returns.
    let private_data = Box::into_raw(Box::new(fs));
    let exit_code = unsafe {
        sys::fuse_main_real(
            args.len() as c_int,
            args.as_mut_ptr(),
            &ops,
            std::mem::size_of::<sys::fuse_operations>(),
            private_data.cast::<c_void>(),
        )
    };
    unsafe { drop(Box::from_raw(private_data)) };

    if exit_code == 0 {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "fuse_main_real exited with code {exit_code}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Attr;
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    /// A tiny in-memory [`MountFilesystem`], independent of `db`/`store`
    /// (which stay `cli`'s concern - see the plan's "What moves where"):
    /// enough to exercise the full read-only op set (`getattr`/`readdir`/
    /// `open`/`read`/`release`/`statfs`) including a nested directory,
    /// without pulling in a real repository.
    struct TestFs {
        files: BTreeMap<&'static str, &'static [u8]>,
    }

    impl MountFilesystem for TestFs {
        fn getattr(&self, path: &str) -> Result<Attr, Errno> {
            if path == "/" || path == "/sub" {
                return Ok(Attr {
                    kind: FileKind::Directory,
                    size: 0,
                    mtime_millis: 0,
                });
            }
            match self.files.get(path) {
                Some(content) => Ok(Attr {
                    kind: FileKind::File,
                    size: content.len() as u64,
                    mtime_millis: 0,
                }),
                None => Err(Errno::ENOENT),
            }
        }

        fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno> {
            let prefix = if path == "/" {
                "/".to_string()
            } else {
                format!("{path}/")
            };
            let mut names: Vec<DirEntry> = self
                .files
                .keys()
                .filter_map(|full_path| {
                    let rest = full_path.strip_prefix(&prefix)?;
                    if rest.contains('/') {
                        None
                    } else {
                        Some(DirEntry {
                            name: rest.to_string(),
                            kind: FileKind::File,
                        })
                    }
                })
                .collect();
            if path == "/" {
                names.push(DirEntry {
                    name: "sub".to_string(),
                    kind: FileKind::Directory,
                });
            }
            Ok(names)
        }

        fn open(&self, path: &str, write_intent: bool) -> Result<Handle, Errno> {
            if write_intent {
                return Err(Errno::EROFS);
            }
            // `BTreeMap`'s iteration order is a deterministic function of
            // its keys, and `files` never mutates after construction, so a
            // key's position doubles as a stable handle - stateless, like
            // `cli`'s current `DedupFs` (which uses the inode number the
            // same way).
            self.files
                .keys()
                .position(|&k| k == path)
                .map(|index| Handle(index as u64))
                .ok_or(Errno::ENOENT)
        }

        fn read(&self, handle: Handle, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
            let content = *self
                .files
                .values()
                .nth(handle.0 as usize)
                .ok_or(Errno::EIO)?;
            let start = (offset as usize).min(content.len());
            let end = start.saturating_add(size as usize).min(content.len());
            Ok(content[start..end].to_vec())
        }

        fn release(&self, _handle: Handle) {}

        fn statfs(&self) -> Result<crate::StatfsInfo, Errno> {
            Ok(crate::StatfsInfo {
                block_size: 512,
                max_name_length: 255,
                ..Default::default()
            })
        }
    }

    /// End-to-end: mounts [`TestFs`] via real libfuse3 (`fuse_main_real`,
    /// not `fuser`), reads it back - including a nested directory - through
    /// ordinary `std::fs` calls, and unmounts with `fusermount3 -u` (the
    /// same tool `cli mount`'s own docs point users at).
    #[test]
    fn mounts_and_serves_the_full_read_only_op_set_via_real_libfuse3() {
        let mut files = BTreeMap::new();
        files.insert("/top.txt", b"top level content".as_slice());
        files.insert("/sub/nested.txt", b"hello from a subdirectory".as_slice());
        let fs = TestFs { files };

        let mount_dir = tempfile::tempdir().unwrap();
        let mount_path = mount_dir.path().to_path_buf();
        let handle = {
            let mount_path = mount_path.clone();
            std::thread::spawn(move || mount(fs, &mount_path, true))
        };

        // The mountpoint exists (and reads as empty) before the mount is
        // live, so "readdir succeeds" alone isn't a valid readiness signal
        // - wait for it to actually start reporting our entries.
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
        names.sort();
        assert_eq!(names, vec!["sub".to_string(), "top.txt".to_string()]);

        assert_eq!(
            std::fs::read(mount_path.join("top.txt")).unwrap(),
            b"top level content"
        );
        assert_eq!(
            std::fs::metadata(mount_path.join("top.txt")).unwrap().len(),
            "top level content".len() as u64
        );
        assert!(std::fs::metadata(mount_path.join("sub")).unwrap().is_dir());
        assert_eq!(
            std::fs::read(mount_path.join("sub").join("nested.txt")).unwrap(),
            b"hello from a subdirectory"
        );

        // A write attempt must be rejected - this is a read-only mount.
        assert!(std::fs::write(mount_path.join("top.txt"), b"nope").is_err());

        let status = std::process::Command::new("fusermount3")
            .arg("-u")
            .arg(&mount_path)
            .status()
            .expect("failed to run fusermount3 -u");
        assert!(status.success(), "fusermount3 -u failed: {status}");

        handle
            .join()
            .expect("mount thread panicked")
            .expect("mount() returned an error");
    }
}
