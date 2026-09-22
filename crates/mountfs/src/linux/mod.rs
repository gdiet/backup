//! Linux backend: hand-written bindings to real system libfuse3, built
//! against the *high-level* `fuse_operations` API (see `sys.rs`) instead of
//! `fuser`'s low-level `/dev/fuse` protocol - the whole point being that
//! this same API surface is also what WinFSP's `cygfuse` layer emulates on
//! Windows. See `docs/design/mount-abstraction.md`.
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
    let ctx = sys::fuse_get_context();
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
            // No separate access/change time tracked - `mtime_millis` fills
            // all three.
            let secs = attr.mtime_millis.div_euclid(1000);
            let nsecs = attr.mtime_millis.rem_euclid(1000) * 1_000_000;
            unsafe {
                (*stbuf).st_atime = secs;
                (*stbuf).st_atime_nsec = nsecs;
                (*stbuf).st_mtime = secs;
                (*stbuf).st_mtime_nsec = nsecs;
                (*stbuf).st_ctime = secs;
                (*stbuf).st_ctime_nsec = nsecs;
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
                // A name that is not representable as a CString (embedded
                // NUL) cannot be a real path component - skip rather than
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

unsafe extern "C" fn dispatch_write<T: MountFilesystem>(
    _path: *const c_char,
    buf: *const c_char,
    size: size_t,
    offset: off_t,
    fi: *mut sys::fuse_file_info,
) -> c_int {
    let fs = unsafe { context::<T>() };
    let handle = Handle(unsafe { (*fi).fh });
    let data = unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), size) };
    match fs.write(handle, offset as u64, data) {
        Ok(written) => written as c_int,
        Err(errno) => -errno.0,
    }
}

unsafe extern "C" fn dispatch_truncate<T: MountFilesystem>(
    path: *const c_char,
    size: off_t,
    _fi: *mut sys::fuse_file_info,
) -> c_int {
    let Some(path) = path_str(path) else {
        return -Errno::EIO.0;
    };
    let fs = unsafe { context::<T>() };
    match fs.truncate(path, size as u64) {
        Ok(()) => 0,
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

unsafe extern "C" fn dispatch_mkdir<T: MountFilesystem>(
    path: *const c_char,
    _mode: libc::mode_t,
) -> c_int {
    let Some(path) = path_str(path) else {
        return -Errno::EIO.0;
    };
    if let Err(errno) = crate::reject_if_name_too_long(path) {
        return -errno.0;
    }
    let fs = unsafe { context::<T>() };
    match fs.mkdir(path) {
        Ok(()) => 0,
        Err(errno) => -errno.0,
    }
}

unsafe extern "C" fn dispatch_create<T: MountFilesystem>(
    path: *const c_char,
    _mode: libc::mode_t,
    fi: *mut sys::fuse_file_info,
) -> c_int {
    let Some(path) = path_str(path) else {
        return -Errno::EIO.0;
    };
    if let Err(errno) = crate::reject_if_name_too_long(path) {
        return -errno.0;
    }
    let fs = unsafe { context::<T>() };
    match fs.create(path) {
        Ok(handle) => {
            unsafe { (*fi).fh = handle.0 };
            0
        }
        Err(errno) => -errno.0,
    }
}

unsafe extern "C" fn dispatch_unlink<T: MountFilesystem>(path: *const c_char) -> c_int {
    let Some(path) = path_str(path) else {
        return -Errno::EIO.0;
    };
    let fs = unsafe { context::<T>() };
    match fs.unlink(path) {
        Ok(()) => 0,
        Err(errno) => -errno.0,
    }
}

unsafe extern "C" fn dispatch_rmdir<T: MountFilesystem>(path: *const c_char) -> c_int {
    let Some(path) = path_str(path) else {
        return -Errno::EIO.0;
    };
    let fs = unsafe { context::<T>() };
    match fs.rmdir(path) {
        Ok(()) => 0,
        Err(errno) => -errno.0,
    }
}

unsafe extern "C" fn dispatch_rename<T: MountFilesystem>(
    old_path: *const c_char,
    new_path: *const c_char,
    flags: libc::c_uint,
) -> c_int {
    let (Some(old_path), Some(new_path)) = (path_str(old_path), path_str(new_path)) else {
        return -Errno::EIO.0;
    };
    if let Err(errno) = crate::reject_if_name_too_long(new_path) {
        return -errno.0;
    }
    let no_replace = match crate::parse_rename_flags(flags) {
        Ok(no_replace) => no_replace,
        Err(errno) => return -errno.0,
    };
    let fs = unsafe { context::<T>() };
    match fs.rename(old_path, new_path, no_replace) {
        Ok(()) => 0,
        Err(errno) => -errno.0,
    }
}

unsafe extern "C" fn dispatch_utimens<T: MountFilesystem>(
    path: *const c_char,
    tv: *const libc::timespec,
    _fi: *mut sys::fuse_file_info,
) -> c_int {
    let Some(path) = path_str(path) else {
        return -Errno::EIO.0;
    };
    let fs = unsafe { context::<T>() };
    // tv[0] is atime, tv[1] is mtime - this crate tracks only mtime. Real
    // UTIME_NOW/UTIME_OMIT sentinels are not special-cased: rare outside
    // low-level tools, and treating them as literal timestamps just means
    // an unusual value gets stored instead of "now"/"unchanged".
    let mtime = unsafe { *tv.add(1) };
    let mtime_millis = mtime.tv_sec * 1000 + mtime.tv_nsec / 1_000_000;
    match fs.utimens(path, mtime_millis) {
        Ok(()) => 0,
        Err(errno) => -errno.0,
    }
}

unsafe extern "C" fn dispatch_chmod<T: MountFilesystem>(
    path: *const c_char,
    _mode: libc::mode_t,
    _fi: *mut sys::fuse_file_info,
) -> c_int {
    let Some(path) = path_str(path) else {
        return -Errno::EIO.0;
    };
    let fs = unsafe { context::<T>() };
    match fs.chmod(path) {
        Ok(()) => 0,
        Err(errno) => -errno.0,
    }
}

unsafe extern "C" fn dispatch_chown<T: MountFilesystem>(
    path: *const c_char,
    _uid: libc::uid_t,
    _gid: libc::gid_t,
    _fi: *mut sys::fuse_file_info,
) -> c_int {
    let Some(path) = path_str(path) else {
        return -Errno::EIO.0;
    };
    let fs = unsafe { context::<T>() };
    match fs.chown(path) {
        Ok(()) => 0,
        Err(errno) => -errno.0,
    }
}

/// Cheap check for whether libfuse3 is actually available right now,
/// without starting a mount. Meant to be called *before* announcing a
/// mount as started - `mount` below only discovers libfuse3's absence
/// mid-call, once `fuse_main_real` actually needs it, which is too late
/// for a caller that already printed a "mounted" message by then (mirrors
/// `windows::preflight`).
pub fn preflight() -> io::Result<()> {
    sys::check_available()
}

/// Mounts `fs` at `mountpoint`, blocking (in the foreground - see the
/// `-f` note below) until it is unmounted (e.g. via `fusermount3 -u
/// <mountpoint>`, `umount <mountpoint>`, or process signal).
///
/// `-f` is required, not optional: without it libfuse's default behavior
/// is to daemonize (fork into the background), which is unsound to trigger
/// from a process that may have other threads running (e.g. a Rust test
/// binary, or any multi-threaded caller) - exactly the kind of footgun
/// `fuser` sidesteps by not going through libfuse at all, and why callers
/// of this binding must not be able to opt out of it.
///
/// `mountpoint` must already exist as a directory - unlike `windows::mount`, libfuse does not
/// create it, and fails outright if it does not. libfuse prints its own diagnostic directly to
/// stderr in that case ("bad mount point: No such file or directory") - this call's own `Err`
/// only carries `fuse_main_real`'s exit code, not that message.
pub fn mount<T: MountFilesystem>(fs: T, mountpoint: &Path, read_only: bool) -> io::Result<()> {
    let ops = sys::fuse_operations {
        getattr: Some(dispatch_getattr::<T>),
        readdir: Some(dispatch_readdir::<T>),
        open: Some(dispatch_open::<T>),
        read: Some(dispatch_read::<T>),
        release: Some(dispatch_release::<T>),
        statfs: Some(dispatch_statfs::<T>),
        mkdir: Some(dispatch_mkdir::<T>),
        create: Some(dispatch_create::<T>),
        unlink: Some(dispatch_unlink::<T>),
        rmdir: Some(dispatch_rmdir::<T>),
        rename: Some(dispatch_rename::<T>),
        utimens: Some(dispatch_utimens::<T>),
        chmod: Some(dispatch_chmod::<T>),
        chown: Some(dispatch_chown::<T>),
        write: Some(dispatch_write::<T>),
        truncate: Some(dispatch_truncate::<T>),
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

    // `fuse_main_real` does not take ownership of `fs` in any Rust sense -
    // it just carries the pointer through to `fuse_get_context()` for the
    // duration of the (blocking) call below, so the `Box` must outlive
    // that call and be reclaimed only after it returns.
    let private_data = Box::into_raw(Box::new(fs));
    let result = unsafe {
        sys::fuse_main_real(
            args.len() as c_int,
            args.as_mut_ptr(),
            &ops,
            std::mem::size_of::<sys::fuse_operations>(),
            private_data.cast::<c_void>(),
        )
    };
    // on_unmount() is a lifecycle hook for a mount that actually started -
    // if fuse_main_real returned Err before that (libfuse3 not found),
    // there was never a mount to unmount.
    if result.is_ok() {
        unsafe { (*private_data).on_unmount() };
    }
    unsafe { drop(Box::from_raw(private_data)) };

    match result? {
        0 => Ok(()),
        exit_code => Err(io::Error::other(format!(
            "fuse_main_real exited with code {exit_code}"
        ))),
    }
}

// Tests prefixed `real_mount_` need a real libfuse3 mount (`/dev/fuse` access) and are excluded
// by `cargo test -- --skip real_mount` in an environment known not to have it - see
// `docs/development.md`'s "Tests" section. They still run by default otherwise: the prefix is a
// skip filter, not an `#[ignore]`.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::Attr;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    /// A tiny in-memory [`MountFilesystem`], independent of any real
    /// repository backend: enough to exercise the full read-only op set
    /// (`getattr`/`readdir`/`open`/`read`/`release`/`statfs`) including a
    /// nested directory, without needing one.
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
            // key's position doubles as a stable, stateless handle.
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
    /// ordinary `std::fs` calls, and unmounts with `fusermount3 -u`.
    #[test]
    fn real_mount_serves_the_full_read_only_op_set_via_libfuse3() {
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
        // live, so "readdir succeeds" alone is not a valid readiness signal
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

    /// Confirms `dispatch_mkdir`/`dispatch_create`/`dispatch_rename` reject
    /// an over-long name themselves, before ever calling into
    /// [`MountFilesystem::mkdir`]/[`MountFilesystem::create`]/
    /// [`MountFilesystem::rename`] - not something each implementor needs
    /// to remember to check itself (see `crate::MAX_NAME_BYTES`'s own doc
    /// comment for why this cannot be left to the OS/protocol layer).
    /// Mounted with `read_only: false` (unlike the test above) so the
    /// *kernel's* own `-oro` enforcement does not intercept the write
    /// attempt before it ever reaches our dispatch layer - [`TestFs`]
    /// itself still rejects any write it does not specifically support
    /// (`EROFS`, its own `open`'s default behavior for `write_intent`), so
    /// a normal-length name reaching that fallback (not our length check)
    /// is exactly the signal that distinguishes "rejected for being too
    /// long" from "rejected for an unrelated reason".
    #[test]
    fn real_mount_dispatch_rejects_a_name_over_max_name_bytes_before_reaching_the_filesystem() {
        let fs = TestFs {
            files: BTreeMap::new(),
        };

        let mount_dir = tempfile::tempdir().unwrap();
        let mount_path = mount_dir.path().to_path_buf();
        let handle = {
            let mount_path = mount_path.clone();
            std::thread::spawn(move || mount(fs, &mount_path, false))
        };

        // `mount_path` exists as a plain, empty, real (writable!) directory
        // *before* FUSE actually attaches to it - `read_dir` alone succeeding
        // is not a valid readiness signal (it'd trivially succeed against
        // that real, empty directory too, racing straight past FUSE
        // entirely and invalidating the whole test). Wait for content
        // specifically: TestFs's `readdir("/")` always reports its
        // hardcoded "sub" entry once the mount is actually live, which the
        // real pre-mount directory never has.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let has_content = std::fs::read_dir(&mount_path)
                .map(|mut entries| entries.next().is_some())
                .unwrap_or(false);
            if has_content {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "mount did not become ready within 5s"
            );
            std::thread::sleep(Duration::from_millis(50));
        }

        let too_long = "a".repeat(crate::MAX_NAME_BYTES + 1);
        let just_right = "a".repeat(crate::MAX_NAME_BYTES);

        let too_long_err = std::fs::create_dir(mount_path.join(&too_long)).unwrap_err();
        let just_right_err = std::fs::create_dir(mount_path.join(&just_right)).unwrap_err();
        assert_ne!(
            too_long_err.raw_os_error(),
            just_right_err.raw_os_error(),
            "an over-long name must fail differently (rejected by our own dispatch layer) \
             than a name that is merely rejected by TestFs's own read-only behavior - \
             too_long: {too_long_err:?}, just_right: {just_right_err:?}"
        );
        assert_eq!(too_long_err.raw_os_error(), Some(Errno::ENAMETOOLONG.0));

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

    /// A writable, in-memory [`MountFilesystem`] whose `write()` records two things a real
    /// libfuse3 mount's own dispatch-thread pool determines - not this crate's own code - which
    /// `agent-todos/done/determine-libfuse3-dispatch-pool-and-stack-size.md` needed real numbers
    /// for: the peak number of `write()` calls libfuse3 runs *concurrently* against this process
    /// (an artificial per-call delay gives concurrent client writers time to actually overlap),
    /// and each distinct dispatch thread's real stack size, via `pthread_getattr_np`/
    /// `pthread_attr_getstack` - precise and direct, unlike the dispatch-pool size, which has no
    /// equivalent direct query and is only observable this indirect way.
    struct DispatchProbeState {
        concurrent_writes: std::sync::atomic::AtomicUsize,
        peak_concurrent_writes: std::sync::atomic::AtomicUsize,
        stack_sizes_by_thread: Mutex<std::collections::HashMap<std::thread::ThreadId, usize>>,
        delay: Duration,
        created_files: Mutex<std::collections::BTreeSet<String>>,
        next_handle: std::sync::atomic::AtomicU64,
    }

    /// Cheap-to-clone handle onto [`DispatchProbeState`] - `mount` takes its filesystem by value,
    /// so this is what actually gets moved into the mount thread, while the test itself keeps its
    /// own clone of the same underlying `Arc` to read the live counters back afterward (no IPC
    /// needed, unlike the Windows side's separate-child-process equivalent).
    #[derive(Clone)]
    struct DispatchProbeFs(std::sync::Arc<DispatchProbeState>);

    /// The stack size of the *calling* thread, read directly from the running thread's own
    /// attributes rather than assumed from a documented default - see this test's own doc comment
    /// for why `/proc/self/task/<tid>/maps`'s `[stack:<tid>]` label was rejected as the mechanism
    /// instead (`developer-todos/ram-budget-and-backpressure-redesign.md`'s "Can this be verified
    /// by a test?" section).
    fn current_thread_stack_size() -> usize {
        unsafe {
            let mut attr: libc::pthread_attr_t = std::mem::zeroed();
            let rc = libc::pthread_getattr_np(libc::pthread_self(), &mut attr);
            assert_eq!(rc, 0, "pthread_getattr_np failed with errno {rc}");
            let mut addr: *mut libc::c_void = std::ptr::null_mut();
            let mut size: libc::size_t = 0;
            let rc = libc::pthread_attr_getstack(&attr, &mut addr, &mut size);
            assert_eq!(rc, 0, "pthread_attr_getstack failed with errno {rc}");
            libc::pthread_attr_destroy(&mut attr);
            size
        }
    }

    impl MountFilesystem for DispatchProbeFs {
        fn getattr(&self, path: &str) -> Result<Attr, Errno> {
            if path == "/" {
                return Ok(Attr {
                    kind: FileKind::Directory,
                    size: 0,
                    mtime_millis: 0,
                });
            }
            if self.0.created_files.lock().unwrap().contains(path) {
                Ok(Attr {
                    kind: FileKind::File,
                    size: 0,
                    mtime_millis: 0,
                })
            } else {
                Err(Errno::ENOENT)
            }
        }

        fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno> {
            if path != "/" {
                return Err(Errno::ENOTDIR);
            }
            Ok(self
                .0
                .created_files
                .lock()
                .unwrap()
                .iter()
                .map(|p| DirEntry {
                    name: p.trim_start_matches('/').to_string(),
                    kind: FileKind::File,
                })
                .collect())
        }

        fn open(&self, path: &str, _write_intent: bool) -> Result<Handle, Errno> {
            if self.0.created_files.lock().unwrap().contains(path) {
                Ok(Handle(
                    self.0
                        .next_handle
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                ))
            } else {
                Err(Errno::ENOENT)
            }
        }

        fn read(&self, _handle: Handle, _offset: u64, _size: u32) -> Result<Vec<u8>, Errno> {
            Ok(Vec::new())
        }

        fn release(&self, _handle: Handle) {}

        fn statfs(&self) -> Result<crate::StatfsInfo, Errno> {
            Ok(crate::StatfsInfo {
                block_size: 512,
                max_name_length: 255,
                ..Default::default()
            })
        }

        fn create(&self, path: &str) -> Result<Handle, Errno> {
            self.0
                .created_files
                .lock()
                .unwrap()
                .insert(path.to_string());
            Ok(Handle(
                self.0
                    .next_handle
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            ))
        }

        fn truncate(&self, _path: &str, _size: u64) -> Result<(), Errno> {
            Ok(())
        }

        /// The one instrumented call - content itself is discarded (this probe only cares about
        /// dispatch-thread behavior, not correctness of stored bytes).
        fn write(&self, _handle: Handle, _offset: u64, data: &[u8]) -> Result<u32, Errno> {
            use std::sync::atomic::Ordering;
            let now = self.0.concurrent_writes.fetch_add(1, Ordering::AcqRel) + 1;
            self.0
                .peak_concurrent_writes
                .fetch_max(now, Ordering::AcqRel);

            self.0
                .stack_sizes_by_thread
                .lock()
                .unwrap()
                .entry(std::thread::current().id())
                .or_insert_with(current_thread_stack_size);

            std::thread::sleep(self.0.delay);

            self.0.concurrent_writes.fetch_sub(1, Ordering::AcqRel);
            Ok(data.len() as u32)
        }
    }

    /// Drives real concurrent write load against a real libfuse3 mount to observe WinFSP's
    /// Windows counterpart already measured (`agent-todos/done/
    /// determine-winfsp-dispatch-pool-and-stack-size.md`): libfuse3's actual dispatch-thread
    /// concurrency, and each dispatch thread's real stack size.
    ///
    /// `#[ignore]`d, same reasoning as the Windows side's equivalent test
    /// (`crates/mountfs/tests/windows_mount.rs`): a slower, load-driving observation test, not a
    /// correctness check with a "right answer" to assert on every run. Run explicitly with `cargo
    /// test -p mountfs --lib -- --ignored real_mount_dispatch_thread_pool_and_stack_size
    /// --nocapture` to see the numbers.
    #[ignore = "slow, load-driving observation test - see doc comment"]
    #[test]
    fn real_mount_dispatch_thread_pool_and_stack_size() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, AtomicUsize};

        let state = Arc::new(DispatchProbeState {
            concurrent_writes: AtomicUsize::new(0),
            peak_concurrent_writes: AtomicUsize::new(0),
            stack_sizes_by_thread: Mutex::new(std::collections::HashMap::new()),
            delay: Duration::from_millis(150),
            created_files: Mutex::new(std::collections::BTreeSet::new()),
            next_handle: AtomicU64::new(1),
        });
        let fs = DispatchProbeFs(Arc::clone(&state));

        let mount_dir = tempfile::tempdir().unwrap();
        let mount_path = mount_dir.path().to_path_buf();
        let handle = {
            let mount_path = mount_path.clone();
            std::thread::spawn(move || mount(fs, &mount_path, false))
        };

        // The probe filesystem starts with an empty root, so readiness has to be an actual write
        // attempt, not a "listing is non-empty" check - retried until it succeeds.
        let probe_path = mount_path.join("_ready_probe.txt");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if std::fs::write(&probe_path, b"x").is_ok() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "mount did not become ready within 5s (requires /dev/fuse access)"
            );
            std::thread::sleep(Duration::from_millis(50));
        }

        // 24 concurrent native OS threads, each writing 3 files in turn - comfortably above any
        // plausible real pool size, so the observed peak reflects libfuse3's own ceiling, not this
        // test's own thread count. 256 KiB per write (not `std::fs::write`'s tiny handful of
        // bytes an earlier version of this test used) plus an explicit `sync_all` - small enough
        // writes were found to sometimes never reach this filesystem's own `write()` dispatch at
        // all before `fusermount3 -u` below, apparently absorbed entirely by the kernel's FUSE
        // page cache (this project's `mount()` requests no `direct_io`) rather than actually
        // dispatched - a real effect, not a bug in the probe's counting logic.
        let payload = vec![7u8; 256 * 1024];
        let writer_threads: Vec<_> = (0..24)
            .map(|i| {
                let mount_path = mount_path.clone();
                let payload = payload.clone();
                std::thread::spawn(move || {
                    for j in 0..3 {
                        let path = mount_path.join(format!("w{i}-{j}.txt"));
                        let mut file = std::fs::File::create(&path)
                            .expect("create against the probe must succeed");
                        std::io::Write::write_all(&mut file, &payload)
                            .expect("write against the probe must succeed");
                        file.sync_all()
                            .expect("sync_all against the probe must succeed");
                    }
                })
            })
            .collect();
        for writer in writer_threads {
            writer.join().expect("writer thread must not panic");
        }

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

        // Read the shared counters directly - this test's filesystem instance lives in this same
        // process, so no IPC is needed (unlike the Windows side's separate-child-process
        // equivalent, which has to write results to a file for the test to read back).
        let peak = state
            .peak_concurrent_writes
            .load(std::sync::atomic::Ordering::Acquire);
        let stack_sizes: Vec<usize> = state
            .stack_sizes_by_thread
            .lock()
            .unwrap()
            .values()
            .copied()
            .collect();

        println!("libfuse3 dispatch-thread pool and stack size, measured on this machine:");
        println!("  peak concurrent write() dispatches: {peak}");
        println!(
            "  distinct dispatch threads observed: {}",
            stack_sizes.len()
        );
        println!("  stack sizes (bytes): {stack_sizes:?}");

        assert!(
            peak >= 1,
            "expected at least one write() to have run, got peak={peak}"
        );
        assert!(
            !stack_sizes.is_empty(),
            "expected at least one dispatch thread's stack size to have been recorded"
        );
        for size in &stack_sizes {
            assert!(
                (256 * 1024..=16 * 1024 * 1024).contains(size),
                "a dispatch thread's stack size ({size} bytes) is outside a plausible range \
                 (256 KiB..=16 MiB) - either the measurement is wrong, or this machine's real \
                 value genuinely needs the RAM-budget reserve calculation revisited"
            );
        }
    }
}
