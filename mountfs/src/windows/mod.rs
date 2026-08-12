//! Windows backend: bindings to WinFSP's FUSE3-compatible API (see
//! `sys.rs`) - the same high-level, path-based shape the Linux backend
//! binds directly against real libfuse3. See
//! `docs/plans/implemented/05-cross-platform-mount-crate.md`, in particular the "Windows
//! checkpoint" note on `fuse_exit`/`fsp_fuse3_exit` (confirmed to crash
//! deterministically inside `winfsp-x64.dll`, not something this crate can
//! fix) - not needed in practice, though: on Ctrl+C, WinFSP's own
//! `fuse_main_real` already unmounts cleanly and returns on its own
//! (confirmed by manual testing in a real terminal - WinFSP itself prints
//! "The service ... has been stopped." and even removes the mountpoint
//! directory it created), exactly mirroring real libfuse's `SIGINT`
//! handling on Linux. [`mount`] relies on that directly: no custom
//! console-control handling here, [`MountFilesystem::on_unmount`] is
//! called right after `fuse_main_real` returns, same as `linux::mount`.
//!
//! [`mount`] dispatches every `fuse_operations` callback to a
//! [`crate::MountFilesystem`] implementation the same way the Linux
//! backend does: monomorphized `extern "C"` trampolines (`dispatch_*`)
//! recover the trait object from `fuse_get_context()->private_data`.

mod sys;

use std::ffi::{CStr, CString, c_char, c_int, c_uint, c_void};
use std::io;
use std::path::Path;

use sys::{fuse_off_t, fuse_stat, fuse_statvfs};

use crate::{DirEntry, Errno, FileKind, Handle, MountFilesystem};

/// Recovers the `&T` passed as `mount`'s `private_data` - only sound to
/// call from within a `dispatch_*` trampoline, i.e. from WinFSP's own
/// calling thread during a callback.
unsafe fn context<'a, T>() -> &'a T {
    let ctx = sys::fuse_get_context();
    unsafe { &*((*ctx).private_data as *const T) }
}

/// `None` on invalid UTF-8. WinFSP paths, like libfuse's, are otherwise
/// always absolute (`/`-rooted, no trailing slash except the root itself).
fn path_str<'a>(path: *const c_char) -> Option<&'a str> {
    unsafe { CStr::from_ptr(path) }.to_str().ok()
}

unsafe extern "C" fn dispatch_getattr<T: MountFilesystem>(
    path: *const c_char,
    stbuf: *mut fuse_stat,
    _fi: *mut sys::fuse_file_info,
) -> c_int {
    let Some(path) = path_str(path) else {
        return -Errno::EIO.0;
    };
    let fs = unsafe { context::<T>() };
    match fs.getattr(path) {
        Ok(attr) => {
            unsafe { std::ptr::write_bytes(stbuf, 0, 1) };
            // WinFSP derives an NT security descriptor from st_uid/st_gid
            // plus the mode bits below and enforces it *before* ever
            // calling this crate's own `mkdir`/`create`/`open`/etc. (unlike
            // real libfuse's kernel VFS, which without `-o
            // default_permissions` just calls through regardless of
            // reported mode/ownership) - leaving st_uid/st_gid at 0 (the
            // zeroing above) doesn't map to the mounting process's own
            // identity, so WinFSP built a security descriptor that denied
            // it access even with fully-open mode bits. Filling in the
            // real caller's uid/gid via `fuse_get_context()` (exactly what
            // WinFSP's own memfs-fuse3.cpp reference does) makes the
            // mounting process the file's owner, so owner-write bits are
            // honored.
            unsafe {
                let ctx = sys::fuse_get_context();
                (*stbuf).st_uid = (*ctx).uid;
                (*stbuf).st_gid = (*ctx).gid;
            }
            match attr.kind {
                FileKind::Directory => unsafe {
                    // World-writable (0o777), not 0o555: consistent with
                    // this crate's "Not modeling permissions" design (see
                    // `MountFilesystem::chmod`'s doc comment) - read-only
                    // mounts are still enforced, just by the `-oro`-derived
                    // `ReadOnlyVolume` WinFSP setting, not by these bits.
                    (*stbuf).st_mode = 0o040000 | 0o777; // S_IFDIR
                    (*stbuf).st_nlink = 2;
                },
                FileKind::File => unsafe {
                    (*stbuf).st_mode = 0o100000 | 0o666; // S_IFREG
                    (*stbuf).st_nlink = 1;
                    (*stbuf).st_size = attr.size as fuse_off_t;
                },
            }
            // No separate access/change time tracked - `mtime_millis` fills
            // all three, matching linux::mod's identical convention.
            let secs = attr.mtime_millis.div_euclid(1000);
            let nsecs = attr.mtime_millis.rem_euclid(1000) * 1_000_000;
            unsafe {
                (*stbuf).st_atim = sys::fuse_timespec {
                    tv_sec: secs,
                    tv_nsec: nsecs,
                };
                (*stbuf).st_mtim = sys::fuse_timespec {
                    tv_sec: secs,
                    tv_nsec: nsecs,
                };
                (*stbuf).st_ctim = sys::fuse_timespec {
                    tv_sec: secs,
                    tv_nsec: nsecs,
                };
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
    _offset: fuse_off_t,
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
    size: usize,
    offset: fuse_off_t,
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
    size: usize,
    offset: fuse_off_t,
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
    size: fuse_off_t,
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
    buf: *mut fuse_statvfs,
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
    _mode: sys::fuse_mode_t,
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
    _mode: sys::fuse_mode_t,
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
    _flags: c_uint,
) -> c_int {
    let (Some(old_path), Some(new_path)) = (path_str(old_path), path_str(new_path)) else {
        return -Errno::EIO.0;
    };
    if let Err(errno) = crate::reject_if_name_too_long(new_path) {
        return -errno.0;
    }
    let fs = unsafe { context::<T>() };
    match fs.rename(old_path, new_path) {
        Ok(()) => 0,
        Err(errno) => -errno.0,
    }
}

unsafe extern "C" fn dispatch_utimens<T: MountFilesystem>(
    path: *const c_char,
    tv: *const sys::fuse_timespec,
    _fi: *mut sys::fuse_file_info,
) -> c_int {
    let Some(path) = path_str(path) else {
        return -Errno::EIO.0;
    };
    let fs = unsafe { context::<T>() };
    // tv[0] is atime, tv[1] is mtime - this crate tracks only mtime, same
    // convention as linux::mod's identical dispatch_utimens.
    let mtime = unsafe { &*tv.add(1) };
    let mtime_millis = mtime.tv_sec * 1000 + mtime.tv_nsec / 1_000_000;
    match fs.utimens(path, mtime_millis) {
        Ok(()) => 0,
        Err(errno) => -errno.0,
    }
}

unsafe extern "C" fn dispatch_chmod<T: MountFilesystem>(
    path: *const c_char,
    _mode: sys::fuse_mode_t,
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
    _uid: sys::fuse_uid_t,
    _gid: sys::fuse_gid_t,
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

/// Mounts `fs` at `mountpoint`, blocking until WinFSP unmounts it (Ctrl+C,
/// the console closing, or the process otherwise ending - see this
/// module's doc comment for why no custom handling is needed here for
/// that). Unlike the Linux backend, no `-f` (foreground) flag is needed:
/// WinFSP's Windows branch never forks (`fsp_fuse_daemonize` is a no-op
/// there - see `sys.rs`), so the footgun `-f` guards against on Linux
/// doesn't exist on this platform.
/// Cheap check for whether WinFSP is actually installed, without starting
/// a mount. Meant to be called *before* announcing a mount as started -
/// `mount` below only discovers WinFSP's absence mid-call, once
/// `fuse_main_real` actually needs it, which is too late for a caller that
/// already printed a "mounted" message by then.
pub fn preflight() -> io::Result<()> {
    sys::check_available()
}

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
    let read_only_flag = CString::new("-oro").unwrap();
    // Shows up as the volume label in Explorer/`vol`/etc. - otherwise
    // blank, unlike a real drive. Matches the Scala predecessor's own
    // `Array("-o", "volname=DedupFS")` (Windows-only there too - this
    // mount option isn't meaningful the same way on Linux, where a FUSE
    // mount's "label" isn't a GUI-visible concept the way it is here).
    let volname_flag = CString::new("-o").unwrap();
    let volname_value = CString::new("volname=DedupFS").unwrap();
    let mountpoint_str = mountpoint.to_str().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "mountpoint is not valid UTF-8")
    })?;
    let mountpoint_c = CString::new(mountpoint_str)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

    let mut args: Vec<*mut c_char> = vec![program_name.as_ptr().cast_mut()];
    if read_only {
        args.push(read_only_flag.as_ptr().cast_mut());
    }
    args.push(volname_flag.as_ptr().cast_mut());
    args.push(volname_value.as_ptr().cast_mut());
    args.push(mountpoint_c.as_ptr().cast_mut());

    // See linux::mount's identical comment: fuse_main_real carries this
    // pointer through to fuse_get_context() for the duration of the
    // (blocking) call below, so the Box must outlive that call.
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
    // if fuse_main_real returned Err before that (WinFSP not found), there
    // was never a mount to unmount.
    if result.is_ok() {
        unsafe { (*private_data).on_unmount() };
    }
    unsafe { drop(Box::from_raw(private_data)) };

    match result? {
        0 => Ok(()),
        exit_code => Err(io::Error::other(format!(
            "WinFSP fuse_main_real exited with code {exit_code}"
        ))),
    }
}
