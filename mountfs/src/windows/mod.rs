//! Windows backend: bindings to WinFSP's FUSE3-compatible API (see
//! `sys.rs`) - the same high-level, path-based shape the Linux backend
//! binds directly against real libfuse3. See
//! `docs/plans/cross-platform-mount-crate.md`, in particular the "Windows
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

use std::ffi::{CStr, CString, c_char, c_int, c_void};
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
            match attr.kind {
                FileKind::Directory => unsafe {
                    (*stbuf).st_mode = 0o040000 | 0o555; // S_IFDIR
                    (*stbuf).st_nlink = 2;
                },
                FileKind::File => unsafe {
                    (*stbuf).st_mode = 0o100000 | 0o444; // S_IFREG
                    (*stbuf).st_nlink = 1;
                    (*stbuf).st_size = attr.size as fuse_off_t;
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

/// Mounts `fs` at `mountpoint`, blocking until WinFSP unmounts it (Ctrl+C,
/// the console closing, or the process otherwise ending - see this
/// module's doc comment for why no custom handling is needed here for
/// that). Unlike the Linux backend, no `-f` (foreground) flag is needed:
/// WinFSP's Windows branch never forks (`fsp_fuse_daemonize` is a no-op
/// there - see `sys.rs`), so the footgun `-f` guards against on Linux
/// doesn't exist on this platform.
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
    let read_only_flag = CString::new("-oro").unwrap();
    let mountpoint_str = mountpoint.to_str().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "mountpoint is not valid UTF-8")
    })?;
    let mountpoint_c = CString::new(mountpoint_str)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

    let mut args: Vec<*mut c_char> = vec![program_name.as_ptr().cast_mut()];
    if read_only {
        args.push(read_only_flag.as_ptr().cast_mut());
    }
    args.push(mountpoint_c.as_ptr().cast_mut());

    // See linux::mount's identical comment: fuse_main_real carries this
    // pointer through to fuse_get_context() for the duration of the
    // (blocking) call below, so the Box must outlive that call.
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
    unsafe { (*private_data).on_unmount() };
    unsafe { drop(Box::from_raw(private_data)) };

    if exit_code == 0 {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "WinFSP fuse_main_real exited with code {exit_code}"
        )))
    }
}
