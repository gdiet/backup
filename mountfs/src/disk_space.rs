//! Real (total, available) byte counts for the filesystem containing a
//! given path.
//!
//! Deliberately a single, targeted query of exactly one path - not an
//! "enumerate every mounted filesystem and find the longest matching mount
//! point" approach (which is what the `sysinfo` crate's `Disks` does, and
//! what an earlier version of the caller here used). That approach is a
//! trap for a process that is itself serving a mount: enumerating every
//! filesystem necessarily includes this process's own mount point, and
//! querying *that* one blocks in the kernel until this same process's own
//! [`crate::MountFilesystem::statfs`] answers it - which, if that call is
//! the one doing the enumerating in the first place (holding a lock the
//! recursive call also needs), deadlocks against itself. Confirmed the
//! hard way: a real Docker container serving a `--read-write` mount over
//! Samba wedged solid (unkillable, `wsl --shutdown` was the only recovery)
//! the first time a real SMB client (Windows Explorer, checking free space
//! before allowing a save) triggered a `statfs` call through to the
//! `sysinfo`-based implementation. A single-path query has no such risk:
//! it never looks at any filesystem's mount point other than the one
//! `path` itself resolves to, so it can never recurse into a mount this
//! process happens to be serving.

use std::io;
use std::path::Path;

#[cfg(target_os = "linux")]
mod imp {
    use super::*;
    use std::ffi::CString;
    use std::mem::MaybeUninit;
    use std::os::unix::ffi::OsStrExt;

    /// Same fields, same formula (`f_bsize * f_blocks` /
    /// `f_bsize * f_bavail`) the `sysinfo` crate itself used - that part
    /// was never wrong (confirmed correct via a real `df -h` on a live
    /// mount), only enumerating *every* mount to find it was.
    pub fn disk_space(path: &Path) -> io::Result<(u64, u64)> {
        let c_path = CString::new(path.as_os_str().as_bytes())
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        let mut stat: MaybeUninit<libc::statvfs> = MaybeUninit::uninit();
        if unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let stat = unsafe { stat.assume_init() };
        // No cast needed: `statvfs`'s relevant fields (`f_bsize`,
        // `f_blocks`, `f_bavail`) are already `u64` on this project's
        // supported Linux targets (clippy itself flags an explicit
        // same-type cast here as dead code).
        let total = stat.f_bsize.saturating_mul(stat.f_blocks);
        let available = stat.f_bsize.saturating_mul(stat.f_bavail);
        Ok((total, available))
    }
}

#[cfg(target_os = "windows")]
mod imp {
    use super::*;
    use std::ffi::c_void;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetDiskFreeSpaceExW(
            lp_directory_name: *const u16,
            lp_free_bytes_available_to_caller: *mut u64,
            lp_total_number_of_bytes: *mut u64,
            lp_total_number_of_free_bytes: *mut c_void,
        ) -> i32;
    }

    /// `lpFreeBytesAvailableToCaller` (not `lpTotalNumberOfFreeBytes`) for
    /// "available", matching Unix `f_bavail`'s semantics (space a normal
    /// caller could actually use, not just space that happens to be free
    /// but reserved/quota-restricted).
    pub fn disk_space(path: &Path) -> io::Result<(u64, u64)> {
        let path_str = path.to_str().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "path is not valid UTF-8")
        })?;
        let wide: Vec<u16> = path_str.encode_utf16().chain(std::iter::once(0)).collect();
        let mut available: u64 = 0;
        let mut total: u64 = 0;
        let ok = unsafe {
            GetDiskFreeSpaceExW(
                wide.as_ptr(),
                &mut available,
                &mut total,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((total, available))
    }
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
mod imp {
    use super::*;

    pub fn disk_space(_path: &Path) -> io::Result<(u64, u64)> {
        Ok((0, 0))
    }
}

pub use imp::disk_space;

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn reports_nonzero_total_and_available_for_a_real_path() {
        let dir = std::env::temp_dir();
        let (total, available) = disk_space(&dir).unwrap();
        assert!(total > 0, "total={total}");
        assert!(available > 0, "available={available}");
    }

    #[test]
    fn errors_for_a_nonexistent_path() {
        let result = disk_space(Path::new("/this/path/does/not/exist/at/all"));
        assert!(result.is_err(), "{result:?}");
    }
}
