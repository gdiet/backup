//! A small trait-based abstraction for exposing a virtual filesystem
//! through a real OS mount point, without writing separate platform-
//! specific glue for each backend: implement [`MountFilesystem`] once (a
//! path-based, synchronous, POSIX-flavored trait - `getattr`, `readdir`,
//! `open`/`read`/`write`/`release`, and the usual structural operations),
//! then call [`mount`] to serve it - on Linux via libfuse3, on Windows via
//! WinFSP's FUSE3-compatible API, behind the identical trait.
//!
//! A good fit for presenting any programmatically-computed or otherwise
//! non-native data as an ordinary directory tree that unmodified
//! applications can open, read, and (optionally) write through normal file
//! I/O - e.g. a virtual filesystem backed by some other data source, a
//! debugging/inspection view of in-memory state, or a compatibility shim
//! that makes something not naturally file-shaped look like files.
//!
//! [`MountFilesystem`]'s write-related methods each default to
//! [`Errno::EROFS`] - see its own doc comment - so a read-only
//! implementation only has to implement the read-side subset and gets
//! correct read-only behavior on everything else for free. Both backends
//! also guarantee [`MountFilesystem::on_unmount`] runs on a clean shutdown
//! (Ctrl+C included), not just on an explicit unmount call - see its own
//! doc comment.
//!
//! See `docs/design/mount-abstraction.md` for why a single trait built
//! against this particular API shape covers both platforms. Both backends
//! implement the full read-only and structural read-write op set;
//! `windows` has one known gap, no working in-process clean-shutdown call
//! (not needed in practice - see `windows`'s own module doc comment).

/// A POSIX error number (e.g. `Errno::ENOENT`), returned by
/// [`MountFilesystem`] methods on failure. Kept as this crate's own type
/// (a positive `errno` value, matching the C convention `-errno.0` return
/// codes are built from) rather than re-exporting `fuser`'s or `libc`'s,
/// since both the Linux (libfuse) and Windows (WinFSP, itself a
/// FUSE-compatible layer) backends expect the same POSIX error numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Errno(pub i32);

impl Errno {
    pub const ENOENT: Errno = Errno(2);
    pub const EIO: Errno = Errno(5);
    pub const EEXIST: Errno = Errno(17);
    pub const ENOTDIR: Errno = Errno(20);
    pub const EISDIR: Errno = Errno(21);
    pub const EROFS: Errno = Errno(30);
    pub const ENOTEMPTY: Errno = Errno(39);
    pub const ENAMETOOLONG: Errno = Errno(36);
    pub const EINVAL: Errno = Errno(22);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    File,
    Directory,
}

#[derive(Debug, Clone, Copy)]
pub struct Attr {
    pub kind: FileKind,
    /// Ignored for directories.
    pub size: u64,
    pub mtime_millis: i64,
}

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub kind: FileKind,
}

/// An opaque, filesystem-chosen handle for an open file, returned by
/// [`MountFilesystem::open`] and threaded back through
/// [`MountFilesystem::read`]/[`MountFilesystem::release`] - deliberately
/// not an inode number (unlike `fuser::FileHandle`/`INodeNo`): the
/// high-level libfuse API this crate binds against is path-based, not
/// inode-based, and stores this value verbatim in `fuse_file_info::fh`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Handle(pub u64);

/// Longest single path-component name (the destination of `mkdir`/
/// `create`/`rename`) accepted by either backend, in UTF-8 bytes - matches
/// Linux's own `NAME_MAX` and stays within NTFS's 255-UTF-16-unit
/// per-component limit (a UTF-8 byte count is always >= the equivalent
/// UTF-16 unit count, so this is conservative on Windows too). Enforced by
/// both backends' dispatch layer, before a call ever reaches
/// [`MountFilesystem::mkdir`]/[`MountFilesystem::create`]/
/// [`MountFilesystem::rename`] - not something each implementor needs to
/// remember to check itself, since neither libfuse nor WinFSP enforce
/// this on a virtual filesystem's behalf (confirmed: creating and reading
/// back a much longer name than this through an unvalidated mount worked
/// with no OS-level error on Linux - `max_name_length` in [`StatfsInfo`]
/// is advisory only, nothing rejects an actual create that exceeds it).
pub const MAX_NAME_BYTES: usize = 255;

/// Rejects `path` with [`Errno::ENAMETOOLONG`] if its final component
/// exceeds [`MAX_NAME_BYTES`] - shared by both backends' `dispatch_mkdir`/
/// `dispatch_create`/`dispatch_rename` (called with `new_path` for the
/// latter, matching `MountFilesystem::rename`'s "old path stays whatever
/// it already was, only the new name is being freshly introduced"
/// asymmetry).
fn reject_if_name_too_long(path: &str) -> Result<(), Errno> {
    let name = path.rsplit_once('/').map_or(path, |(_, name)| name);
    if name.len() > MAX_NAME_BYTES {
        Err(Errno::ENAMETOOLONG)
    } else {
        Ok(())
    }
}

/// `RENAME_NOREPLACE` bit, matching Linux's `renameat2(2)`/real libfuse3's
/// high-level `rename` callback's own `flags` parameter - "do not replace
/// an existing `new_path`, fail instead" (see
/// [`MountFilesystem::rename`]'s doc comment for how this crate surfaces
/// it). Confirmed against real WinFSP (`crates/mountfs/tests/rename_noreplace.rs`)
/// that its `cygfuse` layer never actually sets this bit: WinFSP enforces
/// "reject if the destination already exists" itself, before ever calling
/// into this crate's `rename` dispatch, and passes `flags = 0` for every
/// call that does reach it - regardless of which Win32-level flag the
/// original caller used. An implementation's own `no_replace` handling is
/// therefore unreachable on Windows via an ordinary rename; the collision
/// behavior a caller would want is still correct end to end, just enforced
/// by WinFSP itself rather than by the filesystem.
const RENAME_NOREPLACE: u32 = 1 << 0;

/// `RENAME_EXCHANGE` bit - an atomic two-way swap of two already-existing
/// entries. Out of scope for [`MountFilesystem::rename`] entirely: both
/// backends reject it via [`parse_rename_flags`] before ever calling into
/// the trait, rather than silently mishandling it as an ordinary replace.
const RENAME_EXCHANGE: u32 = 1 << 1;

/// Interprets a `rename` callback's raw `flags` parameter (as delivered by
/// either backend - see [`RENAME_NOREPLACE`]'s doc comment) into the
/// `no_replace` argument [`MountFilesystem::rename`] expects, or rejects
/// the call outright with [`Errno::EINVAL`] if [`RENAME_EXCHANGE`] is set -
/// shared so both backends parse the same bits identically rather than
/// each guessing at their own copy.
fn parse_rename_flags(flags: u32) -> Result<bool, Errno> {
    if flags & RENAME_EXCHANGE != 0 {
        return Err(Errno::EINVAL);
    }
    Ok(flags & RENAME_NOREPLACE != 0)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StatfsInfo {
    pub blocks: u64,
    pub blocks_free: u64,
    pub blocks_available: u64,
    pub files: u64,
    pub files_free: u64,
    pub block_size: u32,
    pub max_name_length: u32,
}

/// Path-based (not inode-based - see [`Handle`]) mount backend, implemented
/// once by the caller and served identically by every platform backend in
/// this crate.
///
/// The write-related methods below (`mkdir`/`create`/`unlink`/`rmdir`/
/// `rename`/`utimens`/`chmod`/`chown`/`write`/`truncate`) each default to
/// `EROFS` (or, for `chmod`/`chown`, an accepted no-op - this crate models
/// no permissions at all), so a read-only implementation (this crate's own
/// test fixtures, for instance) only needs to implement the read-side
/// methods above and gets correct read-only behavior on everything else
/// for free.
pub trait MountFilesystem: Send + Sync + 'static {
    fn getattr(&self, path: &str) -> Result<Attr, Errno>;
    fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno>;
    fn open(&self, path: &str, write_intent: bool) -> Result<Handle, Errno>;
    fn read(&self, handle: Handle, offset: u64, size: u32) -> Result<Vec<u8>, Errno>;
    fn release(&self, handle: Handle);
    fn statfs(&self) -> Result<StatfsInfo, Errno>;

    /// Creates an empty directory. The parent must already exist.
    fn mkdir(&self, path: &str) -> Result<(), Errno> {
        let _ = path;
        Err(Errno::EROFS)
    }

    /// Creates (and opens) a new, empty file. The parent must already
    /// exist; `path` must not already name an active entry.
    fn create(&self, path: &str) -> Result<Handle, Errno> {
        let _ = path;
        Err(Errno::EROFS)
    }

    /// Removes a file.
    fn unlink(&self, path: &str) -> Result<(), Errno> {
        let _ = path;
        Err(Errno::EROFS)
    }

    /// Removes a directory. Implementations must reject a non-empty
    /// directory with [`Errno::ENOTEMPTY`] rather than recursing.
    fn rmdir(&self, path: &str) -> Result<(), Errno> {
        let _ = path;
        Err(Errno::EROFS)
    }

    /// Moves/renames `old_path` to `new_path`. If `new_path` already names
    /// an active entry, an implementation *may* replace it (real POSIX
    /// `rename(2)` semantics) - unless `no_replace` is set (from
    /// `RENAME_NOREPLACE`, `renameat2(2)`'s own flag), in which case an
    /// existing `new_path` must always be rejected with
    /// [`Errno::EEXIST`] regardless of what replacing it would otherwise
    /// have done. Implementations are not required to support replacing an
    /// existing `new_path` at all - returning [`Errno::EEXIST`]
    /// unconditionally remains a valid, documented limitation, not a bug.
    ///
    /// On Windows, `no_replace` is confirmed always `false` for an
    /// ordinary rename (see `docs/design/mount-abstraction.md`'s "Known
    /// limitations"). WinFSP already rejects a colliding no-replace
    /// rename before this method is ever called, so the caller-visible
    /// behavior is still correct; only an implementation that
    /// specifically depends on observing `no_replace = true` itself
    /// (rather than just the collision being rejected) is affected.
    fn rename(&self, old_path: &str, new_path: &str, no_replace: bool) -> Result<(), Errno> {
        let _ = (old_path, new_path, no_replace);
        Err(Errno::EROFS)
    }

    /// Sets `path`'s modification time.
    fn utimens(&self, path: &str, mtime_millis: i64) -> Result<(), Errno> {
        let _ = (path, mtime_millis);
        Err(Errno::EROFS)
    }

    /// Accepted no-op by default: this crate models no permission bits at
    /// all - overriding with `Err` would make every write-capable client
    /// that happens to `chmod` after creating a file fail for no
    /// functional reason.
    fn chmod(&self, path: &str) -> Result<(), Errno> {
        let _ = path;
        Ok(())
    }

    /// See [`MountFilesystem::chmod`].
    fn chown(&self, path: &str) -> Result<(), Errno> {
        let _ = path;
        Ok(())
    }

    /// Writes `data` at `offset` into the file identified by `handle`
    /// (from a prior [`MountFilesystem::open`]/[`MountFilesystem::create`]
    /// call), returning the number of bytes written. Defaults to `EROFS`,
    /// same rationale as the other write-related methods above.
    fn write(&self, handle: Handle, offset: u64, data: &[u8]) -> Result<u32, Errno> {
        let _ = (handle, offset, data);
        Err(Errno::EROFS)
    }

    /// Resizes `path` to `size`, zero-padding if it grows.
    fn truncate(&self, path: &str, size: u64) -> Result<(), Errno> {
        let _ = (path, size);
        Err(Errno::EROFS)
    }

    /// Called once, exactly once, as the mount is shutting down - the one
    /// place to flush caches, close connections, or otherwise wrap up
    /// state before the process goes away. Default no-op (nothing to do
    /// for a stateless read-only implementation); real state matters once
    /// an implementation has something to flush (e.g. write buffering).
    ///
    /// Both backends guarantee this runs on a *clean* shutdown path, not
    /// an external `kill -9`/`TerminateProcess`: called right after the
    /// platform's blocking mount call (`fuse_main_real` on Linux, its
    /// WinFSP equivalent on Windows) returns, which on both platforms
    /// already happens on Ctrl+C without this crate needing to do
    /// anything itself - real libfuse's own `SIGINT`/`SIGTERM` handling on
    /// Linux, WinFSP's own internal handling on Windows (confirmed by
    /// manual testing: WinFSP unmounts cleanly and returns on Ctrl+C on
    /// its own, the same way real libfuse does - see `windows::mount`'s
    /// doc comment). Runs on the mount's own thread on both platforms.
    fn on_unmount(&self) {}
}

mod disk_space;
pub use disk_space::disk_space;

#[cfg(target_os = "linux")]
pub mod linux;

#[cfg(target_os = "linux")]
pub use linux::{mount, preflight};

#[cfg(target_os = "windows")]
pub mod windows;

#[cfg(target_os = "windows")]
pub use windows::{mount, preflight};

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
pub fn preflight() -> std::io::Result<()> {
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
pub fn mount<T: MountFilesystem>(
    _fs: T,
    _mountpoint: &std::path::Path,
    _read_only: bool,
) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "mountfs: no backend implemented yet for this platform \
         (see docs/design/mount-abstraction.md)",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rename_flags_defaults_to_replace_allowed() {
        assert_eq!(parse_rename_flags(0), Ok(false));
    }

    #[test]
    fn parse_rename_flags_recognizes_no_replace() {
        assert_eq!(parse_rename_flags(RENAME_NOREPLACE), Ok(true));
    }

    #[test]
    fn parse_rename_flags_rejects_exchange() {
        assert_eq!(parse_rename_flags(RENAME_EXCHANGE), Err(Errno::EINVAL));
    }

    #[test]
    fn parse_rename_flags_rejects_exchange_even_combined_with_no_replace() {
        assert_eq!(
            parse_rename_flags(RENAME_NOREPLACE | RENAME_EXCHANGE),
            Err(Errno::EINVAL)
        );
    }

    #[test]
    fn parse_rename_flags_ignores_unrelated_bits() {
        assert_eq!(parse_rename_flags(1 << 5), Ok(false));
    }
}
