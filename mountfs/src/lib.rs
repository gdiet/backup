//! Platform-abstracted repository mounting. See
//! `docs/plans/implemented/05-cross-platform-mount-crate.md` for the design and current
//! status - the [`MountFilesystem`] trait and [`mount`] below match that
//! plan's "Crate design" section. Both the Linux (`linux`, real system
//! libfuse3) and Windows (`windows`, WinFSP's FUSE3-compatible API)
//! backends implement the full read-only op set - see the plan's "Windows
//! checkpoint" note for `windows`'s one known gap (no working in-process
//! clean-shutdown path yet).

/// A POSIX error number (e.g. `Errno::ENOENT`), returned by
/// [`MountFilesystem`] methods on failure. Kept as this crate's own type
/// (a positive `errno` value, matching the C convention `-errno.0` return
/// codes are built from) rather than re-exporting `fuser`'s or `libc`'s,
/// since both the Linux (libfuse) and future Windows (WinFSP, itself a
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
/// once by callers (`cli`'s `DedupFs`) and served identically by every
/// platform backend in this crate.
///
/// Phase 2a (read-write structural operations, `docs/plans/fuse-mount-
/// readwrite.md`) adds `mkdir`/`create`/`unlink`/`rmdir`/`rename`/
/// `utimens`/`chmod`/`chown` below, each defaulting to `EROFS` (or, for
/// `chmod`/`chown`, an accepted no-op - this crate models no permissions
/// at all, see the plan) so a read-only-only implementation (this crate's
/// own test fixtures, for instance) doesn't have to implement any of them
/// just to keep compiling. Phase 2b (content writes: `write`, non-trivial
/// `truncate`, the write cache) isn't added yet.
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

    /// Moves/renames `old_path` to `new_path`. Implementations are not
    /// required to support overwriting an existing `new_path` - returning
    /// [`Errno::EEXIST`] in that case is a valid, documented limitation,
    /// not a bug (see the plan doc).
    fn rename(&self, old_path: &str, new_path: &str) -> Result<(), Errno> {
        let _ = (old_path, new_path);
        Err(Errno::EROFS)
    }

    /// Sets `path`'s modification time.
    fn utimens(&self, path: &str, mtime_millis: i64) -> Result<(), Errno> {
        let _ = (path, mtime_millis);
        Err(Errno::EROFS)
    }

    /// Accepted no-op by default: this crate models no permission bits at
    /// all (see the plan doc's "Not modeling permissions") - overriding
    /// with `Err` would make every write-capable client that happens to
    /// `chmod` after creating a file fail for no functional reason.
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
    /// call), returning the number of bytes written. Phase 2b
    /// (`docs/plans/implemented/06-fuse-mount-readwrite.md`) - default `EROFS`, same
    /// rationale as the phase 2a methods above.
    fn write(&self, handle: Handle, offset: u64, data: &[u8]) -> Result<u32, Errno> {
        let _ = (handle, offset, data);
        Err(Errno::EROFS)
    }

    /// Resizes `path` to `size`, zero-padding if it grows. Phase 2b.
    fn truncate(&self, path: &str, size: u64) -> Result<(), Errno> {
        let _ = (path, size);
        Err(Errno::EROFS)
    }

    /// Called once, exactly once, as the mount is shutting down - the one
    /// place to flush caches, close connections, or otherwise wrap up
    /// state before the process goes away. Default no-op (nothing to do
    /// for a stateless read-only filesystem like `cli`'s current
    /// `DedupFs`); real state matters once phase 2 (read-write) adds it.
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

#[cfg(target_os = "linux")]
pub mod linux;

#[cfg(target_os = "linux")]
pub use linux::mount;

#[cfg(target_os = "windows")]
pub mod windows;

#[cfg(target_os = "windows")]
pub use windows::mount;

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
pub fn mount<T: MountFilesystem>(
    _fs: T,
    _mountpoint: &std::path::Path,
    _read_only: bool,
) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "mountfs: no backend implemented yet for this platform \
         (see docs/plans/implemented/05-cross-platform-mount-crate.md)",
    ))
}
