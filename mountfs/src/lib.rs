//! Platform-abstracted repository mounting. See
//! `docs/plans/cross-platform-mount-crate.md` for the design and current
//! status - the [`MountFilesystem`] trait and [`mount`] below match that
//! plan's "Crate design" section; only the Linux backend (`linux` module,
//! real system libfuse3 via its high-level `fuse_operations` API) is
//! implemented so far, read-only. No Windows/WinFSP backend yet.

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
    pub const EROFS: Errno = Errno(30);
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
/// Phase 2 (read-write, `docs/plans/fuse-mount-readwrite.md`) extends this
/// trait with `write`/`create`/`mkdir`/`unlink`/`rmdir`/`rename`/
/// `truncate`/`utimens`/`chmod`/`chown` - not added until that phase.
pub trait MountFilesystem: Send + Sync + 'static {
    fn getattr(&self, path: &str) -> Result<Attr, Errno>;
    fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno>;
    fn open(&self, path: &str, write_intent: bool) -> Result<Handle, Errno>;
    fn read(&self, handle: Handle, offset: u64, size: u32) -> Result<Vec<u8>, Errno>;
    fn release(&self, handle: Handle);
    fn statfs(&self) -> Result<StatfsInfo, Errno>;
}

#[cfg(target_os = "linux")]
pub mod linux;

#[cfg(target_os = "linux")]
pub use linux::mount;

#[cfg(not(target_os = "linux"))]
pub fn mount<T: MountFilesystem>(
    _fs: T,
    _mountpoint: &std::path::Path,
    _read_only: bool,
) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "mountfs: no backend implemented yet for this platform \
         (see docs/plans/cross-platform-mount-crate.md)",
    ))
}
