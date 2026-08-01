//! A byte store for large amounts of data on disk: [`LongTermStore`] reads
//! and writes bytes at arbitrary positions within one large logical address
//! space, internally splitting that space across many fixed-size physical
//! files so no single file grows without bound - transparent to callers,
//! who only ever see one flat, randomly addressable range of bytes.
//!
//! A good fit wherever data needs to be written and read back at caller-
//! chosen byte offsets that don't necessarily arrive in order - e.g. as the
//! backing store for a content-addressable or deduplicated data store
//! (each unique piece of data written once, at a position the caller
//! allocates), an append-mostly log, or generally any case where keeping
//! the logical data set out of the way of a single huge OS file's limits
//! and quirks (filesystem max-file-size, slow whole-file copies) matters
//! more than having one literal file per logical object.
//!
//! This crate is a stateless-per-call, call-from-any-thread primitive that just
//! moves bytes to and from a given position, unaware of how many callers there
//! are or how they're scheduled ([`LongTermStore`]'s own doc comment has
//! its concurrency/atomicity guarantees). Deciding *what* to write *where*
//! and *when* - deduplication, space allocation/reuse, and so on - is
//! entirely up to the caller.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// How many recently-used read file handles each thread keeps open - see
/// `LongTermStore`'s "Performance note" doc comment and
/// `docs/plans/implemented/long-term-store-read-handle-cache.md` for why (repeatedly
/// re-opening for every read call measured 1.2x-3.5x slower than keeping
/// a handle open) and why this is bounded (avoid unbounded
/// file-descriptor growth against a repository with many physical LTS
/// files - not chosen from any specific measurement, just a modest cap).
const READ_HANDLE_CACHE_CAPACITY: usize = 8;

thread_local! {
    // One small LRU of open read handles per OS thread, not a single
    // shared cache - avoids introducing a lock/contention point on what's
    // otherwise a fully lock-free read path (mirrors `cli::store`'s own
    // `READ_CONNECTION` thread-local pattern for DB connections, same
    // reasoning). Keyed by the full absolute file path, so this stays
    // correct even if a single thread uses more than one `LongTermStore`
    // pointed at a different `data_dir` - their paths never collide.
    // Most-recently-used entry at the front; a linear scan to find/evict
    // is fine at this small a capacity, cheaper than hashing.
    static READ_HANDLES: RefCell<VecDeque<(PathBuf, File)>> = const { RefCell::new(VecDeque::new()) };
}

/// Runs `f` with a read handle for `full_path` - reuses a cached handle
/// for this thread if there is one (see [`READ_HANDLES`]), opens (and
/// caches) a fresh one otherwise. Returns `Ok(None)` if `full_path`
/// doesn't exist - a normal, expected condition for [`LongTermStore::
/// read`]'s callers (a data file that was never written, or was deleted
/// out from under the repository), not an error condition to cache or
/// propagate as one.
///
/// Every successfully opened handle is cached after use, regardless of
/// what `f` returned - `f` always `seek`s to an absolute position before
/// reading (see `LongTermStore::read`), so the handle's cursor position
/// never carries meaning across calls, and a transient read error doesn't
/// mean the handle itself is broken.
fn with_read_handle<R>(
    full_path: &Path,
    f: impl FnOnce(&mut File) -> io::Result<R>,
) -> io::Result<Option<R>> {
    let cached = READ_HANDLES.with(|cache| {
        let mut cache = cache.borrow_mut();
        cache
            .iter()
            .position(|(path, _)| path == full_path)
            .map(|idx| cache.remove(idx).expect("just found via position").1)
    });

    let mut file = match cached {
        Some(file) => file,
        None => match File::open(full_path) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        },
    };

    let result = f(&mut file);

    READ_HANDLES.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= READ_HANDLE_CACHE_CAPACITY {
            cache.pop_back(); // evict the least-recently-used entry
        }
        cache.push_front((full_path.to_owned(), file));
    });

    result.map(Some)
}

/// Size of each backing data file in bytes. Must not be changed without a migration.
const FILE_SIZE: u64 = 100_000_000;

/// Returns the relative file path, the offset within that file, and the number of bytes
/// in `position..position + size` that reside within that file.
///
/// Path format `{dir1:02}/{dir2:02}/{file_start:010}` is compatible with the Scala
/// `LongTermStore`:
/// - one file per 100 MB
/// - one second-level directory per 10 GB
/// - one top-level directory per 1 TB
fn path_offset_size(position: u64, size: usize) -> (String, u64, usize) {
    let position_in_file = position % FILE_SIZE;
    let dir2 = position / FILE_SIZE / 100 % 100;
    let dir1 = position / FILE_SIZE / 100 / 100;
    let file_start = position - position_in_file;
    let path = format!("{dir1:02}/{dir2:02}/{file_start:010}");
    let bytes_available = (FILE_SIZE - position_in_file) as usize;
    (path, position_in_file, size.min(bytes_available))
}

/// Outcome of a [`LongTermStore::read`] call.
#[derive(Debug, PartialEq, Eq)]
pub enum ReadIntegrity {
    /// All bytes were read from existing data files.
    Complete,
    /// One or more data files were missing or shorter than expected.
    /// `missing_or_short` contains the relative path of each affected file.
    /// Bytes that could not be read were substituted with zeros.
    Incomplete { missing_or_short: Vec<String> },
}

/// A byte store on disk, addressed by an arbitrary `u64` position within one
/// logical address space (see the module doc comment). Internally splits
/// that space across many fixed-size physical files - transparent to
/// callers, who only ever deal with one flat address space.
///
/// # Thread safety
///
/// `write` opens its own file handle per call, so concurrent writes on any region are
/// safe without external locking. `read` reuses a small per-thread cache of recently
/// opened handles (see `with_read_handle`) instead - still lock-free across threads
/// (each thread's cache is entirely its own), and always `seek`s to an absolute
/// position before reading, so reusing a handle across calls changes nothing
/// observable versus opening fresh each time.
///
/// # Performance note
///
/// Measured (see `store/examples/bench.rs` and `docs/plans/implemented/
/// long-term-store-read-handle-cache.md`): re-opening on every call costs little to
/// nothing for writes, but a real, consistent 1.2x-3.5x for reads, at block sizes in
/// the 64 KiB-256 KiB range - worse on Windows than Linux. `read` caches handles for
/// that reason; `write` doesn't, since the data didn't support it.
///
/// # Atomicity
///
/// Writes are atomic within a single 100 MB data file (the OS serializes concurrent
/// writes to the same file). Writes spanning more than one file are **not** atomic:
/// two concurrent writes to the same large position range may interleave file by file.
/// Not an issue for a caller that only ever writes each position once - true, for
/// example, of a content-addressable/deduplicated store, which never overwrites
/// already-written data.
pub struct LongTermStore {
    data_dir: PathBuf,
    read_only: bool,
}

impl LongTermStore {
    /// Opens a `LongTermStore` backed by `data_dir`.
    ///
    /// When `read_only` is `true`, calls to [`write`][Self::write] return a
    /// [`PermissionDenied`][io::ErrorKind::PermissionDenied] error.
    pub fn new(data_dir: impl AsRef<Path>, read_only: bool) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_owned(),
            read_only,
        }
    }

    /// Writes `data` to the store starting at `position`.
    ///
    /// Creates missing parent directories and data files as needed.
    /// Writes that span a 100 MB file boundary are split across the two files.
    pub fn write(&self, position: u64, data: &[u8]) -> io::Result<()> {
        if self.read_only {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "store is read-only",
            ));
        }

        let mut pos = position;
        let mut remaining = data;

        while !remaining.is_empty() {
            let (path, file_offset, chunk_size) = path_offset_size(pos, remaining.len());
            let chunk = &remaining[..chunk_size];
            let full_path = self.data_dir.join(&path);

            // Try to open directly; create parent directories only when they are missing.
            // truncate(false) is the default; it is set explicitly to make clippy happy.
            // We must not truncate because this file may already contain data written by
            // a previous call at a different position, which must not be discarded.
            let mut file = match OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&full_path)
            {
                Ok(f) => Ok(f),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    if let Some(parent) = full_path.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .open(&full_path)
                }
                Err(e) => Err(e),
            }?;
            file.seek(SeekFrom::Start(file_offset))?;
            file.write_all(chunk)?;

            remaining = &remaining[chunk_size..];
            pos += chunk_size as u64;
        }

        Ok(())
    }

    /// Reads `buf.len()` bytes from the store starting at `position` into `buf`.
    ///
    /// Returns [`ReadIntegrity::Complete`] when all bytes came from existing files,
    /// or [`ReadIntegrity::Incomplete`] with the relative paths of any data files that
    /// were missing or shorter than expected. Unreadable bytes are set to zero.
    pub fn read(&self, position: u64, buf: &mut [u8]) -> io::Result<ReadIntegrity> {
        let mut pos = position;
        let mut remaining: &mut [u8] = buf;
        let mut incomplete: Vec<String> = Vec::new();

        while !remaining.is_empty() {
            let (path, file_offset, chunk_size) = path_offset_size(pos, remaining.len());
            let (chunk, rest) = remaining.split_at_mut(chunk_size);
            let full_path = self.data_dir.join(&path);
            let chunk_len = chunk.len();

            let bytes_read = with_read_handle(&full_path, |file| {
                file.seek(SeekFrom::Start(file_offset))?;
                let mut bytes_read = 0;
                while bytes_read < chunk.len() {
                    match file.read(&mut chunk[bytes_read..]) {
                        Ok(0) => break, // EOF: file is shorter than expected
                        Ok(n) => bytes_read += n,
                        Err(e) => return Err(e),
                    }
                }
                if bytes_read < chunk.len() {
                    chunk[bytes_read..].fill(0);
                }
                Ok(bytes_read)
            })?;

            match bytes_read {
                Some(bytes_read) if bytes_read < chunk_len => incomplete.push(path),
                Some(_) => {}
                None => {
                    chunk.fill(0);
                    incomplete.push(path);
                }
            }

            remaining = rest;
            pos += chunk_size as u64;
        }

        if incomplete.is_empty() {
            Ok(ReadIntegrity::Complete)
        } else {
            Ok(ReadIntegrity::Incomplete {
                missing_or_short: incomplete,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_store(dir: &TempDir, read_only: bool) -> LongTermStore {
        LongTermStore::new(dir.path(), read_only)
    }

    #[test]
    fn round_trip() {
        let dir = TempDir::new().unwrap();
        let s = make_store(&dir, false);

        let data: Vec<u8> = (0..=255u8).cycle().take(1024).collect();
        s.write(0, &data).unwrap();

        let mut buf = vec![0u8; data.len()];
        assert_eq!(s.read(0, &mut buf).unwrap(), ReadIntegrity::Complete);
        assert_eq!(buf, data);
    }

    #[test]
    fn round_trip_at_offset() {
        let dir = TempDir::new().unwrap();
        let s = make_store(&dir, false);

        s.write(42, b"hello world").unwrap();

        let mut buf = [0u8; 11];
        assert_eq!(s.read(42, &mut buf).unwrap(), ReadIntegrity::Complete);
        assert_eq!(&buf, b"hello world");
    }

    #[test]
    fn file_boundary() {
        let dir = TempDir::new().unwrap();
        let s = make_store(&dir, false);

        // 10 bytes straddling the 100 MB boundary (5 in each file)
        let boundary = FILE_SIZE - 5;
        let data: Vec<u8> = (0..10).collect();
        s.write(boundary, &data).unwrap();

        let mut buf = vec![0u8; 10];
        assert_eq!(s.read(boundary, &mut buf).unwrap(), ReadIntegrity::Complete);
        assert_eq!(buf, data);
    }

    #[test]
    fn missing_file_returns_zeros_and_incomplete() {
        let dir = TempDir::new().unwrap();
        let s = make_store(&dir, false);

        let mut buf = vec![0xFFu8; 100];
        let result = s.read(0, &mut buf).unwrap();

        assert!(buf.iter().all(|&b| b == 0));
        assert_eq!(
            result,
            ReadIntegrity::Incomplete {
                missing_or_short: vec!["00/00/0000000000".to_string()]
            }
        );
    }

    #[test]
    fn short_file_fills_remainder_with_zeros() {
        let dir = TempDir::new().unwrap();
        let s = make_store(&dir, false);

        s.write(0, &[0xABu8; 50]).unwrap();

        let mut buf = vec![0u8; 100];
        let result = s.read(0, &mut buf).unwrap();

        assert_eq!(&buf[..50], &[0xABu8; 50]);
        assert_eq!(&buf[50..], &[0u8; 50]);
        assert!(matches!(result, ReadIntegrity::Incomplete { .. }));
    }

    #[test]
    fn read_only_write_returns_permission_denied() {
        let dir = TempDir::new().unwrap();
        let s = make_store(&dir, true);

        let err = s.write(0, b"data").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn path_format_matches_scala() {
        // Position 0: first file
        let (path, offset, chunk) = path_offset_size(0, 100);
        assert_eq!(path, "00/00/0000000000");
        assert_eq!(offset, 0);
        assert_eq!(chunk, 100);

        // Position FILE_SIZE: second file
        let (path, offset, chunk) = path_offset_size(FILE_SIZE, 1);
        assert_eq!(path, "00/00/0100000000");
        assert_eq!(offset, 0);
        assert_eq!(chunk, 1);

        // Last 5 bytes of first file: chunk capped at boundary
        let (path, offset, chunk) = path_offset_size(FILE_SIZE - 5, 10);
        assert_eq!(path, "00/00/0000000000");
        assert_eq!(offset, FILE_SIZE - 5);
        assert_eq!(chunk, 5);

        // Position fileSize * 99: last file before dir2 rolls over
        let (path, _, _) = path_offset_size(FILE_SIZE * 99, 1);
        assert_eq!(path, "00/00/9900000000");

        // Position 10 GB (file_number = 100): first entry in dir2 = "01"
        let pos_10gb = FILE_SIZE * 100; // = 10_000_000_000
        let (path, offset, _) = path_offset_size(pos_10gb, 1);
        assert_eq!(path, "00/01/10000000000");
        assert_eq!(offset, 0);

        // Position 1 TB (file_number = 10_000): dir1 rolls over to "01"
        let pos_1tb = FILE_SIZE * 10_000; // = 1_000_000_000_000
        let (path, offset, _) = path_offset_size(pos_1tb, 1);
        assert_eq!(path, "01/00/1000000000000");
        assert_eq!(offset, 0);
    }

    #[test]
    fn missing_boundary_files_both_listed() {
        // Read across a file boundary where both files are missing.
        let dir = TempDir::new().unwrap();
        let s = make_store(&dir, false);

        let mut buf = vec![0xFFu8; 4];
        let result = s.read(FILE_SIZE - 2, &mut buf).unwrap();

        assert_eq!(buf, [0, 0, 0, 0]);
        assert_eq!(
            result,
            ReadIntegrity::Incomplete {
                missing_or_short: vec![
                    "00/00/0000000000".to_string(),
                    "00/00/0100000000".to_string(),
                ]
            }
        );
    }

    #[test]
    fn cross_boundary_second_file_too_short() {
        // Mirrors the Scala LongTermStoreSpec:
        // write [2,3,4,5] at fileSize-2, then read 6 bytes → [2,3,4,5,0,0]
        // The second file holds only 2 bytes, so 2 zeros must be substituted.
        let dir = TempDir::new().unwrap();
        let s = make_store(&dir, false);

        s.write(FILE_SIZE - 2, &[2u8, 3, 4, 5]).unwrap();

        let mut buf = vec![0u8; 6];
        let result = s.read(FILE_SIZE - 2, &mut buf).unwrap();

        assert_eq!(buf, [2, 3, 4, 5, 0, 0]);
        assert_eq!(
            result,
            ReadIntegrity::Incomplete {
                missing_or_short: vec!["00/00/0100000000".to_string()]
            }
        );
    }

    // --- Read handle cache (`with_read_handle`/`READ_HANDLES`) ---

    #[test]
    fn repeated_reads_reuse_the_cached_handle_and_still_return_correct_data() {
        let dir = TempDir::new().unwrap();
        let s = make_store(&dir, false);
        s.write(0, b"hello world").unwrap();

        // Each iteration hits the cached handle from the previous one.
        for _ in 0..3 {
            let mut buf = [0u8; 11];
            assert_eq!(s.read(0, &mut buf).unwrap(), ReadIntegrity::Complete);
            assert_eq!(&buf, b"hello world");
        }
    }

    #[test]
    fn a_write_after_a_cached_read_is_visible_on_the_next_read() {
        // A cached read handle must never serve stale data - `write` always
        // goes through its own fresh handle, and the OS makes that write
        // visible to any other handle on the same file immediately, cached
        // or not.
        let dir = TempDir::new().unwrap();
        let s = make_store(&dir, false);
        s.write(0, b"before").unwrap();

        let mut buf = [0u8; 6];
        assert_eq!(s.read(0, &mut buf).unwrap(), ReadIntegrity::Complete);
        assert_eq!(&buf, b"before");

        s.write(0, b"after!").unwrap();
        let mut buf = [0u8; 6];
        assert_eq!(s.read(0, &mut buf).unwrap(), ReadIntegrity::Complete);
        assert_eq!(&buf, b"after!");
    }

    #[test]
    fn reading_more_distinct_files_than_the_cache_capacity_still_returns_correct_data() {
        // More distinct physical files than READ_HANDLE_CACHE_CAPACITY, so
        // this exercises LRU eviction - and then re-reads the earliest
        // (now-evicted) ones again, which must re-open correctly rather
        // than somehow returning a stale/wrong cached handle.
        let dir = TempDir::new().unwrap();
        let s = make_store(&dir, false);
        let file_count = READ_HANDLE_CACHE_CAPACITY + 4;

        for i in 0..file_count {
            s.write(i as u64 * FILE_SIZE, &[i as u8; 4]).unwrap();
        }
        for round in 0..2 {
            for i in 0..file_count {
                let mut buf = [0u8; 4];
                assert_eq!(
                    s.read(i as u64 * FILE_SIZE, &mut buf).unwrap(),
                    ReadIntegrity::Complete,
                    "round {round}, file {i}"
                );
                assert_eq!(buf, [i as u8; 4], "round {round}, file {i}");
            }
        }
    }

    #[test]
    fn a_cached_read_handle_does_not_prevent_deleting_its_directory() {
        // Windows-specific concern flagged (but not verified) in
        // docs/plans/implemented/long-term-store-read-handle-cache.md: does keeping a
        // read handle open longer than a single call interfere with
        // deleting the file/directory it belongs to (NTFS share-mode
        // semantics differ from POSIX unlink-while-open)? Checked
        // directly here rather than just trusting that `TempDir`'s own
        // cleanup (which silently ignores errors) never visibly fails.
        let dir = TempDir::new().unwrap();
        let s = make_store(&dir, false);
        s.write(0, b"data").unwrap();

        let mut buf = [0u8; 4];
        assert_eq!(s.read(0, &mut buf).unwrap(), ReadIntegrity::Complete);
        // A read handle for this file is now cached on this thread.

        let dir_path = dir.path().to_path_buf();
        drop(dir); // TempDir::drop -> remove_dir_all, errors ignored
        assert!(
            !dir_path.exists(),
            "directory should be gone even with a cached read handle still open for a \
             file in it"
        );
    }
}
