//! A flat, position-addressed byte store (DESIGN-STORE-001/002 in
//! `docs/design/byte-store.md`): reads, writes, and truncates raw bytes at a given position,
//! with no knowledge of chunks, content identity, or deduplication - callers own deciding what
//! to write and which positions are free to write into.
//!
//! Backing files are fixed-size and named by a formula matching Scala's own store exactly (see
//! the design doc above), so an existing Scala repository's `data/` directory can be reused
//! unchanged.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const FILE_SIZE: u64 = 100_000_000;

/// How many recently-used read file handles each thread keeps open - see
/// [`with_read_handle`]'s doc comment for why, and `docs/design/byte-store.md`'s
/// `DESIGN-STORE-004` for the measurement behind this. Not chosen from any specific
/// measurement itself, just a modest cap against unbounded file-descriptor growth.
const READ_HANDLE_CACHE_CAPACITY: usize = 8;

thread_local! {
    // One small LRU per OS thread, not one shared cache - avoids a lock/contention point on
    // what would otherwise be a fully lock-free read path. Keyed by absolute path, so this
    // stays correct even if a single thread uses more than one ByteStore. Most-recently-used
    // at the front; a linear scan to find/evict is fine at this small a capacity.
    static READ_HANDLES: RefCell<VecDeque<(PathBuf, File)>> = const { RefCell::new(VecDeque::new()) };
}

/// Runs `f` with a read handle for `path`, reusing a cached one for this thread if there is
/// one, opening (and caching) a fresh one otherwise. Returns `Ok(None)` if `path` does not
/// exist - a normal, expected condition for [`ByteStore::read`]'s callers, not an error to
/// propagate as one.
///
/// Every successfully opened handle is cached after use regardless of what `f` returned - `f`
/// always seeks to an absolute position before reading, so a handle's cursor position never
/// carries meaning across calls, and a transient read error does not mean the handle itself is
/// broken. A write through a *different* handle to the same file is always immediately visible
/// here, cached or not - ordinary same-file-different-handle behavior, not something this cache
/// has to do anything special for.
///
/// A file removed by [`ByteStore::truncate_to`] is a different case: on a system where deleting
/// a file does not invalidate handles already open to it (true of the platforms this project
/// targets), a handle cached here before that removal would go on reading the old content
/// instead of correctly reporting it missing, for as long as it stays cached. Not addressed
/// here, on the assumption that nothing calls `truncate_to` for a range a concurrent read could
/// still legitimately want; enforcing that is a caller/allocator concern (DESIGN-STORE-003 in
/// `docs/design/byte-store.md`), not this cache's.
fn with_read_handle<T>(
    path: &Path,
    f: impl FnOnce(&mut File) -> io::Result<T>,
) -> io::Result<Option<T>> {
    let cached = READ_HANDLES.with(|cache| {
        let mut cache = cache.borrow_mut();
        cache
            .iter()
            .position(|(p, _)| p == path)
            .map(|i| cache.remove(i).expect("just found via position").1)
    });

    let mut file = match cached {
        Some(file) => file,
        None => match File::open(path) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        },
    };

    let result = f(&mut file);

    READ_HANDLES.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= READ_HANDLE_CACHE_CAPACITY {
            cache.pop_back();
        }
        cache.push_front((path.to_path_buf(), file));
    });

    result.map(Some)
}

/// Whether a [`ByteStore::read`] call's full requested range was backed by real on-disk data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadIntegrity {
    /// The full requested range was read from real data.
    Complete,
    /// The requested range touched one or more missing or too-short backing files, zero-filled
    /// in the output buffer instead - each affected file's path, relative to the store's own
    /// data directory, once per file touched (not deduplicated: a range spanning the same file
    /// twice is not possible, since each read only ever crosses a given file once).
    Incomplete { missing_or_short: Vec<PathBuf> },
}

/// See the module documentation.
pub struct ByteStore {
    data_dir: PathBuf,
    read_only: bool,
}

impl ByteStore {
    /// `data_dir` must already exist. When `read_only` is `true`, [`ByteStore::write`] and
    /// [`ByteStore::truncate_to`] both fail with [`io::ErrorKind::PermissionDenied`] rather than
    /// touching anything on disk.
    pub fn new(data_dir: impl AsRef<Path>, read_only: bool) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            read_only,
        }
    }

    /// Writes `data` starting at `position`, creating backing files/directories as needed.
    pub fn write(&self, position: u64, mut data: &[u8]) -> io::Result<()> {
        if self.read_only {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "store is read-only",
            ));
        }
        let mut position = position;
        while !data.is_empty() {
            let (path, _, offset_in_file, room) = self.locate(position);
            let n = (data.len() as u64).min(room) as usize;
            // Try opening directly first - DESIGN-STORE-005 in docs/design/byte-store.md. The
            // common case, once the first write into a given dir1/dir2 has already created it,
            // is that it already exists, and create_dir_all still costs a syscall to confirm
            // that even when it has nothing to do. Only pay for it on the actual first write
            // into a new directory (a plain NotFound from open, not yet knowing whether the file
            // or an ancestor directory is what's actually missing).
            let mut file = match OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
            {
                Ok(file) => file,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    if let Some(parent) = path.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .open(&path)?
                }
                Err(e) => return Err(e),
            };
            file.seek(SeekFrom::Start(offset_in_file))?;
            file.write_all(&data[..n])?;
            data = &data[n..];
            position += n as u64;
        }
        Ok(())
    }

    /// Reads `buf.len()` bytes starting at `position`. A missing or too-short backing file is
    /// zero-filled in `buf` rather than failing the call - see [`ReadIntegrity`].
    pub fn read(&self, position: u64, buf: &mut [u8]) -> io::Result<ReadIntegrity> {
        buf.fill(0);
        let mut position = position;
        let mut buf_offset = 0usize;
        let mut missing_or_short = Vec::new();
        while buf_offset < buf.len() {
            let (path, relative_path, offset_in_file, room) = self.locate(position);
            let n = ((buf.len() - buf_offset) as u64).min(room) as usize;
            let target = &mut buf[buf_offset..buf_offset + n];
            let read = with_read_handle(&path, |file| {
                file.seek(SeekFrom::Start(offset_in_file))?;
                read_fully(file, target)
            })?;
            match read {
                Some(read) if read < n => missing_or_short.push(relative_path),
                Some(_) => {}
                None => missing_or_short.push(relative_path),
            }
            position += n as u64;
            buf_offset += n;
        }
        Ok(if missing_or_short.is_empty() {
            ReadIntegrity::Complete
        } else {
            ReadIntegrity::Incomplete { missing_or_short }
        })
    }

    /// Shrinks the store so no byte at or beyond `len` remains: deletes files entirely past
    /// `len`, truncates the one file straddling it, and removes any directory this empties.
    ///
    /// Never scans for what currently exists to decide what to remove - the numbering scheme
    /// itself (DESIGN-STORE-001 in `docs/design/byte-store.md`) already says which file, and
    /// which `dir1`/`dir2` directory, `len` falls into, and anything numbered higher is beyond
    /// `len` by construction, regardless of whether it happens to exist. No assumption about
    /// gaps in what actually exists is needed as a result: a `dir1`/`dir2` numbered higher than
    /// the boundary is removed whole (`remove_dir_all`) whatever it does or does not contain,
    /// and only the straddling file's own `dir1`/`dir2` is ever actually listed, to find sibling
    /// files/directories above the boundary within it.
    pub fn truncate_to(&self, len: u64) -> io::Result<()> {
        if self.read_only {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "store is read-only",
            ));
        }
        if !self.data_dir.is_dir() {
            return Ok(());
        }

        let straddling_start = (len / FILE_SIZE) * FILE_SIZE;
        let straddling_index = straddling_start / FILE_SIZE;
        let straddling_dir2 = straddling_index % 100;
        let straddling_dir1 = straddling_index / 100;
        let (straddling_path, ..) = self.locate(straddling_start);

        if straddling_path.exists() {
            if len > straddling_start {
                OpenOptions::new()
                    .write(true)
                    .open(&straddling_path)?
                    .set_len(len - straddling_start)?;
            } else {
                fs::remove_file(&straddling_path)?;
            }
        }

        // Any other file in the straddling file's own dir2 is either below the boundary
        // (untouched) or strictly beyond it (its own position says so directly) - delete.
        let dir2_path = self
            .data_dir
            .join(format!("{straddling_dir1:02}"))
            .join(format!("{straddling_dir2:02}"));
        if dir2_path.is_dir() {
            for path in dir_entries(&dir2_path)? {
                if numeric_name(&path).is_some_and(|start| start > straddling_start) {
                    fs::remove_file(&path)?;
                }
            }
            remove_if_empty(&dir2_path)?;
        }

        // Any dir2 numbered higher than the straddling one, within the same dir1, is entirely
        // beyond len by construction - remove it whole, not file by file.
        let dir1_path = self.data_dir.join(format!("{straddling_dir1:02}"));
        if dir1_path.is_dir() {
            for path in dir_entries(&dir1_path)? {
                if numeric_name(&path).is_some_and(|n| n > straddling_dir2) {
                    fs::remove_dir_all(&path)?;
                }
            }
            remove_if_empty(&dir1_path)?;
        }

        // Same reasoning one level up: any dir1 numbered higher than the straddling one.
        for path in dir_entries(&self.data_dir)? {
            if numeric_name(&path).is_some_and(|n| n > straddling_dir1) {
                fs::remove_dir_all(&path)?;
            }
        }

        Ok(())
    }

    /// Returns the backing file's absolute path, its path relative to the store's own data
    /// directory, the write/read offset within it, and how many more bytes fit before the next
    /// file starts.
    fn locate(&self, position: u64) -> (PathBuf, PathBuf, u64, u64) {
        let file_start = (position / FILE_SIZE) * FILE_SIZE;
        let offset_in_file = position - file_start;
        let room = FILE_SIZE - offset_in_file;
        let dir1 = file_start / FILE_SIZE / 100 / 100;
        let dir2 = (file_start / FILE_SIZE / 100) % 100;
        let relative_path = PathBuf::from(format!("{dir1:02}"))
            .join(format!("{dir2:02}"))
            .join(format!("{file_start:010}"));
        let path = self.data_dir.join(&relative_path);
        (path, relative_path, offset_in_file, room)
    }
}

fn dir_entries(dir: &Path) -> io::Result<Vec<PathBuf>> {
    Ok(fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect())
}

/// Parses a path's final component (a `dir1`/`dir2` two-digit index or a ten-digit file start
/// position - both are just decimal numbers) as a plain integer.
fn numeric_name(path: &Path) -> Option<u64> {
    path.file_name()?.to_str()?.parse().ok()
}

fn remove_if_empty(dir: &Path) -> io::Result<()> {
    if fs::read_dir(dir)?.next().is_none() {
        fs::remove_dir(dir)?;
    }
    Ok(())
}

/// Reads until `buf` is full or the file is exhausted, returning how many bytes were actually
/// read - unlike a bare `Read::read`, which may return fewer bytes than requested even before
/// EOF.
fn read_fully(file: &mut File, buf: &mut [u8]) -> io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match file.read(&mut buf[total..])? {
            0 => break,
            n => total += n,
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, ByteStore) {
        let dir = tempfile::tempdir().expect("tempdir creation cannot fail here");
        let store = ByteStore::new(dir.path(), false);
        (dir, store)
    }

    #[test]
    fn write_then_read_back_exact_bytes() {
        let (_dir, store) = store();
        store.write(1000, b"hello world").unwrap();
        let mut buf = [0u8; 11];
        let integrity = store.read(1000, &mut buf).unwrap();
        assert_eq!(integrity, ReadIntegrity::Complete);
        assert_eq!(&buf, b"hello world");
    }

    #[test]
    fn write_and_read_span_a_file_boundary() {
        let (_dir, store) = store();
        let position = FILE_SIZE - 5;
        let data = b"0123456789";
        store.write(position, data).unwrap();
        let mut buf = [0u8; 10];
        let integrity = store.read(position, &mut buf).unwrap();
        assert_eq!(integrity, ReadIntegrity::Complete);
        assert_eq!(&buf, data);
    }

    #[test]
    fn read_never_written_position_is_zero_filled_and_reported_incomplete() {
        let (_dir, store) = store();
        let mut buf = [0xFFu8; 20];
        let integrity = store.read(5_000_000_000, &mut buf).unwrap();
        assert_eq!(
            integrity,
            ReadIntegrity::Incomplete {
                missing_or_short: vec![PathBuf::from("00").join("00").join("5000000000")]
            }
        );
        assert_eq!(&buf, &[0u8; 20]);
    }

    #[test]
    fn read_past_a_short_file_reports_its_relative_path() {
        let (_dir, store) = store();
        store.write(0, b"short").unwrap();
        let mut buf = [0xFFu8; 10];
        let integrity = store.read(0, &mut buf).unwrap();
        assert_eq!(
            integrity,
            ReadIntegrity::Incomplete {
                missing_or_short: vec![PathBuf::from("00").join("00").join("0000000000")]
            }
        );
        assert_eq!(&buf[..5], b"short");
        assert_eq!(&buf[5..], &[0u8; 5]);
    }

    #[test]
    fn read_spanning_two_missing_files_reports_both_relative_paths() {
        let (_dir, store) = store();
        let mut buf = [0u8; 10];
        let integrity = store.read(FILE_SIZE - 5, &mut buf).unwrap();
        assert_eq!(
            integrity,
            ReadIntegrity::Incomplete {
                missing_or_short: vec![
                    PathBuf::from("00").join("00").join("0000000000"),
                    PathBuf::from("00").join("00").join("0100000000"),
                ]
            }
        );
    }

    #[test]
    fn locate_matches_scalas_formula() {
        let (_dir, store) = store();
        let (path, relative_path, offset, room) =
            store.locate(FILE_SIZE * 100 * 3 + FILE_SIZE * 7 + 42);
        assert_eq!(
            path,
            store.data_dir.join("00").join("03").join("30700000000")
        );
        assert_eq!(
            relative_path,
            PathBuf::from("00").join("03").join("30700000000")
        );
        assert_eq!(offset, 42);
        assert_eq!(room, FILE_SIZE - 42);
    }

    #[test]
    fn truncate_to_deletes_files_entirely_past_len_and_shrinks_the_straddling_one() {
        let (_dir, store) = store();
        store.write(0, b"first file").unwrap();
        store.write(FILE_SIZE, b"second file").unwrap();
        store.truncate_to(5).unwrap();

        let mut buf = [0u8; 5];
        assert_eq!(store.read(0, &mut buf).unwrap(), ReadIntegrity::Complete);
        assert_eq!(&buf, b"first");

        let mut buf = [0u8; 11];
        assert_eq!(
            store.read(FILE_SIZE, &mut buf).unwrap(),
            ReadIntegrity::Incomplete {
                missing_or_short: vec![PathBuf::from("00").join("00").join("0100000000")]
            }
        );
    }

    #[test]
    fn truncate_to_removes_directories_left_empty() {
        let (dir, store) = store();
        // Contiguous, no gap - truncate_to relies on that, see its own doc comment.
        store.write(0, b"x").unwrap();
        store.write(FILE_SIZE, b"x").unwrap();
        store.truncate_to(0).unwrap();
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn read_only_store_refuses_write_and_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let store = ByteStore::new(dir.path(), true);

        let write_err = store.write(0, b"x").unwrap_err();
        assert_eq!(write_err.kind(), io::ErrorKind::PermissionDenied);

        let truncate_err = store.truncate_to(0).unwrap_err();
        assert_eq!(truncate_err.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn read_only_store_still_allows_reading() {
        let (dir, rw_store) = store();
        rw_store.write(0, b"hello").unwrap();

        let ro_store = ByteStore::new(dir.path(), true);
        let mut buf = [0u8; 5];
        assert_eq!(ro_store.read(0, &mut buf).unwrap(), ReadIntegrity::Complete);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn truncate_to_removes_everything_past_len_even_across_a_gap() {
        let (_dir, store) = store();
        // A gap: nothing at position 0, something further out. truncate_to never scans for
        // what exists, so this is not a problem for it - the file's own position already says
        // it is beyond len, regardless of what else does or does not exist.
        store.write(FILE_SIZE * 3, b"x").unwrap();
        store.truncate_to(0).unwrap();
        let mut buf = [0u8; 1];
        assert!(matches!(
            store.read(FILE_SIZE * 3, &mut buf).unwrap(),
            ReadIntegrity::Incomplete { .. }
        ));
    }

    #[test]
    fn truncate_to_removes_a_whole_dir1_beyond_the_straddling_one() {
        let (dir, store) = store();
        // dir1 boundary is every 100 * 100 files (1 TB) - well beyond what's practical to write
        // a real file for in a test, so this only ever creates the directory, not FILE_SIZE
        // bytes of content: writing 1 byte still forces the file (and its dir1/dir2) to exist.
        store.write(FILE_SIZE * 100 * 100, b"x").unwrap();
        store.truncate_to(0).unwrap();
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn a_write_after_a_cached_read_is_visible_on_the_next_read() {
        let (_dir, store) = store();
        store.write(0, b"before").unwrap();
        let mut buf = [0u8; 6];
        assert_eq!(store.read(0, &mut buf).unwrap(), ReadIntegrity::Complete);
        assert_eq!(&buf, b"before");

        store.write(0, b"after!").unwrap();
        let mut buf = [0u8; 6];
        assert_eq!(store.read(0, &mut buf).unwrap(), ReadIntegrity::Complete);
        assert_eq!(&buf, b"after!");
    }

    #[test]
    fn reading_more_distinct_files_than_the_cache_capacity_still_returns_correct_data() {
        let (_dir, store) = store();
        let file_count = READ_HANDLE_CACHE_CAPACITY + 4;
        for i in 0..file_count {
            store.write(i as u64 * FILE_SIZE, &[i as u8]).unwrap();
        }
        // Two rounds: the second re-reads files the first round's later files should have
        // evicted from the small per-thread cache - must still return correct data, not
        // whatever another file's now-reused handle happens to contain.
        for _ in 0..2 {
            for i in 0..file_count {
                let mut buf = [0u8; 1];
                let integrity = store.read(i as u64 * FILE_SIZE, &mut buf).unwrap();
                assert_eq!(integrity, ReadIntegrity::Complete);
                assert_eq!(buf, [i as u8]);
            }
        }
    }
}
