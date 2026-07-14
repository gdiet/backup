use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Size of each backing data file in bytes. Must not be changed without a migration.
const FILE_SIZE: u64 = 100_000_000;

/// Maximum number of file handles kept open simultaneously.
const MAX_CACHED_HANDLES: usize = 128;

/// Returns the relative file path, the offset within that file, and the number of bytes
/// in `[position, position + size)` that reside within that file.
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

/// A sequential byte store on disk. File format and naming are compatible with the
/// Scala `LongTermStore`.
///
/// # Thread safety
///
/// File handles are cached (up to 128 at a time). Concurrent calls on different files
/// proceed in parallel. Concurrent calls on the same file are serialized by a per-file
/// lock protecting the seek + IO pair.
///
/// # Atomicity
///
/// Writes are atomic within a single 100 MB data file (the OS serializes concurrent
/// writes to the same file). Writes spanning more than one file are **not** atomic:
/// two concurrent writes to the same large position range may interleave file by file.
/// In practice this is not an issue because the dedup store writes each position exactly once.
pub struct LongTermStore {
    data_dir: PathBuf,
    read_only: bool,
    handles: Mutex<HashMap<String, Arc<Handle>>>,
}

struct Handle {
    file: Mutex<File>,
    writable: bool,
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
            handles: Mutex::new(HashMap::new()),
        }
    }

    /// Returns a cached writable handle for `path`, opening (and creating) it if needed.
    /// The cache lock is held briefly during lookup and insert; file opening happens under
    /// the lock, which is acceptable because it is fast (a few µs on SSD).
    fn cached_write_handle(&self, path: &str, full_path: &Path) -> io::Result<Arc<Handle>> {
        let mut handles = self.handles.lock().unwrap();

        if let Some(h) = handles.get(path) {
            if h.writable {
                return Ok(h.clone());
            }
            // Replace read-only handle with a writable one.
            handles.remove(path);
        }

        Self::evict_if_needed(&mut handles);

        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(full_path)?;
        let h = Arc::new(Handle { file: Mutex::new(file), writable: true });
        handles.insert(path.to_owned(), h.clone());
        Ok(h)
    }

    /// Returns a cached handle for `path` suitable for reading, or `None` if the file is
    /// missing. A writable handle already in the cache is reused for reads.
    fn cached_read_handle(&self, path: &str, full_path: &Path) -> io::Result<Option<Arc<Handle>>> {
        let mut handles = self.handles.lock().unwrap();

        if let Some(h) = handles.get(path) {
            return Ok(Some(h.clone()));
        }

        match File::open(full_path) {
            Ok(file) => {
                Self::evict_if_needed(&mut handles);
                let h = Arc::new(Handle { file: Mutex::new(file), writable: false });
                handles.insert(path.to_owned(), h.clone());
                Ok(Some(h))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn evict_if_needed(handles: &mut HashMap<String, Arc<Handle>>) {
        if handles.len() >= MAX_CACHED_HANDLES {
            // Evict an arbitrary entry. With sequential access patterns this is rarely
            // needed, and LRU ordering is not worth the added complexity here.
            if let Some(key) = handles.keys().next().cloned() {
                handles.remove(&key);
            }
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

            let handle = self.cached_write_handle(&path, &full_path)?;
            let mut file = handle.file.lock().unwrap();
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

            match self.cached_read_handle(&path, &full_path)? {
                Some(handle) => {
                    let mut file = handle.file.lock().unwrap();
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
                        incomplete.push(path);
                    }
                }
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
}
