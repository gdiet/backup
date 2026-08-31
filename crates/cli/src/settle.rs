//! Turns a not-yet-persisted generation's complete byte stream into a durably committed
//! `content_id` (DESIGN-METADATA-007 in `docs/design/metadata-schema-with-contents-table.md`,
//! DESIGN-MOUNT-006 in `docs/design/mount-write-path.md`): the chunking, hashing, and
//! `crates/store` write a background settle job runs once a generation is ready to persist. Used
//! by `crate::settle_pool::JobPool` for a released generation's content, and directly by
//! `crate::dedup_fs::DedupFs::create` to settle a new file's canonical empty content
//! (DESIGN-MOUNT-015).

use std::io;

use cdc::{Chunker, ChunkerConfig, ConfiguredChunker};

/// BLAKE3 output truncated to this many bytes for both chunk and content hashes
/// (DESIGN-METADATA-007's "Hash width", `docs/design/metadata-schema-with-contents-table.md`).
const HASH_WIDTH: usize = 20;

/// Read window used to stream a generation's content through the chunker - bounded so settling a
/// large file never needs its complete byte stream in memory at once, unrelated to any chunk
/// boundary (the chunker itself accepts calls of any size, and a chunk may span several windows).
const READ_WINDOW: u32 = 4 * 1024 * 1024;

#[derive(Debug)]
pub enum SettleError {
    Io(io::Error),
    Db(db::Error),
}

impl std::fmt::Display for SettleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SettleError::Io(err) => write!(f, "{err}"),
            SettleError::Db(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for SettleError {}

/// Chunks, hashes, and stores `size` bytes of content read in windowed calls through `read`,
/// following DESIGN-METADATA-007's algorithm exactly: each chunk is hashed on its own raw bytes,
/// then a second hasher accumulates only each chunk's `(length, hash)` pair to produce the
/// content's own hash - so this second pass costs a handful of chunks' worth of metadata,
/// regardless of the content's actual size. Already-known chunks (found via
/// [`db::Repository::find_chunk`]) are linked without writing their bytes to `store` again.
///
/// `cdc_target_size_bits` must already be validated (REQ-CLI-005) - the same value the repository
/// was created with (`db::RepositorySettings::cdc_target_size_bits`).
pub fn settle(
    repo: &db::Repository,
    store: &store::ByteStore,
    cdc_target_size_bits: Option<u32>,
    size: u64,
    mut read: impl FnMut(u64, u32) -> io::Result<Vec<u8>>,
) -> Result<i64, SettleError> {
    let config = ChunkerConfig::new(cdc_target_size_bits)
        .expect("cdc_target_size_bits was already validated when the repository was created");
    let mut settler = Settler::new(repo, store, config.chunker());

    let mut position = 0u64;
    while position < size {
        let window_len = (size - position).min(u64::from(READ_WINDOW)) as u32;
        let data = read(position, window_len).map_err(SettleError::Io)?;
        settler.feed(&data)?;
        position += u64::from(window_len);
    }
    settler.finish(size)
}

/// Streaming chunker/hasher state for one [`settle`] call.
struct Settler<'a> {
    chunker: ConfiguredChunker,
    /// Bytes accumulated for the chunk currently in progress, across however many `feed` calls
    /// it spans - cleared each time a chunk boundary completes.
    chunk_buffer: Vec<u8>,
    chunk_ids: Vec<i64>,
    content_hasher: blake3::Hasher,
    repo: &'a db::Repository,
    store: &'a store::ByteStore,
}

impl<'a> Settler<'a> {
    fn new(
        repo: &'a db::Repository,
        store: &'a store::ByteStore,
        chunker: ConfiguredChunker,
    ) -> Self {
        Self {
            chunker,
            chunk_buffer: Vec::new(),
            chunk_ids: Vec::new(),
            content_hasher: blake3::Hasher::new(),
            repo,
            store,
        }
    }

    /// Feeds one window's worth of bytes through the chunker, completing (and registering) every
    /// chunk boundary found within it.
    fn feed(&mut self, data: &[u8]) -> Result<(), SettleError> {
        let mut bytes_into_chunk = self.chunker.bytes_into_chunk();
        let lengths = self.chunker.next(data);
        let mut rest = data;
        for length in lengths {
            let end_in_rest = (length - bytes_into_chunk) as usize;
            self.chunk_buffer.extend_from_slice(&rest[..end_in_rest]);
            rest = &rest[end_in_rest..];
            self.complete_chunk()?;
            bytes_into_chunk = 0;
        }
        self.chunk_buffer.extend_from_slice(rest);
        Ok(())
    }

    /// Hashes and registers the chunk currently accumulated in `chunk_buffer`: links it if
    /// already known, otherwise reserves storage and writes its bytes, then clears the buffer for
    /// the next chunk.
    fn complete_chunk(&mut self) -> Result<(), SettleError> {
        let hash = blake3::hash(&self.chunk_buffer);
        let chunk_hash = &hash.as_bytes()[..HASH_WIDTH];
        let length = self.chunk_buffer.len() as i64;

        let chunk_id = match self
            .repo
            .find_chunk(length, chunk_hash)
            .map_err(SettleError::Db)?
        {
            Some(id) => id,
            None => {
                let (id, ranges) = self
                    .repo
                    .reserve_and_insert_chunk(length, chunk_hash)
                    .map_err(SettleError::Db)?;
                write_ranges(self.store, &self.chunk_buffer, &ranges).map_err(SettleError::Io)?;
                id
            }
        };

        self.chunk_ids.push(chunk_id);
        self.content_hasher.update(&(length as u64).to_le_bytes());
        self.content_hasher.update(chunk_hash);
        self.chunk_buffer.clear();
        Ok(())
    }

    /// Flushes the final, possibly-partial chunk (if any) and resolves the whole-content
    /// `(length, hash)` row.
    fn finish(mut self, size: u64) -> Result<i64, SettleError> {
        if let Some(length) = self.chunker.flush() {
            debug_assert_eq!(
                length as usize,
                self.chunk_buffer.len(),
                "the chunker's own reported final-chunk length must match what was buffered for it"
            );
            self.complete_chunk()?;
        }

        let mut content_hash = [0u8; HASH_WIDTH];
        self.content_hasher.finalize_xof().fill(&mut content_hash);
        self.repo
            .find_or_create_content(size as i64, &content_hash, &self.chunk_ids)
            .map_err(SettleError::Db)
    }
}

/// Writes `data` into `store` at each of `ranges` in order - a chunk's bytes may be split across
/// more than one physical range if the allocator could not find one large enough gap for it
/// (DESIGN-STORE-003 in `docs/design/byte-store.md`).
fn write_ranges(store: &store::ByteStore, data: &[u8], ranges: &[(u64, u64)]) -> io::Result<()> {
    let mut offset = 0usize;
    for &(start, stop) in ranges {
        let len = (stop - start) as usize;
        store.write(start, &data[offset..offset + len])?;
        offset += len;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_and_store() -> (
        db::Repository,
        tempfile::TempDir,
        store::ByteStore,
        tempfile::TempDir,
    ) {
        let repo_dir = tempfile::tempdir().unwrap();
        let repo_root = repo_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            db::RepositorySettings::new(Some(12), 1_700_000_000_000),
        )
        .unwrap();
        let repo = db::open_repository(&repo_root).unwrap();

        let store_dir = tempfile::tempdir().unwrap();
        let store = store::ByteStore::new(store_dir.path(), false);

        (repo, repo_dir, store, store_dir)
    }

    /// Reads back a settled `content_id`'s complete bytes via `resolve_extents` + `store`, for
    /// tests to check what actually landed in the store rather than trusting `settle`'s own
    /// bookkeeping.
    fn read_back(repo: &db::Repository, store: &store::ByteStore, content_id: i64) -> Vec<u8> {
        let mut result = Vec::new();
        for (start, stop) in repo.resolve_extents(content_id).unwrap() {
            let mut buf = vec![0u8; (stop - start) as usize];
            store.read(start, &mut buf).unwrap();
            result.extend(buf);
        }
        result
    }

    #[test]
    fn settling_an_empty_file_creates_a_zero_length_content_with_no_chunks() {
        let (repo, _rd, store, _sd) = repo_and_store();
        let content_id = settle(&repo, &store, Some(12), 0, |_, _| Ok(Vec::new())).unwrap();
        assert_eq!(repo.resolve_extents(content_id).unwrap(), Vec::new());
        assert_eq!(read_back(&repo, &store, content_id), Vec::new());
    }

    #[test]
    fn settling_a_small_whole_file_chunk_stores_and_reads_back_the_same_bytes() {
        let (repo, _rd, store, _sd) = repo_and_store();
        let content = b"hello world".to_vec();
        let content_id = settle(&repo, &store, None, content.len() as u64, |pos, len| {
            Ok(content[pos as usize..(pos as usize + len as usize)].to_vec())
        })
        .unwrap();
        assert_eq!(read_back(&repo, &store, content_id), content);
    }

    #[test]
    fn settling_identical_content_twice_reuses_the_same_content_and_chunks() {
        let (repo, _rd, store, _sd) = repo_and_store();
        let content = b"the quick brown fox".to_vec();
        let read = |c: Vec<u8>| {
            move |pos: u64, len: u32| Ok(c[pos as usize..(pos as usize + len as usize)].to_vec())
        };
        let first = settle(
            &repo,
            &store,
            None,
            content.len() as u64,
            read(content.clone()),
        )
        .unwrap();
        let second = settle(
            &repo,
            &store,
            None,
            content.len() as u64,
            read(content.clone()),
        )
        .unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn settling_content_spanning_several_read_windows_still_reconstructs_it_correctly() {
        let (repo, _rd, store, _sd) = repo_and_store();
        // Larger than READ_WINDOW so `settle` must issue more than one windowed read call, and
        // varied enough (not a single repeated byte) to actually exercise CDC chunk boundaries.
        let size = (READ_WINDOW as usize) * 2 + 12345;
        let content: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let content_id = settle(&repo, &store, Some(16), size as u64, |pos, len| {
            Ok(content[pos as usize..(pos as usize + len as usize)].to_vec())
        })
        .unwrap();
        assert_eq!(read_back(&repo, &store, content_id), content);
        // A file this size at a 16-bit target should have split into more than one chunk - each
        // landing at its own extent on this otherwise-empty store - otherwise this test would not
        // actually be exercising multi-chunk assembly at all.
        let extent_count = repo.resolve_extents(content_id).unwrap().len();
        assert!(
            extent_count > 1,
            "expected more than one chunk, got {extent_count}"
        );
    }

    #[test]
    fn a_read_failure_is_reported_rather_than_silently_producing_wrong_content() {
        let (repo, _rd, store, _sd) = repo_and_store();
        let err = settle(&repo, &store, None, 10, |_, _| {
            Err(io::Error::other("simulated read failure"))
        })
        .unwrap_err();
        assert!(matches!(err, SettleError::Io(_)));
    }
}
