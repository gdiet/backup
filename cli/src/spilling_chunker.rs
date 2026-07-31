//! A `cdc`-chunker + hasher combination, like `cdc::BufferingHashingChunker`,
//! but backed by a RAM-budgeted, disk-spilling [`WriteCache`] for each
//! chunk's raw bytes instead of a plain growing `Vec<u8>`.
//!
//! `BufferingHashingChunker` buffers a completed chunk's bytes so a caller
//! can write a new (non-duplicate) chunk without re-reading the source -
//! necessary (see its own doc comment on why re-reading isn't safe: the
//! source can change between reads), but its plain `Vec<u8>` buffer means a
//! single very large chunk fully lives in RAM at once. Under CDC that's
//! bounded by the configured max chunk size (up to ~16.6 GB at the largest
//! allowed `target_size_bits`); under `chunking: none`
//! (`cdc::SingleChunkChunker`) it's the *entire file*, however large. This
//! type keeps the same read-once, buffer-until-resolved shape but spills to
//! a temp file once a shared RAM budget is exhausted (the same mechanism
//! `mount --read-write`'s write cache already uses - see
//! `docs/plans/bounded-memory-io-pipeline.md`'s design notes), so neither
//! `store` nor `mount`'s persist pipeline can be pushed into unbounded
//! memory growth by one large chunk or file.
//!
//! Used by both `store.rs` (backing up files) and `mount.rs` (persisting a
//! read-write mount's writes).

use std::path::PathBuf;
use std::sync::Arc;

use cdc::{ChunkHasher, Chunker, LengthHash};

use crate::write_cache::{RamBudget, WriteCache};

/// One completed chunk: its [`LengthHash`] (for the dedup lookup the
/// caller must do) and its raw bytes, held in a dedicated [`WriteCache`]
/// rather than an in-memory `Vec<u8>`. Drop it to discard (a dedup hit
/// needs nothing more - this releases its RAM budget reservation and
/// deletes its spill file, if any); drain it via
/// `chunk_store::write_chunk_from_cache` on a miss.
pub(crate) struct SpilledChunk {
    pub length_hash: LengthHash,
    pub bytes: WriteCache,
}

/// See the module doc comment. `F` mints a fresh, not-yet-used spill path
/// each time a new chunk starts accumulating - every completed chunk keeps
/// its own [`WriteCache`] (handed to the caller via [`SpilledChunk`]), so a
/// new one is needed for whatever comes next.
pub(crate) struct SpillingHashingChunker<H, C, F> {
    chunker: C,
    hasher: H,
    buffer: WriteCache,
    ram_budget: Arc<RamBudget>,
    make_spill_path: F,
}

impl<H, C, F> SpillingHashingChunker<H, C, F>
where
    H: ChunkHasher,
    C: Chunker,
    F: FnMut() -> PathBuf,
{
    pub fn new(hasher: H, chunker: C, ram_budget: Arc<RamBudget>, mut make_spill_path: F) -> Self {
        let buffer = WriteCache::new(Arc::clone(&ram_budget), make_spill_path(), 0);
        Self {
            chunker,
            hasher,
            buffer,
            ram_budget,
            make_spill_path,
        }
    }

    fn fresh_buffer(&mut self) -> WriteCache {
        WriteCache::new(Arc::clone(&self.ram_budget), (self.make_spill_path)(), 0)
    }

    fn append(&mut self, data: &[u8]) -> std::io::Result<()> {
        let pos = self.buffer.size();
        self.buffer.write(pos, data)
    }

    /// Feed `data` to the chunker. Returns every chunk completed by this
    /// call, each with its own bytes - mirrors `cdc::BufferingHashingChunker
    /// ::next`'s slicing logic (duplicated here rather than wrapped, for
    /// the same reason that type's own doc comment gives: `HashingChunker`
    /// hashes and discards internally without exposing the bytes).
    pub fn next(&mut self, data: &[u8]) -> std::io::Result<Vec<SpilledChunk>> {
        let mut bytes_into_chunk = self.chunker.bytes_into_chunk();
        let chunk_lengths = self.chunker.next(data);
        let mut result = Vec::new();
        let mut data = data;

        for length in chunk_lengths {
            // A chunk boundary reported for this call always falls within
            // the current `data` slice, so the byte offset within it fits
            // in `usize`.
            let end_in_data = (length - bytes_into_chunk) as usize;
            let slice = &data[..end_in_data];
            self.hasher.update(slice);
            self.append(slice)?;
            data = &data[end_in_data..];
            let fresh = self.fresh_buffer();
            let bytes = std::mem::replace(&mut self.buffer, fresh);
            result.push(SpilledChunk {
                length_hash: LengthHash {
                    length,
                    hash: self.hasher.finalize_reset(),
                },
                bytes,
            });
            bytes_into_chunk = 0;
        }

        self.hasher.update(data);
        self.append(data)?;
        Ok(result)
    }

    /// Flush the last incomplete chunk, if any. Resets the chunker.
    pub fn flush(&mut self) -> std::io::Result<Option<SpilledChunk>> {
        let Some(length) = self.chunker.flush() else {
            return Ok(None);
        };
        let hash = self.hasher.finalize_reset();
        let fresh = self.fresh_buffer();
        let bytes = std::mem::replace(&mut self.buffer, fresh);
        Ok(Some(SpilledChunk {
            length_hash: LengthHash { length, hash },
            bytes,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdc::{ChunkerConfig, SingleChunkChunker};

    struct SumHasher(u64);

    impl ChunkHasher for SumHasher {
        fn update(&mut self, data: &[u8]) {
            self.0 = self.0.wrapping_add(data.iter().map(|&b| b as u64).sum());
        }

        fn finalize_reset(&mut self) -> Vec<u8> {
            let hash = self.0.to_le_bytes().to_vec();
            self.0 = 0;
            hash
        }
    }

    fn budget(bytes: u64) -> Arc<RamBudget> {
        Arc::new(RamBudget::new(bytes))
    }

    fn spill_path_factory() -> impl FnMut() -> PathBuf {
        // `dir` is moved into (and so kept alive by) the returned closure,
        // for as long as whatever holds the closure needs fresh paths -
        // no `mem::forget` needed here, unlike `write_cache`'s own
        // single-path test helper, since there's no premature drop.
        let dir = tempfile::tempdir().unwrap();
        let mut next = 0u64;
        move || {
            let path = dir.path().join(next.to_string());
            next += 1;
            path
        }
    }

    fn read_all(cache: &mut WriteCache) -> Vec<u8> {
        cache
            .read_filling_gaps(0, cache.size(), |_, _| panic!("no gaps expected"))
            .unwrap()
    }

    #[test]
    fn chunking_none_buffers_the_whole_input_as_one_chunk_only_at_flush() {
        let mut chunker = SpillingHashingChunker::new(
            SumHasher(0),
            SingleChunkChunker::default(),
            budget(1_000_000),
            spill_path_factory(),
        );

        assert!(chunker.next(b"hello ").unwrap().is_empty());
        assert!(chunker.next(b"world").unwrap().is_empty());
        let mut chunk = chunker.flush().unwrap().unwrap();

        assert_eq!(chunk.length_hash.length, 11);
        assert_eq!(read_all(&mut chunk.bytes), b"hello world");
        assert!(chunker.flush().unwrap().is_none(), "nothing left to flush");
    }

    #[test]
    fn cdc_chunking_can_complete_several_chunks_within_one_call() {
        let config = ChunkerConfig::new(Some(6)).unwrap(); // small target size for a short test input
        let mut chunker = SpillingHashingChunker::new(
            SumHasher(0),
            config.chunker(),
            budget(1_000_000),
            spill_path_factory(),
        );

        // Enough varied input to reliably cross at least one chunk boundary
        // at this small target size.
        let input: Vec<u8> = (0u32..20_000).map(|i| (i % 251) as u8).collect();
        let mut completed = chunker.next(&input).unwrap();
        if let Some(last) = chunker.flush().unwrap() {
            completed.push(last);
        }

        assert!(
            completed.len() > 1,
            "expected more than one chunk at this input size/target"
        );
        let mut reassembled = Vec::new();
        for chunk in &mut completed {
            reassembled.extend(read_all(&mut chunk.bytes));
        }
        assert_eq!(reassembled, input);
        let total_length: u64 = completed.iter().map(|c| c.length_hash.length).sum();
        assert_eq!(total_length, input.len() as u64);
    }

    #[test]
    fn falls_back_to_disk_once_the_ram_budget_is_exhausted() {
        let mut chunker = SpillingHashingChunker::new(
            SumHasher(0),
            SingleChunkChunker::default(),
            budget(4), // tiny budget - forces spillover well before the input ends
            spill_path_factory(),
        );

        chunker.next(b"abcdefghij").unwrap();
        let mut chunk = chunker.flush().unwrap().unwrap();

        assert_eq!(chunk.length_hash.length, 10);
        assert_eq!(read_all(&mut chunk.bytes), b"abcdefghij");
    }
}
