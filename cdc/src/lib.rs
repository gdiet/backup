//! Content-defined chunker inspired by fastcdc.

/// A chunker that produces chunk lengths from a stream of bytes.
pub trait Chunker {
    /// Returns the lengths of all complete chunks found in `data`. Chunks may span multiple calls.
    fn next(&mut self, data: &[u8]) -> Vec<u64>;

    /// Returns the length of the last incomplete chunk if any, then resets the chunker.
    fn flush(&mut self) -> Option<u64>;

    /// Returns the number of bytes accumulated so far in the current incomplete chunk,
    /// i.e. since the last chunk boundary (or since construction/reset if none yet).
    fn bytes_into_chunk(&self) -> u64;
}

/// Error returned by [`ChunkerConfig::new`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkerConfigError {
    /// `target_size_bits` is less than 6.
    TargetSizeBitsTooSmall(u32),
    /// `target_size_bits` is greater than 30.
    TargetSizeBitsTooBig(u32),
}

impl std::fmt::Display for ChunkerConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TargetSizeBitsTooSmall(v) => {
                write!(f, "target_size_bits {v} is too small (minimum: 6)")
            }
            Self::TargetSizeBitsTooBig(v) => {
                write!(f, "target_size_bits {v} is too large (maximum: 30)")
            }
        }
    }
}

impl std::error::Error for ChunkerConfigError {}

/// Validated configuration for [`ChunkerConfig::chunker`].
///
/// `target_size_bits: Some(_)` selects content-defined chunking; `None` selects
/// whole-input, single-chunk output (no rolling-fingerprint work at all).
#[derive(Debug, Clone)]
pub struct ChunkerConfig {
    target_size_bits: Option<u32>,
}

impl ChunkerConfig {
    /// Creates a validated chunker config.
    ///
    /// `target_size_bits`, if given, must be between 6 and 30 (inclusive).
    pub fn new(target_size_bits: Option<u32>) -> Result<ChunkerConfig, ChunkerConfigError> {
        if let Some(bits) = target_size_bits {
            if bits < 6 {
                return Err(ChunkerConfigError::TargetSizeBitsTooSmall(bits));
            }
            if bits > 30 {
                return Err(ChunkerConfigError::TargetSizeBitsTooBig(bits));
            }
        }
        Ok(ChunkerConfig { target_size_bits })
    }

    /// Creates the chunker selected by this config.
    ///
    /// With `target_size_bits: None`, returns a [`SingleChunkChunker`]: the entire
    /// input becomes one chunk, with no rolling-fingerprint work.
    ///
    /// With `target_size_bits: Some(_)`, returns a [`CdcChunker`] that produces an
    /// average chunk size slightly above `2^target_size_bits`. The minimum chunk
    /// size is `base_size = 2^(target_size_bits - 1)`, the maximum is
    /// `base_size * (target_size_bits + 1)`.
    ///
    /// Chunk boundaries are detected by checking whether the last N bits of the current
    /// fingerprint are all 0. Every `base_size` bytes, N is reduced by one, until a boundary
    /// is forced once N reaches 0.
    ///
    /// Chunk size distribution is approximately:
    /// - 1×base_size to 2×base_size: 40%
    /// - 2×base_size to 3×base_size: 38%
    /// - 3×base_size to 4×base_size: 19%
    /// - 4×base_size to 5×base_size:  3%
    /// - 5×base_size to 6×base_size:  0.05%
    ///
    /// Larger chunk sizes are rare for data with at least some entropy.
    ///
    /// Example end-of-chunk sequences (found by searching enwik9, see
    /// <http://prize.hutter1.net/>):
    /// - `'ears amidst much internal confl'` - 10 bit mask
    /// - `'ependent khanates. Following th'` - 20 bit mask
    /// - `'ric power|power]] consumption o'` - 29 bit mask
    /// - `hex(c1e4c85714eff62c19d6399112736f3d82bc1f15494286dab830c581b78a5e)` - 31 bit mask
    pub fn chunker(&self) -> ConfiguredChunker {
        match self.target_size_bits {
            Some(target_size_bits) => ConfiguredChunker::Cdc(CdcChunker::new(target_size_bits)),
            None => ConfiguredChunker::Single(SingleChunkChunker::default()),
        }
    }
}

/// The chunker selected by [`ChunkerConfig::chunker`].
// A small delegating enum instead of `Box<dyn Chunker>`, since there are only
// ever these two concrete chunker types and the choice is made once per config.
pub enum ConfiguredChunker {
    Cdc(CdcChunker),
    Single(SingleChunkChunker),
}

impl Chunker for ConfiguredChunker {
    fn next(&mut self, data: &[u8]) -> Vec<u64> {
        match self {
            ConfiguredChunker::Cdc(c) => c.next(data),
            ConfiguredChunker::Single(c) => c.next(data),
        }
    }

    fn flush(&mut self) -> Option<u64> {
        match self {
            ConfiguredChunker::Cdc(c) => c.flush(),
            ConfiguredChunker::Single(c) => c.flush(),
        }
    }

    fn bytes_into_chunk(&self) -> u64 {
        match self {
            ConfiguredChunker::Cdc(c) => c.bytes_into_chunk(),
            ConfiguredChunker::Single(c) => c.bytes_into_chunk(),
        }
    }
}

/// CDC chunker created by [`ChunkerConfig::chunker`].
pub struct CdcChunker {
    base_size: usize,
    bytes_into_chunk: u64,
    current_mask: u32,
    next_mask_shift: u64,
    fingerprint: u32,
}

impl CdcChunker {
    fn new(target_size_bits: u32) -> CdcChunker {
        let base_size: usize = 1 << (target_size_bits - 1);
        CdcChunker {
            base_size,
            bytes_into_chunk: 0,
            current_mask: (2 * base_size - 1) as u32,
            next_mask_shift: (2 * base_size - 1) as u64,
            fingerprint: 0,
        }
    }

    fn reset(&mut self) {
        self.bytes_into_chunk = 0;
        self.current_mask = (2 * self.base_size - 1) as u32;
        self.next_mask_shift = (2 * self.base_size - 1) as u64;
        self.fingerprint = 0;
    }
}

impl Chunker for CdcChunker {
    fn next(&mut self, data: &[u8]) -> Vec<u64> {
        let n = data.len();
        let mut chunk_positions: Vec<u64> = Vec::new();
        let mut i: usize = 0;
        let mut chunk_start_in_data: usize = 0;

        'outer: loop {
            if i >= n {
                break;
            }
            let bic = self.bytes_into_chunk;

            // The table values have 31 bits. Due to the right shift, the current byte
            // and the previous 30 bytes influence the fingerprint. At the chunk start,
            // skip base_size-31 bytes then warm up the fingerprint. That way the
            // minimum chunk size is base_size. target_size_bits must be at least 6 to
            // prevent base_size being less than 31.
            if bic < (self.base_size - 1) as u64 {
                // Skip bytes that don't influence the fingerprint.
                let skip = ((self.base_size - 31) as u64)
                    .saturating_sub(bic)
                    .min((n - i) as u64) as usize;
                i += skip;
                let until = chunk_start_in_data
                    .saturating_add(((self.base_size - 1) as u64 - bic) as usize)
                    .min(n);
                while i < until {
                    self.fingerprint = (self.fingerprint >> 1) ^ TABLE[data[i] as usize];
                    i += 1;
                }
            }

            // Find end of chunk.
            let mut chunk_rel_i = bic + (i - chunk_start_in_data) as u64;
            while i < n {
                if chunk_rel_i == self.next_mask_shift {
                    self.current_mask >>= 1;
                    self.next_mask_shift += self.base_size as u64;
                }
                self.fingerprint = (self.fingerprint >> 1) ^ TABLE[data[i] as usize];
                i += 1;
                chunk_rel_i += 1;
                if self.fingerprint & self.current_mask == 0 {
                    chunk_positions.push(chunk_rel_i);
                    chunk_start_in_data = i;
                    self.bytes_into_chunk = 0;
                    self.current_mask = (2 * self.base_size - 1) as u32;
                    self.next_mask_shift = (2 * self.base_size - 1) as u64;
                    self.fingerprint = 0;
                    continue 'outer;
                }
            }

            break;
        }

        self.bytes_into_chunk += (n - chunk_start_in_data) as u64;
        chunk_positions
    }

    fn flush(&mut self) -> Option<u64> {
        let bic = self.bytes_into_chunk;
        self.reset();
        if bic == 0 { None } else { Some(bic) }
    }

    fn bytes_into_chunk(&self) -> u64 {
        self.bytes_into_chunk
    }
}

/// A chunker that never splits: the entire input becomes one chunk.
#[derive(Default)]
pub struct SingleChunkChunker {
    bytes_into_chunk: u64,
}

impl Chunker for SingleChunkChunker {
    fn next(&mut self, data: &[u8]) -> Vec<u64> {
        self.bytes_into_chunk += data.len() as u64;
        Vec::new()
    }

    fn flush(&mut self) -> Option<u64> {
        let n = self.bytes_into_chunk;
        self.bytes_into_chunk = 0;
        if n > 0 { Some(n) } else { None }
    }

    fn bytes_into_chunk(&self) -> u64 {
        self.bytes_into_chunk
    }
}

/// A resettable cryptographic hasher for use with [`HashingChunker`].
///
/// Implement this trait to plug any hash algorithm into [`HashingChunker`] without
/// adding a dependency on a specific hash crate to `cdc`. For example, implement it
/// for `blake3::Hasher` in a higher-level crate or in tests.
pub trait ChunkHasher {
    /// Feed `data` into the hasher.
    fn update(&mut self, data: &[u8]);

    /// Return the current hash and reset the hasher to its initial state.
    fn finalize_reset(&mut self) -> Vec<u8>;
}

/// The length and hash of a single chunk produced by [`HashingChunker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LengthHash {
    pub length: u64,
    pub hash: Vec<u8>,
}

/// A [`Chunker`]-adapter that hashes each chunk as it is identified.
///
/// Wraps any [`Chunker`] and a [`ChunkHasher`], feeding each completed chunk
/// through the hasher and emitting [`LengthHash`] values.
///
/// Currently unused by any command (`store`/`mount` both need a completed
/// chunk's raw bytes too, for writing a dedup miss - see `cli::
/// spilling_chunker::SpillingHashingChunker`, which duplicates this type's
/// slicing loop rather than wrapping it, since this type hashes and
/// discards each byte slice internally without ever exposing it). Kept as
/// the simpler hash-only building block this crate's other types are
/// described against (see their own doc comments), and as a plausible fit
/// for a future dedup-only use case (e.g. a dry-run/verify mode that only
/// needs to detect duplicates, never write new chunk bytes) - not proven
/// premature, since removing and later re-adding an identical ~15-line
/// type costs more than leaving it.
pub struct HashingChunker<H, C> {
    chunker: C,
    hasher: H,
}

impl<H: ChunkHasher, C: Chunker> HashingChunker<H, C> {
    pub fn new(hasher: H, chunker: C) -> Self {
        Self { chunker, hasher }
    }

    /// Feed `data` to the chunker. Returns all chunks completed by this call.
    pub fn next(&mut self, data: &[u8]) -> Vec<LengthHash> {
        let mut bytes_into_chunk = self.chunker.bytes_into_chunk();
        let chunk_lengths = self.chunker.next(data);
        let mut result = Vec::new();
        let mut data = data;

        for length in chunk_lengths {
            // A chunk boundary reported for this call always falls within the current
            // `data` slice, so the byte offset within it fits in `usize`.
            let end_in_data = (length - bytes_into_chunk) as usize;
            self.hasher.update(&data[..end_in_data]);
            data = &data[end_in_data..];
            result.push(LengthHash {
                length,
                hash: self.hasher.finalize_reset(),
            });
            bytes_into_chunk = 0;
        }

        self.hasher.update(data);
        result
    }

    /// Flush the last incomplete chunk, if any. Resets the chunker.
    pub fn flush(&mut self) -> Option<LengthHash> {
        self.chunker.flush().map(|length| {
            let hash = self.hasher.finalize_reset();
            LengthHash { length, hash }
        })
    }
}

// TABLE contains 31-bit integers for the rolling fingerprint function.
// Each bit is set in exactly 128 entries, ensuring a good distribution of bits.
// 31-bit integers facilitate porting to environments with 32-bit signed integers.
static TABLE: [u32; 256] = [
    0x22612e91, 0x1170f0e6, 0x3303b39b, 0x66bd6edd, 0x01d2f2af, 0x231317fa, 0x2a289c7e, 0x36bd43c9,
    0x1bb014c6, 0x39b82bbf, 0x32ad8dfe, 0x54338a27, 0x5dfd4610, 0x641b1ed2, 0x464b3e2d, 0x30642c32,
    0x08a2c072, 0x471b5497, 0x3d7a654e, 0x58c1a3e4, 0x23a484f4, 0x49843509, 0x218c2950, 0x52572f6e,
    0x1c433479, 0x25f4c5ae, 0x5b6d64bc, 0x3f1f9806, 0x5785d1bd, 0x4e27cfa3, 0x007d56be, 0x0c12e1dd,
    0x364e7dc3, 0x6bd7d89a, 0x1cbb7170, 0x6063a130, 0x4db1ec82, 0x4b9232ec, 0x68652cec, 0x385d7f65,
    0x2379eca0, 0x4131907e, 0x106cb914, 0x4dee34be, 0x6afabe59, 0x1edaf97f, 0x526a7e6b, 0x492f5102,
    0x4fe73be2, 0x560d24d9, 0x025dfed8, 0x1305d8a6, 0x2abe4b97, 0x4062b20d, 0x5a10fa46, 0x26d11bf6,
    0x61aadec3, 0x7db6b0b4, 0x19e9dd99, 0x5f94f409, 0x6540f9c9, 0x4acfe0f3, 0x46df254c, 0x3671050d,
    0x3702ddb9, 0x072c59e3, 0x34306b60, 0x172b6008, 0x59811a7b, 0x30522671, 0x51e219ef, 0x5f3f0a79,
    0x27e99be2, 0x60846cc1, 0x464ed3ad, 0x7a090f46, 0x3c31e5b2, 0x76a7ffb0, 0x36b1c337, 0x0e09462c,
    0x6e70db9a, 0x28257e71, 0x3ec84a59, 0x529ac661, 0x610ff9fa, 0x7aa023b3, 0x1decaf62, 0x5b5fac14,
    0x38d7582b, 0x12e24616, 0x5dc4cd4c, 0x5c55c4f3, 0x4d0c7992, 0x780fe3ad, 0x3e12c00e, 0x71892389,
    0x25c08386, 0x0b7fe407, 0x72c1d05c, 0x72ceac7c, 0x6a8d9f79, 0x30c1ff57, 0x2dc2c4a9, 0x4fa2d586,
    0x02fbdb64, 0x50b45931, 0x0125dbe5, 0x65753475, 0x056cd423, 0x1a9f5465, 0x2da3cd41, 0x3369f68c,
    0x5f680814, 0x0285c614, 0x5ac9946f, 0x6b4b8f0c, 0x23183b4b, 0x2f64362a, 0x1d1b7e13, 0x56199e01,
    0x6c0f77c5, 0x61205bca, 0x39ffc2a4, 0x0813d309, 0x099f028d, 0x6ab61b57, 0x5c2fe165, 0x18567e97,
    0x1b6b32d3, 0x7cd73ba0, 0x7a2ea353, 0x70bb42be, 0x27e2250f, 0x34e1a18e, 0x08c5ba4a, 0x5e6c294f,
    0x4e5e2407, 0x1c3e9cf1, 0x63e1b51a, 0x563076a0, 0x3ae575e5, 0x6d601e3b, 0x6ccdcd71, 0x57a62cf4,
    0x632e0239, 0x488ca595, 0x2514394c, 0x2781d88c, 0x53846075, 0x0442819d, 0x0b38a3c1, 0x28ce59f6,
    0x52508ef8, 0x690bcfb8, 0x6d7e9d2d, 0x766de29c, 0x2c4d75d0, 0x68790e5e, 0x16f97228, 0x0110cad4,
    0x72462c2b, 0x2ccd1a6e, 0x6728dad1, 0x2db6b1f4, 0x4520f6fb, 0x69a59884, 0x369e13d4, 0x2222b521,
    0x42773fe1, 0x3891b606, 0x058ef3d2, 0x2c720b5a, 0x3ba24426, 0x6f357e67, 0x285fb907, 0x5af5a59c,
    0x569e3edb, 0x0b046707, 0x44d20887, 0x034aceea, 0x53c71d0e, 0x2faa5f89, 0x06f18ffc, 0x1ddc49b0,
    0x4f406f39, 0x5f514f11, 0x331a5fd0, 0x577f50d8, 0x4adc8563, 0x35c76d97, 0x0d104f09, 0x6b71648c,
    0x37b9c4df, 0x6c457b3e, 0x0c97591e, 0x77cd2174, 0x30340a37, 0x476d8c4e, 0x555a1980, 0x1456f048,
    0x586a8a66, 0x277b684a, 0x3816066d, 0x378277ce, 0x658682a9, 0x57cb4296, 0x35ada011, 0x297f6b47,
    0x008423d2, 0x58bcdd39, 0x67f639ce, 0x4514afc2, 0x391393be, 0x20b9723a, 0x7efc1093, 0x4bb28ed2,
    0x1e5c4bb9, 0x61778960, 0x1d9685c0, 0x143c8c22, 0x78fba75b, 0x1589a1e9, 0x516a40a7, 0x44db84f3,
    0x7ae18838, 0x4d8b8e64, 0x6c9b609d, 0x2bf0bd4b, 0x5522cb79, 0x32652269, 0x0fdb180b, 0x4edbedbb,
    0x419e1980, 0x0712f4e0, 0x79aabeb5, 0x481f560d, 0x695e289c, 0x65b0f2b3, 0x75785116, 0x1f85469e,
    0x1a9cffab, 0x1ce2752a, 0x1beee272, 0x36b62112, 0x11a3124e, 0x3756b0dc, 0x2d58bbac, 0x51c6b6bf,
    0x34108377, 0x72fba16f, 0x41c9d065, 0x34590797, 0x38b4057b, 0x623e955b, 0x5aacc345, 0x0f8f7b09,
];

#[cfg(test)]
mod tests {
    use super::*;

    // LCG-based pseudo-random data generator with fixed parameters, so that output is
    // reproducible across runs and platforms.
    struct Lcg {
        seed: u32,
    }

    impl Lcg {
        fn next(&mut self) -> u32 {
            self.seed = 1_664_525u32
                .wrapping_mul(self.seed)
                .wrapping_add(1_013_904_223);
            self.seed
        }
    }

    fn pseudo_random_data(seed: u32, length: usize) -> Vec<u8> {
        let mut lcg = Lcg { seed };
        (0..length).map(|_| (lcg.next() >> 16) as u8).collect()
    }

    fn pseudo_random_text(seed: u32, length: usize) -> Vec<u8> {
        let charset: &[u8] =
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,.-     ";
        let mut lcg = Lcg { seed };
        (0..length)
            .map(|_| charset[(lcg.next() >> 16) as usize % charset.len()])
            .collect()
    }

    /// Builds a `CdcChunker` for `target_size_bits` positioned exactly at the point where
    /// the minimum chunk size has just been reached: the warm-up phase is done and
    /// mask-checking starts with the full-width mask (`target_size_bits` bits). Because the
    /// rolling fingerprint's state is fully replaced after 31 bytes regardless of prior
    /// history, feeding exactly 31 fresh bytes from here on reproduces the same fingerprint
    /// as feeding those same 31 bytes at any other position.
    fn chunker_ready_to_scan(target_size_bits: u32) -> CdcChunker {
        let mut chunker = CdcChunker::new(target_size_bits);
        chunker.bytes_into_chunk = chunker.base_size as u64 - 1;
        chunker
    }

    #[test]
    fn test_single_chunk_basic() {
        let mut chunker = SingleChunkChunker::default();
        assert_eq!(chunker.next(b"hello"), vec![]);
        assert_eq!(chunker.flush(), Some(5));
        assert_eq!(chunker.flush(), None); // resets after flush
    }

    #[test]
    fn test_single_chunk_accumulates() {
        let mut chunker = SingleChunkChunker::default();
        assert_eq!(chunker.next(&[0u8; 100]), vec![]);
        assert_eq!(chunker.next(&[0u8; 200]), vec![]);
        assert_eq!(chunker.flush(), Some(300));
    }

    #[test]
    fn test_single_chunk_empty() {
        let mut chunker = SingleChunkChunker::default();
        assert_eq!(chunker.next(&[]), vec![]);
        assert_eq!(chunker.flush(), None);
    }

    #[test]
    fn test_illegal_config() {
        assert!(ChunkerConfig::new(Some(5)).is_err());
        assert!(ChunkerConfig::new(Some(6)).is_ok());
        assert!(ChunkerConfig::new(Some(30)).is_ok());
        assert!(ChunkerConfig::new(Some(31)).is_err());
        assert!(ChunkerConfig::new(None).is_ok());
    }

    #[test]
    fn test_configured_chunker_none_behaves_like_single_chunk() {
        let config = ChunkerConfig::new(None).unwrap();
        let mut chunker = config.chunker();
        assert_eq!(chunker.next(b"hello "), vec![]);
        assert_eq!(chunker.next(b"world"), vec![]);
        assert_eq!(chunker.flush(), Some(11));
        assert_eq!(chunker.flush(), None);
    }

    #[test]
    fn test_configured_chunker_some_behaves_like_cdc() {
        let config = ChunkerConfig::new(Some(20)).unwrap();
        let mut chunker = config.chunker();
        let data = pseudo_random_data(42, 7 * 1024 * 1024);
        let mut chunk_sizes = chunker.next(&data);
        chunk_sizes.extend(chunker.flush());
        assert_eq!(
            chunk_sizes,
            vec![
                1606795, 697894, 638611, 642966, 857992, 829401, 524432, 730375, 811566
            ]
        );
    }

    #[test]
    fn test_basic() {
        // Verify chunking in the most basic case, chunk size 1 MB.
        let config = ChunkerConfig::new(Some(20)).unwrap();
        let mut chunker = config.chunker();
        let data = pseudo_random_data(42, 7 * 1024 * 1024);
        let mut chunk_sizes = chunker.next(&data);
        chunk_sizes.extend(chunker.flush());
        assert_eq!(
            chunk_sizes,
            vec![
                1606795, 697894, 638611, 642966, 857992, 829401, 524432, 730375, 811566
            ]
        );
    }

    #[test]
    fn test_text() {
        // Verify chunking of text-like data, chunk size 1 kB.
        let config = ChunkerConfig::new(Some(10)).unwrap();
        let mut chunker = config.chunker();
        let data = pseudo_random_text(42, 7 * 1024);
        let mut chunk_sizes = chunker.next(&data);
        chunk_sizes.extend(chunker.flush());
        assert_eq!(chunk_sizes, vec![1633, 1205, 1184, 1535, 1168, 443]);
    }

    #[test]
    fn test_repeated_value() {
        // Verify chunking of data consisting of repeated values, chunk size 1 kB.
        let config = ChunkerConfig::new(Some(10)).unwrap();
        // max_chunk_size = base_size * (target_size_bits + 1) = (1<<10)/2 * (10+1)
        let max_chunk_size = (1u64 << 10) / 2 * (10 + 1);
        let mut chunker = config.chunker();
        let data = vec![3u8; 11 * 1024];
        // Not all repeated byte values result in maximum chunk size.
        // Value 3 does produce maximum-sized chunks.
        assert_eq!(chunker.next(&data), vec![max_chunk_size, max_chunk_size]);
    }

    #[test]
    fn test_small() {
        // Verify chunking with very small average chunk sizes (64 B).
        let config = ChunkerConfig::new(Some(6)).unwrap();
        let mut chunker = config.chunker();
        let data = pseudo_random_data(42, 7 * 64);
        let mut chunk_sizes = chunker.next(&data);
        chunk_sizes.extend(chunker.flush());
        assert_eq!(chunk_sizes, vec![80, 56, 38, 39, 40, 102, 93]);
    }

    #[test]
    fn test_average_chunk_size() {
        // Verify that the average chunk size is within a reasonable range of the target.
        // Uses StdRng (ChaCha12) with a fixed seed for high-entropy, reproducible data.
        use rand::{Rng, SeedableRng, rngs::StdRng};

        let mut rng = StdRng::from_seed([0u8; 32]);
        let config = ChunkerConfig::new(Some(6)).unwrap();
        let mut chunker = config.chunker();
        let mut data = vec![0u8; (1 << 6) * 1000];
        rng.fill_bytes(&mut data);
        let chunks = chunker.next(&data);
        assert!(!chunks.is_empty(), "no chunks produced for bits=6");
        let avg_6 = data.len() / chunks.len();

        let mut rng = StdRng::from_seed([0u8; 32]);
        let config = ChunkerConfig::new(Some(16)).unwrap();
        let mut chunker = config.chunker();
        let mut data = vec![0u8; (1 << 16) * 1000];
        rng.fill_bytes(&mut data);
        let chunks = chunker.next(&data);
        assert!(!chunks.is_empty(), "no chunks produced for bits=16");
        let avg_16 = data.len() / chunks.len();

        // Both are ~14% above the respective target (64 and 65536), confirming correct distribution.
        assert_eq!(avg_6, 72);
        assert_eq!(avg_16, 75415);
    }

    #[test]
    fn test_multipart_input() {
        // Verify chunking if the input is provided in multiple parts (e.g. read from a file).
        let data = pseudo_random_data(42, 7 * 1024 * 1024);
        let expected = vec![
            1606795u64, 697894, 638611, 642966, 857992, 829401, 524432, 730375, 811566,
        ];
        let config = ChunkerConfig::new(Some(20)).unwrap();

        let test_cases: &[(&str, &[usize])] = &[
            ("in one piece", &[]),
            (
                "split in fingerprint warm-up of first chunk",
                &[1024 * 1024 / 2 - 15, 5, 15],
            ),
            ("split in middle of first chunk", &[1000000 - 1, 1, 1]),
            (
                "split at chunk border to second chunk",
                &[1606795 - 1, 1, 1],
            ),
            (
                "split before minSize of second chunk",
                &[1606795 + 1000 - 1, 1, 1],
            ),
            (
                "split at minSize border of second chunk",
                &[1606795 + (1 << 19) - 1, 1, 1],
            ),
            (
                "split in middle of second chunk",
                &[1606795 + 1000000 - 1, 1, 1],
            ),
        ];

        for (name, splits) in test_cases {
            let mut chunker = config.chunker();
            let mut remaining = data.as_slice();
            let mut chunk_sizes: Vec<u64> = Vec::new();
            for &split in *splits {
                chunk_sizes.extend(chunker.next(&remaining[..split]));
                remaining = &remaining[split..];
            }
            chunk_sizes.extend(chunker.next(remaining));
            chunk_sizes.extend(chunker.flush());
            assert_eq!(chunk_sizes, expected, "test case: {name}");
        }
    }

    #[test]
    fn test_chunk_end_detection() {
        // Use a known chunk end pattern to verify that chunks end at the expected positions.
        let data = pseudo_random_data(42, 7 * 1024);
        let config = ChunkerConfig::new(Some(10)).unwrap();
        let pattern = b"kR9MVTnItt1y6KUcekTf,wO-ymFECPi";

        let test_cases: &[(&str, &[usize], &[u64])] = &[
            ("no chunk end added", &[], &[1658, 1588, 1536, 1338, 1048]),
            (
                "chunk end added before the start of the first data partition",
                &[511], // Expect the chunk end to be ignored.
                &[1658, 1588, 1536, 1338, 1048],
            ),
            (
                "chunk end added at the start of the first data partition",
                &[512],
                &[512, 1187, 1547, 1536, 1338, 1048],
            ),
            (
                "chunk end added before the mask switch",
                &[1023],
                &[1023, 929, 1294, 1536, 1338, 1048],
            ),
            (
                "chunk end added at the start of the mask switch",
                &[1024],
                &[1024, 928, 1294, 1536, 1338, 1048],
            ),
            (
                "chunk end added immediately after the mask switch",
                &[1025],
                &[1025, 927, 1294, 1536, 1338, 1048],
            ),
        ];

        for (name, pattern_ends_at, expected) in test_cases {
            let mut chunker = config.chunker();
            let mut test_data = data.clone();
            for &end_at in *pattern_ends_at {
                let start = end_at - pattern.len();
                test_data[start..end_at].copy_from_slice(pattern);
            }
            let mut chunk_sizes = chunker.next(&test_data);
            chunk_sizes.extend(chunker.flush());
            assert_eq!(&chunk_sizes, expected, "test case: {name}");
        }
    }

    #[test]
    fn test_verify_mask_8bit_pattern_does_not_match_9bit_mask() {
        // Pattern found (offline, by scanning real-world text) to match exactly an 8-bit
        // mask: fingerprint bits 0..=7 are 0, bit 8 is 1. At the tested position (end of a
        // 1031-byte buffer), target_size_bits=10 has already shifted down to a 9-bit mask
        // (bits 0..=8 must be 0), so this pattern must NOT trigger a chunk boundary there.
        let pattern: [u8; 31] = [
            0x84, 0x0e, 0xc3, 0xc0, 0xfd, 0x58, 0x12, 0x43, 0xce, 0xa3, 0xe8, 0x28, 0xa1, 0x5c,
            0x70, 0xce, 0x9a, 0x7f, 0x3b, 0x59, 0xf9, 0xa2, 0xaa, 0xe3, 0xeb, 0x28, 0xcb, 0x67,
            0x0f, 0x0e, 0x97,
        ];
        let config = ChunkerConfig::new(Some(10)).unwrap();
        let mut chunker = config.chunker();
        let mut data = vec![0u8; 1031];
        let start = data.len() - pattern.len();
        data[start..].copy_from_slice(&pattern);
        assert_eq!(chunker.next(&data), Vec::<u64>::new());
    }

    #[test]
    fn test_verify_mask_9bit_pattern_matches_9bit_mask() {
        // Pattern found (offline, by scanning real-world text) to match exactly a 9-bit
        // mask: fingerprint bits 0..=8 are 0, bit 9 is 1. At the tested position (end of a
        // 1031-byte buffer), target_size_bits=10 uses exactly a 9-bit mask there, so this
        // pattern must trigger a chunk boundary right at its end, verifying the mask-shift
        // timing precisely.
        let pattern: [u8; 31] = [
            0x80, 0xe2, 0x9f, 0x79, 0xc7, 0xea, 0xd1, 0xf1, 0x49, 0x12, 0xf2, 0x38, 0xa5, 0x6d,
            0x59, 0x73, 0x69, 0x08, 0xf3, 0xfe, 0x1a, 0xd8, 0x7a, 0x97, 0xe8, 0xc2, 0xcd, 0x89,
            0xd8, 0xa4, 0x79,
        ];
        let config = ChunkerConfig::new(Some(10)).unwrap();
        let mut chunker = config.chunker();
        let mut data = vec![0u8; 1031];
        let start = data.len() - pattern.len();
        data[start..].copy_from_slice(&pattern);
        assert_eq!(chunker.next(&data), vec![1031]);
    }

    #[test]
    fn test_example_pattern_10bit_mask() {
        // Real-text pattern documented on `ChunkerConfig::chunker` as an example end-of-chunk
        // sequence for a 10 bit mask: right after the minimum chunk size is reached for
        // target_size_bits=10, this pattern ends a chunk exactly at its last byte. The
        // reported chunk length is `base_size - 1` (bytes already "in" the simulated chunk)
        // plus the pattern's own length.
        let pattern = b"ears amidst much internal confl";
        let base_size = 1u64 << (10 - 1);
        let mut chunker = chunker_ready_to_scan(10);
        assert_eq!(
            chunker.next(pattern),
            vec![base_size - 1 + pattern.len() as u64]
        );
    }

    #[test]
    fn test_example_pattern_20bit_mask() {
        // Real-text pattern documented on `ChunkerConfig::chunker` as an example end-of-chunk
        // sequence for a 20 bit mask.
        let pattern = b"ependent khanates. Following th";
        let base_size = 1u64 << (20 - 1);
        let mut chunker = chunker_ready_to_scan(20);
        assert_eq!(
            chunker.next(pattern),
            vec![base_size - 1 + pattern.len() as u64]
        );
    }

    #[test]
    fn test_example_pattern_29bit_mask() {
        // Real-text pattern documented on `ChunkerConfig::chunker` as an example end-of-chunk
        // sequence for a 29 bit mask.
        let pattern = b"ric power|power]] consumption o";
        let base_size = 1u64 << (29 - 1);
        let mut chunker = chunker_ready_to_scan(29);
        assert_eq!(
            chunker.next(pattern),
            vec![base_size - 1 + pattern.len() as u64]
        );
    }

    #[test]
    fn test_example_pattern_31bit_mask() {
        // Pattern documented on `ChunkerConfig::chunker` as an example end-of-chunk sequence for
        // a 31 bit mask, i.e. a fully-zero fingerprint. `target_size_bits` only goes up to 30
        // (the widest mask this crate can produce), but a fully-zero fingerprint satisfies
        // any mask width, so it still ends a chunk right at the pattern's last byte there.
        let pattern: [u8; 31] = [
            0xc1, 0xe4, 0xc8, 0x57, 0x14, 0xef, 0xf6, 0x2c, 0x19, 0xd6, 0x39, 0x91, 0x12, 0x73,
            0x6f, 0x3d, 0x82, 0xbc, 0x1f, 0x15, 0x49, 0x42, 0x86, 0xda, 0xb8, 0x30, 0xc5, 0x81,
            0xb7, 0x8a, 0x5e,
        ];
        let base_size = 1u64 << (30 - 1);
        let mut chunker = chunker_ready_to_scan(30);
        assert_eq!(
            chunker.next(&pattern),
            vec![base_size - 1 + pattern.len() as u64]
        );
    }

    // --- HashingChunker tests ---

    struct XorHasher(u8);

    impl ChunkHasher for XorHasher {
        fn update(&mut self, data: &[u8]) {
            for &b in data {
                self.0 ^= b;
            }
        }
        fn finalize_reset(&mut self) -> Vec<u8> {
            let result = vec![self.0];
            self.0 = 0;
            result
        }
    }

    #[test]
    fn test_hashing_chunker_basic() {
        // HashingChunker emits correct LengthHash values for a simple single-chunk input.
        let mut chunker = HashingChunker::new(XorHasher(0), SingleChunkChunker::default());
        assert_eq!(chunker.next(b"hello"), vec![]);
        assert_eq!(
            chunker.flush(),
            Some(LengthHash {
                length: 5,
                hash: vec![b'h' ^ b'e' ^ b'l' ^ b'l' ^ b'o']
            })
        );
        assert_eq!(chunker.flush(), None);
    }

    #[test]
    fn test_hashing_chunker_multipart() {
        // Feeding data in pieces must produce the same result as in one piece.
        let data: Vec<u8> = (0u8..=255).collect();
        let expected_xor: u8 = data.iter().fold(0u8, |acc, &b| acc ^ b);

        let mut chunker_one = HashingChunker::new(XorHasher(0), SingleChunkChunker::default());
        chunker_one.next(&data);
        let result_one = chunker_one.flush().unwrap();

        let mut chunker_parts = HashingChunker::new(XorHasher(0), SingleChunkChunker::default());
        chunker_parts.next(&data[..100]);
        chunker_parts.next(&data[100..200]);
        chunker_parts.next(&data[200..]);
        let result_parts = chunker_parts.flush().unwrap();

        assert_eq!(result_one.hash, vec![expected_xor]);
        assert_eq!(result_one, result_parts);
    }

    #[test]
    fn test_hashing_chunker_multiple_chunks() {
        // Verify that the hasher is reset between chunks: each chunk's hash is independent.
        let config = ChunkerConfig::new(Some(20)).unwrap();
        let data = pseudo_random_data(42, 7 * 1024 * 1024);
        let mut chunker = HashingChunker::new(XorHasher(0), config.chunker());
        let mut results = chunker.next(&data);
        results.extend(chunker.flush());

        assert_eq!(results.len(), 9);
        // The first chunk covers bytes 0..1606795; verify its length.
        assert_eq!(results[0].length, 1606795);
        assert_eq!(results[0].hash.len(), 1);
        // Each chunk's XOR hash must equal the XOR of its own bytes.
        let chunk_sizes: Vec<u64> = results.iter().map(|lh| lh.length).collect();
        let mut offset: usize = 0;
        for (lh, &len) in results.iter().zip(chunk_sizes.iter()) {
            let len = len as usize;
            let expected_xor = data[offset..offset + len].iter().fold(0u8, |a, &b| a ^ b);
            assert_eq!(lh.hash, vec![expected_xor], "chunk at offset {offset}");
            offset += len;
        }
    }

    mod blake3_tests {
        use super::*;

        impl ChunkHasher for blake3::Hasher {
            fn update(&mut self, data: &[u8]) {
                blake3::Hasher::update(self, data);
            }
            fn finalize_reset(&mut self) -> Vec<u8> {
                let mut bytes = [0u8; 20];
                self.finalize_xof().fill(&mut bytes);
                self.reset();
                bytes.to_vec()
            }
        }

        #[test]
        fn test_hashing_chunker_blake3_expected_values() {
            // Expected values for seed=42, 7 MB, ChunkerConfig target_size_bits=20,
            // blake3 with 20-byte output.
            let expected: &[(u64, &[u8])] = &[
                (
                    1606795,
                    &[
                        0x82, 0x06, 0x1a, 0x7f, 0x0c, 0x42, 0xc2, 0x3f, 0x94, 0x0f, 0x09, 0xbe,
                        0x6c, 0xd8, 0xb1, 0x0b, 0xd4, 0x0c, 0x7e, 0x19,
                    ],
                ),
                (
                    697894,
                    &[
                        0xae, 0x5c, 0x6e, 0x29, 0xc3, 0x30, 0x10, 0x67, 0x18, 0xc0, 0x1c, 0x1a,
                        0xb2, 0x70, 0x1a, 0xab, 0x61, 0xdb, 0x81, 0x6f,
                    ],
                ),
                (
                    638611,
                    &[
                        0xfb, 0x8a, 0xc7, 0x9c, 0x4c, 0x86, 0xd1, 0x9c, 0x44, 0xa8, 0x4c, 0x92,
                        0x83, 0x1a, 0xff, 0x7c, 0xa5, 0x9e, 0x55, 0x03,
                    ],
                ),
                (
                    642966,
                    &[
                        0x73, 0xdb, 0xc1, 0x8a, 0x7b, 0x22, 0x99, 0x85, 0xe6, 0x07, 0x4f, 0xae,
                        0xd9, 0xd7, 0x77, 0x3b, 0xc0, 0xe0, 0x01, 0x3c,
                    ],
                ),
                (
                    857992,
                    &[
                        0x20, 0x71, 0xc8, 0xfe, 0x31, 0xae, 0xca, 0x66, 0x72, 0xe1, 0x02, 0xf9,
                        0xe1, 0xfc, 0xbd, 0x57, 0x84, 0x79, 0x58, 0x4b,
                    ],
                ),
                (
                    829401,
                    &[
                        0xdd, 0x55, 0x0b, 0x3e, 0xf6, 0xa1, 0xd3, 0x5f, 0xd9, 0x36, 0x8b, 0xdd,
                        0x10, 0xdd, 0x67, 0x8d, 0xac, 0xdc, 0x3f, 0x54,
                    ],
                ),
                (
                    524432,
                    &[
                        0xd9, 0x89, 0xbb, 0x02, 0x13, 0x9c, 0x9c, 0x43, 0xe9, 0xd2, 0x04, 0x47,
                        0x4c, 0x0f, 0xb1, 0x6f, 0xac, 0x6a, 0xc0, 0x3d,
                    ],
                ),
                (
                    730375,
                    &[
                        0x12, 0x54, 0x68, 0xc3, 0x7a, 0x5d, 0x00, 0x55, 0x3c, 0xbd, 0x79, 0x15,
                        0x33, 0x9c, 0x29, 0x04, 0x64, 0xf8, 0xaa, 0x43,
                    ],
                ),
                (
                    811566,
                    &[
                        0x47, 0x25, 0xf5, 0x47, 0x87, 0x19, 0x3c, 0x1d, 0x2a, 0xeb, 0x07, 0x7c,
                        0x6b, 0x12, 0xd6, 0xaf, 0xa6, 0x89, 0xa3, 0x70,
                    ],
                ),
            ];

            let data = pseudo_random_data(42, 7 * 1024 * 1024);
            let config = ChunkerConfig::new(Some(20)).unwrap();
            let mut chunker = HashingChunker::new(blake3::Hasher::new(), config.chunker());
            let mut results = chunker.next(&data);
            results.extend(chunker.flush());

            assert_eq!(results.len(), expected.len());
            for (i, (result, &(exp_len, exp_hash))) in
                results.iter().zip(expected.iter()).enumerate()
            {
                assert_eq!(result.length, exp_len, "chunk {i} length");
                assert_eq!(result.hash.as_slice(), exp_hash, "chunk {i} hash");
            }
        }
    }

    #[test]
    fn test_table_properties() {
        // Verify that the table values have each bit set in exactly 128 entries.
        let mut tab = TABLE;
        for b in 1..=31 {
            let count: u32 = tab
                .iter_mut()
                .map(|v| {
                    let bit = *v & 1;
                    *v >>= 1;
                    bit
                })
                .sum();
            assert_eq!(count, 128, "bit count mismatch at bit {b}");
        }
    }
}
