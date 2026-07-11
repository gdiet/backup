// Package cdc contains a content-defined chunker inspired by fastcdc.

/// A content-defined chunker that produces chunk lengths from a stream of bytes.
pub trait Chunker {
    /// Returns the lengths of all complete chunks found in `data`. Chunks may span multiple calls.
    fn next(&mut self, data: &[u8]) -> Vec<usize>;

    /// Returns the length of the last incomplete chunk if any, then resets the chunker.
    fn flush(&mut self) -> Option<usize>;
}

/// Validated configuration for a CDC chunker.
#[derive(Debug, Clone)]
pub struct Config {
    target_size_bits: u32,
}

impl Config {
    /// Creates a validated CDC chunker config.
    ///
    /// `target_size_bits` must be between 6 and 30 (inclusive).
    pub fn new(target_size_bits: u32) -> Result<Config, String> {
        if !(6..=30).contains(&target_size_bits) {
            return Err("target_size_bits must be between 6 and 30 (inclusive)".to_string());
        }
        Ok(Config { target_size_bits })
    }

    /// Creates a CDC chunker that produces an average chunk size slightly above
    /// `2^target_size_bits`. The minimum chunk size is `base_size = 2^(target_size_bits - 1)`,
    /// the maximum is `base_size * (target_size_bits + 1)`.
    ///
    /// Chunk size distribution is approximately:
    /// - 1×base_size to 2×base_size: 40%
    /// - 2×base_size to 3×base_size: 38%
    /// - 3×base_size to 4×base_size: 19%
    /// - 4×base_size to 5×base_size:  3%
    pub fn new_cdc(&self) -> CdcChunker {
        let base_size = 1i64 << (self.target_size_bits - 1);
        let mut c = CdcChunker {
            base_size,
            chunk_start: 0,
            current_mask: 0,
            shift_mask_at: 0,
            fingerprint: 0,
        };
        c.reset();
        c
    }
}

/// CDC chunker created by [`Config::new_cdc`].
pub struct CdcChunker {
    base_size: i64,
    chunk_start: i64,
    current_mask: i64,
    shift_mask_at: i64,
    fingerprint: i64,
}

impl CdcChunker {
    fn reset(&mut self) {
        self.chunk_start = 0;
        self.current_mask = 2 * self.base_size - 1;
        self.shift_mask_at = 2 * self.base_size - 1;
        self.fingerprint = 0;
    }
}

impl Chunker for CdcChunker {
    fn flush(&mut self) -> Option<usize> {
        let chunk_start = self.chunk_start;
        self.reset();
        if chunk_start == 0 {
            None
        } else {
            Some((-chunk_start) as usize)
        }
    }

    fn next(&mut self, data: &[u8]) -> Vec<usize> {
        let mut i: usize = 0;
        let mut chunk_positions: Vec<usize> = Vec::new();
        let n = data.len();

        'outer: while i < n {
            // The table values have 31 bits. Due to the right shift, the current byte
            // and the previous 30 bytes influence the fingerprint. At the chunk start,
            // skip base_size-31 bytes then warm up the fingerprint. That way the
            // minimum chunk size is base_size. base_size cannot be less than 31, so
            // target_size_bits must be at least 6.
            if (i as i64) < self.base_size + self.chunk_start - 1 {
                let skip_to = (self.base_size + self.chunk_start - 31)
                    .max(0)
                    .min(n as i64) as usize;
                i = skip_to;
                let until = (self.base_size + self.chunk_start - 1).max(0).min(n as i64) as usize;
                while i < until {
                    self.fingerprint = (self.fingerprint >> 1) ^ TABLE[data[i] as usize];
                    i += 1;
                }
            }

            // Find end of chunk.
            while i < n {
                if (i as i64) == self.shift_mask_at {
                    self.current_mask >>= 1;
                    self.shift_mask_at += self.base_size;
                }
                self.fingerprint = (self.fingerprint >> 1) ^ TABLE[data[i] as usize];
                i += 1;
                if self.fingerprint & self.current_mask == 0 {
                    chunk_positions.push((i as i64 - self.chunk_start) as usize);
                    self.chunk_start = i as i64;
                    self.current_mask = 2 * self.base_size - 1;
                    self.shift_mask_at = 2 * self.base_size - 1 + i as i64;
                    self.fingerprint = 0;
                    continue 'outer;
                }
            }
        }
        self.chunk_start -= i as i64;
        self.shift_mask_at -= i as i64;
        chunk_positions
    }
}

// TABLE contains 31-bit integers for the rolling fingerprint function.
// Each bit is set in exactly 128 entries, ensuring a good distribution of bits.
static TABLE: [i64; 256] = [
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

    // LCG-based pseudo-random data generator matching Go's testutil package.
    // Do not change parameters or implementation: the output must be reproducible
    // and match the Go implementation exactly.
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

    #[test]
    fn test_illegal_config() {
        assert!(Config::new(5).is_err());
        assert!(Config::new(6).is_ok());
        assert!(Config::new(30).is_ok());
        assert!(Config::new(31).is_err());
    }

    #[test]
    fn test_basic() {
        // Verify chunking in the most basic case, chunk size 1 MB.
        let config = Config::new(20).unwrap();
        let mut chunker = config.new_cdc();
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
        let config = Config::new(10).unwrap();
        let mut chunker = config.new_cdc();
        let data = pseudo_random_text(42, 7 * 1024);
        let mut chunk_sizes = chunker.next(&data);
        chunk_sizes.extend(chunker.flush());
        assert_eq!(chunk_sizes, vec![1633, 1205, 1184, 1535, 1168, 443]);
    }

    #[test]
    fn test_repeated_value() {
        // Verify chunking of data consisting of repeated values, chunk size 1 kB.
        let config = Config::new(10).unwrap();
        // max_chunk_size = base_size * (target_size_bits + 1) = (1<<10)/2 * (10+1)
        let max_chunk_size = (1usize << 10) / 2 * (10 + 1);
        let mut chunker = config.new_cdc();
        let data = vec![3u8; 11 * 1024];
        // Not all repeated byte values result in maximum chunk size.
        // Value 3 does produce maximum-sized chunks.
        assert_eq!(chunker.next(&data), vec![max_chunk_size, max_chunk_size]);
    }

    #[test]
    fn test_small() {
        // Verify chunking with very small average chunk sizes (64 B).
        let config = Config::new(6).unwrap();
        let mut chunker = config.new_cdc();
        let data = pseudo_random_data(42, 7 * 64);
        let mut chunk_sizes = chunker.next(&data);
        chunk_sizes.extend(chunker.flush());
        assert_eq!(chunk_sizes, vec![80, 56, 38, 39, 40, 102, 93]);
    }

    #[test]
    fn test_average_chunk_size() {
        // Verify that the average chunk size is within a reasonable range of the target.
        // Uses StdRng (ChaCha12) with a fixed seed for high-entropy, reproducible data.
        // The Go equivalent uses ChaCha8; exact values differ but statistical properties match.
        use rand::{Rng, SeedableRng, rngs::StdRng};

        let mut rng = StdRng::from_seed([0u8; 32]);
        let config = Config::new(6).unwrap();
        let mut chunker = config.new_cdc();
        let mut data = vec![0u8; (1 << 6) * 1000];
        rng.fill_bytes(&mut data);
        let chunks = chunker.next(&data);
        assert!(!chunks.is_empty(), "no chunks produced for bits=6");
        let avg_6 = data.len() / chunks.len();

        let mut rng = StdRng::from_seed([0u8; 32]);
        let config = Config::new(16).unwrap();
        let mut chunker = config.new_cdc();
        let mut data = vec![0u8; (1 << 16) * 1000];
        rng.fill_bytes(&mut data);
        let chunks = chunker.next(&data);
        assert!(!chunks.is_empty(), "no chunks produced for bits=16");
        let avg_16 = data.len() / chunks.len();

        // Go's ChaCha8 (zero seed) produces avg_6=72, avg_16=75069.
        // Rust's StdRng/ChaCha12 (zero seed) produces avg_6=72, avg_16=75415 (<0.5% difference).
        // Both are ~14% above the respective target (64 and 65536), confirming correct distribution.
        assert_eq!(avg_6, 72);
        assert_eq!(avg_16, 75415);
    }

    #[test]
    fn test_multipart_input() {
        // Verify chunking if the input is provided in multiple parts (e.g. read from a file).
        let data = pseudo_random_data(42, 7 * 1024 * 1024);
        let expected = vec![
            1606795usize,
            697894,
            638611,
            642966,
            857992,
            829401,
            524432,
            730375,
            811566,
        ];
        let config = Config::new(20).unwrap();

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
            let mut chunker = config.new_cdc();
            let mut remaining = data.as_slice();
            let mut chunk_sizes: Vec<usize> = Vec::new();
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
        let config = Config::new(10).unwrap();
        let pattern = b"kR9MVTnItt1y6KUcekTf,wO-ymFECPi";

        let test_cases: &[(&str, &[usize], &[usize])] = &[
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
            let mut chunker = config.new_cdc();
            let mut testdata = data.clone();
            for &end_at in *pattern_ends_at {
                let start = end_at - pattern.len();
                testdata[start..end_at].copy_from_slice(pattern);
            }
            let mut chunk_sizes = chunker.next(&testdata);
            chunk_sizes.extend(chunker.flush());
            assert_eq!(&chunk_sizes, expected, "test case: {name}");
        }
    }

    #[test]
    fn test_table_properties() {
        // Verify that the table values have each bit set in exactly 128 entries.
        let mut tab = TABLE;
        for b in 1..=31 {
            let count: i64 = tab
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
