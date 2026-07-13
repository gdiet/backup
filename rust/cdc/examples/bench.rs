use cdc::{CdcConfig, Chunker};
use std::time::{Duration, Instant};

fn main() {
    // 7 MB of pseudo-random data — matches the Go benchmark exactly (seed=42, same LCG).
    let mut seed: u32 = 42;
    let data: Vec<u8> = (0..7 * 1024 * 1024)
        .map(|_| {
            seed = 1_664_525u32.wrapping_mul(seed).wrapping_add(1_013_904_223);
            (seed >> 16) as u8
        })
        .collect();

    let config = CdcConfig::new(20).unwrap(); // ~1 MB average chunk size
    let mut chunker = config.chunker();

    // Matches the Go benchmark: each iteration calls next() then flush().
    // flush() resets the chunker, so each iteration starts from a clean state.
    let test_duration = Duration::from_secs(10);
    let start = Instant::now();
    let mut total_bytes = 0u64;

    while start.elapsed() < test_duration {
        chunker.next(&data);
        chunker.flush();
        total_bytes += data.len() as u64;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let gb_per_s = total_bytes as f64 / 1e9 / elapsed;
    println!(
        "Throughput: {:.2} GB/s  ({:.0} GB in {:.1} s, target_size_bits=20)",
        gb_per_s,
        total_bytes as f64 / 1e9,
        elapsed
    );
}
