//! Raw hashing throughput: Rust `blake3` (this project's chunk/content hash)
//! vs. Java's `MessageDigest("MD5")` (`Md5Bench.java` in this same
//! directory - the two are meant to be run side by side, same
//! methodology). Not part of the `rust/` Cargo workspace (see this
//! directory's own `Cargo.toml`'s empty `[workspace]` section) and not run
//! as part of `cargo test`/CI - a one-off comparison, kept for reference.
//! See this directory's `README.md` for how to run both sides and the
//! measured results.
//!
//! Run with `cargo run --release` (a debug build would badly understate
//! blake3's real throughput).

use std::time::{Duration, Instant};

/// Large enough that per-call overhead (allocating the hasher, etc.) is
/// negligible relative to the actual hashing work - the point is to
/// measure the algorithm's raw throughput, not small-input call overhead.
const BUFFER_SIZE: usize = 128 * 1024 * 1024;
const WARMUP_DURATION: Duration = Duration::from_secs(1);
const TEST_DURATION: Duration = Duration::from_secs(5);

/// Hashes `data` in a loop for `duration`, mirroring `Blake3Hasher` in
/// `cli/src/store.rs` (finalize as extendable output, truncated to 20
/// bytes) - the same hash this project actually computes per chunk/
/// content, not a generic `finalize()`. Returns total bytes hashed and the
/// actual elapsed time.
fn run_for(duration: Duration, data: &[u8]) -> (u64, Duration) {
    let start = Instant::now();
    let mut total_bytes = 0u64;
    let mut sink = 0u8;
    while start.elapsed() < duration {
        let mut hasher = blake3::Hasher::new();
        hasher.update(data);
        let mut hash = [0u8; 20];
        hasher.finalize_xof().fill(&mut hash);
        sink ^= hash[0];
        total_bytes += data.len() as u64;
    }
    // Keep the compiler from proving the hash result is unused and
    // optimizing the whole loop away.
    std::hint::black_box(sink);
    (total_bytes, start.elapsed())
}

fn main() {
    let data = vec![0xABu8; BUFFER_SIZE];

    run_for(WARMUP_DURATION, &data); // discarded

    let (total_bytes, elapsed) = run_for(TEST_DURATION, &data);
    let gb_per_s = total_bytes as f64 / 1e9 / elapsed.as_secs_f64();
    println!(
        "Rust blake3: {gb_per_s:.2} GB/s ({} MiB buffer, {:.1} s measured)",
        BUFFER_SIZE / 1024 / 1024,
        elapsed.as_secs_f64()
    );
}
