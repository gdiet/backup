//! Benchmarks `LongTermStore::write`/`read` throughput at the block sizes
//! `cli` actually uses, against a same-I/O-pattern baseline that keeps a
//! single file handle open across every call instead of re-opening it -
//! not run as part of `cargo test`/CI, same spirit as `cdc/examples/
//! bench.rs`. Run with:
//!
//! ```text
//! cargo run --release -p store --example bench
//! ```
//!
//! Exists to answer a concrete question: `LongTermStore` re-opens the
//! underlying OS file handle on every `write`/`read` call (see its own
//! doc comment/implementation - no handle is cached across calls, and a
//! call spanning a 100 MB physical file boundary re-opens again for the
//! next file). Does that cost anything measurable at the granularities
//! the rest of the codebase actually calls it with - 64 KiB
//! (`cli::store::READ_BUFFER_SIZE`) and 256 KiB
//! (`cli::chunk_store::DRAIN_PIECE_SIZE`/`cli::mount::PERSIST_CHUNK_SIZE`)?
//! The first pair of numbers (`store`) measures `LongTermStore` as
//! shipped; the second pair (`open handle`) repeats the exact same access
//! pattern (same block size, same positions, same single-file target)
//! against one `File` opened once and kept open for the whole loop - the
//! delta between the two isolates the re-open cost specifically, rather
//! than conflating it with whatever the underlying disk/filesystem costs
//! regardless of open/close behavior.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use store::LongTermStore;

const TEST_DURATION: Duration = Duration::from_secs(5);
const BLOCK_SIZES: [(&str, usize); 2] = [("64 KiB", 64 * 1024), ("256 KiB", 256 * 1024)];
// Spans several physical 100 MB LTS files, so `LongTermStore`'s read
// benchmark also exercises the file-boundary re-open path, not just
// repeated opens of one warm file. The open-handle baseline reads from a
// single plain file of the same size instead - its own re-open behavior
// (none, once at the start) isn't affected by how many LTS files that
// range would have spanned.
const PREFILL_SIZE: u64 = 300 * 1024 * 1024;

fn gb_per_s(total_bytes: u64, elapsed: Duration) -> f64 {
    total_bytes as f64 / 1e9 / elapsed.as_secs_f64()
}

// --- LongTermStore (as shipped: re-opens the file on every call) ---

fn bench_store_write(store: &LongTermStore, block_size: usize, data: &[u8]) -> f64 {
    let start = Instant::now();
    let mut pos = 0u64;
    let mut total = 0u64;
    while start.elapsed() < TEST_DURATION {
        store.write(pos, data).expect("write failed");
        pos += block_size as u64;
        total += block_size as u64;
    }
    gb_per_s(total, start.elapsed())
}

fn bench_store_read(store: &LongTermStore, block_size: usize) -> f64 {
    let mut buf = vec![0u8; block_size];
    let start = Instant::now();
    let mut pos = 0u64;
    let mut total = 0u64;
    while start.elapsed() < TEST_DURATION {
        store.read(pos, &mut buf).expect("read failed");
        pos = (pos + block_size as u64) % PREFILL_SIZE;
        total += block_size as u64;
    }
    gb_per_s(total, start.elapsed())
}

// --- Open-handle baseline: same access pattern, one handle for the whole loop ---

fn bench_open_handle_write(path: &Path, block_size: usize, data: &[u8]) -> f64 {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .expect("open failed");
    let start = Instant::now();
    let mut pos = 0u64;
    let mut total = 0u64;
    while start.elapsed() < TEST_DURATION {
        file.seek(SeekFrom::Start(pos)).expect("seek failed");
        file.write_all(data).expect("write failed");
        pos += block_size as u64;
        total += block_size as u64;
    }
    gb_per_s(total, start.elapsed())
}

fn bench_open_handle_read(path: &Path, block_size: usize) -> f64 {
    let mut file = File::open(path).expect("open failed");
    let mut buf = vec![0u8; block_size];
    let start = Instant::now();
    let mut pos = 0u64;
    let mut total = 0u64;
    while start.elapsed() < TEST_DURATION {
        file.seek(SeekFrom::Start(pos)).expect("seek failed");
        file.read_exact(&mut buf).expect("read failed");
        pos = (pos + block_size as u64) % PREFILL_SIZE;
        total += block_size as u64;
    }
    gb_per_s(total, start.elapsed())
}

fn main() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    println!("data dir: {}", temp_dir.path().display());

    println!();
    println!("-- write --");
    println!("{:>8}  {:>10}  {:>12}", "block", "store", "open handle");
    for &(label, block_size) in &BLOCK_SIZES {
        let store = LongTermStore::new(temp_dir.path().join(format!("write-{block_size}")), false);
        let data = vec![0xABu8; block_size];
        let store_gb_s = bench_store_write(&store, block_size, &data);

        let open_handle_path = temp_dir.path().join(format!("write-open-{block_size}"));
        let open_handle_gb_s = bench_open_handle_write(&open_handle_path, block_size, &data);

        println!("{label:>8}  {store_gb_s:7.2} GB/s  {open_handle_gb_s:9.2} GB/s");
    }

    println!();
    println!(
        "-- read (after a {} MB prefill) --",
        PREFILL_SIZE / 1024 / 1024
    );
    println!("{:>8}  {:>10}  {:>12}", "block", "store", "open handle");
    for &(label, block_size) in &BLOCK_SIZES {
        let store = LongTermStore::new(temp_dir.path().join(format!("read-{block_size}")), false);
        let chunk = vec![0xCDu8; block_size];
        let mut pos = 0u64;
        while pos < PREFILL_SIZE {
            store.write(pos, &chunk).expect("prefill write failed");
            pos += block_size as u64;
        }
        let store_gb_s = bench_store_read(&store, block_size);

        let open_handle_path = temp_dir.path().join(format!("read-open-{block_size}"));
        {
            let mut file = File::create(&open_handle_path).expect("prefill create failed");
            let mut pos = 0u64;
            while pos < PREFILL_SIZE {
                file.write_all(&chunk).expect("prefill write failed");
                pos += block_size as u64;
            }
        }
        let open_handle_gb_s = bench_open_handle_read(&open_handle_path, block_size);

        println!("{label:>8}  {store_gb_s:7.2} GB/s  {open_handle_gb_s:9.2} GB/s");
    }
}
