//! Measures the two optimizations DESIGN-STORE-004/005 (`docs/design/byte-store.md`) claim:
//! the read handle cache, and lazy (try-first) directory creation on write. Each compares this
//! crate's current behavior against a baseline reproducing what it did before that optimization
//! (a fresh `File::open` per read; an unconditional `create_dir_all` per write).
//!
//! Run against a directory on real target-like storage (an external/USB drive, a network mount)
//! by passing it as the first argument - otherwise runs against a fresh directory under the
//! system temp dir, which only shows the syscall-count effect, not what it costs on slower
//! storage.
//!
//! The round count (2000 read-cache rounds x 8 files, 20000 lazy-directory rounds) is tuned for
//! a fast local disk, where each round costs microseconds. On storage with real per-call latency
//! (a network mount), that round count can turn a few seconds into tens of minutes for no added
//! precision - pass a second argument to scale it down (applied directly to the read-cache
//! rounds; the lazy-directory rounds scale with it, keeping their default 10x ratio).
//!
//! ```bash
//! cargo run --release -p store --example store_bench
//! cargo run --release -p store --example store_bench -- /path/on/slow/storage
//! cargo run --release -p store --example store_bench -- /path/on/slow/storage 200
//! ```

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use store::ByteStore;

const FILE_SIZE: u64 = 100_000_000;

fn main() {
    let mut args = env::args().skip(1);
    let base = args.next().map(PathBuf::from).unwrap_or_else(|| {
        env::temp_dir().join(format!("dedupfs-store-bench-{}", std::process::id()))
    });
    let read_rounds: u64 = args
        .next()
        .map(|s| {
            s.parse()
                .expect("rounds argument must be a positive integer")
        })
        .unwrap_or(2000);
    fs::create_dir_all(&base).expect("create bench base directory");
    println!(
        "Benchmarking against {} ({read_rounds} read-cache rounds)",
        base.display()
    );

    bench_read_handle_cache(&base.join("read"), read_rounds);
    bench_lazy_directory_creation(&base.join("write"), read_rounds * 10);

    let _ = fs::remove_dir_all(&base);
}

fn report(name: &str, baseline: Duration, optimized: Duration) {
    println!(
        "{name}: baseline {baseline:?}, optimized {optimized:?} ({:.2}x)",
        baseline.as_secs_f64() / optimized.as_secs_f64()
    );
}

/// A file's relative path under `dir1/dir2/file_start` - duplicated from `ByteStore`'s own
/// private formula deliberately: the baseline here must open files exactly the way `ByteStore`
/// itself does, without going through the cache it exists to measure against.
fn path_for(dir: &Path, file_index: u64) -> PathBuf {
    let dir1 = file_index / 100 / 100;
    let dir2 = (file_index / 100) % 100;
    dir.join(format!("{dir1:02}"))
        .join(format!("{dir2:02}"))
        .join(format!("{:010}", file_index * FILE_SIZE))
}

fn bench_read_handle_cache(dir: &Path, rounds: u64) {
    fs::create_dir_all(dir).unwrap();
    let store = ByteStore::new(dir, false);
    // Within the cache's own capacity (store::READ_HANDLE_CACHE_CAPACITY, currently 8) - this
    // measures the hit case the cache exists for. A working set bigger than capacity is a
    // different, deliberately separate question (see the eviction-correctness test in
    // src/lib.rs; not benchmarked here).
    let file_count = 8u64;
    for i in 0..file_count {
        store.write(i * FILE_SIZE, &[i as u8]).unwrap();
    }

    let mut buf = [0u8; 1];

    let start = Instant::now();
    for _ in 0..rounds {
        for i in 0..file_count {
            let mut file = File::open(path_for(dir, i)).unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.read_exact(&mut buf).unwrap();
        }
    }
    let baseline = start.elapsed();

    let start = Instant::now();
    for _ in 0..rounds {
        for i in 0..file_count {
            store.read(i * FILE_SIZE, &mut buf).unwrap();
        }
    }
    let optimized = start.elapsed();

    report("read handle cache", baseline, optimized);
}

fn bench_lazy_directory_creation(dir: &Path, rounds: u64) {
    fs::create_dir_all(dir).unwrap();

    let baseline_dir = dir.join("baseline");
    let baseline_leaf = baseline_dir.join("00").join("00");
    fs::create_dir_all(&baseline_leaf).unwrap();
    let baseline_file = baseline_leaf.join("0000000000");
    let start = Instant::now();
    for _ in 0..rounds {
        fs::create_dir_all(&baseline_leaf).unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&baseline_file)
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"x").unwrap();
    }
    let baseline = start.elapsed();

    let lazy_dir = dir.join("lazy");
    fs::create_dir_all(&lazy_dir).unwrap();
    let store = ByteStore::new(&lazy_dir, false);
    store.write(0, b"x").unwrap(); // creates dir1/dir2 once, before timing starts
    let start = Instant::now();
    for _ in 0..rounds {
        store.write(0, b"x").unwrap();
    }
    let optimized = start.elapsed();

    report("lazy directory creation", baseline, optimized);
}
