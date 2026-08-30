//! Directory-creation throughput directly against [`db::Repository::mkdir`], with no mount or
//! CLI layer in between - the ceiling any higher layer's directory creation cannot exceed, since
//! it still goes through this same call underneath. Feeds a `location: db-direct` entry into
//! `performance/measurements/`; see `performance/scripts/README.md` and
//! `performance/methodology.md`'s "Statistical approach" for what the printed numbers mean and
//! how to turn them into a measurement protocol - this prints the same
//! run/count/elapsed/ops-per-second shape as the native-filesystem scripts there, on purpose.
//!
//! ```bash
//! cargo run --release -p db --example db_bench
//! cargo run --release -p db --example db_bench -- /path/on/slow/storage
//! cargo run --release -p db --example db_bench -- /path/on/slow/storage 5
//! ```
//! Second argument overrides the per-run window in seconds (default 20).

use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use db::{RepositorySettings, init_repository, open_repository};

fn main() {
    let mut args = env::args().skip(1);
    let base = args.next().map(PathBuf::from).unwrap_or_else(|| {
        env::temp_dir().join(format!("dedupfs-db-bench-{}", std::process::id()))
    });
    let window_secs: u64 = args
        .next()
        .map(|s| {
            s.parse()
                .expect("window argument must be a positive integer of seconds")
        })
        .unwrap_or(20);
    let window = Duration::from_secs(window_secs);

    fs::create_dir_all(&base).expect("create bench base directory");
    let repo_root = base.join("repo");
    init_repository(&repo_root, RepositorySettings::new(None, now_millis()))
        .expect("init bench repository");
    let repo = open_repository(&repo_root).expect("open bench repository");
    let root_id = repo
        .resolve_path("/")
        .expect("resolve_path must succeed")
        .expect("root entry must exist")
        .id;

    println!(
        "Tool: db_bench (crates/db/examples/db_bench.rs), against {} ({window_secs}s per run)",
        repo_root.display()
    );

    let mut counter: u64 = 0;
    for run in 1..=5 {
        let start = Instant::now();
        let mut ops: u64 = 0;
        while start.elapsed() < window {
            counter += 1;
            repo.mkdir(root_id, &format!("d{counter}"), now_millis())
                .expect("mkdir must succeed");
            ops += 1;
        }
        let elapsed = start.elapsed().as_secs_f64();
        println!(
            "{run}: {ops} dirs, {elapsed:.2}s, {:.1} ops/s",
            ops as f64 / elapsed
        );
    }

    let _ = fs::remove_dir_all(&base);
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_millis() as i64
}
