//! Regression test for a bug that only reproduces via a real, separately
//! spawned `backup` process - not as an in-process unit test the way most
//! of this crate's tests work (see `cli/src/store.rs`'s own
//! `assert_store_leaves_no_pending_wal_behind` for the in-process attempt,
//! kept as a cheap functional sanity check but confirmed *not* to catch
//! this, even though it exercises the exact same code: 0 failures across
//! dozens of in-process tries, debug and release, with the underlying fix
//! reverted locally - only a real subprocess reliably showed it). An
//! integration test (not a unit test) specifically so `CARGO_BIN_EXE_backup`
//! is available - Cargo only sets that for integration tests/benches.
//!
//! Background: `store` uses several SQLite connections with independent
//! lifetimes - `main_conn` (the write connection) in its own writer
//! thread, one read connection per chunking worker thread (see
//! `READ_CONNECTION` in `cli/src/store.rs`) - unlike every single-connection
//! command, which gets SQLite's auto-checkpoint-on-clean-close "for free".
//! If the write connection ends up closing before every worker's read
//! connection does, that checkpoint doesn't happen, leaving a non-empty
//! `-wal` behind despite `store` itself having exited perfectly cleanly -
//! surfacing as a read-only command immediately afterwards refusing with
//! `Error::UncheckpointedWal`.
//!
//! **The two variants below are not equally severe, confirmed by reverting
//! the fix locally and re-running each many times**: without `--concurrency`,
//! `store` used to run chunking on rayon's *global* thread pool directly,
//! whose worker threads (and their read connections) are never torn down
//! within the process's own lifetime at all - a deterministic bug, 3/3
//! failures reverted, every single real run. With `--concurrency`, a
//! *scoped* pool was already being built and torn down before this
//! function returned, making it "only" an unsynchronized race between that
//! teardown and the writer thread noticing its channel disconnect - never
//! actually observed losing that race in 9+ reverted tries, but fixed the
//! same way regardless rather than relying on it reliably winning.
//!
//! **A first fix attempt (unifying both branches onto an always-scoped
//! `ThreadPool`, relying on that pool's own `Drop` to join its worker
//! threads before the channel disconnects) turned out to still be
//! incomplete**, caught only by running this very test repeatedly under
//! heavy concurrent system load (many parallel `cargo test --workspace`
//! processes at once): `ThreadPool::drop` calls `Registry::terminate`,
//! which only *signals* worker threads to stop - it does not join them, so
//! a worker's `READ_CONNECTION` can still be alive (and its file lock/WAL
//! participation with it) well after `ThreadPool::drop` has already
//! returned. The actual fix adds an explicit `pool.broadcast(..)` call
//! (confirmed genuinely synchronous, unlike `drop`) that runs on every
//! worker thread and clears its `READ_CONNECTION` before the surrounding
//! block ends - see `cli/src/store.rs`'s `run_store` for the code and full
//! comment. See `docs/plans/implemented/read-only-repository-access.md`'s
//! addendum on `store` for the full investigation (including why the
//! in-process test can't catch either variant reliably: real OS
//! process/thread scheduling behaves differently than `cargo test`'s own
//! long-lived, already-multi-threaded process).

use std::path::PathBuf;
use std::process::Command;

fn init_repo() -> (tempfile::TempDir, PathBuf) {
    let temp_dir = tempfile::tempdir().unwrap();
    let repo_root = temp_dir.path().join("repo");
    db::init_repository(
        &repo_root,
        &db::RepositorySettings::new(12, db::Chunking::Cdc).unwrap(),
    )
    .unwrap();
    (temp_dir, repo_root)
}

/// Runs a real `backup store` as a child process (not in-process), then
/// asserts a read-only command can immediately open the repository
/// afterwards without hitting `Error::UncheckpointedWal` - the actual,
/// real-world claim this whole fix is about.
fn assert_real_store_process_leaves_a_cleanly_readable_repository(concurrency: Option<&str>) {
    let (_temp_dir, repo_root) = init_repo();
    let source_dir = tempfile::tempdir().unwrap();
    std::fs::write(source_dir.path().join("a.txt"), b"hello world").unwrap();

    let backup = env!("CARGO_BIN_EXE_backup");
    let mut cmd = Command::new(backup);
    cmd.arg("--repo").arg(&repo_root).arg("store");
    if let Some(concurrency) = concurrency {
        cmd.arg("--concurrency").arg(concurrency);
    }
    cmd.arg(source_dir.path()).arg("target");

    let status = cmd.status().expect("failed to spawn backup store");
    assert!(status.success(), "backup store exited with {status}");

    let result = db::open_repository_read_only(&repo_root);
    assert!(
        result.is_ok(),
        "a read-only command right after a real, separately-run backup store process exited \
         cleanly must not see a pending WAL: {result:?}"
    );
}

#[test]
fn real_store_process_with_explicit_concurrency_leaves_no_pending_wal_behind() {
    assert_real_store_process_leaves_a_cleanly_readable_repository(Some("2"));
}

#[test]
fn real_store_process_with_default_concurrency_leaves_no_pending_wal_behind() {
    assert_real_store_process_leaves_a_cleanly_readable_repository(None);
}
