//! Windows counterpart to `cli/src/mount.rs`'s (Linux-only, see there for
//! why) in-process mount test: builds a real repository the same way, but
//! mounts it by running the actual `backup` binary as a child process and
//! killing it - matching how a user actually stops `backup mount` on
//! Windows today (Ctrl+C/closing the console also just kills the process).
//! See `docs/plans/cross-platform-mount-crate.md`'s "Windows checkpoint"
//! for why an in-process unmount isn't available yet.
//!
//! An integration test (not a unit test in `cli/src/mount.rs`) specifically
//! so `CARGO_BIN_EXE_backup` is available - Cargo only sets that for
//! integration tests/benches, not the crate's own unit tests.

#![cfg(target_os = "windows")]

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use store::LongTermStore;

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

/// Writes `bytes` to the store and records a matching chunk/content/file in
/// the metadata database - the same minimal stand-in for a real `store`
/// run other commands' tests use (mirrors `cli/src/mount.rs`'s own
/// `seed_file`, duplicated here since integration tests can't share a
/// binary crate's private test helpers).
fn seed_file(repo_root: &Path, parent_id: i64, name: &str, bytes: &[u8]) {
    let data_store = LongTermStore::new(repo_root.join("data"), false);
    let start: i64 = {
        let repository = db::open_repository(repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        conn.query_row(
            "SELECT COALESCE(MAX(stop), 0) FROM chunk_extents",
            (),
            |row| row.get(0),
        )
        .unwrap()
    };
    data_store.write(start as u64, bytes).unwrap();

    let mut hash = [0u8; 20];
    blake3::Hasher::new()
        .update(bytes)
        .finalize_xof()
        .fill(&mut hash);

    let repository = db::open_repository(repo_root).unwrap();
    let mut conn = repository.open_write_connection().unwrap();
    db::apply_backup_batch(
        &mut conn,
        &[db::FileBackupRecord {
            parent_id,
            name: name.to_string(),
            time_millis: 0,
            chunks: if bytes.is_empty() {
                vec![]
            } else {
                vec![db::ChunkRef::New {
                    length: bytes.len() as u64,
                    hash: hash.to_vec(),
                    extents: vec![(start as u64, start as u64 + bytes.len() as u64)],
                }]
            },
            content_hash: hash.to_vec(),
        }],
    )
    .unwrap();
}

#[test]
fn mounts_and_serves_a_real_repository_read_only_via_the_backup_binary() {
    let (_temp_dir, repo_root) = init_repo();
    let repository = db::open_repository(&repo_root).unwrap();
    let conn = repository.open_write_connection().unwrap();
    let sub_id = db::insert_directory(&conn, 0, "sub", 0).unwrap();
    drop(conn);
    seed_file(&repo_root, 0, "top.txt", b"top level content");
    seed_file(&repo_root, sub_id, "a.txt", b"hello fuse");

    // Not pre-created: WinFSP creates the mountpoint itself (see
    // cli/src/mount.rs's validate_mountpoint).
    let parent_dir = tempfile::tempdir().unwrap();
    let mount_path = parent_dir.path().join("mnt");

    let backup = env!("CARGO_BIN_EXE_backup");
    let mut child = std::process::Command::new(backup)
        .arg("--repo")
        .arg(&repo_root)
        .arg("mount")
        .arg(&mount_path)
        .spawn()
        .expect("failed to spawn backup mount");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut names: Vec<String>;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("backup mount exited early with {status}");
        }
        names = std::fs::read_dir(&mount_path)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        if !names.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "mount did not become ready within 10s \
             (requires WinFSP to be installed - investigate if this fails in CI)"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    names.sort();
    assert_eq!(names, vec!["sub".to_string(), "top.txt".to_string()]);

    assert_eq!(
        std::fs::read(mount_path.join("top.txt")).unwrap(),
        b"top level content"
    );
    assert_eq!(
        std::fs::read(mount_path.join("sub").join("a.txt")).unwrap(),
        b"hello fuse"
    );
    assert_eq!(
        std::fs::metadata(mount_path.join("top.txt")).unwrap().len(),
        17
    );
    assert!(std::fs::metadata(mount_path.join("sub")).unwrap().is_dir());

    // A write attempt must be rejected - this is a read-only mount.
    assert!(std::fs::write(mount_path.join("top.txt"), b"nope").is_err());

    child.kill().expect("failed to kill backup mount");
    child.wait().expect("failed to wait for backup mount");
}
