//! Windows counterpart to `cli/src/mount.rs`'s (Linux-only, see there for
//! why) in-process mount test: builds a real repository the same way, but
//! mounts it by running the actual `backup` binary as a child process and
//! killing it - matching how a user actually stops `backup mount` on
//! Windows today (Ctrl+C/closing the console also just kills the process).
//! See `docs/plans/implemented/05-cross-platform-mount-crate.md`'s "Windows checkpoint"
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
            content: db::ContentSource::Resolved {
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
            },
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

/// Windows counterpart to `cli/src/mount.rs`'s
/// `mounts_read_write_and_supports_structural_changes` (Linux-only, same
/// reason as the read-only test above) - exercises phase 2a's structural
/// ops (mkdir/create/utimens/rename/rmdir/unlink, see
/// `docs/plans/implemented/06-fuse-mount-readwrite.md`) through a real `backup mount
/// --read-write` child process and real WinFSP.
#[test]
fn mounts_read_write_and_supports_structural_changes_via_the_backup_binary() {
    let (_temp_dir, repo_root) = init_repo();
    // Same readiness-probe rationale as the read-only test's marker
    // directory: `read_dir` on the mountpoint only ever reports real
    // content once WinFSP is actually serving it.
    {
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        db::insert_directory(&conn, 0, "marker", 0).unwrap();
    }

    let parent_dir = tempfile::tempdir().unwrap();
    let mount_path = parent_dir.path().join("mnt");

    let backup = env!("CARGO_BIN_EXE_backup");
    let mut child = std::process::Command::new(backup)
        .arg("--repo")
        .arg(&repo_root)
        .arg("mount")
        .arg("--read-write")
        .arg(&mount_path)
        .spawn()
        .expect("failed to spawn backup mount --read-write");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("backup mount exited early with {status}");
        }
        let names: Vec<String> = std::fs::read_dir(&mount_path)
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

    // mkdir + create (empty file).
    std::fs::create_dir(mount_path.join("sub")).unwrap();
    assert!(std::fs::metadata(mount_path.join("sub")).unwrap().is_dir());
    std::fs::write(mount_path.join("sub").join("empty.txt"), b"").unwrap();
    assert_eq!(
        std::fs::metadata(mount_path.join("sub").join("empty.txt"))
            .unwrap()
            .len(),
        0
    );

    // mkdir on an existing name fails.
    assert!(std::fs::create_dir(mount_path.join("sub")).is_err());

    // utimens. `File::open` (read-only) isn't enough on Windows: unlike
    // POSIX `futimens` (permission-checked by ownership, not by how the fd
    // was opened - the Linux counterpart of this test uses plain
    // `File::open` for exactly that reason), `SetFileTime` requires the
    // handle to carry `FILE_WRITE_ATTRIBUTES`, which only a write-capable
    // open grants.
    let new_mtime = std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    std::fs::File::options()
        .write(true)
        .open(mount_path.join("sub").join("empty.txt"))
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(new_mtime))
        .unwrap();
    assert_eq!(
        std::fs::metadata(mount_path.join("sub").join("empty.txt"))
            .unwrap()
            .modified()
            .unwrap(),
        new_mtime
    );

    // rename.
    std::fs::rename(
        mount_path.join("sub").join("empty.txt"),
        mount_path.join("sub").join("renamed.txt"),
    )
    .unwrap();
    assert!(std::fs::metadata(mount_path.join("sub").join("empty.txt")).is_err());
    assert!(
        std::fs::metadata(mount_path.join("sub").join("renamed.txt"))
            .unwrap()
            .is_file()
    );

    // rmdir on a non-empty directory fails; unlink then rmdir succeeds.
    assert!(std::fs::remove_dir(mount_path.join("sub")).is_err());
    std::fs::remove_file(mount_path.join("sub").join("renamed.txt")).unwrap();
    std::fs::remove_dir(mount_path.join("sub")).unwrap();
    assert!(std::fs::metadata(mount_path.join("sub")).is_err());

    child.kill().expect("failed to kill backup mount");
    child.wait().expect("failed to wait for backup mount");
}

/// Windows counterpart to `cli/src/mount.rs`'s
/// `mounts_read_write_and_supports_content_writes` (Linux-only, same
/// reason as the other tests above) - exercises phase 2b's real
/// byte-level `write`/`read`/`truncate` (`docs/plans/fuse-mount-
/// readwrite.md`) through a real `backup mount --read-write` child
/// process and real WinFSP, plus confirms a genuinely read-only mount
/// (no `--read-write`) still rejects a content write - the regression
/// this test was added to catch: WinFSP's `-oro`/`ReadOnlyVolume` does
/// *not* block a write-intent `CreateFileW`+`WriteFile` at the driver
/// level the way Linux's `MS_RDONLY` blocks `write(2)`, so `DedupFs`
/// itself has to reject it explicitly (see `DedupFs::read_only`).
#[test]
fn mounts_read_write_and_supports_content_writes_via_the_backup_binary() {
    let (_temp_dir, repo_root) = init_repo();
    {
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        db::insert_directory(&conn, 0, "marker", 0).unwrap();
    }

    let parent_dir = tempfile::tempdir().unwrap();
    let mount_path = parent_dir.path().join("mnt");

    let backup = env!("CARGO_BIN_EXE_backup");
    let mut child = std::process::Command::new(backup)
        .arg("--repo")
        .arg(&repo_root)
        .arg("mount")
        .arg("--read-write")
        .arg(&mount_path)
        .spawn()
        .expect("failed to spawn backup mount --read-write");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("backup mount exited early with {status}");
        }
        let names: Vec<String> = std::fs::read_dir(&mount_path)
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

    // Write content to a new file, read it back.
    std::fs::write(mount_path.join("a.txt"), b"hello write cache").unwrap();
    assert_eq!(
        std::fs::read(mount_path.join("a.txt")).unwrap(),
        b"hello write cache"
    );

    // Overwrite in the middle via an explicit seek+write.
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(mount_path.join("a.txt"))
            .unwrap();
        f.seek(SeekFrom::Start(6)).unwrap();
        f.write_all(b"WRITE").unwrap();
    }
    assert_eq!(
        std::fs::read(mount_path.join("a.txt")).unwrap(),
        b"hello WRITE cache"
    );

    // set_len grow zero-pads; set_len shrink drops the tail.
    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(mount_path.join("a.txt"))
            .unwrap();
        f.set_len(20).unwrap();
    }
    let grown = std::fs::read(mount_path.join("a.txt")).unwrap();
    assert_eq!(grown.len(), 20);
    assert_eq!(&grown[..17], b"hello WRITE cache");
    assert_eq!(&grown[17..], &[0u8, 0, 0]);

    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(mount_path.join("a.txt"))
            .unwrap();
        f.set_len(5).unwrap();
    }
    assert_eq!(std::fs::read(mount_path.join("a.txt")).unwrap(), b"hello");

    child.kill().expect("failed to kill backup mount");
    child.wait().expect("failed to wait for backup mount");

    // Verify persisted state directly against the DB, after the mount
    // process is gone (so its own write connection is guaranteed closed).
    let repository = db::open_repository(&repo_root).unwrap();
    let conn = repository.open_read_connection().unwrap();
    let a = db::resolve_path(&conn, "a.txt").unwrap().unwrap();
    assert_eq!(db::file_size(&conn, &a).unwrap(), 5);
}

/// `--temp` (see `cli/src/mount.rs`'s `MountArgs::temp`) redirects
/// `--read-write`'s write-cache spillover directory, created
/// unconditionally at mount startup (`build_filesystem`) - this confirms
/// it actually lands under the given directory rather than the OS default
/// (`std::env::temp_dir()`), and that a real write-cache spill *file* ends
/// up there too once `--write-cache-mb 0` forces spillover on the very
/// first written byte.
#[test]
fn mount_read_write_uses_a_custom_temp_dir_for_write_cache_spillover_via_the_backup_binary() {
    let (_temp_dir, repo_root) = init_repo();
    {
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        db::insert_directory(&conn, 0, "marker", 0).unwrap();
    }

    let parent_dir = tempfile::tempdir().unwrap();
    let mount_path = parent_dir.path().join("mnt");
    let custom_temp = tempfile::tempdir().unwrap();

    let backup = env!("CARGO_BIN_EXE_backup");
    let mut child = std::process::Command::new(backup)
        .arg("--repo")
        .arg(&repo_root)
        .arg("mount")
        .arg("--read-write")
        .arg("--write-cache-mb")
        .arg("0")
        .arg("--temp")
        .arg(custom_temp.path())
        .arg(&mount_path)
        .spawn()
        .expect("failed to spawn backup mount --read-write --temp");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("backup mount exited early with {status}");
        }
        let names: Vec<String> = std::fs::read_dir(&mount_path)
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

    // The write-cache spill directory is created at mount startup,
    // unconditionally - it must already be under the custom --temp dir by
    // the time the mount is ready to serve requests, never under the OS
    // default temp dir.
    let spill_dirs: Vec<PathBuf> = std::fs::read_dir(custom_temp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().starts_with("backup-mount-write-cache-"))
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(
        spill_dirs.len(),
        1,
        "expected exactly one backup-mount-write-cache-* dir under the custom --temp dir, found {spill_dirs:?}"
    );

    // Force an actual spilled write-cache file: `--write-cache-mb 0` means
    // even the first written byte exceeds the RAM budget. The handle is
    // kept open (not `std::fs::write`, which would close it immediately)
    // so the write cache - and its spill file - stays alive for the
    // check below rather than racing the background persist that runs
    // once the file closes.
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(mount_path.join("a.txt"))
        .expect("failed to open a.txt for writing");
    {
        use std::io::Write;
        f.write_all(b"hello write cache")
            .expect("failed to write to a.txt");
        f.flush().expect("failed to flush a.txt");
    }
    let spill_files: Vec<PathBuf> = std::fs::read_dir(&spill_dirs[0])
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();
    assert!(
        !spill_files.is_empty(),
        "expected at least one spilled write-cache file under the custom --temp dir"
    );
    drop(f);

    child.kill().expect("failed to kill backup mount");
    child.wait().expect("failed to wait for backup mount");
}

/// `--temp` (see `cli/src/mount.rs`'s `MountArgs::temp`) is validated up
/// front, before the mountpoint is ever touched - WinFSP creates the
/// mountpoint itself as part of mounting (see `validate_mountpoint`), so
/// confirming it was never created is a direct check that this failed
/// before reaching that point.
#[test]
fn mount_fails_fast_for_a_nonexistent_temp_dir_without_creating_the_mountpoint() {
    let (_temp_dir, repo_root) = init_repo();
    seed_file(&repo_root, 0, "top.txt", b"top level content");

    let parent_dir = tempfile::tempdir().unwrap();
    let mount_path = parent_dir.path().join("mnt");
    let missing_temp = parent_dir.path().join("does-not-exist-temp");

    let backup = env!("CARGO_BIN_EXE_backup");
    let output = std::process::Command::new(backup)
        .arg("--repo")
        .arg(&repo_root)
        .arg("mount")
        .arg("--temp")
        .arg(&missing_temp)
        .arg(&mount_path)
        .output()
        .expect("failed to run backup mount");

    assert!(
        !output.status.success(),
        "expected backup mount to fail fast for a nonexistent --temp dir, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !mount_path.exists(),
        "mountpoint must never have been created - WinFSP creates it itself, only reached \
         after --temp validation"
    );
}

/// A content write on a genuinely read-only mount (no `--read-write`) must
/// be rejected - see this test module's doc comment on
/// `mounts_read_write_and_supports_content_writes_via_the_backup_binary`
/// for the Windows-specific regression this guards against.
#[test]
fn a_read_only_mount_rejects_content_writes_via_the_backup_binary() {
    let (_temp_dir, repo_root) = init_repo();
    seed_file(&repo_root, 0, "top.txt", b"top level content");

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
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("backup mount exited early with {status}");
        }
        let names: Vec<String> = std::fs::read_dir(&mount_path)
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
            "mount did not become ready within 10s"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    assert!(std::fs::write(mount_path.join("top.txt"), b"nope").is_err());

    child.kill().expect("failed to kill backup mount");
    child.wait().expect("failed to wait for backup mount");
}

/// Deletes the single data file backing a chunk written at store position 0
/// (the same path convention `problems`' own tests use) - simulating
/// missing/short store data without needing a truncated real repository.
fn break_data_at_position_zero(repo_root: &Path) {
    std::fs::remove_file(
        repo_root
            .join("data")
            .join("00")
            .join("00")
            .join("0000000000"),
    )
    .unwrap();
}

fn spawn_and_wait_for_mount(
    backup: &str,
    repo_root: &Path,
    mount_path: &Path,
    extra_args: &[&str],
) -> std::process::Child {
    let mut command = std::process::Command::new(backup);
    command.arg("--repo").arg(repo_root).arg("mount");
    for arg in extra_args {
        command.arg(arg);
    }
    let mut child = command
        .arg(mount_path)
        .spawn()
        .expect("failed to spawn backup mount");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("backup mount exited early with {status}");
        }
        let ready = std::fs::read_dir(mount_path)
            .map(|mut entries| entries.next().is_some())
            .unwrap_or(false);
        if ready {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "mount did not become ready within 10s"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    child
}

/// Default behavior (no `--zero-fill-missing`): a read overlapping missing
/// store data fails with an I/O error, exactly as `docs/plans/implemented/
/// mount-zero-fill-missing.md` intends - data fidelity by default.
#[test]
fn a_missing_chunk_returns_an_io_error_by_default() {
    let (_temp_dir, repo_root) = init_repo();
    seed_file(&repo_root, 0, "a.txt", b"hello world");
    break_data_at_position_zero(&repo_root);

    let parent_dir = tempfile::tempdir().unwrap();
    let mount_path = parent_dir.path().join("mnt");
    let backup = env!("CARGO_BIN_EXE_backup");
    let mut child = spawn_and_wait_for_mount(backup, &repo_root, &mount_path, &[]);

    assert!(
        std::fs::read(mount_path.join("a.txt")).is_err(),
        "a read overlapping missing store data must fail by default"
    );

    child.kill().expect("failed to kill backup mount");
    child.wait().expect("failed to wait for backup mount");
}

/// With `--zero-fill-missing`, the same read instead succeeds, returning
/// the already-zero-filled bytes `store::read` produces for an unreadable
/// range (see that function's own doc comment) - `read_persisted` just
/// stops discarding them.
#[test]
fn zero_fill_missing_serves_zero_bytes_instead_of_failing() {
    let (_temp_dir, repo_root) = init_repo();
    let content = b"hello world";
    seed_file(&repo_root, 0, "a.txt", content);
    break_data_at_position_zero(&repo_root);

    let parent_dir = tempfile::tempdir().unwrap();
    let mount_path = parent_dir.path().join("mnt");
    let backup = env!("CARGO_BIN_EXE_backup");
    let mut child =
        spawn_and_wait_for_mount(backup, &repo_root, &mount_path, &["--zero-fill-missing"]);

    let bytes = std::fs::read(mount_path.join("a.txt"))
        .expect("a read overlapping missing store data must succeed with --zero-fill-missing");
    assert_eq!(bytes, vec![0u8; content.len()]);

    child.kill().expect("failed to kill backup mount");
    child.wait().expect("failed to wait for backup mount");
}
