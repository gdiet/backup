//! Integration test (not a unit test in `mountfs/src/windows/mod.rs`),
//! specifically so `CARGO_BIN_EXE_windows_mount_spike_helper` is available:
//! Cargo only sets that for integration tests/benches, not a lib crate's
//! own unit tests.

#![cfg(target_os = "windows")]

use std::time::{Duration, Instant};

/// End-to-end: mounts a small filesystem (nested directory, real content,
/// a rejected write) via real WinFSP, in a *separate child process*
/// (`windows_mount_spike_helper`, in this crate's `src/bin/`), and reads
/// it back through ordinary `std::fs` calls from this process - exercising
/// every `MountFilesystem` method the way a real FUSE client would,
/// mirroring `mountfs::linux`'s equivalent test.
///
/// A child process rather than an in-process thread (like the Linux
/// version) specifically so it can be torn down with `Child::kill`
/// (`TerminateProcess`) instead of `fuse_exit` - see
/// `mountfs/src/windows/mod.rs`'s doc comment for why `fuse_exit` isn't
/// usable yet (crashes deterministically inside `winfsp-x64.dll`).
/// `TerminateProcess` is also exactly how a real user stops `backup mount`
/// on Windows today (closing the console / Ctrl+C both ultimately kill
/// the process), so this isn't just a test workaround, it's the actual
/// intended shutdown story for now.
#[test]
fn mounts_and_serves_the_full_read_only_op_set_via_real_winfsp() {
    let helper = env!("CARGO_BIN_EXE_windows_mount_spike_helper");
    let parent_dir = tempfile::tempdir().unwrap();
    let mount_path = parent_dir.path().join("mnt");

    let mut child = std::process::Command::new(helper)
        .arg(&mount_path)
        .spawn()
        .expect("failed to spawn windows_mount_spike_helper");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut names: Vec<String>;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("mount helper exited early with {status}");
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
        std::fs::metadata(mount_path.join("top.txt")).unwrap().len(),
        "top level content".len() as u64
    );
    assert!(std::fs::metadata(mount_path.join("sub")).unwrap().is_dir());
    assert_eq!(
        std::fs::read(mount_path.join("sub").join("nested.txt")).unwrap(),
        b"hello from a subdirectory"
    );

    // A write attempt must be rejected - this is a read-only mount.
    assert!(std::fs::write(mount_path.join("top.txt"), b"nope").is_err());

    child.kill().expect("failed to kill the mount helper");
    child.wait().expect("failed to wait for the mount helper");
}
