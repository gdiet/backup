//! Integration test (not a unit test in `crates/mountfs/src/windows/mod.rs`),
//! specifically so `CARGO_BIN_EXE_rename_noreplace_probe` is available:
//! Cargo only sets that for integration tests/benches, not a lib crate's
//! own unit tests.
//!
//! Empirically confirms what bit convention WinFSP's `cygfuse`
//! compatibility layer actually uses for its `rename` callback's `flags`
//! parameter, closing `agent-todos/done/verify-rename-noreplace-against-
//! real-winfsp.md` - see `RENAME_NOREPLACE`'s doc comment in `src/lib.rs`
//! for the confirmed result.

#![cfg(target_os = "windows")]

use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use std::process::Child;
use std::time::{Duration, Instant};

const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;

#[link(name = "kernel32")]
unsafe extern "system" {
    fn MoveFileExW(existing: *const u16, new: *const u16, flags: u32) -> i32;
}

fn wide(path: &Path) -> Vec<u16> {
    path.as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

/// Raw `MoveFileExW`, not `std::fs::rename` - the latter always passes
/// `MOVEFILE_REPLACE_EXISTING` internally (matching POSIX `rename(2)`
/// semantics), so it cannot exercise the no-replace case this test needs.
fn move_file(from: &Path, to: &Path, flags: u32) -> std::io::Result<()> {
    let from_w = wide(from);
    let to_w = wide(to);
    let ok = unsafe { MoveFileExW(from_w.as_ptr(), to_w.as_ptr(), flags) };
    if ok != 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn wait_for_mount_ready(child: &mut Child, mount_path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !mount_path.join("target.txt").exists() {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("mount helper exited early with {status}");
        }
        assert!(
            Instant::now() < deadline,
            "mount did not become ready within 10s \
             (requires WinFSP to be installed - investigate if this fails in CI)"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Confirmed against real WinFSP on 2026-08-19 (see the agent-todo this
/// closes): WinFSP's own layer enforces "reject if the destination
/// already exists" itself, before ever calling into this crate's `rename`
/// dispatch - a no-replace move against an existing target fails at the
/// OS level (`ERROR_ALREADY_EXISTS`) with zero calls reaching
/// `MountFilesystem::rename`, and every call that *does* reach it (a
/// replacing move, or a no-replace move with no collision) is passed
/// `no_replace = false` regardless of which Win32-level flag the caller
/// actually used. `RENAME_NOREPLACE`'s bit is never set by WinFSP's side;
/// see `RENAME_NOREPLACE`'s doc comment for what this means for an
/// implementation.
#[test]
fn winfsp_never_reports_no_replace_and_enforces_collisions_itself() {
    let helper = env!("CARGO_BIN_EXE_rename_noreplace_probe");
    let parent_dir = tempfile::tempdir().unwrap();
    let mount_path = parent_dir.path().join("mnt");
    let log_path = parent_dir.path().join("rename_log.txt");

    let mut child = std::process::Command::new(helper)
        .arg(&mount_path)
        .arg(&log_path)
        .spawn()
        .expect("failed to spawn rename_noreplace_probe");

    wait_for_mount_ready(&mut child, &mount_path);

    let target = mount_path.join("target.txt");
    let source_a = mount_path.join("source_a.txt");
    let source_b = mount_path.join("source_b.txt");
    let new_name = mount_path.join("newname.txt");

    // No-replace against an existing target: WinFSP rejects this itself,
    // never even asking the filesystem.
    let no_replace_collision = move_file(&source_a, &target, 0);
    assert!(
        no_replace_collision.is_err(),
        "a no-replace move against an existing target must fail"
    );

    // Replace against an existing target: reaches `rename`, no_replace=false.
    move_file(&source_b, &target, MOVEFILE_REPLACE_EXISTING)
        .expect("a replacing move against an existing target should succeed");

    // No-replace against a target that does not exist yet: succeeds, and
    // still reaches `rename` with no_replace=false, not true.
    move_file(&target, &new_name, 0)
        .expect("a no-replace move against a nonexistent target should succeed");

    child.kill().expect("failed to kill the mount helper");
    child.wait().expect("failed to wait for the mount helper");

    let log = std::fs::read_to_string(&log_path).unwrap();
    let lines: Vec<&str> = log.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "expected exactly two calls to reach MountFilesystem::rename \
         (the rejected no-replace-collision attempt should never reach it), got:\n{log}"
    );
    assert!(
        lines.iter().all(|line| line.contains("no_replace=false")),
        "WinFSP must never report no_replace=true to this crate's dispatch layer, got:\n{log}"
    );
}
