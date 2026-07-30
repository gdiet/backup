//! Integration test (not a unit test in `mountfs/src/windows/mod.rs`),
//! specifically so `CARGO_BIN_EXE_windows_mount_spike_helper` is available:
//! Cargo only sets that for integration tests/benches, not a lib crate's
//! own unit tests.

#![cfg(target_os = "windows")]

use std::os::windows::process::CommandExt;
use std::process::Child;
use std::time::{Duration, Instant};

const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
const CTRL_C_EVENT: u32 = 0;

#[link(name = "kernel32")]
unsafe extern "system" {
    fn GenerateConsoleCtrlEvent(dw_ctrl_event: u32, dw_process_group_id: u32) -> i32;
    fn AllocConsole() -> i32;
}

/// Waits until `mount_path` reports at least one entry (this crate's
/// spike filesystems always have `top.txt` at the root) - the mountpoint
/// exists and reads as empty before the mount is actually live, so
/// "`read_dir` succeeds" alone isn't a valid readiness signal.
fn wait_for_mount_ready(child: &mut Child, mount_path: &std::path::Path) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("mount helper exited early with {status}");
        }
        let names: Vec<String> = std::fs::read_dir(mount_path)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        if !names.is_empty() {
            return names;
        }
        assert!(
            Instant::now() < deadline,
            "mount did not become ready within 10s \
             (requires WinFSP to be installed - investigate if this fails in CI)"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// End-to-end: mounts a small filesystem (nested directory, real content,
/// a rejected write) via real WinFSP, in a *separate child process*
/// (`windows_mount_spike_helper`, in this crate's `src/bin/`), and reads
/// it back through ordinary `std::fs` calls from this process - exercising
/// every `MountFilesystem` method the way a real FUSE client would,
/// mirroring `mountfs::linux`'s equivalent test.
///
/// A child process rather than an in-process thread (like the Linux
/// version) specifically so it can be torn down with `Child::kill`
/// (`TerminateProcess`), a hard kill with no cleanup - `on_unmount_runs_
/// and_process_exits_cleanly_on_ctrl_c` below covers the *clean* shutdown
/// path (Ctrl+C, `on_unmount` runs, the process exits itself); this test
/// is about the read-only op set, not shutdown, and a hard kill is the
/// simpler way to end it once done.
#[test]
fn mounts_and_serves_the_full_read_only_op_set_via_real_winfsp() {
    let helper = env!("CARGO_BIN_EXE_windows_mount_spike_helper");
    let parent_dir = tempfile::tempdir().unwrap();
    let mount_path = parent_dir.path().join("mnt");

    let mut child = std::process::Command::new(helper)
        .arg(&mount_path)
        .spawn()
        .expect("failed to spawn windows_mount_spike_helper");

    let mut names = wait_for_mount_ready(&mut child, &mount_path);
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

/// Verifies the actual Windows shutdown mechanism (see `mountfs::windows`'s
/// doc comment - WinFSP unmounts and returns from `fuse_main_real` on its
/// own on Ctrl+C, no custom handling needed on this crate's side, manually
/// confirmed in a real terminal): sends a real `CTRL_C_EVENT` (not
/// `Child::kill`, which bypasses the whole thing and just hard-kills the
/// process) to the mount helper, and checks both that
/// `MountFilesystem::on_unmount` ran (via a marker file it writes - the
/// only way to observe this from outside the process) and that the
/// process then exits on its own, with no explicit kill needed.
///
/// Spawned with `CREATE_NEW_PROCESS_GROUP` so `GenerateConsoleCtrlEvent`
/// targets only the helper, not this test process too - by default a
/// child shares its parent's console/process group, and `CTRL_C_EVENT`
/// delivery isn't scoped to a single process, only a whole group.
///
/// Ignored by default: this needs a real Win32 console attached to work
/// (`GenerateConsoleCtrlEvent` has nothing to deliver through otherwise) -
/// confirmed absent in this project's automated dev/build environment
/// (`[System.Console]::CursorLeft` throws "the handle is invalid" there),
/// and `AllocConsole()` before spawning didn't change that (this
/// environment appears not to provide a console subsystem at all, not
/// just a not-yet-attached one). Confirmed working manually instead, in a
/// real terminal, by running `windows_mount_spike_helper.exe` directly and
/// pressing Ctrl+C - run this test with `--ignored` in an environment with
/// a real console to reproduce that as an automated check.
#[ignore = "needs a real attached console - see doc comment"]
#[test]
fn on_unmount_runs_and_process_exits_cleanly_on_ctrl_c() {
    let helper = env!("CARGO_BIN_EXE_windows_mount_spike_helper");
    let parent_dir = tempfile::tempdir().unwrap();
    let mount_path = parent_dir.path().join("mnt");
    let marker_path = parent_dir.path().join("on_unmount_marker");

    // This test process may not have a real console attached (e.g. when
    // run from an automated harness rather than an interactive terminal) -
    // GenerateConsoleCtrlEvent needs one to actually deliver anything.
    // Harmless to call unconditionally: a no-op if a console is already
    // attached.
    unsafe { AllocConsole() };

    let mut child = std::process::Command::new(helper)
        .arg(&mount_path)
        .arg(&marker_path)
        .creation_flags(CREATE_NEW_PROCESS_GROUP)
        .spawn()
        .expect("failed to spawn windows_mount_spike_helper");

    wait_for_mount_ready(&mut child, &mount_path);

    let sent = unsafe { GenerateConsoleCtrlEvent(CTRL_C_EVENT, child.id()) };
    assert_ne!(sent, 0, "GenerateConsoleCtrlEvent failed");

    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Ok(Some(status)) = child.try_wait() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("mount helper did not exit within 10s of Ctrl+C");
        }
        std::thread::sleep(Duration::from_millis(100));
    };

    assert!(
        status.success(),
        "mount helper did not exit cleanly: {status}"
    );
    assert!(
        marker_path.exists(),
        "on_unmount did not run (no marker file written)"
    );
}
