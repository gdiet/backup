//! Integration test (not a unit test), specifically so `CARGO_BIN_EXE_signal_stop_probe` is
//! available. Verifies DESIGN-MOUNT-026: a mount stopped by SIGHUP, SIGINT or SIGTERM ends as a
//! normal stop, with status 0 and without an error message.
//!
//! Needs a real libfuse3 mount (`/dev/fuse`), so the test names carry the `real_mount_` prefix
//! that `cargo test -- --skip real_mount` filters on.

#![cfg(target_os = "linux")]

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(10);

fn stop_probe_with(signal: libc::c_int) -> (std::process::ExitStatus, String) {
    let mountpoint = tempfile::tempdir().unwrap();
    let mut probe = Command::new(env!("CARGO_BIN_EXE_signal_stop_probe"))
        .arg(mountpoint.path())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let mount_entry = format!(" {} fuse", mountpoint.path().display());
    let deadline = Instant::now() + TIMEOUT;
    while !std::fs::read_to_string("/proc/mounts")
        .unwrap()
        .contains(&mount_entry)
    {
        assert!(Instant::now() < deadline, "the mount never appeared");
        assert!(
            probe.try_wait().unwrap().is_none(),
            "the probe exited before mounting"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    // The signal handlers are only recognized once the kernel's first request has been handled.
    // Reading the root directory guarantees that.
    std::fs::read_dir(mountpoint.path()).unwrap().count();

    let pid = libc::pid_t::try_from(probe.id()).unwrap();
    assert_eq!(unsafe { libc::kill(pid, signal) }, 0);

    let deadline = Instant::now() + TIMEOUT;
    let status = loop {
        if let Some(status) = probe.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            probe.kill().unwrap();
            let _ = Command::new("fusermount3")
                .arg("-u")
                .arg("-z")
                .arg(mountpoint.path())
                .status();
            panic!("the probe did not exit after signal {signal}");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let mut stderr = String::new();
    std::io::Read::read_to_string(&mut probe.stderr.take().unwrap(), &mut stderr).unwrap();
    (status, stderr)
}

fn assert_normal_stop(signal: libc::c_int) {
    let (status, stderr) = stop_probe_with(signal);
    assert!(
        status.success(),
        "signal {signal} ended the mount abnormally: {status}, stderr: {stderr}"
    );
    assert!(
        !stderr.contains("mount failed"),
        "unexpected error message: {stderr}"
    );
}

#[test]
fn real_mount_sigterm_ends_the_mount_as_a_normal_stop() {
    assert_normal_stop(libc::SIGTERM);
}

#[test]
fn real_mount_sigint_ends_the_mount_as_a_normal_stop() {
    assert_normal_stop(libc::SIGINT);
}

#[test]
fn real_mount_sighup_ends_the_mount_as_a_normal_stop() {
    assert_normal_stop(libc::SIGHUP);
}
