//! End-to-end check of `dfs mount --best-effort` (REQ-MOUNT-005, DESIGN-MOUNT-027 in
//! `docs/design/mount-write-path.md`) through the real binary and a real FUSE mount. It covers
//! what the unit tests cannot: that the flag reaches the filesystem, and what a reader and the
//! operator actually see.

#![cfg(target_os = "linux")]

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

const CONTENT: &[u8] = b"hello world, hello again\n";
const DEADLINE: Duration = Duration::from_secs(10);

fn dfs() -> Command {
    Command::new(env!("CARGO_BIN_EXE_dfs"))
}

fn run_ok(command: &mut Command) {
    let output = command.output().expect("dfs starts");
    assert!(
        output.status.success(),
        "{command:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A repository whose stored data is gone, and an empty mount point.
struct Fixture {
    _dir: tempfile::TempDir,
    repo: PathBuf,
    mount_point: PathBuf,
}

fn fixture_with_lost_data() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    let source = dir.path().join("src");
    let mount_point = dir.path().join("mnt");
    fs::create_dir(&source).unwrap();
    fs::create_dir(&mount_point).unwrap();
    fs::write(source.join("a.txt"), CONTENT).unwrap();
    run_ok(dfs().arg("create-repo").arg(&repo));
    run_ok(
        dfs()
            .arg("ingest")
            .arg("--repository")
            .arg(&repo)
            .arg(&source)
            .arg("+x"),
    );
    fs::remove_dir_all(repo.join("data")).unwrap();
    Fixture {
        _dir: dir,
        repo,
        mount_point,
    }
}

struct Mounted {
    child: Child,
    mount_point: PathBuf,
}

fn is_mounted(mount_point: &Path) -> bool {
    let marker = format!(" {} fuse", mount_point.display());
    fs::read_to_string("/proc/mounts").is_ok_and(|mounts| mounts.contains(&marker))
}

fn mount(fixture: &Fixture, extra_args: &[&str]) -> Mounted {
    let child = dfs()
        .arg("mount")
        .arg("--repository")
        .arg(&fixture.repo)
        .args(extra_args)
        .arg(&fixture.mount_point)
        .stderr(Stdio::piped())
        .spawn()
        .expect("dfs starts");
    let mut mounted = Mounted {
        child,
        mount_point: fixture.mount_point.clone(),
    };
    let start = Instant::now();
    while !is_mounted(&mounted.mount_point) {
        assert!(start.elapsed() < DEADLINE, "the mount did not come up");
        assert!(
            mounted.child.try_wait().unwrap().is_none(),
            "dfs mount exited early"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    mounted
}

impl Mounted {
    fn file(&self) -> PathBuf {
        self.mount_point.join("x/src/a.txt")
    }

    /// Stops the mount the way a terminal does, with SIGINT. Returns the exit status and stderr.
    fn stop(mut self) -> (ExitStatus, String) {
        // SAFETY: plain signal delivery to a child process this test spawned.
        unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGINT) };
        let start = Instant::now();
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(start.elapsed() < DEADLINE, "dfs mount did not stop");
            std::thread::sleep(Duration::from_millis(50));
        };
        let mut stderr = String::new();
        self.child
            .stderr
            .take()
            .expect("stderr was piped")
            .read_to_string(&mut stderr)
            .unwrap();
        (status, stderr)
    }
}

impl Drop for Mounted {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = Command::new("fusermount3")
                .arg("-u")
                .arg(&self.mount_point)
                .status();
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[test]
fn real_mount_best_effort_reads_missing_data_as_zero_value_bytes() {
    let fixture = fixture_with_lost_data();
    let mounted = mount(&fixture, &["--best-effort"]);

    let data = fs::read(mounted.file()).expect("a best-effort read does not fail");
    assert_eq!(data, vec![0u8; CONTENT.len()]);

    let (status, stderr) = mounted.stop();
    assert!(status.success(), "{status}: {stderr}");
    assert!(
        stderr.contains("warning: --best-effort: missing or unreadable stored data reads as"),
        "{stderr}"
    );
    assert!(
        stderr.contains("is missing or short - reads that touch it return zero-value bytes"),
        "{stderr}"
    );
    assert!(
        stderr.contains("read(s) returned zero-value bytes (0x00)"),
        "{stderr}"
    );
}

#[test]
fn real_mount_without_best_effort_fails_a_read_of_missing_data() {
    let fixture = fixture_with_lost_data();
    let mounted = mount(&fixture, &[]);

    let error = fs::read(mounted.file()).expect_err("the read fails visibly");
    assert_eq!(error.raw_os_error(), Some(libc::EIO));

    let (status, stderr) = mounted.stop();
    assert!(status.success(), "{status}: {stderr}");
    assert!(!stderr.contains("warning:"), "{stderr}");
}
