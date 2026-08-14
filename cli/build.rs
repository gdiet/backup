//! Embeds the git commit this binary was built from into compile-time env vars
//! (`BACKUP_GIT_HASH`, `BACKUP_GIT_COMMIT_DATE`), consumed via `env!(...)` in `version.rs`.
//! Shells out to `git` directly rather than pulling in a crate for this (see
//! `docs/plans/version-command.md`) - two values, no need for the dependency surface.

use std::process::Command;

fn main() {
    // Re-run when the checked-out branch changes (.git/HEAD) or gets a new commit
    // (.git/refs/heads/<branch>) - not on every build.
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/refs/heads");

    println!("cargo:rustc-env=BACKUP_GIT_HASH={}", git_hash());
    println!(
        "cargo:rustc-env=BACKUP_GIT_COMMIT_DATE={}",
        git_commit_date()
    );
}

/// Falls back to "unknown" rather than failing the build - `git` may be missing from `PATH`, or
/// this may be a build from a source tarball without a `.git` directory.
fn git_output(args: &[&str]) -> String {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn git_hash() -> String {
    git_output(&["rev-parse", "--short=8", "HEAD"])
}

/// The commit's own timestamp, not the build's - two builds of the same commit should report the
/// same date, which a build-time timestamp wouldn't give (and would also hurt reproducible builds).
fn git_commit_date() -> String {
    git_output(&["show", "-s", "--format=%cs", "HEAD"])
}
