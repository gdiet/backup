//! Embeds a git-derived date and short commit hash for `--version` (DESIGN-CLI-001 in
//! `docs/design/cli-version.md`) as compile-time env vars, read back via `env!(...)` in `main.rs`.
//! Runs only on the machine building this crate - the compiled binary never shells out to git.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/index");

    let (date, hash) = git_info().unwrap_or_else(|| (fallback_date(), "unknown".to_string()));

    println!("cargo:rustc-env=DFS_VERSION_DATE={date}");
    println!("cargo:rustc-env=DFS_VERSION_HASH={hash}");
}

fn git_info() -> Option<(String, String)> {
    let date = run_git(&["log", "-1", "--format=%cd", "--date=format:%Y-%m-%d"])?;
    let mut hash = run_git(&["rev-parse", "--short=8", "HEAD"])?;
    if !run_git(&["status", "--porcelain"])?.is_empty() {
        hash.push_str("-modified");
    }
    Some((date, hash))
}

fn run_git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_string())
}

/// Fallback for the (rare) case git itself is unavailable at build time - today's date, computed
/// without a date/time dependency via Howard Hinnant's `civil_from_days` algorithm.
fn fallback_date() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (y, m, d) = civil_from_days((secs / 86_400) as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}
