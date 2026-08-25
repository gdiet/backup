//! `dfs create-repo` - REQ-CLI-005 in requirements/functional/cli-commands.md, and REQ-CLI-006's
//! default repository path in the same file.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// REQ-CLI-006's default repository directory name, created next to the running executable.
const DEFAULT_REPO_DIR_NAME: &str = "dedupfs-repository";

/// Resolves REQ-CLI-006's default repository path: a `dedupfs-repository` directory next to the
/// running executable. A thin wrapper around `std::env::current_exe()` - the actual resolution
/// logic lives in [`default_repo_path_from`], which takes the executable path as a plain
/// parameter so it stays testable without depending on where the test binary itself happens to
/// run from.
pub fn default_repo_path() -> std::io::Result<PathBuf> {
    std::env::current_exe().map(|exe_path| default_repo_path_from(&exe_path))
}

fn default_repo_path_from(exe_path: &Path) -> PathBuf {
    exe_path
        .parent()
        .unwrap_or(exe_path)
        .join(DEFAULT_REPO_DIR_NAME)
}

/// `create-repo`'s core logic, separated from `main`'s process-exit/println side effects so it
/// stays testable: returns the message to print on success, or the message to print (to stderr,
/// followed by a non-zero exit) on failure. `default_path_used` distinguishes a `path` the
/// operator gave explicitly from one resolved via [`default_repo_path`], so a failure can point
/// at passing the path explicitly only when there was no explicit path already (REQ-CLI-006,
/// REQ-OPERABILITY-004) - pointing an operator who already gave an explicit path back at that
/// same path would not be actionable.
fn try_run(
    path: &Path,
    cdc_target_size_bits: Option<u32>,
    default_path_used: bool,
) -> Result<String, String> {
    // Validate against cdc's own bounds before touching the filesystem at all -
    // DESIGN-METADATA-009's "CLI validation" section: reuse cdc::ChunkerConfig::new
    // rather than duplicating its bounds here.
    if let Err(err) = cdc::ChunkerConfig::new(cdc_target_size_bits) {
        return Err(format!("error: {err}"));
    }

    let creation_time_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_millis() as i64;
    let settings = db::RepositorySettings::new(cdc_target_size_bits, creation_time_millis);

    match db::init_repository(path, settings) {
        Ok(()) => Ok(success_message(path, cdc_target_size_bits)),
        Err(db::Error::Io(io_err)) if default_path_used => Err(format!(
            "error: the default repository location ({}) is not usable: {io_err}\n\
             Pass a writable location explicitly instead.",
            path.display()
        )),
        Err(err) => Err(format!("error: {err}")),
    }
}

fn success_message(path: &Path, cdc_target_size_bits: Option<u32>) -> String {
    let chunking = match cdc_target_size_bits {
        Some(bits) => format!(
            "content-defined, target size {bits} bits (average chunk size a little above 2^{bits} \
             bytes)"
        ),
        None => "whole-file (no sub-file deduplication)".to_string(),
    };
    format!(
        "created repository at {}\n\
         chunking: {chunking} - fixed for this repository's lifetime, cannot be changed later.",
        path.display()
    )
}

pub fn run(path: &Path, cdc_target_size_bits: Option<u32>, default_path_used: bool) {
    match try_run(path, cdc_target_size_bits, default_path_used) {
        Ok(message) => println!("{message}"),
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn default_repo_path_from_joins_the_dir_name_onto_the_executables_parent() {
        assert_eq!(
            default_repo_path_from(Path::new("/opt/dfs/bin/dfs")),
            Path::new("/opt/dfs/bin/dedupfs-repository")
        );
    }

    #[test]
    fn default_repo_path_from_handles_an_executable_directly_under_root() {
        assert_eq!(
            default_repo_path_from(Path::new("/dfs")),
            Path::new("/dedupfs-repository")
        );
    }

    #[test]
    fn default_repo_path_from_falls_back_to_the_input_itself_when_it_has_no_parent() {
        // Path::new("/").parent() is None - the one case the `unwrap_or(exe_path)` fallback
        // exists for (current_exe() is not expected to ever actually return "/", but the
        // fallback must still behave sensibly rather than panic).
        assert_eq!(
            default_repo_path_from(Path::new("/")),
            Path::new("/dedupfs-repository")
        );
    }

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    /// A fresh, not-yet-existing directory under the OS temp dir, unique per call so parallel
    /// tests never collide.
    fn unique_temp_path() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("dfs-create-repo-test-{}-{n}", std::process::id()))
    }

    #[test]
    fn try_run_creates_a_repository_and_reports_the_defaulted_chunking() {
        let dir = unique_temp_path();
        let message = try_run(&dir, None, false).expect("must succeed on a fresh path");
        assert!(message.contains("created repository at"));
        assert!(message.contains("whole-file"));
        assert!(message.contains("cannot be changed later"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn try_run_reports_an_explicit_cdc_target_size() {
        let dir = unique_temp_path();
        let message = try_run(&dir, Some(22), false).expect("must succeed on a fresh path");
        assert!(message.contains("target size 22 bits"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_is_unusable() {
        // The immediate parent does not exist, so fs::create_dir fails with an io::Error
        // regardless of privilege level (unlike a permission-bits test, which root bypasses).
        let path = unique_temp_path()
            .join("no-such-parent")
            .join("dedupfs-repository");

        let message =
            try_run(&path, None, true).expect_err("must fail - the parent does not exist");
        assert!(
            message.contains("not usable"),
            "expected the actionable default-path message, got: {message}"
        );
        assert!(
            message.contains("explicitly"),
            "expected a hint to pass the path explicitly, got: {message}"
        );
    }

    #[test]
    fn try_run_surfaces_the_raw_error_when_an_explicit_path_is_unusable() {
        // Same broken path as above, but default_path_used = false: an operator who already
        // passed an explicit path gets the plain db::Error, not the default-path hint - there is
        // nothing more specific to tell them.
        let path = unique_temp_path()
            .join("no-such-parent")
            .join("dedupfs-repository");

        let message =
            try_run(&path, None, false).expect_err("must fail - the parent does not exist");
        assert!(
            !message.contains("not usable"),
            "did not expect the default-path hint for an explicit path, got: {message}"
        );
    }
}
