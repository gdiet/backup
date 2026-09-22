//! `dfs create-repo` - REQ-CLI-005 in requirements/functional/cli-commands.md. REQ-CLI-006's
//! default repository path (shared with `mount`) lives in `crate::repo_path`.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::ram_budget::{self, DispatchPool};

/// REQ-STORAGE-003's upper bound - narrower than `cdc::ChunkerConfig`'s own general validation
/// (6 to 30), see DESIGN-MEMORY-001 in `docs/design/ram-budget.md`.
const MAX_CDC_TARGET_SIZE_BITS: u32 = 23;

/// `create-repo`'s core logic, separated from `main`'s process-exit/println side effects so it
/// stays testable: returns the message to print on success, or the message to print (to stderr,
/// followed by a non-zero exit) on failure. `default_path_used` distinguishes a `path` the
/// operator gave explicitly from one resolved via [`crate::repo_path::default_repo_path`], so a
/// failure can point at passing the path explicitly only when there was no explicit path already
/// (REQ-CLI-006, REQ-OPERABILITY-004) - pointing an operator who already gave an explicit path
/// back at that same path would not be actionable.
fn try_run(
    path: &Path,
    cdc_target_size_bits: u32,
    ram_budget_gross_bytes: u64,
    default_path_used: bool,
) -> Result<String, String> {
    // Validate against cdc's own bounds before touching the filesystem at all -
    // DESIGN-METADATA-009's "CLI validation" section: reuse cdc::ChunkerConfig::new
    // rather than duplicating its bounds here.
    let config = match cdc::ChunkerConfig::new(Some(cdc_target_size_bits)) {
        Ok(config) => config,
        Err(err) => return Err(format!("error: {err}")),
    };
    if cdc_target_size_bits > MAX_CDC_TARGET_SIZE_BITS {
        return Err(format!(
            "error: --cdc-target-size-bits {cdc_target_size_bits} is too large (maximum: \
             {MAX_CDC_TARGET_SIZE_BITS}) - REQ-STORAGE-003 caps this lower than cdc's own general \
             range so the largest possible chunk always fits within the RAM budget \
             (REQ-OPERABILITY-006)"
        ));
    }

    // REQ-OPERABILITY-006: refuse to create a repository whose own configured chunking cannot
    // possibly fit within the RAM budget, rather than letting it silently exceed that bound once
    // running. No connection is open yet to read a real `cache_size` from (this check runs before
    // `init_repository`, deliberately, so it never touches the filesystem on rejection) and no
    // worker/dispatch pool runs during `create-repo` itself - both reserves are `0` here, a
    // conservative approximation that only ever overestimates the available budget slightly (an
    // actual mount/ingest run reserves more, via its own real `cache_size` and thread count).
    let max_chunk_size = config
        .max_chunk_size()
        .expect("Some(bits) always yields a bounded max_chunk_size");
    let caching_budget_bytes =
        ram_budget::caching_budget_bytes(ram_budget_gross_bytes, 0, 0, DispatchPool::None);
    if let Err(err) = ram_budget::check_fits_max_chunk_size(caching_budget_bytes, max_chunk_size) {
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

fn success_message(path: &Path, cdc_target_size_bits: u32) -> String {
    format!(
        "created repository at {}\n\
         chunking: content-defined, target size {cdc_target_size_bits} bits (average chunk size a \
         little above 2^{cdc_target_size_bits} bytes) - fixed for this repository's lifetime, \
         cannot be changed later.",
        path.display()
    )
}

pub fn run(path: &Path, cdc_target_size_bits: u32, ram_budget_mb: u64, default_path_used: bool) {
    match try_run(
        path,
        cdc_target_size_bits,
        ram_budget_mb * 1024 * 1024,
        default_path_used,
    ) {
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
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    /// A fresh, not-yet-existing directory under the OS temp dir, unique per call so parallel
    /// tests never collide.
    fn unique_temp_path() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("dfs-create-repo-test-{}-{n}", std::process::id()))
    }

    /// Every test below uses this unless it is specifically exercising the RAM-budget check
    /// itself - large enough that an ordinary target size comfortably fits.
    const GENEROUS_BUDGET: u64 = ram_budget::DEFAULT_GROSS_BUDGET_BYTES;

    #[test]
    fn try_run_creates_a_repository_and_reports_the_chunking() {
        let dir = unique_temp_path();
        let message =
            try_run(&dir, 20, GENEROUS_BUDGET, false).expect("must succeed on a fresh path");
        assert!(message.contains("created repository at"));
        assert!(message.contains("target size 20 bits"));
        assert!(message.contains("cannot be changed later"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn try_run_reports_an_explicit_cdc_target_size() {
        let dir = unique_temp_path();
        let message =
            try_run(&dir, 22, GENEROUS_BUDGET, false).expect("must succeed on a fresh path");
        assert!(message.contains("target size 22 bits"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn try_run_rejects_a_target_size_above_the_23_bit_ceiling() {
        let dir = unique_temp_path();
        let message = try_run(&dir, 24, GENEROUS_BUDGET, false)
            .expect_err("24 bits exceeds REQ-STORAGE-003's own 23-bit ceiling");
        assert!(message.contains("too large"), "got: {message}");
        assert!(!dir.exists(), "must not create anything on rejection");
    }

    #[test]
    fn try_run_refuses_a_ram_budget_too_small_for_the_configured_chunk_size() {
        let dir = unique_temp_path();
        // 23 bits needs a 96 MiB chunk - a 1 KiB gross budget cannot possibly hold one.
        let message = try_run(&dir, 23, 1024, false)
            .expect_err("the RAM budget cannot fit even one chunk of this size");
        assert!(message.contains("RAM budget too small"), "got: {message}");
        assert!(!dir.exists(), "must not create anything on rejection");
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_is_unusable() {
        // The immediate parent does not exist, so fs::create_dir fails with an io::Error
        // regardless of privilege level (unlike a permission-bits test, which root bypasses).
        let path = unique_temp_path()
            .join("no-such-parent")
            .join("dedupfs-repository");

        let message = try_run(&path, 20, GENEROUS_BUDGET, true)
            .expect_err("must fail - the parent does not exist");
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

        let message = try_run(&path, 20, GENEROUS_BUDGET, false)
            .expect_err("must fail - the parent does not exist");
        assert!(
            !message.contains("not usable"),
            "did not expect the default-path hint for an explicit path, got: {message}"
        );
    }
}
