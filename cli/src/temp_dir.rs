//! Shared validation and spill-directory creation for `store`/`mount
//! --read-write`'s `--temp <DIR>` flag - lets either command's chunk-
//! buffer/write-cache disk spillover (see `spillcache::WriteCache`) be
//! created somewhere other than `tempfile`'s own default
//! (`std::env::temp_dir()`), matching the Scala prototype's own README
//! guidance to put this on the fastest available disk, not necessarily
//! the OS default (see `docs/plans/implemented/bounded-memory-io-pipeline.md`'s
//! "operational expectation" requirement).

use std::path::{Path, PathBuf};

/// Fails fast if `dir` doesn't exist or isn't writable, before any other
/// command work happens - mirrors `check_ram_budget`'s up-front placement
/// in `run_store`/`run_mount`. Probes writability by actually creating
/// (and immediately removing, via `TempDir`'s `Drop`) a throwaway
/// directory inside `dir`, rather than just inspecting permission bits:
/// this doubles as the existence check too, and exercises exactly the
/// same `tempfile` machinery [`create_spill_dir`] uses for the real spill
/// directory later, so anything that would make *that* call fail is
/// caught here first, before the target/source/repository work `run_store`/
/// `run_mount` do in between.
pub(crate) fn validate_temp_dir(dir: &Path) -> Result<(), String> {
    tempfile::Builder::new()
        .prefix(".backup-temp-check-")
        .tempdir_in(dir)
        .map(drop)
        .map_err(|err| format!("--temp '{}' is not usable: {err}", dir.display()))
}

/// Creates a uniquely-named spill directory (prefixed `prefix`) for chunk-
/// buffer/write-cache disk spillover - under `temp` if given (already
/// validated by [`validate_temp_dir`]), or under the OS default
/// (`std::env::temp_dir()`) otherwise. Named via `tempfile::Builder`
/// rather than `std::process::id()`: a process id is only unique across
/// *processes*, but both `run_store` and `build_filesystem` can run more
/// than once within a single process (every integration test that drives
/// either does exactly that, concurrently) - a process-id-based name let
/// two concurrent runs collide on the same directory and race to
/// `remove_dir_all` it out from under each other (the bug this was
/// originally fixed for, before `--temp` existed - see
/// `docs/plans/implemented/bounded-memory-io-pipeline.md`).
pub(crate) fn create_spill_dir(prefix: &str, temp: Option<&Path>) -> std::io::Result<PathBuf> {
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    let dir = match temp {
        Some(temp) => builder.tempdir_in(temp),
        None => builder.tempdir(),
    }?;
    Ok(dir.keep())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_temp_dir_accepts_an_existing_writable_directory() {
        let dir = tempfile::tempdir().unwrap();
        assert!(validate_temp_dir(dir.path()).is_ok());
    }

    #[test]
    fn validate_temp_dir_rejects_a_nonexistent_directory() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        let err = validate_temp_dir(&missing).unwrap_err();
        assert!(err.contains("--temp"), "error should mention --temp: {err}");
    }

    #[test]
    fn validate_temp_dir_rejects_a_file_given_instead_of_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a-file");
        std::fs::write(&file, b"not a directory").unwrap();
        assert!(validate_temp_dir(&file).is_err());
    }

    #[test]
    fn create_spill_dir_defaults_to_the_os_temp_dir_when_none_given() {
        let dir = create_spill_dir("temp-dir-test-", None).unwrap();
        assert!(dir.starts_with(std::env::temp_dir()));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn create_spill_dir_uses_the_given_dir_when_provided() {
        let temp = tempfile::tempdir().unwrap();
        let dir = create_spill_dir("temp-dir-test-", Some(temp.path())).unwrap();
        assert!(dir.starts_with(temp.path()));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
