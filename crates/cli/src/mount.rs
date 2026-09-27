//! `dfs mount`'s real implementation - REQ-MOUNT-001. Read-only by default (REQ-MOUNT-002);
//! `--read-write` opts into the directory operations REQ-MOUNT-003 requires (mkdir/rmdir/rename/
//! utimens - see `dedup_fs::DedupFs`) and, before any of that, the whole-session repository write
//! lock (REQ-MAINTENANCE-004, DESIGN-MAINTENANCE-001 in
//! `docs/design/repository-locking.md`) - a read-only mount never acquires it. REQ-CLI-006's
//! default repository path (`crate::repo_path`) applies here too - see `try_run`'s
//! `default_path_used`.

use std::path::Path;

use crate::dedup_fs::{DedupFs, Tuning};

/// Database-connection-opening knobs `try_run` itself consumes, entirely before `DedupFs::new`
/// (and thus `Tuning`, which that constructor needs) ever comes into play. Bundled into their own
/// struct purely to keep `try_run`/[`run`]'s own parameter counts under clippy's
/// `too_many_arguments` threshold - not otherwise a meaningful grouping elsewhere.
pub struct RepoOpenOptions {
    /// DESIGN-METADATA-013's opt-in (`--assume-read-only-medium`) - meaningless together with a
    /// read-write mount, which already holds the repository-wide write lock, ruling out a
    /// concurrent writer for the same reason this assertion would otherwise exist to guarantee.
    /// `try_run` refuses that combination outright rather than silently ignoring it.
    pub assume_read_only_medium: bool,
    /// Whether `--ram-budget-mb`/`--backpressure-free-zone-bytes`/`--backpressure-slope-divisor`
    /// were actually typed on the command line - distinct from their own resolved values
    /// (`Tuning`'s fields), which are always present (`default_value_t`) regardless of whether the
    /// operator gave them explicitly. Needed for REQ-OPERABILITY-007: these flags only matter for
    /// a read-write mount's write cache, so an explicit one given without `--read-write` is
    /// refused rather than silently ignored - but a value that is merely the untouched default
    /// must not trigger that refusal just because it happens to be present in `Tuning` either way.
    pub ram_budget_mb_given: bool,
    pub backpressure_free_zone_bytes_given: bool,
    pub backpressure_slope_divisor_given: bool,
}

/// `mount`'s core logic, separated from `main`'s process-exit/eprintln side effects so the error
/// path stays testable without touching a real mount. `default_path_used` distinguishes a
/// `repo_path` the operator gave explicitly from one resolved via
/// [`crate::repo_path::default_repo_path`], so a failure can point at passing the path explicitly
/// only when there was no explicit path already (REQ-CLI-006, REQ-OPERABILITY-004).
fn try_run(
    repo_path: &Path,
    mountpoint: &Path,
    read_write: bool,
    default_path_used: bool,
    spill_dir: Option<&Path>,
    open_options: RepoOpenOptions,
    tuning: Tuning,
) -> Result<(), String> {
    // DESIGN-METADATA-013's assertion only exists to let a read-only open succeed on storage no
    // writer could reach anyway - a read-write mount already holds the repository-wide write lock
    // (REQ-MAINTENANCE-004), which rules out a concurrent writer for that same reason. Refused
    // outright, rather than silently ignored, so passing both together does not read as "this
    // combination is supported" when it is not.
    if read_write && open_options.assume_read_only_medium {
        return Err(
            "error: --assume-read-only-medium has no effect together with --read-write - a \
             read-write mount already rules out a concurrent writer on its own. Drop one of the \
             two flags."
                .to_string(),
        );
    }

    // REQ-OPERABILITY-007: an option given without the one thing its whole purpose depends on
    // would otherwise be silently accepted and do nothing at all. Two independent prerequisites
    // apply here - a flag can lack either, or (only --purge) both at once - so each is collected
    // and reported separately rather than only ever surfacing the first one found.
    let mut problems = Vec::new();
    // --show-deleted is deliberately never itself a target of either check below: REQ-MOUNT-004/
    // 007 make it meaningful on a read-only mount too (browsing works without --read-write, only
    // recovery needs it), and it depends on nothing else here.
    if !read_write {
        let mut needs_read_write = Vec::new();
        if tuning.allow_purge {
            needs_read_write.push("--purge");
        }
        if spill_dir.is_some() {
            needs_read_write.push("--spill-directory");
        }
        if open_options.ram_budget_mb_given {
            needs_read_write.push("--ram-budget-mb");
        }
        if open_options.backpressure_free_zone_bytes_given {
            needs_read_write.push("--backpressure-free-zone-bytes");
        }
        if open_options.backpressure_slope_divisor_given {
            needs_read_write.push("--backpressure-slope-divisor");
        }
        if !needs_read_write.is_empty() {
            let flags = needs_read_write.join(", ");
            let pronoun = if needs_read_write.len() == 1 {
                "it"
            } else {
                "them"
            };
            problems.push(format!(
                "{flags} only matter together with --read-write - add --read-write, or drop \
                 {pronoun}"
            ));
        }
    }
    if !tuning.show_deleted {
        let mut needs_show_deleted = Vec::new();
        if tuning.allow_purge {
            needs_show_deleted.push("--purge");
        }
        // `time_display` only ever becomes `Utc` by the operator actually passing --utc (`main.rs`'s
        // own `time_display` resolves the bare flag one-to-one) - never a stand-in for "not given",
        // the same distinction REQ-OPERABILITY-007 already requires for `ram_budget_mb_given` et al.
        if tuning.time_display == crate::time_format::TimeDisplay::Utc {
            needs_show_deleted.push("--utc");
        }
        if !needs_show_deleted.is_empty() {
            let flags = needs_show_deleted.join(", ");
            let pronoun = if needs_show_deleted.len() == 1 {
                "it"
            } else {
                "them"
            };
            problems.push(format!(
                "{flags} only matter together with --show-deleted - add --show-deleted, or drop \
                 {pronoun}"
            ));
        }
    }
    if !problems.is_empty() {
        return Err(format!("error: {}.", problems.join(". ")));
    }

    // A read-only mount uses a genuinely read-only connection (DESIGN-METADATA-003) rather than
    // open_repository's write-mode one - it needs neither WAL/foreign_keys/auto_vacuum setup nor
    // migration, and it keeps working on a filesystem where a write-mode open is unreliable
    // (Error::ConnectionUnreliable's case) even though the mount itself never writes.
    //
    // DESIGN-METADATA-013: the read-only branch validates an explicit --assume-read-only-medium
    // against what actually happens, rather than trusting it blindly - see
    // open_repository_read_only_with_medium_assertion's own doc comment. The combination with
    // read_write is already refused above, so assume_read_only_medium can only be true here when
    // read_write is false.
    let open_result = if read_write {
        db::open_repository(repo_path)
    } else {
        db::open_repository_read_only_with_medium_assertion(
            repo_path,
            open_options.assume_read_only_medium,
        )
    };
    let repo = match open_result {
        Ok(repo) => repo,
        Err(db::Error::NoRepositoryHere(_)) if default_path_used => {
            return Err(format!(
                "error: no repository found at the default location ({}).\n\
                 Pass a repository path explicitly instead.",
                repo_path.display()
            ));
        }
        Err(err) => return Err(format!("error: {err}")),
    };

    // Held for the rest of this function, across the blocking `mountfs::mount` call below, for
    // as long as this read-write mount session runs (DESIGN-MOUNT-008) - dropped, releasing the
    // lock, once `mountfs::mount` returns after unmount. A read-only mount never acquires it
    // (REQ-MAINTENANCE-004: read-only operations are unaffected).
    let _write_lock = if read_write {
        Some(db::acquire_write_lock(repo_path).map_err(|err| format!("error: {err}"))?)
    } else {
        None
    };

    // Linux's FUSE backend refuses to mount onto a path that does not already exist, and only
    // reports that via a raw line on stderr, not through this crate's `Err`. Catching it here
    // first gives an actionable message instead (REQ-OPERABILITY-004) - Windows needs no such
    // check, since WinFSP creates the mountpoint itself.
    #[cfg(target_os = "linux")]
    if !mountpoint.is_dir() {
        return Err(format!(
            "error: mountpoint {} does not exist. Create it first (e.g. `mkdir -p`) - unlike on \
             Windows, this platform's FUSE backend does not create the mountpoint directory \
             itself.",
            mountpoint.display()
        ));
    }

    // Checked eagerly, before the blocking mount call, rather than left to surface lazily the
    // first time the write cache actually needs to spill (DESIGN-MOUNT-018) - the same
    // fail-fast-with-an-actionable-message treatment as the mountpoint check above.
    if let Some(dir) = spill_dir
        && !dir.is_dir()
    {
        return Err(format!(
            "error: --spill-directory {} does not exist or is not a directory. Create it first, \
             or omit --spill-directory to use the OS temp directory instead.",
            dir.display()
        ));
    }

    if let Err(err) = mountfs::preflight() {
        return Err(format!("error: {err}"));
    }
    let store = store::ByteStore::new(db::data_dir(repo_path), !read_write);
    let fs = DedupFs::new(
        repo,
        store,
        read_write,
        repo_path,
        spill_dir.map(Path::to_path_buf),
        tuning,
    )
    .map_err(|err| format!("error: {err}"))?;
    if let Err(err) = mountfs::mount(fs, mountpoint, !read_write) {
        return Err(format!("mount failed: {err}"));
    }
    Ok(())
}

pub fn run(
    repo_path: &Path,
    mountpoint: &Path,
    read_write: bool,
    default_path_used: bool,
    spill_dir: Option<&Path>,
    open_options: RepoOpenOptions,
    tuning: Tuning,
) {
    if let Err(message) = try_run(
        repo_path,
        mountpoint,
        read_write,
        default_path_used,
        spill_dir,
        open_options,
        tuning,
    ) {
        eprintln!("{message}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram_budget;

    fn default_tuning() -> Tuning {
        Tuning {
            ram_budget_gross_bytes: ram_budget::DEFAULT_GROSS_BUDGET_BYTES,
            backpressure_free_zone_bytes: crate::backpressure::DEFAULT_FREE_ZONE_BYTES,
            backpressure_slope_divisor: crate::backpressure::DEFAULT_SLOPE_DIVISOR,
            show_deleted: false,
            allow_purge: false,
            // Local, not Utc: matches the "operator never passed --utc" default this new
            // superfluous-flags check depends on - Utc here is not a value a test happens to want,
            // it means "the operator passed --utc," so tests that need that must say so explicitly.
            time_display: crate::time_format::TimeDisplay::Local,
        }
    }

    #[test]
    fn try_run_refuses_read_write_together_with_assume_read_only_medium() {
        // No filesystem setup needed: this check fires before try_run ever touches the repository
        // or mountpoint paths.
        let repo_path = std::env::temp_dir().join("dfs-mount-test-read-write-and-assume-ro-repo");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-read-write-and-assume-ro-mnt");

        let message = try_run(
            &repo_path,
            &mountpoint,
            true,
            false,
            None,
            RepoOpenOptions {
                assume_read_only_medium: true,
                ram_budget_mb_given: false,
                backpressure_free_zone_bytes_given: false,
                backpressure_slope_divisor_given: false,
            },
            default_tuning(),
        )
        .expect_err(
            "must fail - --read-write and --assume-read-only-medium together make no sense",
        );
        assert!(
            message.contains("--assume-read-only-medium"),
            "expected an actionable message naming the flag, got: {message}"
        );
        assert!(
            message.contains("--read-write"),
            "expected an actionable message naming the other flag, got: {message}"
        );
    }

    #[test]
    fn try_run_refuses_read_write_only_flags_given_without_read_write() {
        // No filesystem setup needed: this check fires before try_run ever touches the repository
        // or mountpoint paths.
        let repo_path = std::env::temp_dir().join("dfs-mount-test-rw-only-flags-repo");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-rw-only-flags-mnt");
        let spill_dir = std::env::temp_dir().join("dfs-mount-test-rw-only-flags-spill");

        let message = try_run(
            &repo_path,
            &mountpoint,
            false,
            false,
            Some(&spill_dir),
            RepoOpenOptions {
                assume_read_only_medium: false,
                ram_budget_mb_given: true,
                backpressure_free_zone_bytes_given: true,
                backpressure_slope_divisor_given: false,
            },
            Tuning {
                allow_purge: true,
                ..default_tuning()
            },
        )
        .expect_err("must fail - several read-write-only flags given without --read-write");
        for flag in [
            "--purge",
            "--spill-directory",
            "--ram-budget-mb",
            "--backpressure-free-zone-bytes",
        ] {
            assert!(
                message.contains(flag),
                "expected the message to name {flag}, got: {message}"
            );
        }
        assert!(
            !message.contains("--backpressure-slope-divisor"),
            "must not name a flag that was not actually given, got: {message}"
        );
    }

    #[test]
    fn try_run_does_not_refuse_show_deleted_without_read_write() {
        // --show-deleted is deliberately meaningful on a read-only mount (browsing works without
        // --read-write, only recovery needs it) - unlike the flags the test above covers, it must
        // never be refused here. Fails for the usual "no repository here" reason instead, proving
        // this got past the read-write-only-flags check without being refused for show_deleted.
        let repo_path = std::env::temp_dir().join("dfs-mount-test-show-deleted-repo-here");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-show-deleted-mnt");

        let message = try_run(
            &repo_path,
            &mountpoint,
            false,
            false,
            None,
            RepoOpenOptions {
                assume_read_only_medium: false,
                ram_budget_mb_given: false,
                backpressure_free_zone_bytes_given: false,
                backpressure_slope_divisor_given: false,
            },
            Tuning {
                show_deleted: true,
                ..default_tuning()
            },
        )
        .expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the no-repository message (not a show-deleted refusal), got: {message}"
        );
    }

    #[test]
    fn try_run_refuses_utc_given_without_show_deleted() {
        // No filesystem setup needed: this check fires before try_run ever touches the repository
        // or mountpoint paths.
        let repo_path = std::env::temp_dir().join("dfs-mount-test-utc-without-show-deleted-repo");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-utc-without-show-deleted-mnt");

        let message = try_run(
            &repo_path,
            &mountpoint,
            true,
            false,
            None,
            RepoOpenOptions {
                assume_read_only_medium: false,
                ram_budget_mb_given: false,
                backpressure_free_zone_bytes_given: false,
                backpressure_slope_divisor_given: false,
            },
            Tuning {
                time_display: crate::time_format::TimeDisplay::Utc,
                ..default_tuning()
            },
        )
        .expect_err(
            "must fail - --utc only ever affects the [time] view, unreachable without \
                     --show-deleted",
        );
        assert!(
            message.contains("--utc"),
            "expected an actionable message naming --utc, got: {message}"
        );
        assert!(
            message.contains("--show-deleted"),
            "expected the message to name the missing prerequisite, got: {message}"
        );
    }

    #[test]
    fn try_run_refuses_purge_given_with_read_write_but_without_show_deleted() {
        // Isolates the --show-deleted prerequisite specifically: --read-write is already present,
        // so a refusal here must name --show-deleted, not --read-write.
        let repo_path = std::env::temp_dir().join("dfs-mount-test-purge-without-show-deleted-repo");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-purge-without-show-deleted-mnt");

        let message = try_run(
            &repo_path,
            &mountpoint,
            true,
            false,
            None,
            RepoOpenOptions {
                assume_read_only_medium: false,
                ram_budget_mb_given: false,
                backpressure_free_zone_bytes_given: false,
                backpressure_slope_divisor_given: false,
            },
            Tuning {
                allow_purge: true,
                ..default_tuning()
            },
        )
        .expect_err(
            "must fail - --purge only ever affects the [deleted] view, unreachable \
                     without --show-deleted",
        );
        assert!(
            message.contains("--purge"),
            "expected an actionable message naming --purge, got: {message}"
        );
        assert!(
            message.contains("--show-deleted"),
            "expected the message to name the missing prerequisite, got: {message}"
        );
        assert!(
            !message.contains("--read-write"),
            "must not claim --read-write is missing when it was already given, got: {message}"
        );
    }

    #[test]
    fn try_run_does_not_refuse_utc_given_together_with_show_deleted() {
        let repo_path = std::env::temp_dir().join("dfs-mount-test-utc-with-show-deleted-repo");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-utc-with-show-deleted-mnt");

        let message = try_run(
            &repo_path,
            &mountpoint,
            false,
            false,
            None,
            RepoOpenOptions {
                assume_read_only_medium: false,
                ram_budget_mb_given: false,
                backpressure_free_zone_bytes_given: false,
                backpressure_slope_divisor_given: false,
            },
            Tuning {
                show_deleted: true,
                time_display: crate::time_format::TimeDisplay::Utc,
                ..default_tuning()
            },
        )
        .expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the no-repository message (not a superfluous-flags refusal), got: {message}"
        );
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_holds_no_repository() {
        // No filesystem setup needed: a path that simply does not exist already answers
        // NoRepositoryHere (both db::open_repository and db::open_repository_read_only share the
        // same ensure_repository_exists guard for this), before try_run ever reaches mountfs.
        let repo_path = std::env::temp_dir().join("dfs-mount-test-no-default-repository-here");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-unused-mountpoint");

        let message = try_run(
            &repo_path,
            &mountpoint,
            false,
            true,
            None,
            RepoOpenOptions {
                assume_read_only_medium: false,
                ram_budget_mb_given: false,
                backpressure_free_zone_bytes_given: false,
                backpressure_slope_divisor_given: false,
            },
            default_tuning(),
        )
        .expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the actionable default-path message, got: {message}"
        );
        assert!(
            message.contains("explicitly"),
            "expected a hint to pass the path explicitly, got: {message}"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn try_run_gives_an_actionable_message_when_the_mountpoint_does_not_exist() {
        let repo_path = std::env::temp_dir().join("dfs-mount-test-mountpoint-does-not-exist-repo");
        let _ = std::fs::remove_dir_all(&repo_path);
        db::init_repository(
            &repo_path,
            db::RepositorySettings::new(20, 1_700_000_000_000),
        )
        .expect("repository setup for this test must succeed");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-mountpoint-does-not-exist-mnt");
        let _ = std::fs::remove_dir_all(&mountpoint);

        let message = try_run(
            &repo_path,
            &mountpoint,
            false,
            false,
            None,
            RepoOpenOptions {
                assume_read_only_medium: false,
                ram_budget_mb_given: false,
                backpressure_free_zone_bytes_given: false,
                backpressure_slope_divisor_given: false,
            },
            default_tuning(),
        )
        .expect_err("must fail - mountpoint does not exist");
        assert!(
            message.contains("does not exist"),
            "expected an actionable does-not-exist message, got: {message}"
        );

        std::fs::remove_dir_all(&repo_path).expect("test cleanup must succeed");
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_spill_dir_does_not_exist() {
        let repo_path = std::env::temp_dir().join("dfs-mount-test-spill-dir-does-not-exist-repo");
        let _ = std::fs::remove_dir_all(&repo_path);
        db::init_repository(
            &repo_path,
            db::RepositorySettings::new(20, 1_700_000_000_000),
        )
        .expect("repository setup for this test must succeed");
        // Created (not just built as a path), so the platform-specific mountpoint check above
        // this one in try_run - which on Linux requires the mountpoint to already exist - never
        // gets in the way of reaching the spill_dir check this test actually targets.
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-spill-dir-mountpoint");
        std::fs::create_dir_all(&mountpoint).expect("test mountpoint setup must succeed");
        let spill_dir = std::env::temp_dir().join("dfs-mount-test-spill-dir-that-does-not-exist");
        let _ = std::fs::remove_dir_all(&spill_dir);

        // read_write: true - a superfluous-flags refusal (REQ-OPERABILITY-007) would otherwise
        // pre-empt this test's own target (the spill-dir-existence check) for a read-only mount,
        // since --spill-directory only matters together with --read-write.
        let message = try_run(
            &repo_path,
            &mountpoint,
            true,
            false,
            Some(&spill_dir),
            RepoOpenOptions {
                assume_read_only_medium: false,
                ram_budget_mb_given: false,
                backpressure_free_zone_bytes_given: false,
                backpressure_slope_divisor_given: false,
            },
            default_tuning(),
        )
        .expect_err("must fail - spill_dir does not exist");
        assert!(
            message.contains("--spill-directory"),
            "expected the actionable spill-directory message, got: {message}"
        );
        assert!(
            message.contains("does not exist"),
            "expected an actionable does-not-exist message, got: {message}"
        );

        std::fs::remove_dir_all(&repo_path).expect("test cleanup must succeed");
        std::fs::remove_dir_all(&mountpoint).expect("test cleanup must succeed");
    }

    #[test]
    fn try_run_surfaces_the_raw_error_when_an_explicit_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-mount-test-no-explicit-repository-here");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-unused-mountpoint");

        let message = try_run(
            &repo_path,
            &mountpoint,
            false,
            false,
            None,
            RepoOpenOptions {
                assume_read_only_medium: false,
                ram_budget_mb_given: false,
                backpressure_free_zone_bytes_given: false,
                backpressure_slope_divisor_given: false,
            },
            default_tuning(),
        )
        .expect_err("must fail - repo_path holds no repository");
        assert!(
            !message.contains("Pass a repository path"),
            "did not expect the default-path hint for an explicit path, got: {message}"
        );
    }

    #[test]
    fn try_run_read_write_refuses_a_repository_another_process_already_locked_for_writing() {
        let repo_path =
            std::env::temp_dir().join("dfs-mount-test-read-write-refused-while-locked-repo");
        let _ = std::fs::remove_dir_all(&repo_path);
        db::init_repository(
            &repo_path,
            db::RepositorySettings::new(20, 1_700_000_000_000),
        )
        .expect("repository setup for this test must succeed");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-unused-mountpoint-locked");

        // Simulates a second process already holding the write lock - try_run must be refused
        // before ever reaching the (blocking) mountfs::mount call below.
        let _held_elsewhere = db::acquire_write_lock(&repo_path)
            .expect("acquiring the write lock for the first time must succeed");

        let message = try_run(
            &repo_path,
            &mountpoint,
            true,
            false,
            None,
            RepoOpenOptions {
                assume_read_only_medium: false,
                ram_budget_mb_given: false,
                backpressure_free_zone_bytes_given: false,
                backpressure_slope_divisor_given: false,
            },
            default_tuning(),
        )
        .expect_err("must fail - the write lock is already held");
        assert!(
            message.contains("already locked"),
            "expected an actionable already-locked message, got: {message}"
        );

        std::fs::remove_dir_all(&repo_path).expect("test cleanup must succeed");
    }
}
