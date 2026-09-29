//! `dfs migrate-scala-repo` - REQ-MIGRATION-001 through 005 in
//! `requirements/functional/repository-migration.md` (DESIGN-MIGRATION-001 through 007 in
//! `docs/design/scala-migration-tool.md`): imports a Scala-DedupFS `fsc db-backup` SQL export into
//! a small, durable, queryable staging database via `crate::scala_import`, adopts (or reuses) one
//! destination metadata database per requested `--cdc-target-size-bits` value against the existing
//! repository's own, unchanged `data/` directory (DESIGN-MIGRATION-004), then migrates the actual
//! tree and content into every destination via `crate::migrate_content`. Once every destination has
//! been fully migrated, the staging import and each destination's own progress record
//! (DESIGN-MIGRATION-005) are removed - neither is needed again after that point.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::create_repo;
use crate::migrate_content;
use crate::migration_progress;
use crate::scala_import;

fn try_run(
    repository: &Path,
    script: &Path,
    staging: &Path,
    cdc_target_size_bits: &[u32],
) -> Result<String, String> {
    if cdc_target_size_bits.is_empty() {
        return Err("error: at least one --cdc-target-size-bits value is required".to_string());
    }
    let mut seen = BTreeSet::new();
    for &bits in cdc_target_size_bits {
        create_repo::validate_cdc_target_size_bits(bits)?;
        if !seen.insert(bits) {
            return Err(format!(
                "error: --cdc-target-size-bits {bits} was given more than once"
            ));
        }
    }

    let reused = scala_import::is_reusable(staging);
    if !reused {
        let script_text = scala_import::load_script_text(script)
            .map_err(|err| format!("error: failed to read '{}': {err}", script.display()))?;
        scala_import::import(&script_text, staging)
            .map_err(|err| format!("error: failed to import '{}': {err}", script.display()))?;
    }

    let conn = scala_import::open(staging)
        .map_err(|err| format!("error: failed to open '{}': {err}", staging.display()))?;
    let stats = scala_import::stats(&conn).map_err(|err| format!("error: {err}"))?;

    let mut message = format!(
        "{} metadata import at '{}': {} tree entries, {} data entries.",
        if reused { "Reused existing" } else { "Built" },
        staging.display(),
        stats.tree_entries,
        stats.data_entries
    );

    let single = cdc_target_size_bits.len() == 1;
    let mut destinations: Vec<(PathBuf, db::Repository)> = Vec::new();
    for &bits in cdc_target_size_bits {
        let meta_dir = meta_dir_for(repository, bits, single);
        let repo = if meta_dir.is_dir() {
            let repo = db::open_repository_at(&meta_dir).map_err(|err| {
                format!("error: failed to reopen '{}': {err}", meta_dir.display())
            })?;
            let existing_bits = repo.settings().cdc_target_size_bits();
            if existing_bits != bits {
                return Err(format!(
                    "error: '{}' already exists but was created with target size {existing_bits} \
                     bits, not the requested {bits} - remove it first, or pick a different \
                     --cdc-target-size-bits value",
                    meta_dir.display()
                ));
            }
            message.push_str(&format!(
                "\nReusing existing metadata database at '{}' (target size {bits} bits).",
                meta_dir.display()
            ));
            repo
        } else {
            let creation_time_millis = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after the Unix epoch")
                .as_millis() as i64;
            let settings = db::RepositorySettings::new(bits, creation_time_millis);
            db::adopt_repository(repository, &meta_dir, settings)
                .map_err(|err| format!("error: failed to adopt '{}': {err}", meta_dir.display()))?;
            message.push_str(&format!(
                "\nCreated metadata database at '{}' (target size {bits} bits).",
                meta_dir.display()
            ));
            db::open_repository_at(&meta_dir).map_err(|err| {
                format!(
                    "error: failed to open just-adopted '{}': {err}",
                    meta_dir.display()
                )
            })?
        };
        destinations.push((meta_dir, repo));
    }

    if !single {
        message.push_str(&format!(
            "\nMore than one target size was requested: none of the {} metadata databases above \
             is directly usable yet - once you have picked one, rename it to '{}' (or pass its \
             own path directly) before using it with any other dfs command.",
            cdc_target_size_bits.len(),
            db::meta_dir(repository).display()
        ));
    }

    // REQ-MIGRATION-005: only ever read from the shared data/, never written to - the read-only
    // flag below enforces that even against a coding mistake, not just by omission.
    let old_store = store::ByteStore::new(db::data_dir(repository), true);
    let mut progress: Vec<(PathBuf, rusqlite::Connection)> = Vec::new();
    for (meta_dir, _) in &destinations {
        let progress_path = meta_dir.with_extension("progress");
        let progress_conn = migration_progress::open_or_create(&progress_path).map_err(|err| {
            format!(
                "error: failed to open progress record '{}': {err}",
                progress_path.display()
            )
        })?;
        progress.push((progress_path, progress_conn));
    }

    let migration_stats = {
        let targets: Vec<migrate_content::Target> = destinations
            .iter()
            .zip(progress.iter())
            .map(|((_, repo), (_, progress_conn))| migrate_content::Target {
                repo,
                progress: progress_conn,
            })
            .collect();
        migrate_content::migrate(&conn, &old_store, &targets)
            .map_err(|err| format!("error: {err}"))?
    };
    message.push_str(&format!(
        "\nMigrated content: {} new tree entries, {} distinct old contents re-chunked.",
        migration_stats.tree_entries_created, migration_stats.contents_migrated
    ));

    // migrate_content::migrate only ever returns Ok once every destination's whole tree has been
    // walked successfully (it never returns early for a subset of targets) - so reaching here means
    // every destination is now fully migrated, and both the staging import and each destination's
    // own progress record can be removed (DESIGN-MIGRATION-001/005). Best-effort: a cleanup failure
    // does not undo an otherwise-successful migration, only leaves a harmless stale file behind.
    for (progress_path, progress_conn) in progress {
        drop(progress_conn);
        if let Err(err) = migration_progress::remove(&progress_path) {
            message.push_str(&format!(
                "\nWarning: failed to remove progress record '{}': {err}",
                progress_path.display()
            ));
        }
    }
    drop(conn);
    if let Err(err) = std::fs::remove_file(staging) {
        message.push_str(&format!(
            "\nWarning: failed to remove staging database '{}': {err}",
            staging.display()
        ));
    }

    Ok(message)
}

/// `bits`'s destination metadata database location - `repository`'s own conventional `meta/` when
/// exactly one target size was requested (immediately usable by every other command), or a
/// distinguishable, non-conventional location otherwise (DESIGN-MIGRATION-004), since only one
/// destination could ever occupy the conventional name.
fn meta_dir_for(repository: &Path, bits: u32, single: bool) -> PathBuf {
    if single {
        db::meta_dir(repository)
    } else {
        repository.join(format!("meta-{bits}bit"))
    }
}

pub fn run(repository: &Path, script: &Path, staging: &Path, cdc_target_size_bits: &[u32]) {
    match try_run(repository, script, staging, cdc_target_size_bits) {
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

    const SAMPLE_SCRIPT: &str = r#"
CREATE USER IF NOT EXISTS "SA" SALT 'x' HASH 'y' ADMIN;
CREATE CACHED TABLE "PUBLIC"."TREEENTRIES"(
    "ID" BIGINT NOT NULL, "PARENTID" BIGINT NOT NULL, "NAME" CHARACTER VARYING(255) NOT NULL,
    "TIME" BIGINT NOT NULL, "DELETED" BIGINT DEFAULT 0 NOT NULL, "DATAID" BIGINT DEFAULT NULL
);
INSERT INTO "PUBLIC"."TREEENTRIES" VALUES
(0, 0, '', 1000, 0, NULL);
CREATE CACHED TABLE "PUBLIC"."DATAENTRIES"(
    "ID" BIGINT NOT NULL, "SEQ" INTEGER NOT NULL, "LENGTH" BIGINT, "START" BIGINT NOT NULL,
    "STOP" BIGINT NOT NULL, "HASH" BINARY(16)
);
"#;

    /// A fresh temp dir holding a repository root (with an already-existing `data/`, as any real
    /// Scala repository has), a script fixture, and a not-yet-built staging path.
    fn setup() -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let repository = dir.path().join("repository");
        std::fs::create_dir_all(repository.join("data")).unwrap();
        let script = dir.path().join("script.sql");
        std::fs::write(&script, SAMPLE_SCRIPT).unwrap();
        let staging = dir.path().join("staging.db");
        (dir, repository, script, staging)
    }

    #[test]
    fn try_run_builds_a_fresh_staging_database_and_reports_its_counts() {
        let (_dir, repository, script, staging) = setup();
        let message = try_run(&repository, &script, &staging, &[20]).unwrap();
        assert!(message.starts_with("Built"), "got: {message}");
        assert!(
            message.contains("1 tree entries, 0 data entries"),
            "got: {message}"
        );
        assert!(
            !staging.exists(),
            "a fully successful migration must remove the now-unneeded staging database"
        );
    }

    /// A script with one real content reference (`dataId` 5) whose bytes are never actually
    /// written anywhere in this test's `data/` - phase 2 always fails reading it, which is exactly
    /// what the reuse test below needs: a run that gets past phase 1 (durably building the staging
    /// database) but never reaches the success-only cleanup that would remove it again.
    const SCRIPT_WITH_UNREADABLE_CONTENT: &str = r#"
CREATE CACHED TABLE "PUBLIC"."TREEENTRIES"(
    "ID" BIGINT NOT NULL, "PARENTID" BIGINT NOT NULL, "NAME" CHARACTER VARYING(255) NOT NULL,
    "TIME" BIGINT NOT NULL, "DELETED" BIGINT DEFAULT 0 NOT NULL, "DATAID" BIGINT DEFAULT NULL
);
INSERT INTO "PUBLIC"."TREEENTRIES" VALUES
(0, 0, '', 1000, 0, NULL),
(1, 0, 'b.txt', 1002, 0, 5);
CREATE CACHED TABLE "PUBLIC"."DATAENTRIES"(
    "ID" BIGINT NOT NULL, "SEQ" INTEGER NOT NULL, "LENGTH" BIGINT, "START" BIGINT NOT NULL,
    "STOP" BIGINT NOT NULL, "HASH" BINARY(16)
);
INSERT INTO "PUBLIC"."DATAENTRIES" VALUES
(5, 1, 3, 100, 103, X'0102030405060708090a0b0c0d0e0f10');
"#;

    #[test]
    fn try_run_reuses_an_already_built_staging_database_without_rereading_the_script() {
        let (_dir, repository, script, staging) = setup();
        std::fs::write(&script, SCRIPT_WITH_UNREADABLE_CONTENT).unwrap();

        let first_error = try_run(&repository, &script, &staging, &[20])
            .expect_err("phase 2 must fail - dataId 5's bytes do not actually exist");
        assert!(first_error.contains("dataId 5"), "got: {first_error}");
        assert!(
            staging.exists(),
            "a failed phase 2 must leave phase 1's staging database in place"
        );

        // A script that no longer exists must not matter the second time - a genuinely reused
        // import never needs to read it again.
        std::fs::remove_file(&script).unwrap();
        let second_error = try_run(&repository, &script, &staging, &[20])
            .expect_err("must still fail the same way");
        assert!(second_error.contains("dataId 5"), "got: {second_error}");
    }

    #[test]
    fn try_run_reports_an_actionable_message_for_a_missing_script() {
        let (dir, repository, _script, staging) = setup();
        let missing = dir.path().join("does-not-exist.sql");
        let message = try_run(&repository, &missing, &staging, &[20])
            .expect_err("must fail - the script is missing");
        assert!(message.contains("does-not-exist.sql"), "got: {message}");
    }

    #[test]
    fn try_run_adopts_a_single_target_size_at_the_conventional_meta_location() {
        let (_dir, repository, script, staging) = setup();
        let message = try_run(&repository, &script, &staging, &[18]).unwrap();
        assert!(message.contains("Created metadata database"), "{message}");
        assert!(!message.contains("rename"), "{message}");

        let repo = db::open_repository_at(&db::meta_dir(&repository)).unwrap();
        assert_eq!(repo.settings().cdc_target_size_bits(), 18);
    }

    #[test]
    fn try_run_reuses_an_already_adopted_destination_on_a_second_run() {
        let (_dir, repository, script, staging) = setup();
        try_run(&repository, &script, &staging, &[18]).unwrap();

        let message = try_run(&repository, &script, &staging, &[18]).unwrap();
        assert!(
            message.contains("Reusing existing metadata database"),
            "{message}"
        );
    }

    #[test]
    fn try_run_rejects_a_mismatched_target_size_on_an_already_adopted_destination() {
        let (_dir, repository, script, staging) = setup();
        try_run(&repository, &script, &staging, &[18]).unwrap();

        let message = try_run(&repository, &script, &staging, &[20])
            .expect_err("must fail - the existing destination was created with a different size");
        assert!(message.contains("18 bits"), "{message}");
        assert!(message.contains("20"), "{message}");
    }

    #[test]
    fn try_run_adopts_several_target_sizes_at_distinguishable_locations_and_prints_a_rename_hint() {
        let (_dir, repository, script, staging) = setup();
        let message = try_run(&repository, &script, &staging, &[18, 20]).unwrap();

        assert!(!db::meta_dir(&repository).exists(), "{message}");
        db::open_repository_at(&repository.join("meta-18bit")).unwrap();
        db::open_repository_at(&repository.join("meta-20bit")).unwrap();
        assert!(message.contains("none of the 2"), "{message}");
    }

    #[test]
    fn try_run_rejects_a_repository_with_no_data_directory() {
        let dir = tempfile::tempdir().unwrap();
        let repository = dir.path().join("repository-without-data");
        std::fs::create_dir_all(&repository).unwrap();
        let script = dir.path().join("script.sql");
        std::fs::write(&script, SAMPLE_SCRIPT).unwrap();
        let staging = dir.path().join("staging.db");

        let message = try_run(&repository, &script, &staging, &[20])
            .expect_err("must fail - there is no data/ directory to adopt");
        assert!(message.contains("data"), "{message}");
    }

    #[test]
    fn try_run_rejects_duplicate_target_size_values() {
        let (_dir, repository, script, staging) = setup();
        let message = try_run(&repository, &script, &staging, &[18, 18])
            .expect_err("must fail - 18 was given twice");
        assert!(message.contains("more than once"), "{message}");
    }

    #[test]
    fn try_run_rejects_an_out_of_range_target_size_value() {
        let (_dir, repository, script, staging) = setup();
        let message = try_run(&repository, &script, &staging, &[24])
            .expect_err("must fail - 24 exceeds the 23-bit ceiling");
        assert!(message.contains("too large"), "{message}");
    }

    #[test]
    fn try_run_rejects_an_empty_target_size_list() {
        let (_dir, repository, script, staging) = setup();
        let message =
            try_run(&repository, &script, &staging, &[]).expect_err("must fail - none given");
        assert!(message.contains("at least one"), "{message}");
    }
}
