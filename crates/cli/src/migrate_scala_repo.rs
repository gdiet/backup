//! `dfs migrate-scala-repo` - REQ-MIGRATION-001 through 005 in
//! `requirements/functional/repository-migration.md` (DESIGN-MIGRATION-001 through 004 in
//! `docs/design/scala-migration-tool.md`): imports a Scala-DedupFS `fsc db-backup` SQL export into
//! a small, durable, queryable staging database via `crate::scala_import`, then adopts (or reuses)
//! one destination metadata database per requested `--cdc-target-size-bits` value against the
//! existing repository's own, unchanged `data/` directory (DESIGN-MIGRATION-004).
//!
//! The actual content migration (walking the imported tree, re-chunking and re-hashing each
//! distinct old content reference, and writing the result into each destination -
//! DESIGN-MIGRATION-006) is not implemented yet - this command only builds (or reuses) the staging
//! database, sets up (or reuses) each destination, and reports what it found.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::create_repo;
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
    for &bits in cdc_target_size_bits {
        let meta_dir = meta_dir_for(repository, bits, single);
        if meta_dir.is_dir() {
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
        }
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

    message.push_str(
        "\nPhase 2's actual content migration (walking the tree and re-chunking content) is not \
         implemented yet - see docs/design/scala-migration-tool.md.",
    );
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
    }

    #[test]
    fn try_run_reuses_an_already_complete_staging_database_without_rereading_the_script() {
        let (_dir, repository, script, staging) = setup();
        try_run(&repository, &script, &staging, &[20]).unwrap();

        // A script that no longer exists must not matter the second time - a genuinely reused
        // import never needs to read it again.
        std::fs::remove_file(&script).unwrap();
        let message = try_run(&repository, &script, &staging, &[20]).unwrap();
        assert!(message.starts_with("Reused"), "got: {message}");
        assert!(
            message.contains("1 tree entries, 0 data entries"),
            "got: {message}"
        );
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
