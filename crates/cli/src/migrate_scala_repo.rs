//! `dfs migrate-scala-repo` - REQ-MIGRATION-001 through 006 in
//! `requirements/functional/repository-migration.md` (DESIGN-MIGRATION-001 through 008 in
//! `docs/design/scala-migration-tool.md`): imports a Scala-DedupFS `fsc db-backup` SQL export into
//! a small, durable, queryable staging database via `crate::scala_import`, adopts (or reuses) one
//! destination metadata database per requested `--cdc-target-size-bits` value against the existing
//! repository's own, unchanged `data/` directory (DESIGN-MIGRATION-004), then migrates the actual
//! tree and content into every destination via `crate::migrate_content`. Once every destination has
//! been fully migrated, the staging import is removed, and so are the progress tables each
//! destination kept while it was being migrated (DESIGN-MIGRATION-005) - neither is needed again.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::create_repo;
use crate::migrate_content;
use crate::scala_import;

fn try_run(
    repository: &Path,
    script: &Path,
    staging: &Path,
    cdc_target_size_bits: &[u32],
    tolerate_missing_data: bool,
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
             is directly usable yet - every other dfs command always looks for its repository's \
             metadata at '{}', so rename your chosen one there before using it with any other dfs \
             command.",
            cdc_target_size_bits.len(),
            db::meta_dir(repository).display()
        ));
    }

    // REQ-MIGRATION-005: only ever read from the shared data/, never written to - the read-only
    // flag below enforces that even against a coding mistake, not just by omission.
    let old_store = store::ByteStore::new(db::data_dir(repository), true);

    // A destination an earlier run already migrated completely (its progress tables are gone, its
    // tree is not empty) is left alone; every other one gets its progress tables and is migrated.
    let mut pending: Vec<&db::Repository> = Vec::new();
    for (meta_dir, repo) in &destinations {
        let needs_migration = repo.migration_prepare().map_err(|err| {
            format!(
                "error: failed to prepare '{}' for migration: {err}",
                meta_dir.display()
            )
        })?;
        if needs_migration {
            pending.push(repo);
        } else {
            message.push_str(&format!(
                "\n'{}' was already fully migrated by an earlier run - left as it is.",
                meta_dir.display()
            ));
        }
    }

    if !pending.is_empty() {
        let migration_stats = migrate_content::migrate(
            &conn,
            &old_store,
            &pending,
            migrate_content::Settings::new(tolerate_missing_data),
        )
        .map_err(|err| format!("error: {err}"))?;
        message.push_str(&format!(
            "\nMigrated content: {} new tree entries, {} distinct old contents re-chunked.",
            migration_stats.tree_entries_created, migration_stats.contents_migrated
        ));
        message.push_str(&missing_data_report(repository, &conn, &pending)?);

        // migrate_content::migrate only ever returns Ok once every pending destination's whole
        // tree has been walked and committed - so reaching here means each is fully migrated, and
        // its progress tables can go (DESIGN-MIGRATION-005). Best-effort: a failure here does not
        // undo an otherwise-successful migration, and a later run just finds nothing left to do
        // and drops them then.
        for repo in &pending {
            if let Err(err) = repo.migration_finish() {
                message.push_str(&format!(
                    "\nWarning: failed to drop a destination's migration progress tables: {err}"
                ));
            }
        }
    }

    // Every destination is fully migrated now, so the staging import is not needed again
    // (DESIGN-MIGRATION-001) - removed best-effort for the same reason.
    drop(conn);
    if let Err(err) = std::fs::remove_file(staging) {
        message.push_str(&format!(
            "\nWarning: failed to remove staging database '{}': {err}",
            staging.display()
        ));
    }

    Ok(message)
}

/// Where the full list of contents with missing old data is written (DESIGN-MIGRATION-008).
const MISSING_DATA_REPORT: &str = "migrate-missing-data.txt";
/// How many of those contents the printed message itself lists.
const REPORT_LINES_SHOWN: usize = 20;

/// What to tell the operator about contents whose old data was missing and tolerated
/// (DESIGN-MIGRATION-008) - empty text if there were none. The full list goes to a file in the
/// repository root, since the progress tables that hold it are dropped once the migration is
/// finished, and only the first few contents fit in a message.
///
/// Must run before the destinations drop those tables. Contents noted by an earlier, interrupted
/// run are included: they are stored with the destination, not held in memory.
fn missing_data_report(
    repository: &Path,
    staging_conn: &rusqlite::Connection,
    destinations: &[&db::Repository],
) -> Result<String, String> {
    let mut damaged: BTreeMap<i64, String> = BTreeMap::new();
    for repo in destinations {
        let listed = repo
            .migration_damaged()
            .map_err(|err| format!("error: failed to read the list of damaged contents: {err}"))?;
        for (data_id, detail) in listed {
            damaged.entry(data_id).or_insert(detail);
        }
    }
    if damaged.is_empty() {
        return Ok(String::new());
    }

    let mut lines = Vec::with_capacity(damaged.len());
    for (data_id, detail) in &damaged {
        let used_by = scala_import::staging_paths_for_data_id(
            staging_conn,
            *data_id,
            migrate_content::PATHS_SHOWN,
        )
        .map_err(|err| format!("error: {err}"))?;
        lines.push(format!(
            "dataId {data_id}: missing {detail}; used by: {}",
            used_by.join(", ")
        ));
    }
    let report_path = repository.join(MISSING_DATA_REPORT);
    let report_note = match std::fs::write(&report_path, lines.join("\n") + "\n") {
        Ok(()) => format!("The full list is in '{}'.", report_path.display()),
        Err(err) => format!(
            "The full list could not be written to '{}': {err}",
            report_path.display()
        ),
    };

    let mut text = format!(
        "\nWARNING: {} old content(s) had missing data. Zeros were assumed for the missing bytes. \
         Reading these files through dfs still reports the missing data (`dfs restore` fails them \
         unless run with --best-effort) until the missing data files are restored. {report_note}",
        damaged.len()
    );
    for line in lines.iter().take(REPORT_LINES_SHOWN) {
        text.push_str("\n  ");
        text.push_str(line);
    }
    if lines.len() > REPORT_LINES_SHOWN {
        text.push_str(&format!(
            "\n  ... and {} more",
            lines.len() - REPORT_LINES_SHOWN
        ));
    }
    Ok(text)
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

/// `--staging`'s default location when not given explicitly: inside the repository being adopted,
/// alongside its `data/` directory - the operator does not need to think about where to put a file
/// that is, by design, removed again automatically once every requested target size has been fully
/// migrated (DESIGN-MIGRATION-001).
fn default_staging_path(repository: &Path) -> PathBuf {
    repository.join("migrate-staging.db")
}

pub fn run(
    repository: &Path,
    script: &Path,
    staging: Option<&Path>,
    cdc_target_size_bits: &[u32],
    tolerate_missing_data: bool,
) {
    let default_staging;
    let staging = match staging {
        Some(path) => path,
        None => {
            default_staging = default_staging_path(repository);
            &default_staging
        }
    };
    match try_run(
        repository,
        script,
        staging,
        cdc_target_size_bits,
        tolerate_missing_data,
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

    #[test]
    fn default_staging_path_lives_inside_the_repository() {
        let repository = Path::new("/some/repo");
        assert_eq!(
            default_staging_path(repository),
            Path::new("/some/repo/migrate-staging.db")
        );
    }

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
        let message = try_run(&repository, &script, &staging, &[20], false).unwrap();
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

        let first_error = try_run(&repository, &script, &staging, &[20], false)
            .expect_err("phase 2 must fail - dataId 5's bytes do not actually exist");
        assert!(first_error.contains("dataId 5"), "got: {first_error}");
        assert!(
            staging.exists(),
            "a failed phase 2 must leave phase 1's staging database in place"
        );

        // A script that no longer exists must not matter the second time - a genuinely reused
        // import never needs to read it again.
        std::fs::remove_file(&script).unwrap();
        let second_error = try_run(&repository, &script, &staging, &[20], false)
            .expect_err("must still fail the same way");
        assert!(second_error.contains("dataId 5"), "got: {second_error}");
    }

    #[test]
    fn try_run_reports_an_actionable_message_for_a_missing_script() {
        let (dir, repository, _script, staging) = setup();
        let missing = dir.path().join("does-not-exist.sql");
        let message = try_run(&repository, &missing, &staging, &[20], false)
            .expect_err("must fail - the script is missing");
        assert!(message.contains("does-not-exist.sql"), "got: {message}");
    }

    #[test]
    fn try_run_names_the_option_the_missing_data_and_the_affected_path_when_data_is_missing() {
        let (_dir, repository, script, staging) = setup();
        std::fs::write(&script, SCRIPT_WITH_UNREADABLE_CONTENT).unwrap();

        let message = try_run(&repository, &script, &staging, &[20], false)
            .expect_err("dataId 5's bytes do not exist and nothing is tolerated");
        assert!(message.contains("--tolerate-missing-data"), "{message}");
        assert!(message.contains("data/"), "{message}");
        assert!(message.contains("/b.txt"), "{message}");
    }

    #[test]
    fn try_run_with_tolerance_warns_lists_the_gap_in_a_report_file_and_still_finishes() {
        let (_dir, repository, script, staging) = setup();
        std::fs::write(&script, SCRIPT_WITH_UNREADABLE_CONTENT).unwrap();

        let message = try_run(&repository, &script, &staging, &[20], true)
            .expect("the gap is tolerated, so the migration must complete");
        assert!(message.contains("WARNING"), "{message}");
        assert!(message.contains("dataId 5"), "{message}");
        assert!(message.contains("/b.txt"), "{message}");

        let report = std::fs::read_to_string(repository.join("migrate-missing-data.txt")).unwrap();
        assert!(
            report.contains("dataId 5") && report.contains("/b.txt"),
            "{report}"
        );
        assert!(
            !staging.exists(),
            "a finished migration removes its staging file, gaps or not"
        );
        let repo = db::open_repository(&repository).unwrap();
        assert!(repo.resolve_path("/b.txt").unwrap().is_some());
    }

    #[test]
    fn try_run_writes_no_report_when_nothing_was_missing() {
        let (_dir, repository, script, staging) = setup();
        let message = try_run(&repository, &script, &staging, &[20], true).unwrap();
        assert!(!message.contains("WARNING"), "{message}");
        assert!(!repository.join("migrate-missing-data.txt").exists());
    }

    #[test]
    fn try_run_adopts_a_single_target_size_at_the_conventional_meta_location() {
        let (_dir, repository, script, staging) = setup();
        let message = try_run(&repository, &script, &staging, &[18], false).unwrap();
        assert!(message.contains("Created metadata database"), "{message}");
        assert!(!message.contains("rename"), "{message}");

        let repo = db::open_repository_at(&db::meta_dir(&repository)).unwrap();
        assert_eq!(repo.settings().cdc_target_size_bits(), 18);
    }

    #[test]
    fn try_run_reuses_an_already_adopted_destination_on_a_second_run() {
        let (_dir, repository, script, staging) = setup();
        try_run(&repository, &script, &staging, &[18], false).unwrap();

        let message = try_run(&repository, &script, &staging, &[18], false).unwrap();
        assert!(
            message.contains("Reusing existing metadata database"),
            "{message}"
        );
    }

    #[test]
    fn try_run_rejects_a_mismatched_target_size_on_an_already_adopted_destination() {
        let (_dir, repository, script, staging) = setup();
        try_run(&repository, &script, &staging, &[18], false).unwrap();

        let message = try_run(&repository, &script, &staging, &[20], false)
            .expect_err("must fail - the existing destination was created with a different size");
        assert!(message.contains("18 bits"), "{message}");
        assert!(message.contains("20"), "{message}");
    }

    #[test]
    fn try_run_adopts_several_target_sizes_at_distinguishable_locations_and_prints_a_rename_hint() {
        let (_dir, repository, script, staging) = setup();
        let message = try_run(&repository, &script, &staging, &[18, 20], false).unwrap();

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

        let message = try_run(&repository, &script, &staging, &[20], false)
            .expect_err("must fail - there is no data/ directory to adopt");
        assert!(message.contains("data"), "{message}");
    }

    #[test]
    fn try_run_rejects_duplicate_target_size_values() {
        let (_dir, repository, script, staging) = setup();
        let message = try_run(&repository, &script, &staging, &[18, 18], false)
            .expect_err("must fail - 18 was given twice");
        assert!(message.contains("more than once"), "{message}");
    }

    #[test]
    fn try_run_rejects_an_out_of_range_target_size_value() {
        let (_dir, repository, script, staging) = setup();
        let message = try_run(&repository, &script, &staging, &[24], false)
            .expect_err("must fail - 24 exceeds the 23-bit ceiling");
        assert!(message.contains("too large"), "{message}");
    }

    #[test]
    fn try_run_rejects_an_empty_target_size_list() {
        let (_dir, repository, script, staging) = setup();
        let message = try_run(&repository, &script, &staging, &[], false)
            .expect_err("must fail - none given");
        assert!(message.contains("at least one"), "{message}");
    }
}
