//! `dfs migrate-scala-repo` - the metadata-import phase of REQ-MIGRATION-001 through 004 in
//! `requirements/functional/repository-migration.md` (DESIGN-MIGRATION-001/002 in
//! `docs/design/scala-migration-tool.md`): imports a Scala-DedupFS `fsc db-backup` SQL export into
//! a small, durable, queryable staging database via `crate::scala_import`.
//!
//! Phase 2 (walking the imported tree, re-chunking and re-hashing each distinct old content
//! reference, and writing the result into one or more new repositories) is not implemented yet -
//! this command only builds (or reuses) the staging database and reports what it found.

use std::path::Path;

use crate::scala_import;

fn try_run(script: &Path, staging: &Path) -> Result<String, String> {
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

    Ok(format!(
        "{} metadata import at '{}': {} tree entries, {} data entries.\n\
         Phase 2 (actual content migration) is not implemented yet - see \
         docs/design/scala-migration-tool.md.",
        if reused { "Reused existing" } else { "Built" },
        staging.display(),
        stats.tree_entries,
        stats.data_entries
    ))
}

pub fn run(script: &Path, staging: &Path) {
    match try_run(script, staging) {
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

    fn setup() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("script.sql");
        std::fs::write(&script, SAMPLE_SCRIPT).unwrap();
        let staging = dir.path().join("staging.db");
        (dir, script, staging)
    }

    #[test]
    fn try_run_builds_a_fresh_staging_database_and_reports_its_counts() {
        let (_dir, script, staging) = setup();
        let message = try_run(&script, &staging).unwrap();
        assert!(message.starts_with("Built"), "got: {message}");
        assert!(
            message.contains("1 tree entries, 0 data entries"),
            "got: {message}"
        );
    }

    #[test]
    fn try_run_reuses_an_already_complete_staging_database_without_rereading_the_script() {
        let (_dir, script, staging) = setup();
        try_run(&script, &staging).unwrap();

        // A script that no longer exists must not matter the second time - a genuinely reused
        // import never needs to read it again.
        std::fs::remove_file(&script).unwrap();
        let message = try_run(&script, &staging).unwrap();
        assert!(message.starts_with("Reused"), "got: {message}");
        assert!(
            message.contains("1 tree entries, 0 data entries"),
            "got: {message}"
        );
    }

    #[test]
    fn try_run_reports_an_actionable_message_for_a_missing_script() {
        let (dir, _script, staging) = setup();
        let missing = dir.path().join("does-not-exist.sql");
        let message = try_run(&missing, &staging).expect_err("must fail - the script is missing");
        assert!(message.contains("does-not-exist.sql"), "got: {message}");
    }
}
