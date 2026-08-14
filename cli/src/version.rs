//! `backup version` - also supplies the one-line summary shown by `-V`/`--version` (wired up in
//! `main.rs`), so both spellings of "what version is this" agree. See
//! `docs/plans/version-command.md`.

use std::path::Path;
use std::process::ExitCode;

/// `<app version> (<git hash>, <commit date>)`, e.g. `0.1.0 (a1b2c3d4, 2026-08-14)` - what
/// `Command::version` in `main.rs` is set to (clap prepends the app name itself for `-V`/
/// `--version`, so this deliberately excludes it - [`version_line`] is what has it, for `version`'s
/// own `println!`, which doesn't go through clap's formatting). The hash/date come from `build.rs`
/// (`BACKUP_GIT_HASH`/`BACKUP_GIT_COMMIT_DATE`, embedded at compile time) - `"unknown"` if `git`
/// wasn't available at build time.
pub fn version_number() -> String {
    format!(
        "{} ({}, {})",
        env!("CARGO_PKG_VERSION"),
        env!("BACKUP_GIT_HASH"),
        env!("BACKUP_GIT_COMMIT_DATE"),
    )
}

/// `backup <version_number()>`, e.g. `backup 0.1.0 (a1b2c3d4, 2026-08-14)` - matches what `-V`/
/// `--version` prints (see [`version_number`]'s doc comment), so both spellings agree.
pub fn version_line() -> String {
    format!("backup {}", version_number())
}

/// `version_line()` plus the schema version this build supports and, best-effort, `repo`'s own
/// current schema version - a side-by-side comparison for diagnosing an
/// [`db::Error::SchemaTooNew`] without having to hit the error first. Never fails: a repo that
/// doesn't exist or can't be opened just gets an explanatory line instead of a number, same as any
/// other read command would report, but `version` itself always succeeds.
pub fn run_version(repo: &Path) -> ExitCode {
    println!("{}", version_line());
    println!("schema version supported: {}", db::CURRENT_SCHEMA_VERSION);
    println!("{}", repository_schema_line(repo));
    ExitCode::SUCCESS
}

fn repository_schema_line(repo: &Path) -> String {
    match db::open_repository_read_only(repo) {
        Ok(repository) => match repository.open_read_connection() {
            Ok(conn) => {
                match conn.pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0)) {
                    Ok(version) => format!("repository schema ({}): {version}", repo.display()),
                    Err(err) => {
                        format!(
                            "repository schema ({}): unavailable ({err})",
                            repo.display()
                        )
                    }
                }
            }
            Err(err) => format!(
                "repository schema ({}): unavailable ({err})",
                repo.display()
            ),
        },
        Err(db::Error::SchemaTooNew { db_version, .. }) => format!(
            "repository schema ({}): {db_version} (newer than this build supports)",
            repo.display()
        ),
        Err(err) => format!(
            "repository schema ({}): unavailable ({err})",
            repo.display()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_line_includes_the_cargo_package_version() {
        assert!(version_line().starts_with(&format!("backup {}", env!("CARGO_PKG_VERSION"))));
    }

    #[test]
    fn repository_schema_line_reports_the_actual_version_for_an_initialized_repo() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        let settings = db::RepositorySettings::new(20, db::Chunking::Cdc).unwrap();
        db::init_repository(&repo_root, &settings).unwrap();

        let line = repository_schema_line(&repo_root);

        assert_eq!(
            line,
            format!(
                "repository schema ({}): {}",
                repo_root.display(),
                db::CURRENT_SCHEMA_VERSION
            )
        );
    }

    #[test]
    fn repository_schema_line_is_best_effort_for_a_missing_repo() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("does-not-exist");

        let line = repository_schema_line(&repo_root);

        assert!(
            line.starts_with(&format!(
                "repository schema ({}): unavailable",
                repo_root.display()
            )),
            "{line}"
        );
    }

    #[test]
    fn repository_schema_line_reports_both_numbers_when_the_schema_is_too_new() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        let settings = db::RepositorySettings::new(20, db::Chunking::Cdc).unwrap();
        db::init_repository(&repo_root, &settings).unwrap();
        let conn =
            rusqlite::Connection::open(db::db_file_path(&repo_root)).expect("db file exists");
        conn.pragma_update(None, "user_version", 99).unwrap();
        drop(conn);

        let line = repository_schema_line(&repo_root);

        assert_eq!(
            line,
            format!(
                "repository schema ({}): 99 (newer than this build supports)",
                repo_root.display()
            )
        );
    }
}
