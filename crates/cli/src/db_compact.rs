//! `dfs db-compact` - REQ-MAINTENANCE-003 in `requirements/functional/maintenance.md`. Compacts a
//! repository's metadata store, without mounting.

use std::path::Path;

fn try_run(repo_path: &Path, default_path_used: bool) -> Result<String, String> {
    let repo = match db::open_repository(repo_path) {
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
    // Held for the rest of this run (DESIGN-MAINTENANCE-001 in
    // `docs/design/repository-locking.md`) - REQ-MAINTENANCE-004's exclusivity applies to
    // compaction like any other mutating operation.
    let _write_lock = db::acquire_write_lock(repo_path).map_err(|err| format!("error: {err}"))?;

    let result = repo.compact().map_err(|err| format!("error: {err}"))?;
    let reclaimed = result.before_bytes.saturating_sub(result.after_bytes);
    Ok(format!(
        "compacted repository metadata: {} bytes -> {} bytes ({reclaimed} bytes reclaimed)",
        result.before_bytes, result.after_bytes
    ))
}

pub fn run(repo_path: &Path, default_path_used: bool) {
    match try_run(repo_path, default_path_used) {
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

    fn setup() -> (db::Repository, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        db::init_repository(
            &repo_root,
            db::RepositorySettings::new(Some(20), 1_700_000_000_000),
        )
        .unwrap();
        let repo = db::open_repository(&repo_root).unwrap();
        (repo, dir)
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-db-compact-test-no-default-repository-here");

        let message =
            try_run(&repo_path, true).expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the actionable default-path message, got: {message}"
        );
    }

    #[test]
    fn try_run_reports_the_before_and_after_size() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false).expect("must succeed");
        assert!(message.contains("compacted repository metadata"));
        assert!(message.contains("bytes reclaimed"));
    }
}
