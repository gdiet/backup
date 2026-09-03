//! `dfs reclaim` - REQ-STORAGE-004 in `requirements/functional/storage.md`. Bulk-purges every
//! soft-deleted entry that has stayed soft-deleted for at least a caller-chosen minimum age,
//! reclaiming the storage each purge frees along the way, without mounting.

use std::path::Path;

const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

fn try_run(
    repo_path: &Path,
    default_path_used: bool,
    min_age_days: u32,
    now_millis: i64,
) -> Result<String, String> {
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
    // `docs/design/repository-locking.md`) - REQ-MAINTENANCE-004's exclusivity applies to reclaim
    // like any other mutating operation.
    let _write_lock = db::acquire_write_lock(repo_path).map_err(|err| format!("error: {err}"))?;

    let min_age_millis = i64::from(min_age_days) * MILLIS_PER_DAY;
    let result = repo
        .reclaim(now_millis, min_age_millis)
        .map_err(|err| format!("error: {err}"))?;
    Ok(format!(
        "reclaimed {} soft-deleted entries, {} bytes",
        result.purged, result.reclaimed_bytes
    ))
}

pub fn run(repo_path: &Path, default_path_used: bool, min_age_days: u32, now_millis: i64) {
    match try_run(repo_path, default_path_used, min_age_days, now_millis) {
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

    fn create_and_delete_file(repo: &db::Repository, deleted_at: i64) {
        let (chunk_id, _ranges) = repo.reserve_and_insert_chunk(10, &[0xAA; 20]).unwrap();
        let content_id = repo
            .find_or_create_content(10, &[0xBB; 20], &[chunk_id])
            .unwrap();
        let id = repo
            .settle_file(0, "a.txt", 1_700_000_000_000, content_id)
            .unwrap();
        repo.unlink_file(id, deleted_at).unwrap();
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-reclaim-test-no-default-repository-here");

        let message = try_run(&repo_path, true, 0, 1_700_000_100_000)
            .expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the actionable default-path message, got: {message}"
        );
    }

    #[test]
    fn try_run_reclaims_an_aged_soft_deleted_entry_and_reports_bytes_freed() {
        let (repo, dir) = setup();
        create_and_delete_file(&repo, 1_700_000_100_000);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message =
            try_run(&repo_root, false, 0, 1_700_000_200_000).expect("reclaim must succeed");
        assert!(
            message.contains("reclaimed 1 soft-deleted entries"),
            "got: {message}"
        );
        assert!(message.contains("10 bytes"), "got: {message}");

        let repo = db::open_repository(&repo_root).unwrap();
        assert!(
            crate::deleted::resolve(&repo, "/[deleted]/a.txt")
                .unwrap()
                .is_none(),
            "a reclaimed entry must no longer be addressable at all"
        );
    }

    #[test]
    fn try_run_skips_an_entry_younger_than_the_minimum_age() {
        let (repo, dir) = setup();
        create_and_delete_file(&repo, 1_700_000_100_000);
        drop(repo);
        let repo_root = dir.path().join("repo");

        // Deleted only 100_000ms before "now" - well under a 1-day minimum age.
        let message =
            try_run(&repo_root, false, 1, 1_700_000_200_000).expect("reclaim must succeed");
        assert!(
            message.contains("reclaimed 0 soft-deleted entries"),
            "got: {message}"
        );

        let repo = db::open_repository(&repo_root).unwrap();
        assert!(
            matches!(
                crate::deleted::resolve(&repo, "/[deleted]/a.txt").unwrap(),
                Some(crate::deleted::Resolved::Deleted(_))
            ),
            "an entry younger than the minimum age must be left alone"
        );
    }
}
