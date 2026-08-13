use std::path::Path;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Args;

use crate::repo_lock::RepoLock;

const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

#[derive(Args)]
pub struct ReclaimSpaceArgs {
    /// Preserve entries deleted more recently than this many days ago.
    #[arg(long, default_value_t = 0)]
    keep_days: u32,

    /// Skip the automatic database backup normally taken before this
    /// (unlike `del`, genuinely irreversible) operation.
    #[arg(long)]
    no_backup: bool,

    /// How long to wait, in seconds, for the repository's lock to become
    /// free if another `store`/`mount --read-write`/`compact-store`/
    /// `reclaim-space` run already holds it, before giving up. Default:
    /// don't wait, fail immediately.
    #[arg(long = "lock-wait", default_value_t = 0)]
    lock_wait_secs: u64,
}

/// Hard-deletes soft-deleted entries older than `--keep-days` and sweeps
/// orphaned chunks/contents - see `db::reclaim_space`'s doc comment for why
/// this is safe as a few plain SQL statements here, unlike the Scala tool
/// this replaces, which needs a manual orphan scan and an iterative repair
/// pass for exactly the partially-deleted-subtree states `del`'s atomic
/// recursive soft-delete never produces in the first place.
///
/// Backs up the database first by default (matching the Scala tool's
/// `reclaimSpace`, the one default worth keeping - this is the one genuinely
/// irreversible operation of the whole command set; `del` alone never is).
pub fn run_reclaim_space(repo: &Path, args: ReclaimSpaceArgs) -> ExitCode {
    if !args.no_backup {
        let exit = crate::db_maintenance::run_backup(repo);
        if exit != ExitCode::SUCCESS {
            eprintln!("error: aborting reclaim-space: backup failed");
            return exit;
        }
    }

    let repository = match db::open_repository(repo) {
        Ok(r) => r,
        Err(err) => {
            eprintln!(
                "error: failed to open repository at {}: {err}",
                repo.display()
            );
            return ExitCode::FAILURE;
        }
    };

    // Exclusive against every other command that physically
    // allocates/relocates store bytes or changes what byte ranges are free
    // to reuse (`store`, `mount --read-write`, `compact-store`, and a
    // second `reclaim-space`) - see
    // `docs/plans/cross-process-repository-locking.md`.
    let _lock = match RepoLock::acquire(
        &db::meta_dir(repo),
        Duration::from_secs(args.lock_wait_secs),
    ) {
        Ok(Some(lock)) => lock,
        Ok(None) => {
            eprintln!(
                "error: another command is already running against this repository \
                 (meta/.lock is held) - try again once it finishes, or pass --lock-wait to wait"
            );
            return ExitCode::FAILURE;
        }
        Err(err) => {
            eprintln!("error: failed to acquire the repository lock: {err}");
            return ExitCode::FAILURE;
        }
    };

    let mut conn = match repository.open_write_connection() {
        Ok(c) => c,
        Err(err) => {
            eprintln!("error: failed to open the metadata database: {err}");
            return ExitCode::FAILURE;
        }
    };

    let cutoff = now_millis() - i64::from(args.keep_days) * MILLIS_PER_DAY;
    match db::reclaim_space(&mut conn, cutoff) {
        Ok(stats) => {
            println!(
                "Purged {} tree entry/entries, {} content(s), {} chunk(s).",
                stats.tree_entries_purged, stats.contents_purged, stats.chunks_purged
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn init_repo() -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            &db::RepositorySettings::new(12, db::Chunking::Cdc).unwrap(),
        )
        .unwrap();
        (temp_dir, repo_root)
    }

    #[test]
    fn purges_old_soft_deleted_entries_and_backs_up_first_by_default() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind, deleted_at)
             VALUES (1, 0, 'a.txt', 0, 'file', 1)",
            (),
        )
        .unwrap();
        drop(conn);

        let exit = run_reclaim_space(
            &repo_root,
            ReclaimSpaceArgs {
                keep_days: 0,
                no_backup: false,
                lock_wait_secs: 0,
            },
        );

        assert_eq!(exit, ExitCode::SUCCESS);
        let backups_dir = db::meta_dir(&repo_root).join("backups");
        assert_eq!(std::fs::read_dir(&backups_dir).unwrap().count(), 1);
        let conn = db::open_repository(&repo_root)
            .unwrap()
            .open_read_connection()
            .unwrap();
        let remaining: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tree_entries WHERE id = 1",
                (),
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 0);
    }

    #[test]
    fn no_backup_flag_skips_the_backup() {
        let (_temp_dir, repo_root) = init_repo();

        assert_eq!(
            run_reclaim_space(
                &repo_root,
                ReclaimSpaceArgs {
                    keep_days: 0,
                    no_backup: true,
                    lock_wait_secs: 0,
                }
            ),
            ExitCode::SUCCESS
        );
        let backups_dir = db::meta_dir(&repo_root).join("backups");
        assert!(!backups_dir.exists());
    }

    #[test]
    fn keep_days_preserves_recently_deleted_entries() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        let now = now_millis();
        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind, deleted_at)
             VALUES (1, 0, 'a.txt', 0, 'file', ?1)",
            [now],
        )
        .unwrap();
        drop(conn);

        assert_eq!(
            run_reclaim_space(
                &repo_root,
                ReclaimSpaceArgs {
                    keep_days: 30,
                    no_backup: true,
                    lock_wait_secs: 0,
                }
            ),
            ExitCode::SUCCESS
        );

        let conn = db::open_repository(&repo_root)
            .unwrap()
            .open_read_connection()
            .unwrap();
        let remaining: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tree_entries WHERE id = 1",
                (),
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 1, "just deleted, well within a 30-day window");
    }

    #[test]
    fn run_reclaim_space_refuses_when_the_lock_is_already_held() {
        let (_temp_dir, repo_root) = init_repo();
        let _lock = RepoLock::acquire(&db::meta_dir(&repo_root), Duration::ZERO)
            .unwrap()
            .unwrap();

        assert_eq!(
            run_reclaim_space(
                &repo_root,
                ReclaimSpaceArgs {
                    keep_days: 0,
                    no_backup: true,
                    lock_wait_secs: 0,
                }
            ),
            ExitCode::FAILURE
        );
    }
}
