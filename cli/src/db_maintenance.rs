use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Args, Subcommand};

use crate::format::timestamp_for_filename;

#[derive(Args)]
pub struct DbArgs {
    #[command(subcommand)]
    command: DbCommand,
}

#[derive(Subcommand)]
enum DbCommand {
    /// Create a timestamped backup of the metadata database.
    Backup,
    /// Restore the metadata database from a backup file (overwrites the
    /// live database).
    Restore {
        /// Backup file to restore from - either a path, or (if that path
        /// doesn't exist as given) a bare filename looked up under
        /// meta/backups/.
        file: PathBuf,
    },
    /// Reclaim free pages in the metadata database file, shrinking it in
    /// place.
    Compact,
}

pub fn run_db(repo: &Path, args: DbArgs) -> ExitCode {
    match args.command {
        DbCommand::Backup => run_backup(repo),
        DbCommand::Restore { file } => run_restore_db(repo, &file),
        DbCommand::Compact => run_compact(repo),
    }
}

/// Creates a full, already-compacted, self-contained snapshot of the live
/// metadata database via `VACUUM INTO` - safe to run against a live WAL-mode
/// database (the single writer and any readers are never blocked by it, and
/// it never touches the live database file), unlike a raw file copy of a
/// database that might still be open for writing elsewhere. Always
/// timestamped, so repeated backups accumulate instead of silently
/// overwriting a previous one.
pub(crate) fn run_backup(repo: &Path) -> ExitCode {
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
    let backups_dir = db::meta_dir(repo).join("backups");
    if let Err(err) = fs::create_dir_all(&backups_dir) {
        eprintln!(
            "error: failed to create backups directory '{}': {err}",
            backups_dir.display()
        );
        return ExitCode::FAILURE;
    }
    let target = backups_dir.join(format!(
        "repository_{}.sqlite3",
        timestamp_for_filename(now_millis())
    ));

    let conn = match repository.open_read_connection() {
        Ok(c) => c,
        Err(err) => {
            eprintln!("error: failed to open the metadata database: {err}");
            return ExitCode::FAILURE;
        }
    };
    match conn.execute("VACUUM INTO ?1", [target.to_string_lossy().as_ref()]) {
        Ok(_) => {
            println!("Backup created: {}", target.display());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("error: failed to create backup: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Restores the live metadata database from a backup file, replacing it
/// entirely. Doesn't open/validate the *current* database first (unlike most
/// other commands) - this needs to work even if it's the current database
/// that's broken, which is the situation this command exists to recover
/// from.
fn run_restore_db(repo: &Path, file: &Path) -> ExitCode {
    let backup_path = if file.is_file() {
        file.to_path_buf()
    } else {
        db::meta_dir(repo).join("backups").join(file)
    };
    if !backup_path.is_file() {
        eprintln!(
            "error: backup file '{}' does not exist",
            backup_path.display()
        );
        return ExitCode::FAILURE;
    }

    let db_file = db::db_file_path(repo);
    // A VACUUM INTO backup has no WAL sidecars of its own, but the live file
    // being replaced might - remove any stale ones at the destination first,
    // so the next connection doesn't pair the freshly restored main file
    // with leftover WAL frames from the file it replaced.
    for suffix in ["-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{suffix}", db_file.display()));
        let _ = fs::remove_file(sidecar);
    }

    match fs::copy(&backup_path, &db_file) {
        Ok(_) => {
            println!("Database restored from '{}'.", backup_path.display());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("error: failed to restore database: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Reclaims pages freed by past deletions (`del`/`reclaim-space`) via
/// `PRAGMA incremental_vacuum`, shrinking the database file in place. Cheap
/// compared to a full `VACUUM`: no ~2x disk space or long exclusive lock,
/// since `auto_vacuum = INCREMENTAL` (set when the repository was created)
/// already tracks which pages are free - see the db crate's `open_connection`
/// doc comment for why that mode was chosen over `FULL`/leaving it off.
fn run_compact(repo: &Path) -> ExitCode {
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
    let conn = match repository.open_write_connection() {
        Ok(c) => c,
        Err(err) => {
            eprintln!("error: failed to open the metadata database: {err}");
            return ExitCode::FAILURE;
        }
    };
    match conn.execute_batch("PRAGMA incremental_vacuum;") {
        Ok(()) => {
            println!("Database compacted.");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("error: failed to compact database: {err}");
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
    fn backup_creates_a_readable_snapshot_under_meta_backups() {
        let (_temp_dir, repo_root) = init_repo();

        assert_eq!(run_backup(&repo_root), ExitCode::SUCCESS);

        let backups_dir = db::meta_dir(&repo_root).join("backups");
        let entries: Vec<_> = fs::read_dir(&backups_dir).unwrap().collect();
        assert_eq!(entries.len(), 1);
        let backup_file = entries.into_iter().next().unwrap().unwrap().path();
        assert!(
            backup_file
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("repository_")
        );

        // The backup must be a valid, independently openable database.
        let conn = rusqlite::Connection::open(&backup_file).unwrap();
        let chunking: String = conn
            .query_row(
                "SELECT chunking FROM repository_settings WHERE id = 1",
                (),
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(chunking, "cdc");
    }

    #[test]
    fn restore_replaces_the_live_database_with_the_backup_content() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(run_backup(&repo_root), ExitCode::SUCCESS);
        let backup_file = fs::read_dir(db::meta_dir(&repo_root).join("backups"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();

        // Mutate the live database after the backup was taken.
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        db::insert_directory(&conn, 0, "after-backup", 0).unwrap();
        drop(conn);

        let exit = run_restore_db(&repo_root, &backup_file);

        assert_eq!(exit, ExitCode::SUCCESS);
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        assert_eq!(db::resolve_path(&conn, "after-backup").unwrap(), None);
    }

    #[test]
    fn restore_resolves_a_bare_filename_under_meta_backups() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(run_backup(&repo_root), ExitCode::SUCCESS);
        let backup_file = fs::read_dir(db::meta_dir(&repo_root).join("backups"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let bare_name = PathBuf::from(backup_file.file_name().unwrap());

        assert_eq!(run_restore_db(&repo_root, &bare_name), ExitCode::SUCCESS);
    }

    #[test]
    fn restore_fails_for_a_missing_backup_file() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_restore_db(&repo_root, &PathBuf::from("no-such-backup.sqlite3")),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn compact_succeeds_on_a_fresh_repository() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(run_compact(&repo_root), ExitCode::SUCCESS);
    }
}
