use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Args, Subcommand};

use crate::format::timestamp_for_filename;

/// Deflate level `db backup` compresses with - the fastest end of the
/// scale (1..=9, zlib convention), not the default (6) or best (9). A
/// faster level was chosen deliberately over the default based on an
/// initial estimate (see `docs/plans/implemented/db-backup-compression.md`);
/// measured for real with this actual level, against the real ~760 MB
/// `dedup/` repository's database (release build): `VACUUM INTO` plus zip
/// together took 30.8s total (down from ~36s for `VACUUM INTO` alone,
/// before compression existed - compression added surprisingly little),
/// producing a 399 MB zip (52.6% of the original, so ~47% smaller).
const BACKUP_ZIP_COMPRESSION_LEVEL: i64 = 1;

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

/// Creates a full, already-compacted, self-contained, zip-compressed
/// snapshot of the live metadata database via `VACUUM INTO` (into a
/// temporary plain-SQLite file first, since `VACUUM INTO` can only write a
/// real database file, not stream into a zip entry directly - then zipped
/// and the temporary file removed) - safe to run against a live WAL-mode
/// database (the single writer and any readers are never blocked by it, and
/// it never touches the live database file), unlike a raw file copy of a
/// database that might still be open for writing elsewhere. Always
/// timestamped, so repeated backups accumulate instead of silently
/// overwriting a previous one.
///
/// Compressed at the fastest deflate level, not the default - see
/// `BACKUP_ZIP_COMPRESSION_LEVEL`'s doc comment for why. On any failure
/// past the `VACUUM INTO` step, removes both the temporary uncompressed
/// file and any partial zip, leaving nothing half-finished behind.
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
    let snapshot_name = format!(
        "repository_{}.sqlite3",
        timestamp_for_filename(now_millis())
    );
    let uncompressed = backups_dir.join(format!("{snapshot_name}.tmp"));
    let target = backups_dir.join(format!("{snapshot_name}.zip"));

    let conn = match repository.open_read_connection() {
        Ok(c) => c,
        Err(err) => {
            eprintln!("error: failed to open the metadata database: {err}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(err) = conn.execute("VACUUM INTO ?1", [uncompressed.to_string_lossy().as_ref()]) {
        eprintln!("error: failed to create backup: {err}");
        let _ = fs::remove_file(&uncompressed);
        return ExitCode::FAILURE;
    }
    drop(conn);

    match zip_and_remove(&uncompressed, &target, &snapshot_name) {
        Ok(()) => {
            println!("Backup created: {}", target.display());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("error: failed to compress backup: {err}");
            let _ = fs::remove_file(&uncompressed);
            let _ = fs::remove_file(&target);
            ExitCode::FAILURE
        }
    }
}

/// Compresses `source` into a single-entry zip at `target` (the entry named
/// `entry_name`, matching the plain filename `db restore` should see once
/// it extracts this again), then removes `source`.
fn zip_and_remove(source: &Path, target: &Path, entry_name: &str) -> io::Result<()> {
    let zip_file = File::create(target)?;
    let mut writer = zip::ZipWriter::new(zip_file);
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .compression_level(Some(BACKUP_ZIP_COMPRESSION_LEVEL));
    writer
        .start_file(entry_name, options)
        .map_err(io::Error::other)?;
    let mut source_file = File::open(source)?;
    io::copy(&mut source_file, &mut writer)?;
    writer.finish().map_err(io::Error::other)?;
    fs::remove_file(source)?;
    Ok(())
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

    // `db backup` has produced zipped snapshots since
    // docs/plans/implemented/db-backup-compression.md; older, plain
    // `.sqlite3` backups (from before that, or restored from elsewhere)
    // keep working unchanged - this is purely additive.
    let is_zip = backup_path
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zip"));
    let extracted;
    let backup_path = if is_zip {
        match extract_single_entry(&backup_path) {
            Ok(tmp) => {
                extracted = tmp;
                extracted.path()
            }
            Err(err) => {
                eprintln!(
                    "error: failed to extract backup zip '{}': {err}",
                    backup_path.display()
                );
                return ExitCode::FAILURE;
            }
        }
    } else {
        backup_path.as_path()
    };

    let db_file = db::db_file_path(repo);
    // A VACUUM INTO backup has no WAL sidecars of its own, but the live file
    // being replaced might - remove any stale ones at the destination first,
    // so the next connection doesn't pair the freshly restored main file
    // with leftover WAL frames from the file it replaced.
    for suffix in ["-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{suffix}", db_file.display()));
        let _ = fs::remove_file(sidecar);
    }

    match fs::copy(backup_path, &db_file) {
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

/// Extracts a `db backup` zip's single entry to a fresh temporary file,
/// returned still-open so the caller can use its path immediately - the
/// file is removed automatically once the returned handle is dropped.
/// Expects exactly one entry (matching what `run_backup`/`zip_and_remove`
/// always produce); anything else is treated as an error rather than
/// guessing which entry to use.
fn extract_single_entry(zip_path: &Path) -> io::Result<tempfile::NamedTempFile> {
    let file = File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(file).map_err(io::Error::other)?;
    if archive.len() != 1 {
        return Err(io::Error::other(format!(
            "expected exactly one entry, found {}",
            archive.len()
        )));
    }
    let mut entry = archive.by_index(0).map_err(io::Error::other)?;
    let mut extracted = tempfile::NamedTempFile::new()?;
    io::copy(&mut entry, &mut extracted)?;
    Ok(extracted)
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
        assert_eq!(entries.len(), 1, "the uncompressed .tmp file must be gone");
        let backup_file = entries.into_iter().next().unwrap().unwrap().path();
        let file_name = backup_file.file_name().unwrap().to_str().unwrap();
        assert!(file_name.starts_with("repository_"));
        assert!(file_name.ends_with(".sqlite3.zip"));

        // The zip's single entry must be a valid, independently openable
        // database, under the plain (unzipped) filename `db restore` would
        // expect after extracting it.
        let extracted = extract_single_entry(&backup_file).unwrap();
        assert_eq!(
            zip::ZipArchive::new(File::open(&backup_file).unwrap())
                .unwrap()
                .by_index(0)
                .unwrap()
                .name(),
            file_name.trim_end_matches(".zip")
        );
        let conn = rusqlite::Connection::open(extracted.path()).unwrap();
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
    fn restore_still_accepts_an_old_plain_uncompressed_backup() {
        let (_temp_dir, repo_root) = init_repo();
        let backups_dir = db::meta_dir(&repo_root).join("backups");
        fs::create_dir_all(&backups_dir).unwrap();
        let plain_backup = backups_dir.join("repository_old_style.sqlite3");
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        conn.execute("VACUUM INTO ?1", [plain_backup.to_string_lossy().as_ref()])
            .unwrap();
        drop(conn);

        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        db::insert_directory(&conn, 0, "after-backup", 0).unwrap();
        drop(conn);

        assert_eq!(run_restore_db(&repo_root, &plain_backup), ExitCode::SUCCESS);
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        assert_eq!(db::resolve_path(&conn, "after-backup").unwrap(), None);
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
