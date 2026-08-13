use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Args, Subcommand};

use crate::format::timestamp_for_filename;
use crate::repo_lock::RepoLock;

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
    /// live database). Excludes `store`/`mount --read-write`/`compact-store`/
    /// `reclaim-space` automatically, same as those four exclude each other
    /// (see `--lock-wait`) - but run this only when nothing else at all is
    /// accessing the repository regardless: unlike every other command, it
    /// replaces the whole database file at once rather than a single
    /// committing transaction, so a concurrent *read-only* command (which
    /// isn't blocked by the lock - see
    /// `docs/plans/implemented/cross-process-repository-locking.md`) can
    /// still see undefined behavior, not a clean snapshot, while this runs.
    Restore {
        /// Backup file to restore from - either a path, or (if that path
        /// doesn't exist as given) a bare filename looked up under
        /// meta/backups/.
        file: PathBuf,

        /// How long to wait, in seconds, for the repository's lock to
        /// become free if another `store`/`mount --read-write`/
        /// `compact-store`/`reclaim-space` run already holds it, before
        /// giving up. Default: don't wait, fail immediately.
        #[arg(long = "lock-wait", default_value_t = 0)]
        lock_wait_secs: u64,
    },
    /// Reclaim free pages in the metadata database file, shrinking it in
    /// place.
    Compact,
}

pub fn run_db(repo: &Path, args: DbArgs) -> ExitCode {
    match args.command {
        DbCommand::Backup => run_backup(repo),
        DbCommand::Restore {
            file,
            lock_wait_secs,
        } => run_restore_db(repo, &file, Duration::from_secs(lock_wait_secs)),
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
    let repository = match db::open_repository_read_only(repo) {
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
            ExitCode::FAILURE
        }
    }
}

/// Compresses `source` into a single-entry zip at `target` (the entry named
/// `entry_name`, matching the plain filename `db restore` should see once
/// it extracts this again), then removes `source`.
///
/// Writes to a same-directory staging path first and only `rename`s it to
/// `target` once fully written, rather than writing `target` directly - a
/// same-volume rename is atomic on both Windows and POSIX, so a kill
/// (SIGINT/SIGKILL/power loss) mid-write never leaves a truncated,
/// corrupt file sitting at the filename `db restore` would otherwise treat
/// as a complete, ready-to-use backup.
fn zip_and_remove(source: &Path, target: &Path, entry_name: &str) -> io::Result<()> {
    let staging = target.with_extension("zip.tmp");
    if let Err(err) = write_zip(&staging, source, entry_name) {
        let _ = fs::remove_file(&staging);
        return Err(err);
    }
    fs::rename(&staging, target)?;
    fs::remove_file(source)?;
    Ok(())
}

fn write_zip(staging: &Path, source: &Path, entry_name: &str) -> io::Result<()> {
    let zip_file = File::create(staging)?;
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
    Ok(())
}

/// Restores the live metadata database from a backup file, replacing it
/// entirely. Doesn't open/validate the *current* database first (unlike most
/// other commands) - this needs to work even if it's the current database
/// that's broken, which is the situation this command exists to recover
/// from.
///
/// Takes the same repository lock `store`/`mount --read-write`/
/// `compact-store`/`reclaim-space` do (see `repo_lock`), so it's
/// automatically excluded from running alongside any of those four - but
/// that's a partial guarantee, not a full one: replacing the database file
/// wholesale (rather than via a single committing transaction, like every
/// other write path in this codebase) is also unsafe next to a concurrent
/// *reader*, and the lock deliberately doesn't cover readers at all (see
/// `docs/plans/implemented/cross-process-repository-locking.md`'s "Is a
/// reader-side lock worth adding? No"). `DbCommand::Restore`'s doc comment
/// (surfaced via `--help`) spells this out for the user; nothing here
/// attempts to detect or block a concurrent reader.
fn run_restore_db(repo: &Path, file: &Path, lock_wait: Duration) -> ExitCode {
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

    let _lock = match RepoLock::acquire(&db::meta_dir(repo), lock_wait) {
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

    if let Some(msg) = stale_backup_warning(
        try_read_store_generation(backup_path),
        try_read_store_generation(&db_file),
    ) {
        eprintln!("warning: {msg} - proceeding anyway.");
    }

    // A VACUUM INTO backup has no WAL sidecars of its own, but the live file
    // being replaced might - remove any stale ones at the destination first,
    // so the next connection doesn't pair the freshly restored main file
    // with leftover WAL frames from the file it replaced.
    for suffix in ["-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{suffix}", db_file.display()));
        let _ = fs::remove_file(sidecar);
    }

    // Copy to a same-directory staging file first, then atomically `rename`
    // it into place - a plain `fs::copy` straight onto `db_file` would leave
    // a half-old-half-new, corrupted database if killed mid-copy (SIGINT/
    // SIGKILL/power loss), an especially bad failure mode for the one
    // command that's specifically the recovery path for a broken database.
    // A same-volume rename is atomic on both Windows and POSIX: either the
    // live file is untouched, or it's fully replaced - no observable
    // in-between state either way.
    let staging = db_file.with_extension("sqlite3.restoring");
    if let Err(err) = fs::copy(backup_path, &staging) {
        eprintln!("error: failed to stage restored database: {err}");
        let _ = fs::remove_file(&staging);
        return ExitCode::FAILURE;
    }
    match fs::rename(&staging, &db_file) {
        Ok(()) => {
            println!("Database restored from '{}'.", backup_path.display());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("error: failed to restore database: {err}");
            let _ = fs::remove_file(&staging);
            ExitCode::FAILURE
        }
    }
}

/// Best-effort read of `store_generation` from `path` as a standalone,
/// genuinely read-only SQLite connection - `None` for any failure (file
/// doesn't exist or isn't a database, doesn't have the column yet, or any
/// other read error), not just the specific "predates this feature" case.
/// Used for both the backup file being restored and the live database
/// (which, per `run_restore_db`'s own doc comment, might currently be
/// broken - that's fine here, it just means the comparison below is
/// skipped rather than blocking the recovery this command exists for).
fn try_read_store_generation(path: &Path) -> Option<i64> {
    let conn = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;
    db::store_generation(&conn).ok()
}

/// Decides whether restoring a backup with `backup_generation` (see
/// [`try_read_store_generation`]) is worth warning about, given the live
/// repository's `live_generation`. See `docs/plans/stale-backup-guard.md` -
/// warns, never blocks: the user restoring a backup may have a specific
/// reason to do so even when it's behind.
fn stale_backup_warning(
    backup_generation: Option<i64>,
    live_generation: Option<i64>,
) -> Option<String> {
    let live = live_generation?;
    match backup_generation {
        Some(backup) if backup < live => Some(format!(
            "this backup is {} data-store-changing maintenance run(s) (reclaim-space/compact-store) \
             behind the live repository - restoring it may resolve some entries to the wrong \
             physical bytes",
            live - backup
        )),
        Some(_) => None,
        None => Some(
            "this backup predates the store-staleness safety check and can't be verified - it may \
             resolve some entries to the wrong physical bytes"
                .to_string(),
        ),
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
///
/// Doesn't explicitly checkpoint the write-ahead log: SQLite already does
/// that itself (folding pending writes into the main file and removing the
/// `-wal`/`-shm` sidecars) whenever the *last* open connection to the
/// database closes cleanly - which this one, opened just above, normally
/// is by the time this function returns. An explicit `PRAGMA
/// wal_checkpoint(TRUNCATE)` would only add anything in the narrower case
/// of another connection (e.g. a concurrent read-only `mount`) still being
/// open at the same time - deliberately left out until that's an actual
/// problem in practice, not a hypothetical one; see
/// `docs/plans/implemented/read-only-repository-access.md` for the
/// reasoning and the empirical check behind this.
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

        let exit = run_restore_db(&repo_root, &backup_file, Duration::ZERO);

        assert_eq!(exit, ExitCode::SUCCESS);
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        assert_eq!(db::resolve_path(&conn, "after-backup").unwrap(), None);
    }

    #[test]
    fn restore_refuses_when_the_lock_is_already_held() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(run_backup(&repo_root), ExitCode::SUCCESS);
        let backup_file = fs::read_dir(db::meta_dir(&repo_root).join("backups"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let _lock = RepoLock::acquire(&db::meta_dir(&repo_root), Duration::ZERO)
            .unwrap()
            .unwrap();

        assert_eq!(
            run_restore_db(&repo_root, &backup_file, Duration::ZERO),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn restore_waits_for_the_lock_via_lock_wait_and_then_succeeds() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(run_backup(&repo_root), ExitCode::SUCCESS);
        let backup_file = fs::read_dir(db::meta_dir(&repo_root).join("backups"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let lock = RepoLock::acquire(&db::meta_dir(&repo_root), Duration::ZERO)
            .unwrap()
            .unwrap();

        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            drop(lock);
        });

        assert_eq!(
            run_restore_db(&repo_root, &backup_file, Duration::from_secs(2)),
            ExitCode::SUCCESS
        );
        releaser.join().unwrap();
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

        assert_eq!(
            run_restore_db(&repo_root, &bare_name, Duration::ZERO),
            ExitCode::SUCCESS
        );
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

        assert_eq!(
            run_restore_db(&repo_root, &plain_backup, Duration::ZERO),
            ExitCode::SUCCESS
        );
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        assert_eq!(db::resolve_path(&conn, "after-backup").unwrap(), None);
    }

    #[test]
    fn restore_fails_for_a_missing_backup_file() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_restore_db(
                &repo_root,
                &PathBuf::from("no-such-backup.sqlite3"),
                Duration::ZERO
            ),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn stale_backup_warning_is_none_when_generations_match_or_live_is_unknown() {
        assert_eq!(stale_backup_warning(Some(3), Some(3)), None);
        assert_eq!(
            stale_backup_warning(Some(3), None),
            None,
            "can't compare against a live database whose generation couldn't be read \
             (e.g. it's currently broken - exactly the case restore exists to recover from)"
        );
    }

    #[test]
    fn stale_backup_warning_fires_with_the_generation_delta_when_the_backup_is_behind() {
        let msg = stale_backup_warning(Some(2), Some(5)).unwrap();
        assert!(msg.contains('3'), "delta of 3 should appear in: {msg}");
    }

    #[test]
    fn stale_backup_warning_fires_for_an_unknown_backup_generation() {
        assert!(stale_backup_warning(None, Some(1)).is_some());
        assert!(
            stale_backup_warning(None, Some(0)).is_some(),
            "unknown is treated as possibly-stale even against a live generation of 0"
        );
    }

    #[test]
    fn try_read_store_generation_reads_the_live_value_and_none_for_a_missing_file() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            try_read_store_generation(&db::db_file_path(&repo_root)),
            Some(0)
        );
        assert_eq!(
            try_read_store_generation(&repo_root.join("does-not-exist.sqlite3")),
            None
        );
    }

    #[test]
    fn restore_warns_but_still_succeeds_when_the_backup_is_behind_the_live_generation() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(run_backup(&repo_root), ExitCode::SUCCESS);
        let backup_file = fs::read_dir(db::meta_dir(&repo_root).join("backups"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();

        // Bump the live generation past the backup's, the same way
        // reclaim_space does when it actually purges a chunk - see
        // db::maintenance::reclaim_space_bumps_store_generation_only_when_chunks_are_purged
        // for that unit itself; here just simulate the resulting state.
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        conn.execute(
            "UPDATE repository_settings SET store_generation = store_generation + 1",
            (),
        )
        .unwrap();
        drop(conn);

        // The warning goes to stderr, not the return value - this only
        // confirms restoring a stale-generation backup still succeeds
        // rather than being blocked (the explicit "warn, don't hard-block"
        // decision).
        assert_eq!(
            run_restore_db(&repo_root, &backup_file, Duration::ZERO),
            ExitCode::SUCCESS
        );
    }

    #[test]
    fn compact_succeeds_on_a_fresh_repository() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(run_compact(&repo_root), ExitCode::SUCCESS);
    }
}
