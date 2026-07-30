//! SQLite-backed metadata storage for a deduplicating backup repository.
//!
//! This crate currently only implements repository initialization
//! ([`init_repository`]): creating the on-disk directory layout and the metadata
//! database with its schema and initial rows.
//!
//! Planned access pattern once backup ingestion writes to this database: many
//! short-lived read connections (e.g. one per parallel chunking worker, for the
//! per-chunk dedup lookup) plus a single dedicated write connection that batches
//! inserts into few, larger transactions. WAL mode lets readers and the writer run
//! without blocking each other, but only ever admits one writer transaction at a
//! time - so multiple concurrent write connections would just contend for that
//! single writer slot instead of adding real throughput, and would defeat
//! transaction batching (the actual lever for insert performance). `busy_timeout`
//! below exists for the transient contention this single writer can still hit
//! (e.g. a WAL checkpoint in progress), not as a substitute for this design.

mod backup;
mod error;
mod maintenance;
mod migrations;
mod query;
mod settings;
mod tree;

pub use backup::{ChunkRef, FileBackupRecord, apply_backup_batch, find_chunk};
pub use error::Error;
pub use maintenance::{ReclaimStats, reclaim_space, soft_delete};
pub use query::{
    ChunkInfo, PathEntry, SubtreeStats, all_chunks, chunk_extents, chunk_extents_sorted, file_size,
    free_space_summary, list_children, ordered_content_chunks, resolve_path,
    subtree_entries_with_paths, subtree_stats,
};
pub use settings::{CDC_TARGET_SIZE_BITS_RANGE, Chunking, RepositorySettings, SettingsError};
pub use tree::{
    EntryKind, TreeEntryRow, find_tree_entry, get_tree_entry, insert_directory, rename_entry,
    touch_mtime,
};

use std::fs;
use std::path::Path;

use rusqlite::Connection;

/// Directory (relative to the repository root) holding the metadata database.
const META_DIR: &str = "meta";
/// File name of the metadata database within [`META_DIR`].
const META_DB_FILE: &str = "repository.db";
/// Directory (relative to the repository root) holding the chunk data store.
const DATA_DIR: &str = "data";

/// The metadata database file path for a repository at `repo_root`, without
/// opening or validating it - unlike [`open_repository`]. Useful for
/// maintenance operations (like restoring a backup over the live database)
/// that need to work even when the current database is unreadable or
/// corrupt, which `open_repository` would fail on.
pub fn db_file_path(repo_root: &Path) -> std::path::PathBuf {
    repo_root.join(META_DIR).join(META_DB_FILE)
}

/// The directory holding the metadata database (and, by convention, its
/// backups) for a repository at `repo_root`. Like [`db_file_path`], doesn't
/// require opening or validating the repository.
pub fn meta_dir(repo_root: &Path) -> std::path::PathBuf {
    repo_root.join(META_DIR)
}

/// Opens (creating if missing) the SQLite database at `path` with the pragmas
/// required for correct and durable operation.
fn open_connection(path: &Path) -> Result<Connection, Error> {
    let conn = Connection::open(path)?;
    // foreign_keys and synchronous are not stored in the database file: they're
    // purely per-connection settings that default to off/FULL, so they must be
    // set here every time, on every connection.
    conn.pragma_update(None, "foreign_keys", "ON")?;
    // NORMAL trades a small amount of durability (the last few committed
    // transactions may be lost on power loss or an OS crash) for substantially
    // fewer fsync calls per write. This is safe (the database file itself cannot
    // be corrupted this way) specifically because it's paired with WAL mode below
    // - see https://www.sqlite.org/pragma.html#pragma_synchronous.
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    // Wait rather than immediately failing with SQLITE_BUSY when the single
    // writer lock (see the module-level doc comment) is momentarily held by
    // another connection, e.g. during a WAL checkpoint.
    conn.pragma_update(None, "busy_timeout", 5000)?;
    // Like journal_mode below, auto_vacuum is persisted in the database file
    // itself, and can only be established for free (no VACUUM required) on a
    // brand new, empty database with no committed pages yet - which is exactly
    // what this is the first time it runs, since open_connection always runs
    // before any migration creates a table. This must come before the
    // journal_mode switch below: switching to WAL itself already forces a
    // write/commit of the file header, and that alone is enough to make
    // SQLite treat the "brand new, empty database" fast path as no longer
    // available. INCREMENTAL tracks freed pages for later on-demand reclaiming
    // via `PRAGMA incremental_vacuum` (see the `db compact` command) without
    // eagerly truncating the file on every commit that frees a page the way
    // `FULL` would - this workload deletes rows only rarely
    // (`del`/`reclaim-space`), so paying that cost on every write would be
    // wasted most of the time.
    conn.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
    // journal_mode returns the resulting mode as a row, so pragma_update_and_check
    // (rather than pragma_update) is required here. Unlike auto_vacuum above,
    // WAL mode doesn't need to go first - switching to it is unaffected by
    // auto_vacuum mode. WAL mode is persisted in the database file itself once
    // set, so on later opens of an already-WAL file this is just a cheap no-op
    // check, not a real mode switch. Setting it here regardless keeps this
    // function correct on its own even if it's ever called on a pre-WAL
    // database file.
    conn.pragma_update_and_check(None, "journal_mode", "WAL", |_row| Ok(()))?;
    Ok(conn)
}

/// Creates a new repository at `repo_root`.
///
/// This creates the directory layout (a `meta/` subdirectory for the metadata
/// database and a `data/` subdirectory for the future chunk data store) and
/// initializes the metadata database: the schema (which seeds the root tree
/// entry) and the given `settings`.
///
/// # Errors
///
/// Returns [`Error::RepositoryAlreadyExists`] if `repo_root` already exists.
pub fn init_repository(repo_root: &Path, settings: &RepositorySettings) -> Result<(), Error> {
    if repo_root.exists() {
        return Err(Error::RepositoryAlreadyExists(repo_root.to_path_buf()));
    }

    fs::create_dir_all(repo_root.join(META_DIR))?;
    fs::create_dir_all(repo_root.join(DATA_DIR))?;

    let mut conn = open_connection(&repo_root.join(META_DIR).join(META_DB_FILE))?;
    migrations::migrations().to_latest(&mut conn)?;

    conn.execute(
        "INSERT INTO repository_settings (id, cdc_target_size_bits, chunking) VALUES (1, ?1, ?2)",
        (
            settings.cdc_target_size_bits(),
            settings.chunking().as_str(),
        ),
    )?;

    Ok(())
}

/// A handle to an existing repository, opened via [`open_repository`].
pub struct Repository {
    repo_root: std::path::PathBuf,
    settings: RepositorySettings,
}

impl Repository {
    /// The settings this repository was created with.
    pub fn settings(&self) -> RepositorySettings {
        self.settings
    }

    /// The directory holding the chunk data store.
    pub fn data_dir(&self) -> std::path::PathBuf {
        self.repo_root.join(DATA_DIR)
    }

    fn meta_db_path(&self) -> std::path::PathBuf {
        self.repo_root.join(META_DIR).join(META_DB_FILE)
    }

    /// Opens a new connection to this repository's metadata database.
    ///
    /// There is no distinction between a "read" and a "write" connection at the
    /// SQLite level - both are opened the same way (see [`open_connection`]) - but
    /// callers should still open one dedicated connection for writing and any
    /// number of separate connections for reading, per the module-level doc
    /// comment: WAL only ever admits one writer transaction at a time, so treating
    /// every connection as a potential writer would only add lock contention
    /// without adding throughput.
    pub fn open_read_connection(&self) -> Result<Connection, Error> {
        open_connection(&self.meta_db_path())
    }

    /// See [`Repository::open_read_connection`]; use exactly one of these per
    /// repository at a time.
    pub fn open_write_connection(&self) -> Result<Connection, Error> {
        open_connection(&self.meta_db_path())
    }
}

/// Opens an existing repository at `repo_root`, reading back its settings.
pub fn open_repository(repo_root: &Path) -> Result<Repository, Error> {
    let conn = open_connection(&repo_root.join(META_DIR).join(META_DB_FILE))?;

    let (cdc_target_size_bits, chunking): (u32, String) = conn.query_row(
        "SELECT cdc_target_size_bits, chunking FROM repository_settings WHERE id = 1",
        (),
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let settings = RepositorySettings::new(cdc_target_size_bits, Chunking::from_db_str(&chunking))?;

    Ok(Repository {
        repo_root: repo_root.to_path_buf(),
        settings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_settings() -> RepositorySettings {
        RepositorySettings::new(20, Chunking::Cdc).unwrap()
    }

    #[test]
    fn db_file_path_and_meta_dir_work_without_the_repository_existing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");

        assert_eq!(
            db_file_path(&repo_root),
            repo_root.join("meta").join("repository.db")
        );
        assert_eq!(meta_dir(&repo_root), repo_root.join("meta"));
    }

    #[test]
    fn init_repository_creates_the_expected_layout() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");

        init_repository(&repo_root, &test_settings()).unwrap();

        assert!(repo_root.join(META_DIR).join(META_DB_FILE).is_file());
        assert!(repo_root.join(DATA_DIR).is_dir());
    }

    #[test]
    fn init_repository_enables_incremental_auto_vacuum() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();

        let conn = open_connection(&repo_root.join(META_DIR).join(META_DB_FILE)).unwrap();
        let auto_vacuum: i64 = conn
            .pragma_query_value(None, "auto_vacuum", |row| row.get(0))
            .unwrap();
        assert_eq!(auto_vacuum, 2, "2 = INCREMENTAL");
    }

    #[test]
    fn open_repository_reads_back_the_settings_it_was_created_with() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(
            &repo_root,
            &RepositorySettings::new(18, Chunking::None).unwrap(),
        )
        .unwrap();

        let repo = open_repository(&repo_root).unwrap();

        assert_eq!(repo.settings().cdc_target_size_bits(), 18);
        assert_eq!(repo.settings().chunking(), Chunking::None);
        assert_eq!(repo.data_dir(), repo_root.join(DATA_DIR));
    }

    #[test]
    fn init_repository_writes_settings_and_root_entry() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();

        let conn = Connection::open(repo_root.join(META_DIR).join(META_DB_FILE)).unwrap();

        let (cdc_target_size_bits, chunking): (u32, String) = conn
            .query_row(
                "SELECT cdc_target_size_bits, chunking FROM repository_settings WHERE id = 1",
                (),
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(cdc_target_size_bits, 20);
        assert_eq!(chunking, "cdc");

        let (id, parent_id, name): (i64, i64, String) = conn
            .query_row(
                "SELECT id, parent_id, name FROM tree_entries WHERE id = 0",
                (),
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(id, 0);
        assert_eq!(parent_id, 0);
        assert_eq!(name, "");
    }

    #[test]
    fn init_repository_fails_if_the_repository_already_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();

        let result = init_repository(&repo_root, &test_settings());

        assert!(matches!(result, Err(Error::RepositoryAlreadyExists(path)) if path == repo_root));
    }

    /// Regression test for a schema bug caught during review: a nullable `parent_id`
    /// (NULL for the root) would have let the partial unique index silently accept
    /// duplicate active names at the top level, since SQL never considers two NULLs
    /// equal. The root's `parent_id` must be non-null (self-referential) so the
    /// index actually enforces uniqueness there too.
    #[test]
    fn duplicate_active_names_under_the_root_are_rejected() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();

        let mut conn = open_connection(&repo_root.join(META_DIR).join(META_DB_FILE)).unwrap();
        let tx = conn.transaction().unwrap();
        tx.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind) VALUES (1, 0, 'a', 0, 'dir')",
            (),
        )
        .unwrap();

        let result = tx.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind) VALUES (2, 0, 'a', 0, 'dir')",
            (),
        );

        assert!(result.is_err());
    }

    fn content_ref_count(conn: &Connection, content_id: i64) -> i64 {
        conn.query_row(
            "SELECT ref_count FROM contents WHERE id = ?1",
            [content_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn chunk_ref_count(conn: &Connection, chunk_id: i64) -> i64 {
        conn.query_row(
            "SELECT ref_count FROM chunks WHERE id = ?1",
            [chunk_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// `contents.ref_count` must track live `tree_entries` references: it rises
    /// when an entry starts pointing at a content and falls again once that entry
    /// row is actually deleted, so unreferenced content can be found via
    /// `ref_count = 0` without scanning `tree_entries`.
    #[test]
    fn tree_entries_maintain_content_ref_count() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();
        let conn = open_connection(&repo_root.join(META_DIR).join(META_DB_FILE)).unwrap();

        conn.execute(
            "INSERT INTO contents (id, length, hash) VALUES (1, 0, x'AA')",
            (),
        )
        .unwrap();
        assert_eq!(content_ref_count(&conn, 1), 0);

        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, content_id, kind) VALUES (1, 0, 'a', 0, 1, 'file')",
            (),
        )
        .unwrap();
        assert_eq!(content_ref_count(&conn, 1), 1);

        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, content_id, kind) VALUES (2, 0, 'b', 0, 1, 'file')",
            (),
        )
        .unwrap();
        assert_eq!(content_ref_count(&conn, 1), 2);

        // Soft-deleting an entry must not release its content: it's still needed
        // to keep the entry recoverable.
        conn.execute("UPDATE tree_entries SET deleted_at = 1 WHERE id = 1", ())
            .unwrap();
        assert_eq!(content_ref_count(&conn, 1), 2);

        conn.execute("DELETE FROM tree_entries WHERE id = 1", ())
            .unwrap();
        assert_eq!(content_ref_count(&conn, 1), 1);

        conn.execute("DELETE FROM tree_entries WHERE id = 2", ())
            .unwrap();
        assert_eq!(content_ref_count(&conn, 1), 0);
    }

    /// `chunks.ref_count` must track live `content_chunks` references, and
    /// purging an unreferenced content (`DELETE ... WHERE ref_count = 0`) must
    /// cascade into deleting its `content_chunks` rows, which in turn releases
    /// the chunks that only that content used.
    #[test]
    fn content_chunks_maintain_chunk_ref_count_and_cascade_on_content_deletion() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();
        let conn = open_connection(&repo_root.join(META_DIR).join(META_DB_FILE)).unwrap();

        conn.execute(
            "INSERT INTO chunks (id, length, hash) VALUES (1, 3, x'AA')",
            (),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO contents (id, length, hash) VALUES (1, 3, x'BB')",
            (),
        )
        .unwrap();
        assert_eq!(chunk_ref_count(&conn, 1), 0);

        conn.execute(
            "INSERT INTO content_chunks (content_id, seq, chunk_id) VALUES (1, 0, 1)",
            (),
        )
        .unwrap();
        assert_eq!(chunk_ref_count(&conn, 1), 1);

        // ref_count = 0, so this content is eligible for purging.
        assert_eq!(content_ref_count(&conn, 1), 0);
        conn.execute("DELETE FROM contents WHERE id = 1 AND ref_count = 0", ())
            .unwrap();

        let remaining_content_chunks: i64 = conn
            .query_row("SELECT COUNT(*) FROM content_chunks", (), |row| row.get(0))
            .unwrap();
        assert_eq!(remaining_content_chunks, 0);
        assert_eq!(chunk_ref_count(&conn, 1), 0);
    }
}
