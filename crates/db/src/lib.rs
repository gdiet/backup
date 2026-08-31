//! SQLite-backed repository metadata: the file/directory tree, the
//! deduplication index, and repository settings - DESIGN-METADATA-001
//! through DESIGN-METADATA-011 in `docs/design/metadata-storage.md` and
//! `docs/design/metadata-schema-with-contents-table.md`.
//!
//! A deliberately narrow public interface - no `pub` `rusqlite::Connection`
//! constructor outside this crate's own control, no raw SQL exposed to
//! callers (DESIGN-METADATA-006) - since REQ-TREE-006's atomic-write
//! guarantee holds only because every write to this database goes through
//! curated operations, never ad hoc SQL from elsewhere in the workspace.

mod allocation;
mod connection;
mod content;
mod lock;
mod migrations;
mod settings;
mod tree;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rusqlite::Connection;

pub use lock::WriteLock;
pub use settings::RepositorySettings;
pub use tree::{Entry, EntryKind};

// Repository on-disk layout - DESIGN-REPOSITORY-001 in
// docs/design/repository-layout.md.
const META_DIR: &str = "meta";
const META_TMP_DIR: &str = "meta.tmp";
const META_DB_FILE: &str = "repository.sqlite3";
const DATA_DIR: &str = "data";

/// `repo_root`'s byte-store directory (DESIGN-REPOSITORY-001) - where a caller opening
/// `crates/store`'s `ByteStore` against this same repository points it.
pub fn data_dir(repo_root: &Path) -> PathBuf {
    repo_root.join(DATA_DIR)
}

/// `repo_root`'s metadata directory (DESIGN-REPOSITORY-001), alongside the database itself -
/// where a caller placing its own repository-scoped file next to it (e.g. DESIGN-MOUNT-009's
/// background write-failure log) points it.
pub fn meta_dir(repo_root: &Path) -> PathBuf {
    repo_root.join(META_DIR)
}

#[derive(Debug)]
pub enum Error {
    /// [`init_repository`] was called against a `repo_root` that already holds a repository.
    RepositoryAlreadyExists(PathBuf),
    /// [`init_repository`] was called against a `repo_root` that already exists and is not empty.
    TargetNotEmpty(PathBuf),
    /// [`open_repository`] was called against a `repo_root` with no `meta/` subdirectory - nothing
    /// ever created a repository there.
    NoRepositoryHere(PathBuf),
    /// No live entry with this id exists (a stale id, or one that was since soft-deleted).
    NoSuchEntry(i64),
    /// The entry is a file where a directory was required, or vice versa.
    WrongKind(i64),
    /// [`Repository::rmdir`] was called against a directory that still has live children
    /// (REQ-TREE-008).
    DirectoryNotEmpty(i64),
    /// The target name is already taken by another live entry in the same directory.
    EntryAlreadyExists {
        parent_id: i64,
        name: String,
    },
    /// [`Repository::rename`] would move a directory into its own subtree, which a tree cannot
    /// represent (REQ-MOUNT-009).
    WouldCreateCycle,
    /// The root entry cannot be removed.
    CannotRemoveRoot,
    /// Another thread using this [`Repository`] panicked while holding its connection lock.
    Poisoned,
    /// [`acquire_write_lock`] found the repository's write lock already held by another process
    /// (REQ-MAINTENANCE-004 in `requirements/functional/maintenance.md`) - only one
    /// repository-mutating session runs at a time.
    AlreadyLocked(PathBuf),
    /// [`acquire_write_lock`] failed for a reason other than the lock already being held - most
    /// plausibly the underlying storage not actually enforcing locking at all (DESIGN-MAINTENANCE-001
    /// in `docs/design/repository-locking.md`'s "Known limitation": expected on a network-mounted
    /// repository, not on local/removable storage).
    LockUnavailable {
        path: PathBuf,
        source: std::io::Error,
    },
    /// SQLite reported a `journal_mode` other than `wal` after `configure_write_connection`
    /// requested it - e.g. an unsupported filesystem (SQLite silently falls back instead of
    /// failing outright). Carries whatever mode SQLite actually settled on.
    WalUnavailable(String),
    Io(std::io::Error),
    Sqlite(rusqlite::Error),
    Migration(rusqlite_migration::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::RepositoryAlreadyExists(path) => {
                write!(f, "a repository already exists at {}", path.display())
            }
            Error::TargetNotEmpty(path) => {
                write!(f, "{} already exists and is not empty", path.display())
            }
            Error::NoRepositoryHere(path) => {
                write!(
                    f,
                    "no repository at {} (no meta/ directory)",
                    path.display()
                )
            }
            Error::NoSuchEntry(id) => write!(f, "no live entry with id {id}"),
            Error::WrongKind(id) => write!(f, "entry {id} is not the expected kind"),
            Error::DirectoryNotEmpty(id) => write!(f, "directory {id} is not empty"),
            Error::EntryAlreadyExists { parent_id, name } => {
                write!(f, "{name:?} already exists in directory {parent_id}")
            }
            Error::WouldCreateCycle => {
                write!(f, "cannot move a directory into its own subtree")
            }
            Error::CannotRemoveRoot => write!(f, "cannot remove the root entry"),
            Error::Poisoned => write!(f, "repository connection lock was poisoned"),
            Error::AlreadyLocked(path) => {
                write!(
                    f,
                    "the repository at {} is already locked for writing by another process",
                    path.display()
                )
            }
            Error::LockUnavailable { path, source } => {
                write!(
                    f,
                    "could not acquire the write lock for the repository at {} ({source}) - this \
                     can happen on a network-mounted repository, where the underlying storage may \
                     not actually enforce locking; see README.md's \"Known Limitations\"",
                    path.display()
                )
            }
            Error::WalUnavailable(mode) => {
                write!(
                    f,
                    "WAL journal mode unavailable - SQLite reports {mode:?} instead"
                )
            }
            Error::Io(err) => write!(f, "{err}"),
            Error::Sqlite(err) => write!(f, "{err}"),
            Error::Migration(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Error::Sqlite(err)
    }
}

impl From<rusqlite_migration::Error> for Error {
    fn from(err: rusqlite_migration::Error) -> Self {
        Error::Migration(err)
    }
}

/// A handle to an existing, open repository.
///
/// Holds one connection for its whole lifetime, behind a mutex, rather than opening a fresh one
/// per call - DESIGN-METADATA-003's "one coordinated writer" model requires exactly that for
/// writes. Reads share the same connection/lock for now too: a simplification for this first
/// directory-only mount milestone, not a correctness requirement - WAL mode already supports
/// splitting reads onto their own, unlocked connections (DESIGN-METADATA-003), worth doing once
/// read concurrency under a real mount actually needs it.
#[derive(Debug)]
pub struct Repository {
    settings: RepositorySettings,
    conn: Mutex<Connection>,
}

impl Repository {
    pub fn settings(&self) -> RepositorySettings {
        self.settings
    }

    fn with_connection<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let conn = self.conn.lock().map_err(|_| Error::Poisoned)?;
        f(&conn)
    }

    /// Like [`Self::with_connection`], but runs `f` inside an explicit transaction, committed only
    /// if `f` returns `Ok` - a multi-statement operation either lands as a whole or not at all,
    /// rather than leaving a partial result behind if interrupted partway through (a crash, a
    /// panic unwinding through `f`, `rusqlite::Transaction`'s own drop-without-commit rollback).
    fn with_transaction<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut conn = self.conn.lock().map_err(|_| Error::Poisoned)?;
        let tx = conn.transaction()?;
        let result = f(&tx)?;
        tx.commit()?;
        Ok(result)
    }

    /// Looks up the live entry at `path` (`/`-separated, e.g. `/a/b`; `/` itself resolves to the
    /// root). `Ok(None)` if any path component does not exist (or is soft-deleted) - not itself
    /// an error, since "does this path exist" is a legitimate question to ask.
    pub fn resolve_path(&self, path: &str) -> Result<Option<Entry>, Error> {
        self.with_connection(|conn| tree::resolve_path(conn, path))
    }

    /// Lists the live, direct children of the directory entry `parent_id`.
    pub fn list_children(&self, parent_id: i64) -> Result<Vec<(String, EntryKind)>, Error> {
        self.with_connection(|conn| tree::list_children(conn, parent_id))
    }

    /// Creates a new, empty directory named `name` inside the directory `parent_id`, bumping its
    /// parent's modification time (REQ-TREE-005). Returns the new entry's id.
    pub fn mkdir(&self, parent_id: i64, name: &str, time_millis: i64) -> Result<i64, Error> {
        self.with_transaction(|conn| tree::mkdir(conn, parent_id, name, time_millis))
    }

    /// Soft-deletes the directory entry `id` (REQ-TREE-002), refusing if it still has live
    /// children (REQ-TREE-008). Bumps its parent's modification time.
    pub fn rmdir(&self, id: i64, time_millis: i64) -> Result<(), Error> {
        self.with_transaction(|conn| tree::rmdir(conn, id, time_millis))
    }

    /// Soft-deletes the live file entry `id` (REQ-TREE-002). Bumps its parent's modification
    /// time - unlike DESIGN-MOUNT-011's pure content overwrite, removing a name is a structural
    /// change (REQ-TREE-005). A directory at `id` is refused.
    pub fn unlink_file(&self, id: i64, time_millis: i64) -> Result<(), Error> {
        self.with_transaction(|conn| tree::unlink_file(conn, id, time_millis))
    }

    /// Looks up the live entry by its own id, rather than by path - `Ok(None)` if it does not
    /// exist or is soft-deleted, the same as [`Self::resolve_path`].
    pub fn entry_by_id(&self, id: i64) -> Result<Option<Entry>, Error> {
        self.with_connection(|conn| tree::get_by_id(conn, id))
    }

    /// The live entry `id`'s current `(parent_id, name)` - `None` if it does not exist or is
    /// soft-deleted, reflecting any `rename` since `id` was first obtained.
    pub fn parent_and_name(&self, id: i64) -> Result<Option<(i64, String)>, Error> {
        self.with_connection(|conn| tree::parent_and_name(conn, id))
    }

    /// Settles a background write job's already-resolved content (see [`Self::find_or_create_content`])
    /// into the tree as a file named `name` inside `parent_id` - DESIGN-METADATA-008/
    /// DESIGN-MOUNT-011 in `docs/design/mount-write-path.md`. If a live entry already occupies
    /// that name, it is soft-deleted first and the new entry becomes a separate REQ-TREE-004
    /// history entry for that path, rather than updating the existing row's `content_id` in
    /// place; a directory at that name is refused. Returns the new entry's id.
    pub fn settle_file(
        &self,
        parent_id: i64,
        name: &str,
        time_millis: i64,
        content_id: i64,
    ) -> Result<i64, Error> {
        self.with_transaction(|conn| {
            tree::settle_file(conn, parent_id, name, time_millis, content_id)
        })
    }

    /// Looks up an already-known chunk by its own `(length, hash)` - REQ-STORAGE-002.
    pub fn find_chunk(&self, length: i64, hash: &[u8]) -> Result<Option<i64>, Error> {
        self.with_connection(|conn| content::find_chunk(conn, length, hash))
    }

    /// Reserves storage for a chunk not already known (caller already checked [`Self::find_chunk`]
    /// returned `None`) and records it - DESIGN-STORE-003 in `docs/design/byte-store.md`. Returns
    /// the new chunk id and the exact `(start, stop)` ranges to write `length` bytes into through
    /// `crates/store`, in order.
    pub fn reserve_and_insert_chunk(
        &self,
        length: i64,
        hash: &[u8],
    ) -> Result<(i64, Vec<(u64, u64)>), Error> {
        self.with_transaction(|conn| content::reserve_and_insert_chunk(conn, length, hash))
    }

    /// Finds or creates the whole-content `(length, hash)` row (DESIGN-METADATA-007's
    /// hash-of-chunk-hashes), linking `chunk_ids` (in order, from [`Self::find_chunk`]/
    /// [`Self::reserve_and_insert_chunk`]) if it did not already exist. Returns the content id.
    pub fn find_or_create_content(
        &self,
        length: i64,
        hash: &[u8],
        chunk_ids: &[i64],
    ) -> Result<i64, Error> {
        self.with_transaction(|conn| content::find_or_create_content(conn, length, hash, chunk_ids))
    }

    /// Returns `content_id`'s complete physical layout in `crates/store` - every backing
    /// `(start, stop)` range, in logical order. Concatenating the bytes at these ranges, in this
    /// order, reproduces the content's own bytes exactly. An unknown `content_id` returns an
    /// empty `Vec`, the same as a genuinely zero-length content.
    pub fn resolve_extents(&self, content_id: i64) -> Result<Vec<(u64, u64)>, Error> {
        self.with_connection(|conn| content::resolve_extents(conn, content_id))
    }

    /// Sets `id`'s own modification time (REQ-MOUNT-003's `utimens`).
    pub fn set_mtime(&self, id: i64, time_millis: i64) -> Result<(), Error> {
        self.with_connection(|conn| tree::set_mtime(conn, id, time_millis))
    }

    /// Moves/renames the entry named `old_name` inside `old_parent_id` to `new_name` inside
    /// `new_parent_id` - REQ-MOUNT-009. Bumps both parents' modification times (one, if they are
    /// the same directory).
    #[allow(clippy::too_many_arguments)]
    pub fn rename(
        &self,
        old_parent_id: i64,
        old_name: &str,
        new_parent_id: i64,
        new_name: &str,
        no_replace: bool,
        time_millis: i64,
    ) -> Result<(), Error> {
        self.with_transaction(|conn| {
            tree::rename(
                conn,
                old_parent_id,
                old_name,
                new_parent_id,
                new_name,
                no_replace,
                time_millis,
            )
        })
    }
}

/// Creates a new repository at `repo_root`: the `meta/`/`data/` directory
/// layout and the metadata database within it (the schema, seeding the root
/// tree entry, and `settings`) - DESIGN-REPOSITORY-001.
///
/// `repo_root` may already exist, as long as it is empty (REQ-CLI-005) - the
/// most likely real target, an already-mounted external drive, can never be
/// "not yet existing" itself. Refused if `repo_root` already holds a
/// repository, or already holds anything else at all.
pub fn init_repository(repo_root: &Path, settings: RepositorySettings) -> Result<(), Error> {
    if repo_root.join(META_DIR).exists() {
        return Err(Error::RepositoryAlreadyExists(repo_root.to_path_buf()));
    }
    match fs::create_dir(repo_root) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::read_dir(repo_root)?.next().is_some() {
                return Err(Error::TargetNotEmpty(repo_root.to_path_buf()));
            }
        }
        Err(err) => return Err(err.into()),
    }

    fs::create_dir_all(repo_root.join(DATA_DIR))?;

    // Built in a staging directory and only renamed into place once fully
    // committed, so a process killed mid-creation leaves either no meta/ at
    // all or a complete one - never a half-initialized meta/ that a later
    // open could mistake for a real repository. repo_root is already known
    // empty at this point (checked above), so staging_meta cannot already
    // exist here.
    let staging_meta = repo_root.join(META_TMP_DIR);
    fs::create_dir(&staging_meta)?;

    let mut conn = Connection::open(staging_meta.join(META_DB_FILE))?;
    connection::configure_write_connection(&conn)?;
    migrations::migrations().to_latest(&mut conn)?;
    conn.execute(
        "INSERT INTO repository_settings (id, cdc_target_size_bits, creation_time) VALUES (1, ?1, ?2)",
        (settings.cdc_target_size_bits(), settings.creation_time_millis()),
    )?;
    drop(conn);

    fs::rename(&staging_meta, repo_root.join(META_DIR))?;

    Ok(())
}

/// Opens an existing repository at `repo_root`, reading back the settings it
/// was created with. Applies any pending schema migration automatically
/// (DESIGN-METADATA-005).
pub fn open_repository(repo_root: &Path) -> Result<Repository, Error> {
    let meta_dir = repo_root.join(META_DIR);
    if !meta_dir.is_dir() {
        return Err(Error::NoRepositoryHere(repo_root.to_path_buf()));
    }

    let db_path = meta_dir.join(META_DB_FILE);
    let mut conn = Connection::open(&db_path)?;
    connection::configure_write_connection(&conn)?;
    migrations::migrations().to_latest(&mut conn)?;

    let (cdc_target_size_bits, creation_time_millis): (Option<u32>, i64) = conn.query_row(
        "SELECT cdc_target_size_bits, creation_time FROM repository_settings WHERE id = 1",
        (),
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    Ok(Repository {
        settings: RepositorySettings::new(cdc_target_size_bits, creation_time_millis),
        conn: Mutex::new(conn),
    })
}

/// Acquires `repo_root`'s repository-wide write lock, failing immediately (never blocking) if
/// another process already holds it (`Error::AlreadyLocked`) - REQ-MAINTENANCE-004's "only one
/// repository-mutating operation runs against a repository at a time", DESIGN-MAINTENANCE-001 in
/// `docs/design/repository-locking.md`. Every caller that will mutate the repository - a
/// read-write mount for its whole session (DESIGN-MOUNT-008), a future directed import, reclaim,
/// or compaction run - acquires this once and holds the returned [`WriteLock`] for as long as
/// that session runs; a purely read-only caller never needs to call this at all.
///
/// `repo_root` must already hold a repository (i.e. [`open_repository`] against it would
/// succeed) - the `meta/` directory the lock file lives in is not created here.
pub fn acquire_write_lock(repo_root: &Path) -> Result<WriteLock, Error> {
    lock::try_acquire_write_lock(&repo_root.join(META_DIR))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> RepositorySettings {
        RepositorySettings::new(Some(20), 1_700_000_000_000)
    }

    #[test]
    fn init_repository_creates_meta_and_data_directories() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, settings()).expect("init on a fresh path must succeed");

        assert!(repo_root.join("meta").is_dir());
        assert!(repo_root.join("meta").join("repository.sqlite3").is_file());
        assert!(repo_root.join("data").is_dir());
        assert!(!repo_root.join("meta.tmp").exists());
    }

    #[test]
    fn init_repository_accepts_an_existing_empty_directory() {
        let dir = tempfile::tempdir().unwrap();
        init_repository(dir.path(), settings())
            .expect("init on an existing, empty directory must succeed");
        assert!(dir.path().join("meta").is_dir());
    }

    #[test]
    fn init_repository_refuses_a_non_empty_directory() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("unrelated.txt"), b"hi").unwrap();

        let err = init_repository(dir.path(), settings()).unwrap_err();
        assert!(matches!(err, Error::TargetNotEmpty(_)));
    }

    #[test]
    fn init_repository_refuses_a_leftover_staging_directory_from_a_killed_prior_attempt() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("meta.tmp")).unwrap();

        let err = init_repository(dir.path(), settings()).unwrap_err();
        assert!(matches!(err, Error::TargetNotEmpty(_)));
    }

    #[test]
    fn open_repository_fails_on_a_directory_that_was_never_created_as_one() {
        let dir = tempfile::tempdir().unwrap();
        let err = open_repository(dir.path()).unwrap_err();
        assert!(matches!(err, Error::NoRepositoryHere(_)));
    }

    #[test]
    fn init_repository_refuses_an_already_initialized_repository() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, settings()).expect("first init must succeed");

        let err = init_repository(&repo_root, settings()).unwrap_err();
        assert!(matches!(err, Error::RepositoryAlreadyExists(_)));
    }

    #[test]
    fn open_repository_reads_back_the_settings_it_was_created_with() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, settings()).expect("init must succeed");

        let repo = open_repository(&repo_root).expect("open must succeed");
        assert_eq!(repo.settings(), settings());
    }

    #[test]
    fn open_repository_actually_runs_in_wal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, settings()).expect("init must succeed");
        let repo = open_repository(&repo_root).expect("open must succeed");

        let mode: String = repo
            .with_connection(
                |conn| Ok(conn.query_row("PRAGMA journal_mode", (), |row| row.get(0))?),
            )
            .unwrap();
        assert_eq!(mode, "wal");
    }

    #[test]
    fn init_repository_actually_enables_incremental_auto_vacuum() {
        // auto_vacuum only takes effect for free on a database with no tables yet - a real,
        // previously-hit regression here specifically is it silently staying at NONE (0) because
        // something ran after tables already existed. 2 = INCREMENTAL.
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, settings()).expect("init must succeed");
        let repo = open_repository(&repo_root).expect("open must succeed");

        let mode: i64 = repo
            .with_connection(|conn| Ok(conn.query_row("PRAGMA auto_vacuum", (), |row| row.get(0))?))
            .unwrap();
        assert_eq!(mode, 2, "expected INCREMENTAL (2), got {mode}");
    }

    #[test]
    fn open_repository_enforces_foreign_keys() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, settings()).expect("init must succeed");
        let repo = open_repository(&repo_root).expect("open must succeed");

        let err = repo
            .with_connection(|conn| {
                Ok(conn.execute(
                    "INSERT INTO content_chunks (content_id, seq, chunk_id) VALUES (999, 0, 999)",
                    (),
                )?)
            })
            .unwrap_err();
        assert!(matches!(err, Error::Sqlite(_)));
    }

    #[test]
    fn deleting_a_content_row_cascades_to_content_chunks_and_chunk_ref_counts() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, settings()).expect("init must succeed");
        let repo = open_repository(&repo_root).expect("open must succeed");

        repo.with_connection(|conn| {
            conn.execute(
                "INSERT INTO chunks (id, length, hash) \
                 VALUES (1, 3, X'0102030405060708090A0B0C0D0E0F1011121314')",
                (),
            )?;
            conn.execute(
                "INSERT INTO contents (id, length, hash) \
                 VALUES (1, 3, X'2122232425262728292A2B2C2D2E2F3031323334')",
                (),
            )?;
            conn.execute(
                "INSERT INTO content_chunks (content_id, seq, chunk_id) VALUES (1, 0, 1)",
                (),
            )?;
            Ok(())
        })
        .expect("inserts must succeed");

        let chunk_ref_count: i64 = repo
            .with_connection(|conn| {
                Ok(
                    conn.query_row("SELECT ref_count FROM chunks WHERE id = 1", (), |row| {
                        row.get(0)
                    })?,
                )
            })
            .unwrap();
        assert_eq!(chunk_ref_count, 1);

        repo.with_connection(|conn| Ok(conn.execute("DELETE FROM contents WHERE id = 1", ())?))
            .expect("delete must succeed");

        let content_chunks_left: i64 = repo
            .with_connection(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM content_chunks WHERE content_id = 1",
                    (),
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(
            content_chunks_left, 0,
            "ON DELETE CASCADE must have removed it"
        );

        let chunk_ref_count: i64 = repo
            .with_connection(|conn| {
                Ok(
                    conn.query_row("SELECT ref_count FROM chunks WHERE id = 1", (), |row| {
                        row.get(0)
                    })?,
                )
            })
            .unwrap();
        assert_eq!(
            chunk_ref_count, 0,
            "the cascade delete must have fired content_chunks_ref_count_del too"
        );
    }

    #[test]
    fn open_repository_seeds_the_root_tree_entry() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, settings()).expect("init must succeed");

        let repo = open_repository(&repo_root).expect("open must succeed");
        let root = repo
            .resolve_path("/")
            .expect("resolve must succeed")
            .expect("root must exist");
        assert_eq!(root.id, 0);
        assert_eq!(root.kind, EntryKind::Dir);
    }

    #[test]
    fn dedup_ref_counts_follow_tree_entry_insert_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, settings()).expect("init must succeed");
        let repo = open_repository(&repo_root).expect("open must succeed");

        repo.with_connection(|conn| {
            conn.execute(
                "INSERT INTO contents (id, length, hash) \
                 VALUES (2, 3, X'0102030405060708090A0B0C0D0E0F1011121314')",
                (),
            )?;
            conn.execute(
                "INSERT INTO tree_entries (id, parent_id, name, time, content_id, kind) \
                 VALUES (1, 0, 'a.txt', 0, 2, 1)",
                (),
            )?;
            Ok(())
        })
        .expect("inserts must succeed");

        let ref_count = |repo: &Repository| {
            repo.with_connection(|conn| {
                Ok(
                    conn.query_row("SELECT ref_count FROM contents WHERE id = 2", (), |row| {
                        row.get::<_, i64>(0)
                    })?,
                )
            })
            .expect("row must exist")
        };
        assert_eq!(ref_count(&repo), 1);

        repo.with_connection(|conn| Ok(conn.execute("DELETE FROM tree_entries WHERE id = 1", ())?))
            .expect("delete must succeed");
        assert_eq!(ref_count(&repo), 0);
    }
}
