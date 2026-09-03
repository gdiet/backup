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
mod name_cache;
mod settings;
mod tree;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rusqlite::{Connection, OpenFlags};

pub use content::ChunkLocation;
pub use lock::{UnlockOutcome, WriteLock};
pub use settings::RepositorySettings;
pub use tree::{Entry, EntryKind};

// Repository on-disk layout - DESIGN-REPOSITORY-001 in
// docs/design/repository-layout.md.
const META_DIR: &str = "meta";
const META_TMP_DIR: &str = "meta.tmp";
const META_DB_FILE: &str = "repository.sqlite3";
const DATA_DIR: &str = "data";

// How many directories' name_cache entries to keep at once - DESIGN-MOUNT-017 in
// docs/design/tree-namespace-case-sensitivity.md; see that design entry and
// crates/db/src/name_cache.rs. Chosen by feel ("a small LRU cache"), not by measurement or a
// memory budget - it bounds only the number of cached directories, not the size of any one of them
// (each holds every live child of that directory, however many there are).
const NAME_CACHE_CAPACITY: usize = 16;

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
    /// [`open_repository_read_only`] found the repository's schema behind the version this code
    /// expects - a read-only connection cannot run the pending migration itself (no write
    /// permission), unlike [`open_repository`], which always migrates automatically
    /// (DESIGN-METADATA-005).
    SchemaNeedsMigration(PathBuf),
    /// A [`Repository`] opened via [`open_repository_read_only`] was asked to perform a
    /// repository-mutating operation - refused before ever touching the read-only connection,
    /// rather than surfacing SQLite's own `SQLITE_READONLY` as a bare [`Error::Sqlite`].
    ReadOnlyRepository,
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
    /// Another thread using this [`Repository`] panicked while holding its internal lock (guarding
    /// the connection and DESIGN-MOUNT-017's name cache alongside it).
    Poisoned,
    /// [`acquire_write_lock`] found the repository's write lock file already present - either
    /// genuinely held by another process, or left behind by one that exited without releasing it
    /// (DESIGN-MAINTENANCE-002 in `docs/design/repository-locking.md` deliberately does not tell
    /// the two apart here) - REQ-MAINTENANCE-004's "only one repository-mutating session runs at a
    /// time". [`unlock_stale_write_lock`] is the explicit way to check which case it actually is.
    AlreadyLocked(PathBuf),
    /// [`acquire_write_lock`] failed for a reason other than the lock already being held - most
    /// plausibly the underlying storage not actually enforcing locking at all (DESIGN-MAINTENANCE-001
    /// in `docs/design/repository-locking.md`'s "Known limitation": expected on a network-mounted
    /// repository, not on local/removable storage).
    LockUnavailable {
        path: PathBuf,
        source: std::io::Error,
    },
    /// [`acquire_write_lock`] could not even create the write lock file - after a short retry for
    /// the one known transient cause (a concurrent release's Windows "pending delete" window, see
    /// `lock.rs`'s `create_new_lock_file_with_pending_delete_retry`), most plausibly a genuine
    /// filesystem-permissions problem rather than lock contention (that case is `AlreadyLocked`
    /// instead).
    LockFileInaccessible {
        path: PathBuf,
        source: std::io::Error,
    },
    /// SQLite reported a `journal_mode` other than `wal` after `configure_write_connection`
    /// requested it - e.g. an unsupported filesystem (SQLite silently falls back instead of
    /// failing outright). Carries whatever mode SQLite actually settled on.
    WalUnavailable(String),
    /// A connection-configuring `PRAGMA` (`connection::configure_write_connection`) hard-failed
    /// with a locking- or I/O-category SQLite error, rather than either succeeding or falling back
    /// silently (that case is [`Error::WalUnavailable`] instead) - observed over a WSL<->Windows 9p
    /// bridge, where the filesystem cannot support WAL's locking requirements at all. Distinct from
    /// [`Error::LockUnavailable`]/[`Error::AlreadyLocked`], which are about DESIGN-MAINTENANCE-001's
    /// separate `flock`-based write lock, not the database connection itself.
    ConnectionUnreliable(rusqlite::Error),
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
            Error::SchemaNeedsMigration(path) => {
                write!(
                    f,
                    "the repository at {} needs a schema migration, which a read-only open cannot \
                     perform - open it once with a write-capable operation (e.g. `dfs mount \
                     --read-write`) first",
                    path.display()
                )
            }
            Error::ReadOnlyRepository => {
                write!(
                    f,
                    "this repository was opened read-only; this operation needs write access"
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
            Error::Poisoned => write!(f, "an internal repository lock was poisoned"),
            Error::AlreadyLocked(path) => {
                write!(
                    f,
                    "the repository at {} is already locked for writing by another process (or, \
                     if that process crashed without releasing it, a leftover lock file) - if you \
                     are sure no other process is using this repository, run `dfs unlock` to check \
                     and clear it",
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
            Error::LockFileInaccessible { path, source } => {
                write!(
                    f,
                    "could not create the write lock file for the repository at {} ({source}) - \
                     most likely a filesystem-permissions problem (check that this process can \
                     create files under {}); briefly retried in case another process's release \
                     was still completing, but the failure persisted",
                    path.display(),
                    path.display()
                )
            }
            Error::WalUnavailable(mode) => {
                write!(
                    f,
                    "WAL journal mode unavailable - SQLite reports {mode:?} instead"
                )
            }
            Error::ConnectionUnreliable(source) => {
                write!(
                    f,
                    "could not open the repository's database ({source}) - this can happen on a \
                     network-mounted or WSL<->Windows-bridged filesystem, where SQLite's WAL \
                     locking is not reliably supported; run dfs from the machine where the \
                     repository physically resides. See README.md's \"Known Limitations\""
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

/// [`Repository`]'s connection and DESIGN-MOUNT-017's name cache, held together
/// behind one [`Mutex`] rather than two separately-locked fields - see [`Repository`]'s own doc
/// comment and `crates/db/src/name_cache.rs` for why: the cache's own correctness depends on never
/// being reached except while the connection is already locked, and putting both behind the same
/// `Mutex` makes that impossible to violate, rather than relying on every caller to remember it.
#[derive(Debug)]
struct Locked {
    conn: Connection,
    name_cache: name_cache::NameCache,
}

/// A handle to an existing, open repository.
///
/// Holds one connection for its whole lifetime, behind a mutex, rather than opening a fresh one
/// per call - DESIGN-METADATA-003's "one coordinated writer" model requires exactly that for
/// writes. A [`Repository`] from [`open_repository`] shares that same connection/lock between
/// reads and writes; one from [`open_repository_read_only`] holds a genuinely read-only
/// connection instead and refuses any mutating call outright (see `with_transaction` below) -
/// DESIGN-METADATA-003's eventual split of reads onto their own, unlocked connection(s) is still
/// only this one-connection-per-`Repository` step, not that fuller design.
#[derive(Debug)]
pub struct Repository {
    settings: RepositorySettings,
    locked: Mutex<Locked>,
    read_only: bool,
}

impl Repository {
    pub fn settings(&self) -> RepositorySettings {
        self.settings
    }

    fn with_connection<T>(
        &self,
        f: impl FnOnce(&Connection, &mut name_cache::NameCache) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut locked = self.locked.lock().map_err(|_| Error::Poisoned)?;
        let Locked { conn, name_cache } = &mut *locked;
        f(conn, name_cache)
    }

    /// Like [`Self::with_connection`], but runs `f` inside an explicit transaction, committed only
    /// if `f` returns `Ok` - a multi-statement operation either lands as a whole or not at all,
    /// rather than leaving a partial result behind if interrupted partway through (a crash, a
    /// panic unwinding through `f`, `rusqlite::Transaction`'s own drop-without-commit rollback).
    ///
    /// Every mutating [`Repository`] method goes through this one choke point, so a
    /// [`open_repository_read_only`] connection is refused here, before ever touching the
    /// connection itself, rather than letting each call site rediscover SQLite's own
    /// `SQLITE_READONLY` independently.
    fn with_transaction<T>(
        &self,
        f: impl FnOnce(&Connection, &mut name_cache::NameCache) -> Result<T, Error>,
    ) -> Result<T, Error> {
        if self.read_only {
            return Err(Error::ReadOnlyRepository);
        }
        let mut locked = self.locked.lock().map_err(|_| Error::Poisoned)?;
        let Locked { conn, name_cache } = &mut *locked;
        let tx = conn.transaction()?;
        let result = f(&tx, name_cache)?;
        tx.commit()?;
        Ok(result)
    }

    /// Looks up the live entry at `path` (`/`-separated, e.g. `/a/b`; `/` itself resolves to the
    /// root). `Ok(None)` if any path component does not exist (or is soft-deleted) - not itself
    /// an error, since "does this path exist" is a legitimate question to ask.
    pub fn resolve_path(&self, path: &str) -> Result<Option<Entry>, Error> {
        self.with_connection(|conn, cache| tree::resolve_path(conn, cache, path))
    }

    /// Lists the live, direct children of the directory entry `parent_id`, each paired with its
    /// own [`Entry`] (so a caller gets kind/size/mtime without a separate lookup per child).
    pub fn list_children(&self, parent_id: i64) -> Result<Vec<(String, Entry)>, Error> {
        self.with_connection(|conn, _cache| tree::list_children(conn, parent_id))
    }

    /// Creates a new, empty directory named `name` inside the directory `parent_id`, bumping its
    /// parent's modification time (REQ-TREE-005). Returns the new entry's id.
    pub fn mkdir(&self, parent_id: i64, name: &str, time_millis: i64) -> Result<i64, Error> {
        self.with_transaction(|conn, cache| tree::mkdir(conn, cache, parent_id, name, time_millis))
    }

    /// Soft-deletes the directory entry `id` (REQ-TREE-002), refusing if it still has live
    /// children (REQ-TREE-008). Bumps its parent's modification time.
    pub fn rmdir(&self, id: i64, time_millis: i64) -> Result<(), Error> {
        self.with_transaction(|conn, cache| tree::rmdir(conn, cache, id, time_millis))
    }

    /// Soft-deletes the live file entry `id` (REQ-TREE-002). Bumps its parent's modification
    /// time - unlike DESIGN-MOUNT-011's pure content overwrite, removing a name is a structural
    /// change (REQ-TREE-005). A directory at `id` is refused.
    pub fn unlink_file(&self, id: i64, time_millis: i64) -> Result<(), Error> {
        self.with_transaction(|conn, cache| tree::unlink_file(conn, cache, id, time_millis))
    }

    /// Looks up the live entry by its own id, rather than by path - `Ok(None)` if it does not
    /// exist or is soft-deleted, the same as [`Self::resolve_path`].
    pub fn entry_by_id(&self, id: i64) -> Result<Option<Entry>, Error> {
        self.with_connection(|conn, _cache| tree::get_by_id(conn, id))
    }

    /// The live entry `id`'s current `(parent_id, name)` - `None` if it does not exist or is
    /// soft-deleted, reflecting any `rename` since `id` was first obtained.
    pub fn parent_and_name(&self, id: i64) -> Result<Option<(i64, String)>, Error> {
        self.with_connection(|conn, _cache| tree::parent_and_name(conn, id))
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
        self.with_transaction(|conn, cache| {
            tree::settle_file(conn, cache, parent_id, name, time_millis, content_id)
        })
    }

    /// Like [`Self::settle_file`], except a live entry that is still exactly id
    /// `collapsible_placeholder_id` is hard-deleted instead of soft-deleted (DESIGN-MOUNT-016) -
    /// a `create()`-only empty placeholder still untouched at its own file's first real settle,
    /// never independently meaningful. Any other live entry there is soft-deleted as usual.
    pub fn settle_file_collapsing_placeholder(
        &self,
        parent_id: i64,
        name: &str,
        time_millis: i64,
        content_id: i64,
        collapsible_placeholder_id: i64,
    ) -> Result<i64, Error> {
        self.with_transaction(|conn, cache| {
            tree::settle_file_collapsing_placeholder(
                conn,
                cache,
                parent_id,
                name,
                time_millis,
                content_id,
                collapsible_placeholder_id,
            )
        })
    }

    /// Looks up an already-known chunk by its own `(length, hash)` - REQ-STORAGE-002.
    pub fn find_chunk(&self, length: i64, hash: &[u8]) -> Result<Option<i64>, Error> {
        self.with_connection(|conn, _cache| content::find_chunk(conn, length, hash))
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
        self.with_transaction(|conn, _cache| content::reserve_and_insert_chunk(conn, length, hash))
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
        self.with_transaction(|conn, _cache| {
            content::find_or_create_content(conn, length, hash, chunk_ids)
        })
    }

    /// Returns `content_id`'s complete physical layout in `crates/store` - every backing
    /// `(start, stop)` range, in logical order. Concatenating the bytes at these ranges, in this
    /// order, reproduces the content's own bytes exactly. An unknown `content_id` returns an
    /// empty `Vec`, the same as a genuinely zero-length content.
    pub fn resolve_extents(&self, content_id: i64) -> Result<Vec<(u64, u64)>, Error> {
        self.with_connection(|conn, _cache| content::resolve_extents(conn, content_id))
    }

    /// Like [`Self::resolve_extents`], but grouped back into `content_id`'s individual chunk
    /// occurrences, each carrying its own recorded `(length, hash)` alongside its extents - what a
    /// caller verifying restored bytes against their recorded hash needs.
    pub fn resolve_chunks(&self, content_id: i64) -> Result<Vec<content::ChunkLocation>, Error> {
        self.with_connection(|conn, _cache| content::resolve_chunks(conn, content_id))
    }

    /// Sets `id`'s own modification time (REQ-MOUNT-003's `utimens`).
    pub fn set_mtime(&self, id: i64, time_millis: i64) -> Result<(), Error> {
        self.with_connection(|conn, _cache| tree::set_mtime(conn, id, time_millis))
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
        self.with_transaction(|conn, cache| {
            tree::rename(
                conn,
                cache,
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

    if let Err(err) = init_repository_contents(repo_root, settings) {
        // Best-effort: remove only what this call itself just created (data/, meta.tmp/), so a
        // retry after a transient failure (e.g. Error::ConnectionUnreliable over an unsupported
        // filesystem) does not also have to fight a confusing "already exists and is not empty"
        // pointing at debris from this same failed attempt. Never touch repo_root itself, which
        // may have pre-existed (REQ-CLI-005's "already-mounted external drive" case) and must be
        // left exactly as it was found; the cleanup errors themselves are not reported - nothing
        // useful to do about them, and the original error is what actually matters here.
        let _ = fs::remove_dir_all(repo_root.join(DATA_DIR));
        let _ = fs::remove_dir_all(repo_root.join(META_TMP_DIR));
        return Err(err);
    }

    Ok(())
}

/// The actual creation work, factored out of [`init_repository`] so its caller can clean up
/// whatever partial state this leaves behind on failure.
fn init_repository_contents(repo_root: &Path, settings: RepositorySettings) -> Result<(), Error> {
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

/// Cheaply checks that `repo_root` looks like a repository - i.e. that [`open_repository`] against
/// it would not immediately fail with `Error::NoRepositoryHere` - without opening a database
/// connection or running migrations. For a caller that only needs this existence guard, not the
/// database itself: notably `dfs unlock`'s own guard, which must keep working even when the
/// database file itself is unreadable (DESIGN-MAINTENANCE-003 in
/// `docs/design/repository-locking.md` - exactly the situation it exists to recover from).
pub fn ensure_repository_exists(repo_root: &Path) -> Result<(), Error> {
    if repo_root.join(META_DIR).is_dir() {
        Ok(())
    } else {
        Err(Error::NoRepositoryHere(repo_root.to_path_buf()))
    }
}

/// Opens an existing repository at `repo_root`, reading back the settings it
/// was created with. Applies any pending schema migration automatically
/// (DESIGN-METADATA-005).
pub fn open_repository(repo_root: &Path) -> Result<Repository, Error> {
    ensure_repository_exists(repo_root)?;

    let db_path = repo_root.join(META_DIR).join(META_DB_FILE);
    let mut conn = Connection::open(&db_path)?;
    connection::configure_write_connection(&conn)?;
    migrations::migrations().to_latest(&mut conn)?;

    let settings = read_settings(&conn)?;
    Ok(Repository {
        settings,
        locked: Mutex::new(Locked {
            conn,
            name_cache: name_cache::NameCache::new(NAME_CACHE_CAPACITY),
        }),
        read_only: false,
    })
}

/// Opens an existing repository at `repo_root` for reading only - a genuinely `SQLITE_OPEN_READ_ONLY`
/// connection (`connection::configure_read_only_connection`), never [`open_repository`]'s WAL/
/// `auto_vacuum`/`foreign_keys`/`synchronous` setup, since none of that is either meaningful or
/// permitted on a connection that never writes. Every mutating [`Repository`] method refuses
/// outright against the result (`Error::ReadOnlyRepository`) rather than reaching SQLite at all.
///
/// Unlike [`open_repository`], this never migrates: a read-only connection cannot run one (no
/// write permission), so a schema behind the version this code expects is
/// [`Error::SchemaNeedsMigration`] instead - open the repository once with a write-capable
/// operation first (that migrates it, per DESIGN-METADATA-005), then read-only opens work again.
///
/// Meant for a caller that only ever reads - a read-only mount (REQ-MOUNT-002), in particular -
/// and specifically for one that still needs to work even when the filesystem cannot reliably
/// support a full write-mode connection open at all (observed over a WSL<->Windows 9p bridge; see
/// `Error::ConnectionUnreliable` and README.md's "Known Limitations").
pub fn open_repository_read_only(repo_root: &Path) -> Result<Repository, Error> {
    ensure_repository_exists(repo_root)?;

    let db_path = repo_root.join(META_DIR).join(META_DB_FILE);
    let conn = Connection::open_with_flags(
        &db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    connection::configure_read_only_connection(&conn)?;

    if migrations::migrations().pending_migrations(&conn)? != 0 {
        return Err(Error::SchemaNeedsMigration(repo_root.to_path_buf()));
    }

    let settings = read_settings(&conn)?;
    Ok(Repository {
        settings,
        locked: Mutex::new(Locked {
            conn,
            name_cache: name_cache::NameCache::new(NAME_CACHE_CAPACITY),
        }),
        read_only: true,
    })
}

/// Reads back the single `repository_settings` row - shared by [`open_repository`] and
/// [`open_repository_read_only`], which differ only in how `conn` itself was opened.
fn read_settings(conn: &Connection) -> Result<RepositorySettings, Error> {
    let (cdc_target_size_bits, creation_time_millis): (Option<u32>, i64) = conn.query_row(
        "SELECT cdc_target_size_bits, creation_time FROM repository_settings WHERE id = 1",
        (),
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(RepositorySettings::new(
        cdc_target_size_bits,
        creation_time_millis,
    ))
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

/// Explicitly checks whether `repo_root`'s write lock is genuinely stale (an OS-level `flock`
/// test, not a heuristic over the diagnostic marker's content) and clears it if so -
/// DESIGN-MAINTENANCE-003's manual counterpart to [`acquire_write_lock`]'s unconditional refusal
/// (DESIGN-MAINTENANCE-002) on a merely-present lock file. Never removes an actively held lock: a
/// still-held lock is reported back as [`UnlockOutcome::StillLocked`], not modified.
///
/// `repo_root` must already hold a repository, same as [`acquire_write_lock`].
pub fn unlock_stale_write_lock(repo_root: &Path) -> Result<UnlockOutcome, Error> {
    lock::try_unlock_stale_write_lock(&repo_root.join(META_DIR))
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
    fn init_repository_cleans_up_data_and_meta_tmp_after_a_failed_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        // Out of range on purpose - not validated by RepositorySettings::new itself (see its own
        // doc comment), so this reaches the repository_settings table's CHECK constraint and fails
        // there, deterministically, only after init_repository_contents has already created data/
        // and meta.tmp/ - exercising the cleanup path without needing a real unsupported
        // filesystem.
        let err = init_repository(
            &repo_root,
            RepositorySettings::new(Some(3), 1_700_000_000_000),
        )
        .expect_err("an out-of-range cdc_target_size_bits must fail via the CHECK constraint");
        assert!(
            matches!(err, Error::Sqlite(_)),
            "expected a CHECK constraint failure, got: {err:?}"
        );

        assert!(
            !repo_root.join(DATA_DIR).exists(),
            "a failed init_repository must clean up the data/ directory it created"
        );
        assert!(
            !repo_root.join(META_TMP_DIR).exists(),
            "a failed init_repository must clean up the meta.tmp/ directory it created"
        );
        assert!(
            repo_root.is_dir() && fs::read_dir(&repo_root).unwrap().next().is_none(),
            "repo_root itself must be left alone - still present and empty, ready for a retry"
        );
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
    fn open_repository_read_only_fails_on_a_directory_that_was_never_created_as_one() {
        let dir = tempfile::tempdir().unwrap();
        let err = open_repository_read_only(dir.path()).unwrap_err();
        assert!(matches!(err, Error::NoRepositoryHere(_)));
    }

    #[test]
    fn open_repository_read_only_reads_back_the_settings_it_was_created_with() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, settings()).expect("init must succeed");

        let repo = open_repository_read_only(&repo_root).expect("read-only open must succeed");
        assert_eq!(repo.settings(), settings());
    }

    #[test]
    fn open_repository_read_only_can_read_the_tree() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, settings()).expect("init must succeed");

        let repo = open_repository_read_only(&repo_root).expect("read-only open must succeed");
        let root = repo
            .resolve_path("/")
            .expect("resolving the root must succeed on a read-only connection")
            .expect("the root entry must exist");
        assert_eq!(root.kind, EntryKind::Dir);
    }

    #[test]
    fn open_repository_read_only_refuses_a_mutating_call() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, settings()).expect("init must succeed");

        let repo = open_repository_read_only(&repo_root).expect("read-only open must succeed");
        let err = repo.mkdir(0, "d", 1_700_000_000_000).unwrap_err();
        assert!(
            matches!(err, Error::ReadOnlyRepository),
            "expected ReadOnlyRepository, got: {err:?}"
        );
    }

    #[test]
    fn open_repository_read_only_refuses_a_repository_behind_the_expected_schema_version() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        // A meta/ directory holding a fresh, unmigrated database (user_version = 0, SQLite's own
        // default) - simulates a repository older than this code's schema, without needing a real
        // prior schema version to exist (this crate is pre-release, single-migration - see
        // DESIGN-METADATA-005's "Pre-release: a single, freely rewritten v1 migration").
        fs::create_dir_all(repo_root.join(META_DIR)).unwrap();
        Connection::open(repo_root.join(META_DIR).join(META_DB_FILE)).unwrap();

        let err = open_repository_read_only(&repo_root).unwrap_err();
        assert!(
            matches!(err, Error::SchemaNeedsMigration(_)),
            "expected SchemaNeedsMigration, got: {err:?}"
        );
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
            .with_connection(|conn, _cache| {
                Ok(conn.query_row("PRAGMA journal_mode", (), |row| row.get(0))?)
            })
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
            .with_connection(|conn, _cache| {
                Ok(conn.query_row("PRAGMA auto_vacuum", (), |row| row.get(0))?)
            })
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
            .with_connection(|conn, _cache| {
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

        repo.with_connection(|conn, _cache| {
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
            .with_connection(|conn, _cache| {
                Ok(
                    conn.query_row("SELECT ref_count FROM chunks WHERE id = 1", (), |row| {
                        row.get(0)
                    })?,
                )
            })
            .unwrap();
        assert_eq!(chunk_ref_count, 1);

        repo.with_connection(|conn, _cache| {
            Ok(conn.execute("DELETE FROM contents WHERE id = 1", ())?)
        })
        .expect("delete must succeed");

        let content_chunks_left: i64 = repo
            .with_connection(|conn, _cache| {
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
            .with_connection(|conn, _cache| {
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

        repo.with_connection(|conn, _cache| {
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
            repo.with_connection(|conn, _cache| {
                Ok(
                    conn.query_row("SELECT ref_count FROM contents WHERE id = 2", (), |row| {
                        row.get::<_, i64>(0)
                    })?,
                )
            })
            .expect("row must exist")
        };
        assert_eq!(ref_count(&repo), 1);

        repo.with_connection(|conn, _cache| {
            Ok(conn.execute("DELETE FROM tree_entries WHERE id = 1", ())?)
        })
        .expect("delete must succeed");
        assert_eq!(ref_count(&repo), 0);
    }
}
