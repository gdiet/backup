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
mod compact;
mod error;
mod maintenance;
mod migrations;
mod query;
mod settings;
mod tree;

pub use backup::{
    ChunkRef, ContentSource, FileBackupRecord, apply_backup_batch, find_chunk, resolve_content,
};
pub use compact::{
    bump_store_generation, bytes_to_relocate, next_chunk_to_relocate, relocate_chunk,
    total_live_bytes,
};
pub use error::Error;
pub use maintenance::{
    ReclaimStats, reclaim_space, soft_delete, soft_delete_and_replace_with_empty, undelete,
};
pub use query::{
    ChunkInfo, DeletedEntry, PathEntry, SubtreeStats, all_chunks, chunk_extents,
    chunk_extents_sorted, contents_for_chunk, deleted_entries, entries_for_content, file_size,
    free_space_summary, has_deleted_children, list_children, ordered_content_chunks, path_of,
    resolve_path, store_generation, subtree_entries_with_paths, subtree_stats,
};
pub use settings::{CDC_TARGET_SIZE_BITS_RANGE, Chunking, RepositorySettings, SettingsError};
pub use tree::{
    EntryKind, TreeEntryRow, finalize_as_empty_if_undecided, find_tree_entry, get_tree_entry,
    insert_directory, insert_historical_tree_entry, is_deleted, parent_id, rename_entry,
    touch_mtime,
};

use std::fs;
use std::path::Path;

use rusqlite::{Connection, OpenFlags, OptionalExtension};
use rusqlite_migration::SchemaVersion;

/// Directory (relative to the repository root) holding the metadata database.
const META_DIR: &str = "meta";
/// Staging directory [`create_repository_files`] builds [`META_DIR`]'s
/// contents under before the final atomic rename - see its doc comment.
const META_TMP_DIR: &str = "meta.tmp";
/// File name of the metadata database within [`META_DIR`].
const META_DB_FILE: &str = "repository.sqlite3";
/// Directory (relative to the repository root) holding the chunk data store.
const DATA_DIR: &str = "data";

/// The `contents.id` every genuinely empty file (zero chunks) resolves to -
/// seeded once, at a fixed id, by `migrations.rs`'s `SCHEMA_V1` (`length =
/// 0`, `hash` = BLAKE3's XOF output for an empty input), not created on
/// demand the way every other `contents` row is. This is what makes
/// `content_id IS NULL` on a *file* `tree_entries` row unambiguous: it no
/// longer means "empty file" (that's now `Some(EMPTY_CONTENT_ID)`, exactly
/// like any other deduplicated content) - it means specifically "no content
/// decided yet", the mount's `create()` placeholder before its first write
/// (`ContentSource::Known(None)`, see that variant's own doc comment). A
/// directory is unaffected either way, still identified by `kind` alone,
/// never by `content_id`.
///
/// [`crate::reclaim_space`] never purges this row even at `ref_count = 0`
/// (see its own doc comment) - `resolve_content` returns it directly,
/// without re-checking it still exists, so it must always exist.
pub const EMPTY_CONTENT_ID: i64 = 1;

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
///
/// Uses `rusqlite::Connection::open`'s own default flags rather than
/// `open_with_flags` with an explicit list, unlike
/// `open_connection_read_only` below - worth noting so the two don't look
/// inconsistent at a glance: those defaults are `SQLITE_OPEN_READ_WRITE |
/// SQLITE_OPEN_CREATE | SQLITE_OPEN_NO_MUTEX | SQLITE_OPEN_URI`, i.e.
/// already `NO_MUTEX` (safe here because a `Connection` is never shared
/// across threads without external synchronization - every caller already
/// holds one behind its own `Mutex` or owns it for a single function
/// call's duration) and already `URI`, matching `open_connection_read_only`
/// on both.
fn open_connection(path: &Path) -> Result<Connection, Error> {
    open_connection_inner(path).map_err(|err| classify_open_error(err, path))
}

/// The fallible body of [`open_connection`], split out so its single
/// `rusqlite::Error` exit point can be classified exactly once by the
/// caller - see [`classify_open_error`]'s doc comment for why that matters
/// here: which one of the calls below is actually the one that first fails
/// with `SQLITE_CANTOPEN` against a read-only underlying filesystem turns
/// out not to be reliably the same call every time (traced empirically:
/// depends on incidental details of when SQLite lazily sets up its WAL
/// shared-memory index on first real use, not simply "whichever call looks
/// like it touches the file most directly") - wrapping every call
/// individually would be fragile (easy to add a future call here and
/// forget it needs the same treatment) where wrapping the whole function
/// once isn't.
fn open_connection_inner(path: &Path) -> rusqlite::Result<Connection> {
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

/// Rewrites a `rusqlite::Error` into [`Error::CannotOpenForWriting`] if it's
/// specifically `SQLITE_CANTOPEN` - SQLite's one, generic "the OS-level
/// `open()` call failed" code, covering many distinct underlying causes
/// (missing parent directory, a read-only filesystem/mount, an unrelated
/// permission problem, too many open files, ...) with no way to tell them
/// apart from the error alone. So rather than claim a specific cause, the
/// rewritten message only *asks* whether the storage might be read-only -
/// honest about the uncertainty, unlike a flat assertion would be.
///
/// Deliberately doesn't try to determine the real cause itself (e.g. via an
/// OS-level write probe before opening) the way an earlier version of this
/// function did: that approach needed to open a real file handle with
/// write intent to get a trustworthy answer, which turned out to have a
/// platform-specific catch - Microsoft's own `SetFileTime` docs note that
/// merely holding a handle opened with write access can touch a file's
/// last-write time on Windows/NTFS when closed, *even if nothing was ever
/// written* - a side effect this project has no way to verify one way or
/// the other without a real Windows environment to test against. Relying
/// on the error SQLite already produces (the exact same C library, hence
/// the exact same `SQLITE_CANTOPEN` code, on every platform) instead of an
/// extra probe of our own sidesteps that platform risk entirely, at the
/// cost of a less certain-sounding message.
///
/// Applied once, to whatever `rusqlite::Error` [`open_connection_inner`]'s
/// call sequence exits with - not to each call individually. Traced
/// empirically against a real read-only-mounted *existing* database file:
/// `Connection::open` alone doesn't fail at all (SQLite's own `unixOpen`
/// silently retries as read-only internally, since reading an existing
/// file never needs write access) - some *later* pragma in the sequence is
/// what actually fails, once SQLite lazily sets up the WAL shared-memory
/// index on first real use. Which specific pragma that turns out to be is
/// not consistent (observed `synchronous` failing in one run, `journal_mode`
/// in another, depending on incidental init-order details this crate has
/// no control over) - exactly why this is applied to the whole function's
/// outcome rather than guessed at a single call site. For a not-yet-existing
/// file (e.g. `init_repository` targeting a read-only directory),
/// `Connection::open` itself is where it appears instead, since there's no
/// existing file for that same retry-as-read-only fallback to read.
fn classify_open_error(err: rusqlite::Error, path: &Path) -> Error {
    if let rusqlite::Error::SqliteFailure(ffi_err, _) = &err
        && ffi_err.code == rusqlite::ErrorCode::CannotOpen
    {
        return Error::CannotOpenForWriting(path.to_path_buf());
    }
    Error::Sqlite(err)
}

/// Opens the SQLite database at `path` genuinely read-only at the SQLite
/// level (`SQLITE_OPEN_READ_ONLY`) - not just "a connection this crate's
/// callers happen to only issue `SELECT`s through", the way every
/// connection used to be before this existed. A stray write attempt
/// through a connection from this function now fails outright instead of
/// silently succeeding.
///
/// Doesn't create the file if missing (`open_connection` does, via
/// SQLite's default flags) - callers only ever use this once a repository
/// is already known to exist.
///
/// Only sets the pragmas that matter for a connection that will never
/// write: `busy_timeout`, in case a read still transiently contends with
/// the writer (e.g. mid-checkpoint - see the module-level doc comment).
/// `synchronous`/`auto_vacuum`/`journal_mode` all govern write/commit
/// behavior and are meaningless here; `journal_mode` in particular can't be
/// reasserted on a read-only connection even as a same-value no-op (SQLite
/// requires write access to execute the pragma's assignment form at all) -
/// harmless to skip, since by the time any caller opens a read-only
/// connection, [`open_repository`] has already run migrations (and with
/// them, `open_connection`'s own `journal_mode` switch) over a regular
/// read-write connection first.
fn open_connection_read_only(path: &Path) -> Result<Connection, Error> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.pragma_update(None, "busy_timeout", 5000)?;
    Ok(conn)
}

/// Percent-encodes `path` for SQLite's `file:` URI filename syntax
/// (<https://www.sqlite.org/uri.html>), for use by
/// [`open_connection_immutable`] - a real filesystem path can contain any
/// byte that syntax would otherwise misinterpret as a query (`?`), fragment
/// (`#`), or escape (`%`) delimiter, so every byte outside the small set
/// URIs leave unencoded is escaped here. Backslashes are normalized to `/`
/// first: SQLite's URI parser expects forward slashes even for a Windows
/// drive path (`file:C:/repo/...`), per the same doc's Windows section.
/// Lossy for non-UTF-8 paths (matches this codebase's existing convention
/// for turning a `Path` into a displayable/transportable string, e.g.
/// `Path::display` in every "failed to open repository at {}" error
/// message) - a mismatched byte would make SQLite fail to find the file
/// rather than open the wrong one, which is a safe failure mode.
fn sqlite_uri_path(path: &Path) -> String {
    let normalized = path.to_string_lossy().replace('\\', "/");
    let mut encoded = String::with_capacity(normalized.len());
    for byte in normalized.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' | b':' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

/// Opens the SQLite database at `path` genuinely read-only, via SQLite's
/// `immutable=1` URI query parameter
/// (<https://www.sqlite.org/uri.html#uriimmutable>) rather than plain
/// `SQLITE_OPEN_READ_ONLY` (see [`open_connection_read_only`]) - the two
/// sound equivalent but aren't: even a plain read-only connection to a
/// WAL-mode database still needs to create/write the `-shm` sidecar file
/// (readers record their "read mark" there), and - unlike a writer - can
/// never remove it again on close, since only a connection that can write
/// the *main* database file can ever checkpoint. `immutable=1` instead
/// tells SQLite to trust that the file won't change underneath it and read
/// straight from the main database file, never touching `-wal`/`-shm` at
/// all. Only correct to use once the caller has already confirmed there's
/// no pending (non-empty) `-wal` to ignore - see [`open_repository_read_only`],
/// the only caller - otherwise "ignoring WAL entirely" would mean silently
/// serving stale data instead of the equivalent-to-checkpointed state it
/// relies on here.
///
/// No `busy_timeout` pragma, unlike [`open_connection_read_only`]: an
/// immutable connection never takes any SQLite-level lock, so there is
/// nothing for it to ever wait on.
fn open_connection_immutable(path: &Path) -> Result<Connection, Error> {
    let uri = format!("file:{}?immutable=1", sqlite_uri_path(path));
    let conn = Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    Ok(conn)
}

/// Fails with [`Error::SchemaTooNew`] if `conn`'s database schema is newer
/// than any migration this build of `backup` knows about - i.e. it was
/// created or last opened by a newer version of the program. Checked via
/// [`rusqlite_migration::Migrations::current_version`], which only reads
/// `PRAGMA user_version` (works on a read-only connection, doesn't migrate),
/// so this can run before [`open_repository`] attempts `to_latest` (which
/// would otherwise fail on the same condition with a far less actionable
/// error - see [`Error::SchemaTooNew`]'s doc comment) and before
/// [`open_repository_read_only`] does anything else.
fn reject_if_schema_too_new(conn: &Connection) -> Result<(), Error> {
    if let SchemaVersion::Outside(db_version) = migrations::migrations().current_version(conn)? {
        return Err(Error::SchemaTooNew {
            db_version: db_version.get(),
        });
    }
    Ok(())
}

/// The exact `(length, hash)` [`EMPTY_CONTENT_ID`]'s seed row must have -
/// BLAKE3's XOF output for an empty input, truncated to 20 bytes (see
/// [`EMPTY_CONTENT_ID`]'s own doc comment). Kept here as a plain byte
/// literal, matching the same value hardcoded as a SQL hex literal in
/// `migrations.rs`'s `SCHEMA_V1` seed `INSERT` - a `const &str` SQL
/// migration string can't interpolate a Rust constant, so the two can't
/// share a single source of truth; [`verify_empty_content_seed`] is what
/// catches the two ever drifting apart (or the seed never having run at
/// all - see its own doc comment).
const EMPTY_CONTENT_HASH: [u8; 20] = [
    0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6, 0xa0, 0x40, 0x4d, 0xea, 0x36, 0xdc, 0xc9, 0x49,
    0x9b, 0xcb, 0x25, 0xc9,
];

/// Fails with [`Error::EmptyContentSeedMismatch`] unless `contents.id =
/// EMPTY_CONTENT_ID` holds exactly the seeded empty-content row this
/// crate's schema is supposed to guarantee (`length = 0`, `hash =
/// EMPTY_CONTENT_HASH`). Catches, loudly and at open time, a repository
/// that predates this seed: `SCHEMA_V1` is a single, already-squashed
/// migration (see its own doc comment), so `to_latest` never re-runs it for
/// a database already at `user_version = 1` - which is every repository
/// ever `init`ed, since there's only one schema version. Without this
/// check, the first command to persist an empty file against such a
/// repository would silently call [`resolve_content`] and get back
/// `EMPTY_CONTENT_ID`, aliasing the new "empty" file onto whatever
/// unrelated content already happened to occupy that id - no foreign-key
/// violation, no error, just wrong data read back later. Found for real in
/// a repository that predated this check - see `docs/plans/implemented/
/// verify-empty-content-seed-on-open.md`.
///
/// Deliberately just a loud failure, not a migration that fixes it in
/// place: every repository known to be affected today is disposable test
/// data (delete and re-`init`), so a real in-place migration (relocating
/// the empty-content row to an id that isn't already taken, and repointing
/// every existing `content_id`-NULL-as-empty row at it) is a bigger, more
/// careful piece of work than is justified until an actual real repository
/// needs it.
fn verify_empty_content_seed(conn: &Connection) -> Result<(), Error> {
    let found: Option<(i64, Vec<u8>)> = conn
        .query_row(
            "SELECT length, hash FROM contents WHERE id = ?1",
            [EMPTY_CONTENT_ID],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    match found {
        Some((0, hash)) if hash == EMPTY_CONTENT_HASH => Ok(()),
        _ => Err(Error::EmptyContentSeedMismatch),
    }
}

/// Creates the `meta`/`data` directory layout at `repo_root` and initializes
/// the metadata database within it: the schema (which seeds the root tree
/// entry) and the given `settings`. Shared by [`init_repository`] and
/// [`adopt_repository_in_place`], which differ only in what they require of
/// `repo_root` beforehand.
///
/// Builds the database under a sibling [`META_TMP_DIR`] staging directory
/// first, and only `fs::rename`s it to the real [`META_DIR`] - atomic on the
/// same volume, on both Windows and POSIX - once schema and settings are
/// fully committed. A process killed anywhere before that rename leaves at
/// most a `meta.tmp/` directory behind and no `meta/` at all, so a re-run's
/// `RepositoryAlreadyExists` check (which only ever looks at `meta/`, both
/// here and in [`init_repository`]/[`adopt_repository_in_place`]) still
/// correctly treats the repository as not yet created; any stale
/// `meta.tmp/` from that killed attempt is simply removed and rebuilt from
/// scratch here rather than ever being resumed in place. `data/` is still
/// created directly (no staging) - it was never part of the
/// "already exists" signal, and an empty or partially-adopted `data/` left
/// behind by a killed run is harmless clutter, not a correctness problem.
fn create_repository_files(repo_root: &Path, settings: &RepositorySettings) -> Result<(), Error> {
    fs::create_dir_all(repo_root.join(DATA_DIR))?;

    let staging_meta = repo_root.join(META_TMP_DIR);
    if staging_meta.exists() {
        fs::remove_dir_all(&staging_meta)?;
    }
    fs::create_dir_all(&staging_meta)?;

    let mut conn = open_connection(&staging_meta.join(META_DB_FILE))?;
    migrations::migrations().to_latest(&mut conn)?;

    conn.execute(
        "INSERT INTO repository_settings (id, cdc_target_size_bits, chunking) VALUES (1, ?1, ?2)",
        (
            settings.cdc_target_size_bits(),
            settings.chunking().as_str(),
        ),
    )?;
    drop(conn);

    fs::rename(&staging_meta, repo_root.join(META_DIR))?;

    Ok(())
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
    create_repository_files(repo_root, settings)
}

/// Initializes repository metadata at `repo_root`, like [`init_repository`],
/// but without requiring `repo_root` itself to not yet exist - only that
/// `repo_root/meta` doesn't. For adopting a directory that already has
/// *unrelated* content alongside where `meta/` will go: specifically,
/// `migrate_scala_repo`'s in-place adoption of an old Scala repository's
/// `data/` directory, reusing its byte store as-is (same on-disk layout,
/// see `docs/plans/implemented/scala-rust-store-migration.md`) rather than
/// copying it - `repo_root` there is the existing Scala repository root
/// (already containing `data/` and `fsdb/`), and this just adds `meta/`
/// alongside them.
///
/// # Errors
///
/// Returns [`Error::RepositoryAlreadyExists`] if `repo_root/meta` already exists.
pub fn adopt_repository_in_place(
    repo_root: &Path,
    settings: &RepositorySettings,
) -> Result<(), Error> {
    if repo_root.join(META_DIR).exists() {
        return Err(Error::RepositoryAlreadyExists(repo_root.to_path_buf()));
    }
    create_repository_files(repo_root, settings)
}

/// A handle to an existing repository, opened via [`open_repository`] or
/// [`open_repository_read_only`].
#[derive(Debug)]
pub struct Repository {
    repo_root: std::path::PathBuf,
    settings: RepositorySettings,
    /// Whether this was opened via [`open_repository_read_only`] rather
    /// than [`open_repository`] - determines which kind of connection
    /// [`Repository::open_read_connection`] opens (see its doc comment).
    /// Doesn't affect [`Repository::open_write_connection`]: nothing in
    /// this crate stops a caller from calling it on a `read_only`
    /// `Repository` (the type alone can't enforce that), but every
    /// `open_repository_read_only` caller in `cli` simply never does.
    read_only: bool,
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

    /// Opens a new, genuinely read-only connection to this repository's
    /// metadata database: any write attempt through it fails outright
    /// rather than merely being something callers are expected not to do.
    /// Open as many of these as needed; per the module-level doc comment,
    /// WAL only ever admits one writer transaction at a time, so treating
    /// every connection as a potential writer would only add lock
    /// contention without adding throughput - which a read-only connection
    /// can't do even by accident now.
    ///
    /// Uses `open_connection_immutable` for a `Repository` obtained via
    /// [`open_repository_read_only`], `open_connection_read_only` otherwise.
    /// See `open_connection_immutable`'s doc comment for why these aren't
    /// interchangeable: only the immutable path never needs to write
    /// `-wal`/`-shm`, which is the entire point of `open_repository_read_only`
    /// existing.
    pub fn open_read_connection(&self) -> Result<Connection, Error> {
        if self.read_only {
            open_connection_immutable(&self.meta_db_path())
        } else {
            open_connection_read_only(&self.meta_db_path())
        }
    }

    /// Opens a new read-write connection to this repository's metadata
    /// database. Use exactly one of these per repository at a time - see
    /// [`Repository::open_read_connection`] and the module-level doc
    /// comment.
    pub fn open_write_connection(&self) -> Result<Connection, Error> {
        open_connection(&self.meta_db_path())
    }
}

/// Opens an existing repository at `repo_root`, reading back its settings.
///
/// Also brings the metadata database up to the latest schema, applying any
/// migration added since this repository was created (e.g. a repository
/// from before `SCHEMA_V2` existed - `create_repository_files` only ever
/// applies the latest schema to a *brand new* database; an already-existing
/// one only ever gets upgraded by actually being opened, which is here, not
/// in `open_connection` - called once per command via this function, not
/// once per connection, which matters for `store`'s per-worker-thread read
/// connections: applying a migration is safe to repeat (a no-op once
/// already current), but there's no reason to even check on every one of
/// those when checking once up front already covers the whole run).
pub fn open_repository(repo_root: &Path) -> Result<Repository, Error> {
    let mut conn = open_connection(&repo_root.join(META_DIR).join(META_DB_FILE))?;
    reject_if_schema_too_new(&conn)?;
    migrations::migrations().to_latest(&mut conn)?;
    verify_empty_content_seed(&conn)?;

    let (cdc_target_size_bits, chunking): (u32, String) = conn.query_row(
        "SELECT cdc_target_size_bits, chunking FROM repository_settings WHERE id = 1",
        (),
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let settings = RepositorySettings::new(cdc_target_size_bits, Chunking::from_db_str(&chunking))?;

    Ok(Repository {
        repo_root: repo_root.to_path_buf(),
        settings,
        read_only: false,
    })
}

/// Opens an existing repository at `repo_root` like [`open_repository`], but
/// without ever opening a read-write connection - suitable for a repository
/// directory that is genuinely read-only on disk (e.g. bind-mounted `:ro`),
/// which [`open_repository`] can't handle: it always opens a read-write
/// connection first, to check for and apply pending migrations. The
/// [`Repository`] this returns opens every later
/// [`Repository::open_read_connection`] via `open_connection_immutable`,
/// which - unlike a plain `SQLITE_OPEN_READ_ONLY` connection - never
/// touches `-wal`/`-shm` at all (see that function's doc comment for why
/// the distinction matters).
///
/// Fails, rather than silently degrading, in the three ways a read-only
/// command could otherwise misbehave against such a repository:
/// - [`Error::UncheckpointedWal`] if a non-empty `-wal` sidecar is present
///   next to the database file - i.e. there are writes not yet folded into
///   the main database file. An *empty* `-wal` (and any `-shm`, which never
///   by itself indicates pending writes - see `open_connection_immutable`)
///   is harmless and ignored: merely opening an ordinary read connection
///   leaves exactly that behind (readers need `-shm` for their "read mark"
///   slot, but never write actual data to `-wal`), so treating any
///   *presence* as disqualifying would make every read-only command fail
///   after the very first one ever run against a repository - refusing
///   would defeat the purpose of this function rather than serve it. Point
///   the user at `db compact`, which checkpoints (folding pending writes
///   into the main file) and removes both sidecars entirely.
/// - [`Error::SchemaTooNew`] if the schema is newer than this build knows
///   about (see `reject_if_schema_too_new`).
/// - [`Error::MigrationsPending`] if migrations are pending - unlike
///   [`open_repository`], this function never applies them itself.
pub fn open_repository_read_only(repo_root: &Path) -> Result<Repository, Error> {
    let db_path = repo_root.join(META_DIR).join(META_DB_FILE);
    let wal_path = Path::new(&format!("{}-wal", db_path.display())).to_path_buf();
    if wal_path.metadata().is_ok_and(|metadata| metadata.len() > 0) {
        return Err(Error::UncheckpointedWal);
    }

    let conn = open_connection_immutable(&db_path)?;
    reject_if_schema_too_new(&conn)?;
    if migrations::migrations().pending_migrations(&conn)? > 0 {
        return Err(Error::MigrationsPending);
    }
    verify_empty_content_seed(&conn)?;

    let (cdc_target_size_bits, chunking): (u32, String) = conn.query_row(
        "SELECT cdc_target_size_bits, chunking FROM repository_settings WHERE id = 1",
        (),
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let settings = RepositorySettings::new(cdc_target_size_bits, Chunking::from_db_str(&chunking))?;

    Ok(Repository {
        repo_root: repo_root.to_path_buf(),
        settings,
        read_only: true,
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
            repo_root.join("meta").join("repository.sqlite3")
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
    fn adopt_repository_in_place_works_alongside_preexisting_unrelated_content() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        std::fs::create_dir_all(repo_root.join(DATA_DIR)).unwrap();
        std::fs::write(repo_root.join(DATA_DIR).join("00000000000"), b"x").unwrap();
        std::fs::create_dir_all(repo_root.join("fsdb")).unwrap();

        adopt_repository_in_place(&repo_root, &test_settings()).unwrap();

        assert!(repo_root.join(META_DIR).join(META_DB_FILE).is_file());
        // Pre-existing, unrelated content must survive untouched.
        assert!(repo_root.join(DATA_DIR).join("00000000000").is_file());
        assert!(repo_root.join("fsdb").is_dir());
    }

    #[test]
    fn adopt_repository_in_place_refuses_if_meta_already_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        adopt_repository_in_place(&repo_root, &test_settings()).unwrap();

        let result = adopt_repository_in_place(&repo_root, &test_settings());

        assert!(matches!(result, Err(Error::RepositoryAlreadyExists(path)) if path == repo_root));
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

    /// A repository `init`ed by current code already has `EMPTY_CONTENT_ID`
    /// correctly seeded - both open paths must accept it without complaint,
    /// the common case `verify_empty_content_seed` must stay a no-op for.
    #[test]
    fn open_repository_accepts_a_correctly_seeded_empty_content_row() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();

        assert!(open_repository(&repo_root).is_ok());
        assert!(open_repository_read_only(&repo_root).is_ok());
    }

    /// Regression test for `docs/plans/implemented/
    /// verify-empty-content-seed-on-open.md`: a repository that predates
    /// `EMPTY_CONTENT_ID`'s seed row (simulated here by deleting it after
    /// the fact - `SCHEMA_V1`'s squashed-migration seed can't otherwise be
    /// un-run to reproduce the real scenario) must be refused, not silently
    /// accepted.
    #[test]
    fn open_repository_refuses_a_missing_empty_content_row() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();
        let conn = open_connection(&repo_root.join(META_DIR).join(META_DB_FILE)).unwrap();
        conn.execute("DELETE FROM contents WHERE id = 1", ())
            .unwrap();
        drop(conn);

        assert!(matches!(
            open_repository(&repo_root),
            Err(Error::EmptyContentSeedMismatch)
        ));
        assert!(matches!(
            open_repository_read_only(&repo_root),
            Err(Error::EmptyContentSeedMismatch)
        ));
    }

    /// The actual collision case found for real in `backup-repository/`:
    /// `contents.id = 1` already taken by unrelated content (different
    /// `length`/`hash`) rather than being absent - must be refused just as
    /// loudly as the missing-row case above, not treated as "close enough".
    #[test]
    fn open_repository_refuses_a_mismatched_empty_content_row() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();
        let conn = open_connection(&repo_root.join(META_DIR).join(META_DB_FILE)).unwrap();
        conn.execute(
            "UPDATE contents SET length = 101, hash = x'AABBCC' WHERE id = 1",
            (),
        )
        .unwrap();
        drop(conn);

        assert!(matches!(
            open_repository(&repo_root),
            Err(Error::EmptyContentSeedMismatch)
        ));
        assert!(matches!(
            open_repository_read_only(&repo_root),
            Err(Error::EmptyContentSeedMismatch)
        ));
    }

    #[test]
    fn open_repository_read_only_reads_back_the_settings_of_a_freshly_initialized_repository() {
        // A freshly initialized repository has no WAL sidecars left behind
        // (create_repository_files drops its sole connection before
        // returning, which auto-checkpoints them away) - so this must
        // succeed without needing a `db compact` in between.
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(
            &repo_root,
            &RepositorySettings::new(18, Chunking::None).unwrap(),
        )
        .unwrap();

        let repo = open_repository_read_only(&repo_root).unwrap();

        assert_eq!(repo.settings().cdc_target_size_bits(), 18);
        assert_eq!(repo.settings().chunking(), Chunking::None);
    }

    #[cfg(unix)]
    #[test]
    fn open_repository_read_only_works_even_when_the_meta_directory_is_not_writable() {
        // Chmodding just the database *file* wouldn't actually exercise the
        // scenario this whole function exists for (a `:ro`-bind-mounted
        // repository directory, see docs/plans/read-only-repository-access.md):
        // creating a *new* file (-wal/-shm) needs write permission on the
        // containing *directory*, not the database file itself - a plain
        // SQLITE_OPEN_READ_ONLY connection would have happily opened a
        // read-only-permissioned file in an otherwise-writable directory
        // and then failed trying to create -shm there, so this must chmod
        // meta/ itself to actually prove the fix.
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();
        let meta_dir = meta_dir(&repo_root);
        let mut perms = fs::metadata(&meta_dir).unwrap().permissions();
        perms.set_mode(0o555);
        fs::set_permissions(&meta_dir, perms).unwrap();

        let repo = open_repository_read_only(&repo_root).unwrap();
        let conn = repo.open_read_connection().unwrap();
        let result = resolve_path(&conn, "");

        // Restore write permission before the tempdir's own Drop tries to
        // remove it - not needed for the assertion itself.
        let mut perms = fs::metadata(&meta_dir).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&meta_dir, perms).unwrap();

        assert!(result.is_ok(), "{result:?}");
        let wal_path = meta_dir.join(format!("{META_DB_FILE}-wal"));
        let shm_path = meta_dir.join(format!("{META_DB_FILE}-shm"));
        assert!(!wal_path.exists(), "immutable mode must never create -wal");
        assert!(!shm_path.exists(), "immutable mode must never create -shm");
    }

    #[test]
    fn open_repository_read_only_fails_when_the_wal_has_unwritten_data() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();
        let repo = open_repository(&repo_root).unwrap();
        // Kept open so the write below doesn't auto-checkpoint away its own
        // WAL sidecars when it closes - see the equivalent trick in
        // cli::db_maintenance's WAL-checkpoint test. Without this, `-wal`
        // would end up empty (0 bytes) rather than actually holding this
        // write's frames, which is exactly the harmless case the next test
        // (`..._survives_a_prior_read_only_open`) exists to distinguish
        // from this one.
        let outlasting_conn = repo.open_write_connection().unwrap();
        let write_conn = repo.open_write_connection().unwrap();
        insert_directory(&write_conn, 0, "sub", 0).unwrap();
        drop(write_conn);

        let result = open_repository_read_only(&repo_root);

        assert!(
            matches!(result, Err(Error::UncheckpointedWal)),
            "{result:?}"
        );
        drop(outlasting_conn);
    }

    #[test]
    fn open_repository_read_only_survives_a_prior_plain_read_only_connection() {
        // A regression test for the bug this whole design revolves around:
        // merely *opening* a plain SQLITE_OPEN_READ_ONLY connection to a
        // WAL-mode database (what `open_read_connection` used to do
        // unconditionally, before it started branching on `read_only` -
        // see `open_connection_read_only`'s own doc comment) creates an
        // (empty) `-wal` and a (fixed-size, content-free) `-shm` sidecar of
        // its own - readers need `-shm` for their "read mark" slot - and,
        // unlike a writer, can never remove them again on close (only a
        // connection able to write the *main* file can ever checkpoint).
        // Simulates that history directly (rather than relying on
        // `open_repository_read_only`'s own internals, which now avoid this
        // entirely via `open_connection_immutable`) so this test still
        // means something once that implementation detail changes: if
        // `open_repository_read_only` ever went back to treating mere
        // sidecar *presence* as disqualifying (rather than checking
        // `-wal`'s actual size), every read-only command would fail after
        // the very first plain read-only connection anyone ever opened
        // against a repository.
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();
        drop(open_connection_read_only(&db_file_path(&repo_root)).unwrap());

        let result = open_repository_read_only(&repo_root);

        assert!(result.is_ok(), "{result:?}");
    }

    #[test]
    fn open_repository_read_only_fails_when_migrations_are_pending() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();
        let conn = open_connection(&repo_root.join(META_DIR).join(META_DB_FILE)).unwrap();
        conn.pragma_update(None, "user_version", 0).unwrap();
        drop(conn);

        let result = open_repository_read_only(&repo_root);

        assert!(
            matches!(result, Err(Error::MigrationsPending)),
            "{result:?}"
        );
    }

    #[test]
    fn open_repository_read_only_fails_with_the_actual_version_when_the_schema_is_too_new() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();
        let conn = open_connection(&repo_root.join(META_DIR).join(META_DB_FILE)).unwrap();
        conn.pragma_update(None, "user_version", 99).unwrap();
        drop(conn);

        let result = open_repository_read_only(&repo_root);

        assert!(
            matches!(result, Err(Error::SchemaTooNew { db_version: 99 })),
            "{result:?}"
        );
    }

    #[test]
    fn open_repository_also_fails_with_the_actual_version_when_the_schema_is_too_new() {
        // The write path (open_repository) must give the same actionable
        // error, not just open_repository_read_only - it hit the same
        // confusing rusqlite_migration error before reject_if_schema_too_new
        // existed.
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();
        let conn = open_connection(&repo_root.join(META_DIR).join(META_DB_FILE)).unwrap();
        conn.pragma_update(None, "user_version", 99).unwrap();
        drop(conn);

        let result = open_repository(&repo_root);

        assert!(
            matches!(result, Err(Error::SchemaTooNew { db_version: 99 })),
            "{result:?}"
        );
    }

    #[test]
    fn classify_open_error_rewrites_only_cantopen_not_other_sqlite_errors() {
        let path = Path::new("/some/repo/meta/repository.sqlite3");

        let cantopen = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
            Some("unable to open database file".to_string()),
        );
        let result = classify_open_error(cantopen, path);
        assert!(
            matches!(&result, Error::CannotOpenForWriting(p) if p == path),
            "{result:?}"
        );

        // Some other SQLite failure (e.g. a locked database) must pass
        // through unchanged, not get relabeled as a possible read-only
        // filesystem too.
        let busy = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            Some("database is locked".to_string()),
        );
        let result = classify_open_error(busy, path);
        assert!(matches!(result, Error::Sqlite(_)), "{result:?}");
    }

    #[test]
    fn open_repository_gives_the_cannot_open_for_writing_error_for_a_missing_parent_directory() {
        // Portable (works the same on every platform, no chmod/mount
        // tricks needed) way to reproduce a genuine SQLITE_CANTOPEN: the
        // containing directory for the database file doesn't exist at all.
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("does-not-exist").join("repo");

        let result = open_repository(&repo_root);

        assert!(
            matches!(result, Err(Error::CannotOpenForWriting(_))),
            "{result:?}"
        );
    }

    #[test]
    fn a_read_connection_is_genuinely_read_only_and_can_still_read_after_a_write() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();
        let repo = open_repository(&repo_root).unwrap();

        let write_conn = repo.open_write_connection().unwrap();
        crate::insert_directory(&write_conn, 0, "sub", 0).unwrap();
        drop(write_conn);

        let read_conn = repo.open_read_connection().unwrap();
        // Sees the committed write above - not a stale snapshot from before it.
        assert!(
            crate::find_tree_entry(&read_conn, 0, "sub")
                .unwrap()
                .is_some()
        );
        // A write attempt through it fails outright rather than merely being
        // discouraged by convention.
        let err = read_conn
            .execute(
                "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (0, 'x', 0, 'dir')",
                (),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            rusqlite::Error::SqliteFailure(e, _) if e.code == rusqlite::ErrorCode::ReadOnly
        ));
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

        // id=1 is taken by the seeded EMPTY_CONTENT_ID row (see
        // migrations.rs), so this test's own content uses id=2 throughout.
        conn.execute(
            "INSERT INTO contents (id, length, hash) VALUES (2, 0, x'AA')",
            (),
        )
        .unwrap();
        assert_eq!(content_ref_count(&conn, 2), 0);

        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, content_id, kind) VALUES (1, 0, 'a', 0, 2, 'file')",
            (),
        )
        .unwrap();
        assert_eq!(content_ref_count(&conn, 2), 1);

        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, content_id, kind) VALUES (2, 0, 'b', 0, 2, 'file')",
            (),
        )
        .unwrap();
        assert_eq!(content_ref_count(&conn, 2), 2);

        // Soft-deleting an entry must not release its content: it's still needed
        // to keep the entry recoverable.
        conn.execute("UPDATE tree_entries SET deleted_at = 1 WHERE id = 1", ())
            .unwrap();
        assert_eq!(content_ref_count(&conn, 2), 2);

        conn.execute("DELETE FROM tree_entries WHERE id = 1", ())
            .unwrap();
        assert_eq!(content_ref_count(&conn, 2), 1);

        conn.execute("DELETE FROM tree_entries WHERE id = 2", ())
            .unwrap();
        assert_eq!(content_ref_count(&conn, 2), 0);
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

        // Content id=2, not 1 - id=1 is taken by the seeded EMPTY_CONTENT_ID row.
        conn.execute(
            "INSERT INTO chunks (id, length, hash) VALUES (1, 3, x'AA')",
            (),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO contents (id, length, hash) VALUES (2, 3, x'BB')",
            (),
        )
        .unwrap();
        assert_eq!(chunk_ref_count(&conn, 1), 0);

        conn.execute(
            "INSERT INTO content_chunks (content_id, seq, chunk_id) VALUES (2, 0, 1)",
            (),
        )
        .unwrap();
        assert_eq!(chunk_ref_count(&conn, 1), 1);

        // ref_count = 0, so this content is eligible for purging.
        assert_eq!(content_ref_count(&conn, 2), 0);
        conn.execute("DELETE FROM contents WHERE id = 2 AND ref_count = 0", ())
            .unwrap();

        let remaining_content_chunks: i64 = conn
            .query_row("SELECT COUNT(*) FROM content_chunks", (), |row| row.get(0))
            .unwrap();
        assert_eq!(remaining_content_chunks, 0);
        assert_eq!(chunk_ref_count(&conn, 1), 0);
    }
}
