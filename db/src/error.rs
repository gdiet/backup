use std::fmt;
use std::path::PathBuf;

use crate::SettingsError;

/// Error returned by this crate's repository operations.
#[derive(Debug)]
pub enum Error {
    /// The repository directory passed to [`crate::init_repository`] already
    /// exists, or the `meta` subdirectory passed to
    /// [`crate::adopt_repository_in_place`] already exists.
    RepositoryAlreadyExists(PathBuf),
    /// The provided [`crate::RepositorySettings`] failed validation.
    InvalidSettings(SettingsError),
    /// [`crate::insert_directory`] was called for a name that already exists as a
    /// file (not a directory) under `parent_id`.
    NotADirectory { parent_id: i64, name: String },
    /// [`crate::apply_backup_batch`] was asked to record a file at a name that
    /// already exists as a directory (not a file) under `parent_id`.
    NotAFile { parent_id: i64, name: String },
    /// [`crate::rename_entry`]/[`crate::undelete`] was asked to move/recover
    /// an entry to a `(parent_id, name)` that already has an active entry,
    /// and either `no_replace` was set or the entry being moved doesn't
    /// resolve to that same active entry - the "would otherwise replace it"
    /// cases below don't apply, so nothing happened.
    AlreadyExists { parent_id: i64, name: String },
    /// [`crate::rename_entry`]/[`crate::undelete`]'s destination is
    /// occupied by an active directory, but the entry being moved/recovered
    /// there is a file - real `rename(2)` semantics reject replacing a
    /// directory with a file (`EISDIR` at the FUSE/WinFSP layer), rather
    /// than silently coercing either side.
    TargetIsADirectory { parent_id: i64, name: String },
    /// [`crate::rename_entry`]/[`crate::undelete`]'s destination is
    /// occupied by an active file, but the entry being moved/recovered
    /// there is a directory - real `rename(2)` semantics reject replacing a
    /// file with a directory (`ENOTDIR` at the FUSE/WinFSP layer).
    TargetIsAFile { parent_id: i64, name: String },
    /// [`crate::rename_entry`]/[`crate::undelete`]'s destination is
    /// occupied by a non-empty active directory - real `rename(2)`
    /// semantics reject this (`ENOTEMPTY` at the FUSE/WinFSP layer),
    /// mirroring the mount's own `rmdir` non-empty check.
    TargetNotEmpty { parent_id: i64, name: String },
    /// Creating the repository directory layout failed.
    Io(std::io::Error),
    /// A SQLite operation failed.
    Sqlite(rusqlite::Error),
    /// Applying database migrations failed.
    Migration(rusqlite_migration::Error),
    /// The database's schema version is newer than any migration this build
    /// of `backup` knows about - it was created or last opened by a newer
    /// version of the program. Detected up front (via
    /// [`rusqlite_migration::Migrations::current_version`]) rather than left
    /// to surface as the much less actionable
    /// [`rusqlite_migration::MigrationDefinitionError::DatabaseTooFarAhead`]
    /// error `to_latest` would otherwise fail with.
    SchemaTooNew { db_version: usize },
    /// [`crate::open_repository_read_only`] found migrations that haven't
    /// been applied yet - a read-only connection can't apply them itself,
    /// unlike [`crate::open_repository`].
    MigrationsPending,
    /// [`crate::open_repository_read_only`] found a non-empty `-wal`
    /// sidecar next to the metadata database file - writes not yet folded
    /// into the main database file, which a read-only connection could
    /// only ever see by ignoring (this function relies on `immutable=1`
    /// SQLite connections internally, which is only correct once this case
    /// is ruled out first). An empty `-wal`/any `-shm` don't trigger this -
    /// see [`crate::open_repository_read_only`]'s own doc comment for why.
    UncheckpointedWal,
    /// [`crate::open_repository`]/[`crate::open_repository_read_only`] found
    /// `contents.id = EMPTY_CONTENT_ID` missing, or present with a different
    /// `length`/`hash` than expected - this repository predates
    /// `EMPTY_CONTENT_ID`'s seed row (a single-migration schema never
    /// re-runs its seed data for an already-`init`ed database) or that id
    /// was already taken by unrelated content before the seed could claim
    /// it. Using this repository further risks silently aliasing a future
    /// empty file's content onto whatever already occupies that id.
    EmptyContentSeedMismatch,
    /// A write connection failed with SQLite's generic `SQLITE_CANTOPEN`
    /// ("unable to open database file") - see `classify_open_error` in
    /// `lib.rs` for the full reasoning. Deliberately doesn't claim a
    /// specific cause (e.g. "read-only filesystem") the way the other
    /// variants here do: `SQLITE_CANTOPEN` alone can't distinguish that
    /// from several other unrelated causes (a missing parent directory, an
    /// unrelated permission problem, too many open files, ...), so the
    /// message only asks the question rather than asserting an answer it
    /// doesn't actually have.
    CannotOpenForWriting(PathBuf),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RepositoryAlreadyExists(path) => {
                write!(f, "repository already exists: {}", path.display())
            }
            Self::InvalidSettings(err) => write!(f, "invalid repository settings: {err}"),
            Self::NotADirectory { parent_id, name } => {
                write!(
                    f,
                    "'{name}' under tree entry {parent_id} is a file, not a directory"
                )
            }
            Self::NotAFile { parent_id, name } => {
                write!(
                    f,
                    "'{name}' under tree entry {parent_id} is a directory, not a file"
                )
            }
            Self::AlreadyExists { parent_id, name } => {
                write!(f, "'{name}' already exists under tree entry {parent_id}")
            }
            Self::TargetIsADirectory { parent_id, name } => write!(
                f,
                "'{name}' under tree entry {parent_id} is a directory, can't replace it with a \
                 file"
            ),
            Self::TargetIsAFile { parent_id, name } => write!(
                f,
                "'{name}' under tree entry {parent_id} is a file, can't replace it with a \
                 directory"
            ),
            Self::TargetNotEmpty { parent_id, name } => write!(
                f,
                "'{name}' under tree entry {parent_id} is a non-empty directory, can't replace it"
            ),
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Sqlite(err) => write!(f, "SQLite error: {err}"),
            Self::Migration(err) => write!(f, "database migration error: {err}"),
            Self::SchemaTooNew { db_version } => write!(
                f,
                "this repository's database schema (version {db_version}) is newer than this \
                 version of `backup` understands - please update `backup`"
            ),
            Self::MigrationsPending => write!(
                f,
                "this repository has pending database migrations that a read-only command can't \
                 apply - run any write command (e.g. `db compact`) once to bring it up to date"
            ),
            Self::UncheckpointedWal => write!(
                f,
                "found a pending write-ahead-log file (-wal) next to the metadata database, not \
                 yet folded into it - run `db compact` once to clean this up before using a \
                 read-only command"
            ),
            Self::CannotOpenForWriting(path) => write!(
                f,
                "the metadata database at {} could not be opened for writing - is it on a \
                 read-only filesystem? If you only need to read the repository, a read-only \
                 command (restore/stats/list/find/check/problems/deleted/db backup/mount \
                 without --read-write) doesn't need write access at all",
                path.display()
            ),
            Self::EmptyContentSeedMismatch => write!(
                f,
                "this repository predates the shared empty-content row every empty file is \
                 supposed to share, or that row has been overwritten by something else - it \
                 isn't safe to use further as-is (a future empty file could silently alias onto \
                 unrelated content). If this is disposable test data, delete the repository and \
                 run `init` again; there is no supported in-place fix for this"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RepositoryAlreadyExists(_) => None,
            Self::InvalidSettings(err) => Some(err),
            Self::NotADirectory { .. } => None,
            Self::NotAFile { .. } => None,
            Self::AlreadyExists { .. } => None,
            Self::TargetIsADirectory { .. } => None,
            Self::TargetIsAFile { .. } => None,
            Self::TargetNotEmpty { .. } => None,
            Self::Io(err) => Some(err),
            Self::Sqlite(err) => Some(err),
            Self::Migration(err) => Some(err),
            Self::SchemaTooNew { .. } => None,
            Self::MigrationsPending => None,
            Self::UncheckpointedWal => None,
            Self::CannotOpenForWriting(_) => None,
            Self::EmptyContentSeedMismatch => None,
        }
    }
}

impl From<SettingsError> for Error {
    fn from(err: SettingsError) -> Self {
        Self::InvalidSettings(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Self::Sqlite(err)
    }
}

impl From<rusqlite_migration::Error> for Error {
    fn from(err: rusqlite_migration::Error) -> Self {
        Self::Migration(err)
    }
}
