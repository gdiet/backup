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

mod migrations;
mod settings;

use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::Connection;

pub use settings::RepositorySettings;

// Repository on-disk layout - DESIGN-REPOSITORY-001 in
// docs/design/repository-layout.md.
const META_DIR: &str = "meta";
const META_TMP_DIR: &str = "meta.tmp";
const META_DB_FILE: &str = "repository.sqlite3";
const DATA_DIR: &str = "data";

#[derive(Debug)]
pub enum Error {
    /// `repo_root` (or, for [`open_repository`], its `meta/` subdirectory)
    /// already holds a repository.
    RepositoryAlreadyExists(PathBuf),
    /// `repo_root` already exists and is not empty.
    TargetNotEmpty(PathBuf),
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
#[derive(Debug)]
pub struct Repository {
    repo_root: PathBuf,
    settings: RepositorySettings,
}

impl Repository {
    pub fn settings(&self) -> RepositorySettings {
        self.settings
    }

    /// The directory holding the byte store's data files (REQ-STORAGE-007).
    pub fn data_dir(&self) -> PathBuf {
        self.repo_root.join(DATA_DIR)
    }

    fn meta_db_path(&self) -> PathBuf {
        self.repo_root.join(META_DIR).join(META_DB_FILE)
    }

    /// Opens a new connection to this repository's metadata database.
    pub fn open_connection(&self) -> Result<Connection, Error> {
        Ok(Connection::open(self.meta_db_path())?)
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
    // open (or re-run of this function) could mistake for a real repository.
    let staging_meta = repo_root.join(META_TMP_DIR);
    if staging_meta.exists() {
        fs::remove_dir_all(&staging_meta)?;
    }
    fs::create_dir_all(&staging_meta)?;

    let mut conn = Connection::open(staging_meta.join(META_DB_FILE))?;
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
    let db_path = repo_root.join(META_DIR).join(META_DB_FILE);
    let mut conn = Connection::open(&db_path)?;
    migrations::migrations().to_latest(&mut conn)?;

    let (cdc_target_size_bits, creation_time_millis): (Option<u32>, i64) = conn.query_row(
        "SELECT cdc_target_size_bits, creation_time FROM repository_settings WHERE id = 1",
        (),
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    Ok(Repository {
        repo_root: repo_root.to_path_buf(),
        settings: RepositorySettings::new(cdc_target_size_bits, creation_time_millis),
    })
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
    fn open_repository_fails_on_a_directory_that_was_never_created_as_one() {
        let dir = tempfile::tempdir().unwrap();
        let err = open_repository(dir.path()).unwrap_err();
        assert!(matches!(err, Error::Sqlite(_)));
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
    fn open_repository_seeds_the_root_tree_entry() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, settings()).expect("init must succeed");

        let repo = open_repository(&repo_root).expect("open must succeed");
        let conn = repo.open_connection().expect("connection must open");
        let (parent_id, kind): (i64, i64) = conn
            .query_row(
                "SELECT parent_id, kind FROM tree_entries WHERE id = 0",
                (),
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("root row must exist");
        assert_eq!(parent_id, 0);
        assert_eq!(kind, 0);
    }

    #[test]
    fn dedup_ref_counts_follow_tree_entry_insert_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, settings()).expect("init must succeed");
        let repo = open_repository(&repo_root).expect("open must succeed");
        let conn = repo.open_connection().expect("connection must open");

        conn.execute(
            "INSERT INTO contents (id, length, hash) \
             VALUES (2, 3, X'0102030405060708090A0B0C0D0E0F1011121314')",
            (),
        )
        .expect("insert must succeed");
        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, content_id, kind) \
             VALUES (1, 0, 'a.txt', 0, 2, 1)",
            (),
        )
        .expect("insert must succeed");

        let ref_count: i64 = conn
            .query_row("SELECT ref_count FROM contents WHERE id = 2", (), |row| {
                row.get(0)
            })
            .expect("row must exist");
        assert_eq!(ref_count, 1);

        conn.execute("DELETE FROM tree_entries WHERE id = 1", ())
            .expect("delete must succeed");
        let ref_count: i64 = conn
            .query_row("SELECT ref_count FROM contents WHERE id = 2", (), |row| {
                row.get(0)
            })
            .expect("row must exist");
        assert_eq!(ref_count, 0);
    }
}
