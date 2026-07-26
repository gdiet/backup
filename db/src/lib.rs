//! SQLite-backed metadata storage for a deduplicating backup repository.
//!
//! This crate currently only implements repository initialization
//! ([`init_repository`]): creating the on-disk directory layout and the metadata
//! database with its schema and initial rows.

mod error;
mod migrations;
mod settings;

pub use error::Error;
pub use settings::{
    CDC_TARGET_SIZE_BITS_RANGE, Chunking, HashAlgorithm, RepositorySettings, SettingsError,
};

use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection;

/// Directory (relative to the repository root) holding the metadata database.
const META_DIR: &str = "meta";
/// File name of the metadata database within [`META_DIR`].
const META_DB_FILE: &str = "repository.db";
/// Directory (relative to the repository root) holding the chunk data store.
const DATA_DIR: &str = "data";

/// Opens (creating if missing) the SQLite database at `path` with the pragmas
/// required for correct and durable operation.
fn open_connection(path: &Path) -> Result<Connection, Error> {
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    // journal_mode returns the resulting mode as a row, so pragma_update_and_check
    // (rather than pragma_update) is required here.
    conn.pragma_update_and_check(None, "journal_mode", "WAL", |_row| Ok(()))?;
    Ok(conn)
}

/// Creates a new repository at `repo_root`.
///
/// This creates the directory layout (a `meta/` subdirectory for the metadata
/// database and a `data/` subdirectory for the future chunk data store) and
/// initializes the metadata database: the schema, the given `settings`, and an
/// empty file tree (just the root entry).
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

    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after the Unix epoch")
        .as_millis() as i64;

    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO repository_settings (id, cdc_target_size_bits, chunking, hash_algorithm) \
         VALUES (1, ?1, ?2, ?3)",
        (
            settings.cdc_target_size_bits(),
            settings.chunking().as_str(),
            settings.hash_algorithm().as_str(),
        ),
    )?;
    tx.execute(
        "INSERT INTO tree_entries (id, parent_id, name, time) VALUES (0, 0, '', ?1)",
        (now_millis,),
    )?;
    tx.commit()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_settings() -> RepositorySettings {
        RepositorySettings::new(20, Chunking::Cdc).unwrap()
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
    fn init_repository_writes_settings_and_root_entry() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        init_repository(&repo_root, &test_settings()).unwrap();

        let conn = Connection::open(repo_root.join(META_DIR).join(META_DB_FILE)).unwrap();

        let (cdc_target_size_bits, chunking, hash_algorithm): (u32, String, String) = conn
            .query_row(
                "SELECT cdc_target_size_bits, chunking, hash_algorithm FROM repository_settings WHERE id = 1",
                (),
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(cdc_target_size_bits, 20);
        assert_eq!(chunking, "cdc");
        assert_eq!(hash_algorithm, "blake3");

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
}
