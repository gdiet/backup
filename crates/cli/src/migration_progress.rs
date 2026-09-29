//! Phase 2's durable progress record - DESIGN-MIGRATION-005 in
//! `docs/design/scala-migration-tool.md`: one small, separate SQLite file per destination metadata
//! database, tracking which old content has already been re-chunked (`content_cache`) and which
//! old tree entries have already been recreated (`migrated`), so a resumed migration only ever
//! redoes whatever an interruption actually left unfinished.
//!
//! Temporary, the same as every other Scala-repository-migration-only addition in this project -
//! nothing here is meant to outlive that tool.

use std::fmt;
use std::path::Path;

use rusqlite::{Connection, OptionalExtension, params};

#[derive(Debug)]
pub enum ProgressError {
    Io(std::io::Error),
    Sqlite(rusqlite::Error),
}

impl fmt::Display for ProgressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProgressError::Io(err) => write!(f, "{err}"),
            ProgressError::Sqlite(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for ProgressError {}

impl From<std::io::Error> for ProgressError {
    fn from(err: std::io::Error) -> Self {
        ProgressError::Io(err)
    }
}

impl From<rusqlite::Error> for ProgressError {
    fn from(err: rusqlite::Error) -> Self {
        ProgressError::Sqlite(err)
    }
}

/// Opens `path`'s progress record, creating its schema if `path` does not exist yet - safe to call
/// on every run, resumed or not, since a fresh file and an already-populated one from an
/// interrupted attempt are opened identically.
pub fn open_or_create(path: &Path) -> Result<Connection, ProgressError> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS content_cache (
             old_data_id INTEGER PRIMARY KEY,
             content_id  INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS migrated (
             old_tree_id INTEGER PRIMARY KEY,
             new_id      INTEGER NOT NULL
         );",
    )?;
    Ok(conn)
}

/// The destination `content_id` already recorded for `old_data_id`, if this or an earlier
/// (possibly interrupted) run already migrated it - `None` means it still needs to be read,
/// re-chunked, and re-hashed.
pub fn cached_content(conn: &Connection, old_data_id: i64) -> Result<Option<i64>, ProgressError> {
    conn.query_row(
        "SELECT content_id FROM content_cache WHERE old_data_id = ?1",
        params![old_data_id],
        |row| row.get(0),
    )
    .optional()
    .map_err(ProgressError::from)
}

/// Records `old_data_id`'s resolved `content_id`, so a later run's [`cached_content`] can skip
/// re-reading and re-chunking it.
pub fn record_content(
    conn: &Connection,
    old_data_id: i64,
    content_id: i64,
) -> Result<(), ProgressError> {
    conn.execute(
        "INSERT INTO content_cache (old_data_id, content_id) VALUES (?1, ?2)",
        params![old_data_id, content_id],
    )?;
    Ok(())
}

/// The destination `new_id` already recorded for `old_tree_id`, if this or an earlier (possibly
/// interrupted) run already recreated it - `None` means it still needs to be inserted.
pub fn migrated_id(conn: &Connection, old_tree_id: i64) -> Result<Option<i64>, ProgressError> {
    conn.query_row(
        "SELECT new_id FROM migrated WHERE old_tree_id = ?1",
        params![old_tree_id],
        |row| row.get(0),
    )
    .optional()
    .map_err(ProgressError::from)
}

/// Records `old_tree_id`'s recreated `new_id`, so a later run's [`migrated_id`] can skip
/// recreating it - and, for a directory, still knows which new id to recurse into for its
/// not-yet-migrated children.
pub fn record_migrated(
    conn: &Connection,
    old_tree_id: i64,
    new_id: i64,
) -> Result<(), ProgressError> {
    conn.execute(
        "INSERT INTO migrated (old_tree_id, new_id) VALUES (?1, ?2)",
        params![old_tree_id, new_id],
    )?;
    Ok(())
}

/// Removes the progress record file entirely - called once a target size's migration has
/// completed successfully (DESIGN-MIGRATION-001), so a resume after that point has nothing left to
/// consult. Tolerates the file already being gone.
pub fn remove(path: &Path) -> Result<(), ProgressError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_content_is_none_before_anything_is_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_or_create(&dir.path().join("progress.db")).unwrap();
        assert_eq!(cached_content(&conn, 5).unwrap(), None);
    }

    #[test]
    fn record_content_makes_it_findable_by_the_same_old_data_id() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_or_create(&dir.path().join("progress.db")).unwrap();
        record_content(&conn, 5, 42).unwrap();
        assert_eq!(cached_content(&conn, 5).unwrap(), Some(42));
        assert_eq!(cached_content(&conn, 6).unwrap(), None);
    }

    #[test]
    fn migrated_id_is_none_before_anything_is_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_or_create(&dir.path().join("progress.db")).unwrap();
        assert_eq!(migrated_id(&conn, 7).unwrap(), None);
    }

    #[test]
    fn record_migrated_makes_it_findable_by_the_same_old_tree_id() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_or_create(&dir.path().join("progress.db")).unwrap();
        record_migrated(&conn, 7, 99).unwrap();
        assert_eq!(migrated_id(&conn, 7).unwrap(), Some(99));
    }

    #[test]
    fn open_or_create_reopens_an_already_populated_file_without_losing_its_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("progress.db");
        let conn = open_or_create(&path).unwrap();
        record_content(&conn, 1, 2).unwrap();
        drop(conn);

        let reopened = open_or_create(&path).unwrap();
        assert_eq!(cached_content(&reopened, 1).unwrap(), Some(2));
    }

    #[test]
    fn remove_deletes_the_file_and_tolerates_it_already_being_gone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("progress.db");
        open_or_create(&path).unwrap();
        assert!(path.exists());

        remove(&path).unwrap();
        assert!(!path.exists());
        remove(&path).unwrap();
    }
}
