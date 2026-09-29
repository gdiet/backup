//! Temporary - exists only for the Scala-repository migration tool (DESIGN-MIGRATION-005 in
//! `docs/design/scala-migration-tool.md`); remove it together with the tool, see DESIGN-MIGRATION-004's
//! own removal note.
//!
//! The migration's durable progress record: two tables that live in the destination database file
//! itself, so that a migrated entry and the note that it has been migrated are written by one and
//! the same transaction - a hard kill or a power loss can then never leave one without the other.
//! Both tables are created when a migration starts and dropped again once it has finished, so a
//! finished repository carries none of it.
//!
//! `pub(crate)` only, reached exclusively through [`crate::Repository`] (DESIGN-METADATA-006).

use rusqlite::{Connection, OptionalExtension, params};

use crate::Error;

const TABLE_NAMES: &str =
    "('migration_content_cache', 'migration_migrated', 'migration_damaged_content')";

/// Makes sure the progress tables exist. Returns `false` - creating nothing - when this database
/// has already been fully migrated: the tables are gone (they are dropped by [`finish`]) but the
/// tree holds more than the root entry every repository starts with. A database that was only just
/// adopted (root entry only) is not that, and gets its tables.
pub(crate) fn prepare(conn: &Connection) -> Result<bool, Error> {
    let tables: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN {TABLE_NAMES}"
        ),
        [],
        |row| row.get(0),
    )?;
    if tables == 0 {
        let entries: i64 =
            conn.query_row("SELECT COUNT(*) FROM tree_entries", [], |row| row.get(0))?;
        if entries > 1 {
            return Ok(false);
        }
    }
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS migration_content_cache (
             old_data_id INTEGER PRIMARY KEY,
             content_id  INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS migration_migrated (
             old_tree_id INTEGER PRIMARY KEY,
             new_id      INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS migration_damaged_content (
             old_data_id INTEGER PRIMARY KEY,
             detail      TEXT NOT NULL
         );",
    )?;
    Ok(true)
}

/// Drops the progress tables again.
pub(crate) fn finish(conn: &Connection) -> Result<(), Error> {
    conn.execute_batch(
        "DROP TABLE IF EXISTS migration_content_cache;
         DROP TABLE IF EXISTS migration_migrated;
         DROP TABLE IF EXISTS migration_damaged_content;",
    )?;
    Ok(())
}

pub(crate) fn cached_content(conn: &Connection, old_data_id: i64) -> Result<Option<i64>, Error> {
    conn.query_row(
        "SELECT content_id FROM migration_content_cache WHERE old_data_id = ?1",
        params![old_data_id],
        |row| row.get(0),
    )
    .optional()
    .map_err(Error::from)
}

pub(crate) fn record_content(
    conn: &Connection,
    old_data_id: i64,
    content_id: i64,
) -> Result<(), Error> {
    conn.execute(
        "INSERT INTO migration_content_cache (old_data_id, content_id) VALUES (?1, ?2)",
        params![old_data_id, content_id],
    )?;
    Ok(())
}

/// Notes that `old_data_id`'s content was migrated although some of its old bytes were missing
/// (`detail` says which data files) - see DESIGN-MIGRATION-008.
pub(crate) fn record_damaged(
    conn: &Connection,
    old_data_id: i64,
    detail: &str,
) -> Result<(), Error> {
    conn.execute(
        "INSERT INTO migration_damaged_content (old_data_id, detail) VALUES (?1, ?2)",
        params![old_data_id, detail],
    )?;
    Ok(())
}

/// Every content noted by [`record_damaged`], by ascending old id.
pub(crate) fn damaged(conn: &Connection) -> Result<Vec<(i64, String)>, Error> {
    let mut stmt = conn.prepare(
        "SELECT old_data_id, detail FROM migration_damaged_content ORDER BY old_data_id",
    )?;
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub(crate) fn migrated_id(conn: &Connection, old_tree_id: i64) -> Result<Option<i64>, Error> {
    conn.query_row(
        "SELECT new_id FROM migration_migrated WHERE old_tree_id = ?1",
        params![old_tree_id],
        |row| row.get(0),
    )
    .optional()
    .map_err(Error::from)
}

pub(crate) fn record_migrated(
    conn: &Connection,
    old_tree_id: i64,
    new_id: i64,
) -> Result<(), Error> {
    conn.execute(
        "INSERT INTO migration_migrated (old_tree_id, new_id) VALUES (?1, ?2)",
        params![old_tree_id, new_id],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{RepositorySettings, init_repository, open_repository};

    fn repo_root() -> (std::path::PathBuf, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, RepositorySettings::new(20, 1_700_000_000_000)).unwrap();
        (repo_root, dir)
    }

    #[test]
    fn prepare_is_idempotent_on_a_freshly_adopted_repository() {
        let (root, _dir) = repo_root();
        let repo = open_repository(&root).unwrap();
        assert!(repo.migration_prepare().unwrap());
        assert!(repo.migration_prepare().unwrap());
    }

    #[test]
    fn prepare_recognizes_a_repository_that_was_already_fully_migrated() {
        let (root, _dir) = repo_root();
        let repo = open_repository(&root).unwrap();
        assert!(repo.migration_prepare().unwrap());
        repo.mkdir(0, "migrated", 100).unwrap();
        repo.migration_finish().unwrap();

        assert!(
            !repo.migration_prepare().unwrap(),
            "tables gone but content present means an earlier run already finished"
        );
    }

    #[test]
    fn prepare_treats_a_root_only_repository_as_not_yet_migrated_even_after_finish() {
        let (root, _dir) = repo_root();
        let repo = open_repository(&root).unwrap();
        assert!(repo.migration_prepare().unwrap());
        repo.migration_finish().unwrap();
        assert!(repo.migration_prepare().unwrap());
    }

    #[test]
    fn progress_records_round_trip() {
        let (root, _dir) = repo_root();
        let repo = open_repository(&root).unwrap();
        repo.migration_prepare().unwrap();

        assert_eq!(repo.migration_cached_content(5).unwrap(), None);
        repo.migration_record_content(5, 42).unwrap();
        assert_eq!(repo.migration_cached_content(5).unwrap(), Some(42));

        assert_eq!(repo.migration_migrated_id(7).unwrap(), None);
        repo.migration_record_migrated(7, 99).unwrap();
        assert_eq!(repo.migration_migrated_id(7).unwrap(), Some(99));
    }

    #[test]
    fn damaged_contents_are_recorded_listed_in_order_and_dropped_by_finish() {
        let (root, _dir) = repo_root();
        let repo = open_repository(&root).unwrap();
        repo.migration_prepare().unwrap();
        assert_eq!(repo.migration_damaged().unwrap(), Vec::new());

        repo.migration_record_damaged(9, "data/00/01/x").unwrap();
        repo.migration_record_damaged(3, "data/00/00/y").unwrap();
        assert_eq!(
            repo.migration_damaged().unwrap(),
            vec![
                (3, "data/00/00/y".to_string()),
                (9, "data/00/01/x".to_string())
            ]
        );

        repo.migration_finish().unwrap();
        repo.migration_prepare().unwrap();
        assert_eq!(
            repo.migration_damaged().unwrap(),
            Vec::new(),
            "the table must not survive finish"
        );
    }

    #[test]
    fn an_entry_and_its_progress_record_land_together_once_the_batch_is_committed() {
        let (root, _dir) = repo_root();
        {
            let repo = open_repository(&root).unwrap();
            repo.migration_prepare().unwrap();
            repo.migration_begin_batch().unwrap();
            let id = repo.insert_migrated_entry(0, "a", 1, None, None).unwrap();
            repo.migration_record_migrated(5, id).unwrap();
            repo.migration_commit_batch().unwrap();
        }
        let repo = open_repository(&root).unwrap();
        let entry = repo.resolve_path("/a").unwrap().unwrap();
        assert_eq!(repo.migration_migrated_id(5).unwrap(), Some(entry.id));
    }

    #[test]
    fn an_uncommitted_batch_leaves_neither_the_entry_nor_its_progress_record_behind() {
        let (root, _dir) = repo_root();
        {
            let repo = open_repository(&root).unwrap();
            repo.migration_prepare().unwrap();
            repo.migration_begin_batch().unwrap();
            let id = repo.insert_migrated_entry(0, "a", 1, None, None).unwrap();
            repo.migration_record_migrated(5, id).unwrap();
            // Dropped without committing - what a killed process amounts to.
        }
        let repo = open_repository(&root).unwrap();
        assert!(repo.resolve_path("/a").unwrap().is_none());
        assert_eq!(repo.migration_migrated_id(5).unwrap(), None);
    }

    #[test]
    fn a_failing_call_inside_a_batch_is_undone_without_ending_the_batch() {
        let (root, _dir) = repo_root();
        let repo = open_repository(&root).unwrap();
        repo.migration_prepare().unwrap();
        repo.migration_begin_batch().unwrap();

        repo.insert_migrated_entry(0, "a", 1, None, None).unwrap();
        repo.insert_migrated_entry(0, "a", 2, None, None)
            .expect_err("a second live entry of that name must be refused");
        repo.insert_migrated_entry(0, "b", 3, None, None).unwrap();
        repo.migration_commit_batch().unwrap();

        assert!(repo.resolve_path("/a").unwrap().is_some());
        assert!(repo.resolve_path("/b").unwrap().is_some());
    }

    #[test]
    fn batch_ops_count_the_calls_since_the_batch_began_and_reset_on_commit() {
        let (root, _dir) = repo_root();
        let repo = open_repository(&root).unwrap();
        repo.migration_prepare().unwrap();
        repo.migration_begin_batch().unwrap();
        assert_eq!(repo.migration_batch_ops().unwrap(), 0);

        repo.insert_migrated_entry(0, "a", 1, None, None).unwrap();
        repo.migration_record_migrated(1, 1).unwrap();
        assert_eq!(repo.migration_batch_ops().unwrap(), 2);

        repo.migration_commit_batch().unwrap();
        assert_eq!(repo.migration_batch_ops().unwrap(), 0);
    }
}
