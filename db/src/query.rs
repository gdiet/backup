//! Read-only tree queries shared by the reporting/inspection commands
//! (`stats`, `list`, `find`, `check`, `restore`). Nothing here mutates state.

use rusqlite::Connection;

use crate::Error;
use crate::tree::{TreeEntryRow, find_tree_entry, get_tree_entry};

/// Resolves a `/`-separated path (relative to the repository root) to its tree
/// entry, if every component exists and is active (non-soft-deleted). An empty
/// path (`""`) resolves to the repository root itself.
pub fn resolve_path(conn: &Connection, path: &str) -> Result<Option<TreeEntryRow>, Error> {
    let mut current = get_tree_entry(conn, 0)?.expect("the root entry always exists");
    for component in path.split('/').filter(|c| !c.is_empty()) {
        match find_tree_entry(conn, current.id, component)? {
            Some(entry) => current = entry,
            None => return Ok(None),
        }
    }
    Ok(Some(current))
}

/// The active (non-soft-deleted) direct children of `parent_id`, sorted by kind
/// (directories first) then by name.
pub fn list_children(conn: &Connection, parent_id: i64) -> Result<Vec<TreeEntryRow>, Error> {
    let mut stmt = conn.prepare(
        "SELECT id, name, time, kind, content_id FROM tree_entries
         WHERE parent_id = ?1 AND id != ?1 AND deleted_at IS NULL
         ORDER BY kind ASC, name",
    )?;
    let rows = stmt
        .query_map([parent_id], crate::tree::row_to_tree_entry)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Aggregate stats over every active descendant of `id` (not including `id`
/// itself), computed with one recursive query instead of walking the tree
/// client-side one directory at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SubtreeStats {
    pub files: i64,
    pub dirs: i64,
    /// Sum of `contents.length` over every file in the subtree - i.e. the
    /// logical size as the user would see it, not deduplicated against
    /// content shared with entries outside this subtree.
    pub total_logical_bytes: i64,
}

pub fn subtree_stats(conn: &Connection, id: i64) -> Result<SubtreeStats, Error> {
    conn.query_row(
        "WITH RECURSIVE subtree(id) AS (
             SELECT id FROM tree_entries WHERE parent_id = ?1 AND id != ?1 AND deleted_at IS NULL
             UNION ALL
             SELECT t.id FROM tree_entries t JOIN subtree s ON t.parent_id = s.id
             WHERE t.deleted_at IS NULL
         )
         SELECT
             COALESCE(SUM(CASE WHEN t.kind = 'file' THEN 1 ELSE 0 END), 0),
             COALESCE(SUM(CASE WHEN t.kind = 'dir' THEN 1 ELSE 0 END), 0),
             COALESCE(SUM(c.length), 0)
         FROM subtree s
         JOIN tree_entries t ON t.id = s.id
         LEFT JOIN contents c ON c.id = t.content_id",
        [id],
        |row| {
            Ok(SubtreeStats {
                files: row.get(0)?,
                dirs: row.get(1)?,
                total_logical_bytes: row.get(2)?,
            })
        },
    )
    .map_err(Error::from)
}

/// One entry found under a subtree walk, with its path relative to the walk's
/// starting point (no leading slash, `/`-separated, e.g. `"sub/file.txt"`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathEntry {
    pub path: String,
    pub entry: TreeEntryRow,
}

/// Every active descendant of `root_id` (not including `root_id` itself), each
/// with its path relative to `root_id`. Pass `0` (the repository root) to walk
/// the entire tree. One recursive query, replacing the two-stage
/// SQL-LIKE-then-reconstruct-path-in-a-loop approach this is based on (see the
/// plan doc for why that approach was fragile).
pub fn subtree_entries_with_paths(
    conn: &Connection,
    root_id: i64,
) -> Result<Vec<PathEntry>, Error> {
    let mut stmt = conn.prepare(
        "WITH RECURSIVE walk(id, path) AS (
             SELECT id, name FROM tree_entries WHERE parent_id = ?1 AND id != ?1 AND deleted_at IS NULL
             UNION ALL
             SELECT t.id, walk.path || '/' || t.name
             FROM tree_entries t JOIN walk ON t.parent_id = walk.id
             WHERE t.deleted_at IS NULL
         )
         SELECT walk.path, t.id, t.name, t.time, t.kind, t.content_id
         FROM walk JOIN tree_entries t ON t.id = walk.id",
    )?;
    let rows = stmt
        .query_map([root_id], |row| {
            Ok(PathEntry {
                path: row.get(0)?,
                entry: crate::tree::row_to_tree_entry_at(row, 1)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// The logical size of a file entry: `0` for an empty file (`content_id` is
/// `None`), otherwise the referenced content's length. Only meaningful for a
/// `File` entry - a directory also has `content_id == None`, so this silently
/// returns `0` for one rather than distinguishing it; callers should check
/// `entry.kind` first.
pub fn file_size(conn: &Connection, entry: &TreeEntryRow) -> Result<i64, Error> {
    match entry.content_id {
        None => Ok(0),
        Some(content_id) => conn
            .query_row(
                "SELECT length FROM contents WHERE id = ?1",
                [content_id],
                |row| row.get(0),
            )
            .map_err(Error::from),
    }
}

/// A chunk making up part of a content's byte sequence, as stored - not to be
/// confused with `db::ChunkRef`, which describes a chunk a backup worker has
/// just resolved (possibly not yet persisted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRange {
    pub chunk_id: i64,
    pub length: i64,
    pub hash: Vec<u8>,
    pub start: i64,
    pub stop: i64,
}

fn row_to_chunk_range(row: &rusqlite::Row) -> rusqlite::Result<ChunkRange> {
    Ok(ChunkRange {
        chunk_id: row.get(0)?,
        length: row.get(1)?,
        hash: row.get(2)?,
        start: row.get(3)?,
        stop: row.get(4)?,
    })
}

/// The chunks making up `content_id`'s byte sequence, in order.
pub fn ordered_content_chunks(
    conn: &Connection,
    content_id: i64,
) -> Result<Vec<ChunkRange>, Error> {
    let mut stmt = conn.prepare(
        "SELECT ch.id, ch.length, ch.hash, ch.start, ch.stop
         FROM content_chunks cc JOIN chunks ch ON ch.id = cc.chunk_id
         WHERE cc.content_id = ?1
         ORDER BY cc.seq",
    )?;
    let rows = stmt
        .query_map([content_id], row_to_chunk_range)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Every chunk in the repository, in no particular order - including chunks
/// with `ref_count = 0` (unreferenced, pending cleanup by `reclaim-space`),
/// unlike [`ordered_content_chunks`] which only ever returns chunks actually
/// reachable from a content.
pub fn all_chunks(conn: &Connection) -> Result<Vec<ChunkRange>, Error> {
    let mut stmt = conn.prepare("SELECT id, length, hash, start, stop FROM chunks")?;
    let rows = stmt
        .query_map([], row_to_chunk_range)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Chunking, RepositorySettings};
    use rusqlite::Connection;

    fn test_connection() -> (tempfile::TempDir, Connection) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        crate::init_repository(
            &repo_root,
            &RepositorySettings::new(20, Chunking::Cdc).unwrap(),
        )
        .unwrap();
        let conn = crate::open_repository(&repo_root)
            .unwrap()
            .open_write_connection()
            .unwrap();
        (temp_dir, conn)
    }

    fn insert_file(conn: &Connection, parent_id: i64, name: &str, content_id: Option<i64>) -> i64 {
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind, content_id)
             VALUES (?1, ?2, 0, 'file', ?3)",
            rusqlite::params![parent_id, name, content_id],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_content(conn: &Connection, length: i64, hash: &[u8]) -> i64 {
        conn.execute(
            "INSERT INTO contents (length, hash) VALUES (?1, ?2)",
            rusqlite::params![length, hash],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    #[test]
    fn file_size_is_zero_for_empty_files_and_the_content_length_otherwise() {
        let (_temp_dir, conn) = test_connection();
        let content_id = insert_content(&conn, 42, b"h");
        insert_file(&conn, 0, "empty.txt", None);
        insert_file(&conn, 0, "a.txt", Some(content_id));

        let empty = resolve_path(&conn, "empty.txt").unwrap().unwrap();
        let a = resolve_path(&conn, "a.txt").unwrap().unwrap();
        assert_eq!(file_size(&conn, &empty).unwrap(), 0);
        assert_eq!(file_size(&conn, &a).unwrap(), 42);
    }

    #[test]
    fn resolve_path_walks_components_and_handles_root() {
        let (_temp_dir, conn) = test_connection();
        let sub_id = crate::insert_directory(&conn, 0, "sub", 0).unwrap();
        insert_file(&conn, sub_id, "a.txt", None);

        assert_eq!(resolve_path(&conn, "").unwrap().unwrap().id, 0);
        assert_eq!(resolve_path(&conn, "sub").unwrap().unwrap().id, sub_id);
        assert_eq!(
            resolve_path(&conn, "sub/a.txt").unwrap().unwrap().name,
            "a.txt"
        );
        assert_eq!(resolve_path(&conn, "sub/missing").unwrap(), None);
        assert_eq!(resolve_path(&conn, "missing/a.txt").unwrap(), None);
    }

    #[test]
    fn list_children_sorts_dirs_before_files_alphabetically() {
        let (_temp_dir, conn) = test_connection();
        insert_file(&conn, 0, "b.txt", None);
        crate::insert_directory(&conn, 0, "a-dir", 0).unwrap();
        insert_file(&conn, 0, "a.txt", None);

        let names: Vec<String> = list_children(&conn, 0)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(names, vec!["a-dir", "a.txt", "b.txt"]);
    }

    #[test]
    fn subtree_stats_counts_descendants_not_the_root_itself() {
        let (_temp_dir, conn) = test_connection();
        let content_id = insert_content(&conn, 100, b"h1");
        let sub_id = crate::insert_directory(&conn, 0, "sub", 0).unwrap();
        insert_file(&conn, sub_id, "a.txt", Some(content_id));
        let nested_id = crate::insert_directory(&conn, sub_id, "nested", 0).unwrap();
        insert_file(&conn, nested_id, "b.txt", Some(content_id));

        let stats = subtree_stats(&conn, sub_id).unwrap();
        assert_eq!(stats.files, 2);
        assert_eq!(stats.dirs, 1, "'nested' counts, 'sub' itself does not");
        assert_eq!(stats.total_logical_bytes, 200);
    }

    #[test]
    fn subtree_stats_at_root_does_not_infinite_loop_on_self_parent() {
        let (_temp_dir, conn) = test_connection();
        insert_file(&conn, 0, "a.txt", None);

        let stats = subtree_stats(&conn, 0).unwrap();
        assert_eq!(stats.files, 1);
        assert_eq!(stats.dirs, 0);
    }

    #[test]
    fn subtree_entries_with_paths_builds_relative_paths() {
        let (_temp_dir, conn) = test_connection();
        let sub_id = crate::insert_directory(&conn, 0, "sub", 0).unwrap();
        insert_file(&conn, sub_id, "a.txt", None);
        insert_file(&conn, 0, "top.txt", None);

        let mut paths: Vec<String> = subtree_entries_with_paths(&conn, 0)
            .unwrap()
            .into_iter()
            .map(|e| e.path)
            .collect();
        paths.sort();
        assert_eq!(paths, vec!["sub", "sub/a.txt", "top.txt"]);
    }

    #[test]
    fn subtree_entries_with_paths_excludes_deleted_entries() {
        let (_temp_dir, conn) = test_connection();
        let id = insert_file(&conn, 0, "a.txt", None);
        conn.execute("UPDATE tree_entries SET deleted_at = 1 WHERE id = ?1", [id])
            .unwrap();

        assert_eq!(subtree_entries_with_paths(&conn, 0).unwrap(), vec![]);
    }

    #[test]
    fn ordered_content_chunks_returns_chunks_in_sequence_order() {
        let (_temp_dir, conn) = test_connection();
        conn.execute(
            "INSERT INTO chunks (id, length, hash, start, stop) VALUES
             (1, 5, x'AA', 10, 15), (2, 3, x'BB', 15, 18)",
            (),
        )
        .unwrap();
        let content_id = insert_content(&conn, 8, b"content");
        conn.execute(
            "INSERT INTO content_chunks (content_id, seq, chunk_id) VALUES
             (?1, 1, 2), (?1, 0, 1)",
            [content_id],
        )
        .unwrap();

        let chunks = ordered_content_chunks(&conn, content_id).unwrap();
        assert_eq!(
            chunks,
            vec![
                ChunkRange {
                    chunk_id: 1,
                    length: 5,
                    hash: vec![0xAA],
                    start: 10,
                    stop: 15
                },
                ChunkRange {
                    chunk_id: 2,
                    length: 3,
                    hash: vec![0xBB],
                    start: 15,
                    stop: 18
                },
            ]
        );
    }

    #[test]
    fn all_chunks_returns_every_chunk_including_unreferenced_ones() {
        let (_temp_dir, conn) = test_connection();
        conn.execute(
            "INSERT INTO chunks (id, length, hash, start, stop) VALUES (1, 5, x'AA', 0, 5)",
            (),
        )
        .unwrap();

        let chunks = all_chunks(&conn).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_id, 1);
    }
}
