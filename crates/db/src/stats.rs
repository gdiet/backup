//! Repository-wide and path-scoped usage statistics - REQ-QUERY-003 in
//! `requirements/functional/query.md`.

use rusqlite::{Connection, params};

use crate::Error;
use crate::tree::{self, EntryKind};

/// One [`crate::Repository::stats`]/[`crate::Repository::stats_for`] result - REQ-QUERY-003's item
/// counts, sizes, and the figures derived from the counted files. Repository age is not part of
/// this struct: it applies only to the repository-wide query, and is already available from
/// [`crate::Repository::settings`] directly, without a database round trip. The other
/// repository-wide figures are in [`RepositoryStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Stats {
    pub dirs: u64,
    pub files: u64,
    /// The combined size every counted file would have if none of its content were deduplicated -
    /// each file's own `contents.length`, counted once per file even when several files share the
    /// same content.
    pub logical_size: u64,
    /// The combined size of the distinct chunks the counted files actually depend on - REQ-QUERY-003's
    /// "actual physical storage used". Counted once per distinct chunk, however many of the counted
    /// files reference it, and regardless of whether that chunk is also referenced by a file outside
    /// the counted scope.
    pub physical_size: u64,
    /// The number of distinct chunks behind [`Self::physical_size`], counted the same way.
    pub chunks: u64,
    /// The counted files whose content is empty.
    pub empty_files: u64,
    /// The oldest modification time among the counted files, in Unix epoch milliseconds. `None`
    /// when no file is counted.
    pub oldest_file_time: Option<i64>,
    /// The newest modification time among the counted files, in Unix epoch milliseconds. `None`
    /// when no file is counted.
    pub newest_file_time: Option<i64>,
}

/// The repository-wide figures that no path scope has - REQ-QUERY-003's storage and metadata
/// facts. Unlike [`Stats`], these describe everything stored, including content that only
/// soft-deleted entries still use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RepositoryStats {
    /// Every stored chunk.
    pub chunks: u64,
    /// The combined length of every stored chunk.
    pub chunk_bytes: u64,
    /// Every stored chunk extent. Never smaller than [`Self::chunks`], because each chunk has at
    /// least one extent.
    pub chunk_extents: u64,
    /// The end of the stored data: the highest position any chunk extent reaches. `0` for a
    /// repository without chunks.
    pub data_end: u64,
    /// The part of the stored data up to [`Self::data_end`] that no chunk extent covers.
    pub data_unused: u64,
    /// The size of the metadata database file.
    pub metadata_size: u64,
    pub deleted_dirs: u64,
    pub deleted_files: u64,
}

/// What [`counts`] reads about the counted entries, before the chunk figures join in.
struct Counts {
    dirs: u64,
    files: u64,
    logical_size: u64,
    empty_files: u64,
    oldest_file_time: Option<i64>,
    newest_file_time: Option<i64>,
}

/// Repository-wide statistics - every live entry except the root itself.
pub(crate) fn stats(conn: &Connection) -> Result<Stats, Error> {
    let counts = counts(
        conn,
        "SELECT \
           COALESCE(SUM(CASE WHEN te.kind = 0 THEN 1 ELSE 0 END), 0), \
           COALESCE(SUM(CASE WHEN te.kind = 1 THEN 1 ELSE 0 END), 0), \
           COALESCE(SUM(CASE WHEN te.kind = 1 THEN c.length ELSE 0 END), 0), \
           COALESCE(SUM(CASE WHEN te.kind = 1 AND c.length = 0 THEN 1 ELSE 0 END), 0), \
           MIN(CASE WHEN te.kind = 1 THEN te.time END), \
           MAX(CASE WHEN te.kind = 1 THEN te.time END) \
         FROM tree_entries te LEFT JOIN contents c ON c.id = te.content_id \
         WHERE te.deleted_at IS NULL AND te.id != 0",
        [],
    )?;
    let (chunks, physical_size) = conn.query_row(
        "SELECT COUNT(*), COALESCE(SUM(ch.length), 0) FROM ( \
           SELECT DISTINCT cc.chunk_id AS id, ch.length AS length \
           FROM tree_entries te \
           JOIN content_chunks cc ON cc.content_id = te.content_id \
           JOIN chunks ch ON ch.id = cc.chunk_id \
           WHERE te.deleted_at IS NULL AND te.kind = 1 \
         ) ch",
        [],
        |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
    )?;
    Ok(Stats::from_parts(counts, chunks, physical_size))
}

/// The repository-wide figures of [`RepositoryStats`].
pub(crate) fn repository_stats(conn: &Connection) -> Result<RepositoryStats, Error> {
    let (chunks, chunk_bytes) = conn.query_row(
        "SELECT COUNT(*), COALESCE(SUM(length), 0) FROM chunks",
        [],
        |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
    )?;
    let (chunk_extents, data_end, extent_bytes) = conn.query_row(
        "SELECT COUNT(*), COALESCE(MAX(stop), 0), COALESCE(SUM(stop - start), 0) \
         FROM chunk_extents",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, i64>(1)? as u64,
                row.get::<_, i64>(2)? as u64,
            ))
        },
    )?;
    let (deleted_dirs, deleted_files) = conn.query_row(
        "SELECT \
           COALESCE(SUM(CASE WHEN kind = 0 THEN 1 ELSE 0 END), 0), \
           COALESCE(SUM(CASE WHEN kind = 1 THEN 1 ELSE 0 END), 0) \
         FROM tree_entries WHERE deleted_at IS NOT NULL",
        [],
        |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
    )?;
    Ok(RepositoryStats {
        chunks,
        chunk_bytes,
        chunk_extents,
        data_end,
        data_unused: data_end.saturating_sub(extent_bytes),
        metadata_size: crate::database_size_bytes(conn)?,
        deleted_dirs,
        deleted_files,
    })
}

/// Path-scoped statistics - `dir_id`'s own live descendants, recursively; `dir_id` itself is not
/// counted. Refuses an `id` that does not exist ([`Error::NoSuchEntry`]) or is not a directory
/// ([`Error::WrongKind`]).
pub(crate) fn stats_for(conn: &Connection, dir_id: i64) -> Result<Stats, Error> {
    let entry = tree::get_by_id(conn, dir_id)?.ok_or(Error::NoSuchEntry(dir_id))?;
    if entry.kind != EntryKind::Dir {
        return Err(Error::WrongKind(dir_id));
    }

    let counts = counts(
        conn,
        "WITH RECURSIVE subtree(id) AS ( \
           SELECT id FROM tree_entries WHERE parent_id = ?1 AND deleted_at IS NULL \
           UNION ALL \
           SELECT te.id FROM tree_entries te JOIN subtree s ON te.parent_id = s.id \
           WHERE te.deleted_at IS NULL \
         ) \
         SELECT \
           COALESCE(SUM(CASE WHEN te.kind = 0 THEN 1 ELSE 0 END), 0), \
           COALESCE(SUM(CASE WHEN te.kind = 1 THEN 1 ELSE 0 END), 0), \
           COALESCE(SUM(CASE WHEN te.kind = 1 THEN c.length ELSE 0 END), 0), \
           COALESCE(SUM(CASE WHEN te.kind = 1 AND c.length = 0 THEN 1 ELSE 0 END), 0), \
           MIN(CASE WHEN te.kind = 1 THEN te.time END), \
           MAX(CASE WHEN te.kind = 1 THEN te.time END) \
         FROM subtree JOIN tree_entries te ON te.id = subtree.id \
         LEFT JOIN contents c ON c.id = te.content_id",
        params![dir_id],
    )?;
    let (chunks, physical_size) = conn.query_row(
        "WITH RECURSIVE subtree(id) AS ( \
           SELECT id FROM tree_entries WHERE parent_id = ?1 AND deleted_at IS NULL \
           UNION ALL \
           SELECT te.id FROM tree_entries te JOIN subtree s ON te.parent_id = s.id \
           WHERE te.deleted_at IS NULL \
         ) \
         SELECT COUNT(*), COALESCE(SUM(ch.length), 0) FROM ( \
           SELECT DISTINCT cc.chunk_id AS id, ch.length AS length \
           FROM subtree s \
           JOIN tree_entries te ON te.id = s.id AND te.kind = 1 \
           JOIN content_chunks cc ON cc.content_id = te.content_id \
           JOIN chunks ch ON ch.id = cc.chunk_id \
         ) ch",
        params![dir_id],
        |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
    )?;
    Ok(Stats::from_parts(counts, chunks, physical_size))
}

impl Stats {
    fn from_parts(counts: Counts, chunks: u64, physical_size: u64) -> Self {
        Self {
            dirs: counts.dirs,
            files: counts.files,
            logical_size: counts.logical_size,
            physical_size,
            chunks,
            empty_files: counts.empty_files,
            oldest_file_time: counts.oldest_file_time,
            newest_file_time: counts.newest_file_time,
        }
    }
}

fn counts(conn: &Connection, sql: &str, params: impl rusqlite::Params) -> Result<Counts, Error> {
    conn.query_row(sql, params, |row| {
        Ok(Counts {
            dirs: row.get::<_, i64>(0)? as u64,
            files: row.get::<_, i64>(1)? as u64,
            logical_size: row.get::<_, i64>(2)? as u64,
            empty_files: row.get::<_, i64>(3)? as u64,
            oldest_file_time: row.get(4)?,
            newest_file_time: row.get(5)?,
        })
    })
    .map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RepositorySettings, init_repository, open_repository};

    fn repo() -> (crate::Repository, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        let settings = RepositorySettings::new(20, 1_700_000_000_000);
        init_repository(&repo_root, settings).expect("init must succeed");
        let repo = open_repository(&repo_root).expect("open must succeed");
        (repo, dir)
    }

    /// Inserts a content row with one owning chunk of the given length, and settles a file
    /// referencing it - enough to exercise both logical and physical size, unlike a bare
    /// zero-chunk placeholder.
    fn create_file(
        repo: &crate::Repository,
        parent: i64,
        name: &str,
        length: i64,
        hash_byte: u8,
    ) -> i64 {
        let (chunk_id, _ranges) = repo
            .reserve_and_insert_chunk(length, &[hash_byte; 20])
            .unwrap();
        let content_id = repo
            .find_or_create_content(length, &[hash_byte.wrapping_add(1); 20], &[chunk_id])
            .unwrap();
        repo.settle_file(parent, name, 1_700_000_000_000, content_id)
            .unwrap()
    }

    #[test]
    fn stats_counts_an_empty_repository_as_all_zero() {
        let (repo, _dir) = repo();
        let stats = repo.stats().unwrap();
        assert_eq!(stats, Stats::default());
    }

    #[test]
    fn stats_counts_dirs_files_and_logical_size_repository_wide() {
        let (repo, _dir) = repo();
        repo.mkdir(0, "a", 100).unwrap();
        create_file(&repo, 0, "one.txt", 10, 0xAA);
        create_file(&repo, 0, "two.txt", 20, 0xBB);

        let stats = repo.stats().unwrap();
        assert_eq!(stats.dirs, 1);
        assert_eq!(stats.files, 2);
        assert_eq!(stats.logical_size, 30);
    }

    #[test]
    fn stats_counts_shared_content_once_per_file_logically_but_once_overall_physically() {
        let (repo, _dir) = repo();
        let (chunk_id, _ranges) = repo.reserve_and_insert_chunk(10, &[0xAA; 20]).unwrap();
        let content_id = repo
            .find_or_create_content(10, &[0xBB; 20], &[chunk_id])
            .unwrap();
        repo.settle_file(0, "one.txt", 100, content_id).unwrap();
        repo.settle_file(0, "two.txt", 100, content_id).unwrap();

        let stats = repo.stats().unwrap();
        assert_eq!(stats.files, 2);
        assert_eq!(
            stats.logical_size, 20,
            "logical size counts each file's own size, deduplication or not"
        );
        assert_eq!(
            stats.physical_size, 10,
            "physical size counts the one shared chunk once, not twice"
        );
    }

    #[test]
    fn stats_never_counts_a_soft_deleted_entry() {
        let (repo, _dir) = repo();
        let id = create_file(&repo, 0, "gone.txt", 10, 0xAA);
        repo.unlink_file(id, 200).unwrap();

        let stats = repo.stats().unwrap();
        assert_eq!(stats.files, 0);
        assert_eq!(stats.logical_size, 0);
        assert_eq!(stats.physical_size, 0);
    }

    #[test]
    fn stats_for_scopes_to_a_directorys_own_recursive_descendants() {
        let (repo, _dir) = repo();
        let a_id = repo.mkdir(0, "a", 100).unwrap();
        create_file(&repo, a_id, "in-a.txt", 10, 0xAA);
        let nested_id = repo.mkdir(a_id, "nested", 100).unwrap();
        create_file(&repo, nested_id, "in-nested.txt", 20, 0xBB);
        create_file(&repo, 0, "outside.txt", 99, 0xCC);

        let stats = repo.stats_for(a_id).unwrap();
        assert_eq!(stats.dirs, 1, "the nested directory, not `a` itself");
        assert_eq!(stats.files, 2);
        assert_eq!(stats.logical_size, 30);
    }

    #[test]
    fn stats_counts_distinct_chunks_empty_files_and_the_file_time_range() {
        let (repo, _dir) = repo();
        let (chunk_id, _ranges) = repo.reserve_and_insert_chunk(10, &[0xAA; 20]).unwrap();
        let shared = repo
            .find_or_create_content(10, &[0xBB; 20], &[chunk_id])
            .unwrap();
        repo.settle_file(0, "one.txt", 300, shared).unwrap();
        repo.settle_file(0, "two.txt", 100, shared).unwrap();
        let empty = repo.find_or_create_content(0, &[0xCC; 20], &[]).unwrap();
        repo.settle_file(0, "empty.txt", 200, empty).unwrap();

        let stats = repo.stats().unwrap();
        assert_eq!(stats.files, 3);
        assert_eq!(stats.chunks, 1, "the shared chunk counts once");
        assert_eq!(stats.empty_files, 1);
        assert_eq!(stats.oldest_file_time, Some(100));
        assert_eq!(stats.newest_file_time, Some(300));
    }

    #[test]
    fn stats_has_no_file_time_range_without_files() {
        let (repo, _dir) = repo();
        repo.mkdir(0, "a", 100).unwrap();

        let stats = repo.stats().unwrap();
        assert_eq!(stats.oldest_file_time, None);
        assert_eq!(stats.newest_file_time, None);
    }

    #[test]
    fn stats_for_reports_chunks_empty_files_and_file_times_of_its_own_subtree_only() {
        let (repo, _dir) = repo();
        let a_id = repo.mkdir(0, "a", 100).unwrap();
        create_file(&repo, a_id, "in-a.txt", 10, 0xAA);
        let empty = repo.find_or_create_content(0, &[0xCC; 20], &[]).unwrap();
        repo.settle_file(a_id, "empty.txt", 500, empty).unwrap();
        repo.settle_file(0, "outside.txt", 9_000, empty).unwrap();
        create_file(&repo, 0, "other.txt", 99, 0xDD);

        let stats = repo.stats_for(a_id).unwrap();
        assert_eq!(stats.chunks, 1);
        assert_eq!(stats.empty_files, 1);
        assert_eq!(stats.oldest_file_time, Some(500));
        assert_eq!(
            stats.newest_file_time,
            Some(1_700_000_000_000),
            "create_file stamps its files with a fixed time"
        );
    }

    #[test]
    fn repository_stats_of_an_empty_repository_has_only_the_metadata_size() {
        let (repo, _dir) = repo();
        let stats = repo.repository_stats().unwrap();
        assert_eq!(
            RepositoryStats {
                metadata_size: 0,
                ..stats
            },
            RepositoryStats::default()
        );
        assert!(stats.metadata_size > 0, "the schema alone fills pages");
    }

    #[test]
    fn repository_stats_counts_chunks_extents_and_the_end_of_the_stored_data() {
        let (repo, _dir) = repo();
        create_file(&repo, 0, "a.txt", 10, 0xAA);
        create_file(&repo, 0, "b.txt", 20, 0xBB);

        let stats = repo.repository_stats().unwrap();
        assert_eq!(stats.chunks, 2);
        assert_eq!(stats.chunk_bytes, 30);
        assert_eq!(stats.chunk_extents, 2);
        assert_eq!(stats.data_end, 30);
        assert_eq!(stats.data_unused, 0);
    }

    #[test]
    fn repository_stats_counts_soft_deleted_entries_and_content_only_they_use() {
        let (repo, _dir) = repo();
        let dir_id = repo.mkdir(0, "d", 100).unwrap();
        let file_id = create_file(&repo, 0, "gone.txt", 10, 0xAA);
        repo.rmdir(dir_id, 200).unwrap();
        repo.unlink_file(file_id, 200).unwrap();

        let stats = repo.repository_stats().unwrap();
        assert_eq!(stats.deleted_dirs, 1);
        assert_eq!(stats.deleted_files, 1);
        assert_eq!(stats.chunks, 1, "the deleted file's chunk is still stored");
        assert_eq!(
            repo.stats().unwrap().physical_size,
            0,
            "while the live statistics no longer count it"
        );
    }

    #[test]
    fn repository_stats_reports_the_gap_a_purge_leaves_below_the_end_of_the_data() {
        let (repo, _dir) = repo();
        let first = create_file(&repo, 0, "first.txt", 10, 0xAA);
        create_file(&repo, 0, "second.txt", 20, 0xBB);
        repo.unlink_file(first, 200).unwrap();
        repo.reclaim(1_000, 0).unwrap();

        let stats = repo.repository_stats().unwrap();
        assert_eq!(stats.chunks, 1);
        assert_eq!(stats.chunk_extents, 1);
        assert_eq!(stats.data_end, 30);
        assert_eq!(stats.data_unused, 10);
        assert_eq!(stats.deleted_files, 0, "a purged entry is gone");
    }

    #[test]
    fn stats_for_refuses_a_file() {
        let (repo, _dir) = repo();
        let id = create_file(&repo, 0, "a.txt", 10, 0xAA);
        let err = repo.stats_for(id).unwrap_err();
        assert!(matches!(err, Error::WrongKind(_)));
    }

    #[test]
    fn stats_for_refuses_a_nonexistent_id() {
        let (repo, _dir) = repo();
        let err = repo.stats_for(999).unwrap_err();
        assert!(matches!(err, Error::NoSuchEntry(999)));
    }
}
