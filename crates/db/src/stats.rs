//! Repository-wide and path-scoped usage statistics - REQ-QUERY-003 in
//! `requirements/functional/query.md`.

use rusqlite::{Connection, params};

use crate::Error;
use crate::tree::{self, EntryKind};

/// One [`crate::Repository::stats`]/[`crate::Repository::stats_for`] result - REQ-QUERY-003's item
/// counts, logical size, and physical size. Repository age is not part of this struct: it applies
/// only to the repository-wide query, and is already available from
/// [`crate::Repository::settings`] directly, without a database round trip.
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
}

/// Repository-wide statistics - every live entry except the root itself.
pub(crate) fn stats(conn: &Connection) -> Result<Stats, Error> {
    let (dirs, files, logical_size) = counts_and_logical_size(
        conn,
        "SELECT \
           COALESCE(SUM(CASE WHEN te.kind = 0 THEN 1 ELSE 0 END), 0), \
           COALESCE(SUM(CASE WHEN te.kind = 1 THEN 1 ELSE 0 END), 0), \
           COALESCE(SUM(CASE WHEN te.kind = 1 THEN c.length ELSE 0 END), 0) \
         FROM tree_entries te LEFT JOIN contents c ON c.id = te.content_id \
         WHERE te.deleted_at IS NULL AND te.id != 0",
        [],
    )?;
    let physical_size = conn.query_row(
        "SELECT COALESCE(SUM(ch.length), 0) FROM ( \
           SELECT DISTINCT cc.chunk_id AS id, ch.length AS length \
           FROM tree_entries te \
           JOIN content_chunks cc ON cc.content_id = te.content_id \
           JOIN chunks ch ON ch.id = cc.chunk_id \
           WHERE te.deleted_at IS NULL AND te.kind = 1 \
         ) ch",
        [],
        |row| row.get::<_, i64>(0),
    )? as u64;
    Ok(Stats {
        dirs,
        files,
        logical_size,
        physical_size,
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

    let (dirs, files, logical_size) = counts_and_logical_size(
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
           COALESCE(SUM(CASE WHEN te.kind = 1 THEN c.length ELSE 0 END), 0) \
         FROM subtree JOIN tree_entries te ON te.id = subtree.id \
         LEFT JOIN contents c ON c.id = te.content_id",
        params![dir_id],
    )?;
    let physical_size = conn.query_row(
        "WITH RECURSIVE subtree(id) AS ( \
           SELECT id FROM tree_entries WHERE parent_id = ?1 AND deleted_at IS NULL \
           UNION ALL \
           SELECT te.id FROM tree_entries te JOIN subtree s ON te.parent_id = s.id \
           WHERE te.deleted_at IS NULL \
         ) \
         SELECT COALESCE(SUM(ch.length), 0) FROM ( \
           SELECT DISTINCT cc.chunk_id AS id, ch.length AS length \
           FROM subtree s \
           JOIN tree_entries te ON te.id = s.id AND te.kind = 1 \
           JOIN content_chunks cc ON cc.content_id = te.content_id \
           JOIN chunks ch ON ch.id = cc.chunk_id \
         ) ch",
        params![dir_id],
        |row| row.get::<_, i64>(0),
    )? as u64;
    Ok(Stats {
        dirs,
        files,
        logical_size,
        physical_size,
    })
}

fn counts_and_logical_size(
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<(u64, u64, u64), Error> {
    conn.query_row(sql, params, |row| {
        let dirs: i64 = row.get(0)?;
        let files: i64 = row.get(1)?;
        let logical_size: i64 = row.get(2)?;
        Ok((dirs as u64, files as u64, logical_size as u64))
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
        let settings = RepositorySettings::new(Some(20), 1_700_000_000_000);
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
