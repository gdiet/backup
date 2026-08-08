use rusqlite::{Connection, OptionalExtension, params};

use crate::Error;
use crate::tree::{EntryKind, find_tree_entry};

/// Looks up an existing chunk by its dedup key `(length, hash)`.
pub fn find_chunk(conn: &Connection, length: u64, hash: &[u8]) -> Result<Option<i64>, Error> {
    conn.query_row(
        "SELECT id FROM chunks WHERE length = ?1 AND hash = ?2",
        params![length as i64, hash],
        |row| row.get(0),
    )
    .optional()
    .map_err(Error::from)
}

/// A chunk making up a file's content, as resolved by the caller (typically a
/// backup worker) before handing the file off to [`apply_backup_batch`].
#[derive(Debug, Clone)]
pub enum ChunkRef {
    /// An already-existing chunk (a dedup hit against [`find_chunk`]).
    Existing { id: i64, length: u64 },
    /// A chunk not found by [`find_chunk`], whose bytes the caller has already
    /// written to the byte store at `extents` (one or more half-open byte
    /// ranges, in the order they must be concatenated to reconstruct the
    /// chunk - see `chunk_extents` in `migrations.rs`) -
    /// `apply_backup_batch` only ever touches metadata, never chunk bytes.
    New {
        length: u64,
        hash: Vec<u8>,
        extents: Vec<(u64, u64)>,
    },
}

impl ChunkRef {
    fn length(&self) -> u64 {
        match self {
            ChunkRef::Existing { length, .. } | ChunkRef::New { length, .. } => *length,
        }
    }
}

/// How a [`FileBackupRecord`]'s content is determined.
#[derive(Debug, Clone)]
pub enum ContentSource {
    /// Chunked, hashed, and deduplicated normally - `apply_backup_batch`
    /// resolves this into a `content_id` via [`resolve_content`].
    Resolved {
        /// The file's chunks in order. Empty for a zero-length file.
        chunks: Vec<ChunkRef>,
        /// Hash over the ordered sequence of chunk lengths and hashes (see
        /// the `contents.hash` doc comment in `migrations.rs`). Ignored for
        /// an empty file (no `contents` row is created for those - see the
        /// `kind` doc comment).
        content_hash: Vec<u8>,
    },
    /// An already-known `content_id`, used as-is - e.g. reused from a
    /// reference file's tree entry (see `docs/plans/implemented/backup-reference.md`),
    /// skipping chunking/hashing/dedup-lookup entirely. `None` for an empty
    /// file, mirroring `Resolved` with empty `chunks`.
    Known(Option<i64>),
}

/// A file ready to be recorded, produced by a worker after chunking, hashing,
/// deduplicating and (for new chunks) writing bytes to the store - or, for a
/// reference hit, with no worker involvement at all (see [`ContentSource::Known`]).
#[derive(Debug, Clone)]
pub struct FileBackupRecord {
    pub parent_id: i64,
    pub name: String,
    pub time_millis: i64,
    pub content: ContentSource,
}

/// Resolves `chunks` against the dedup index (inserting any not-yet-known
/// chunk and its extents) and resolves-or-inserts a `contents` row for
/// `content_hash`, returning the resulting `content_id` - `None` for an
/// empty file (`chunks` is empty, matching the `kind` doc comment in
/// `migrations.rs`: an empty file has `content_id IS NULL` too).
///
/// Insert-or-get for both `chunks` and `contents` is race-safe: if another
/// caller already created a row for the same `(length, hash)`/`content_hash`
/// (e.g. an earlier batch within the same [`apply_backup_batch`] call, or a
/// concurrent one), `ON CONFLICT ... DO NOTHING` makes the insert a no-op and
/// the subsequent `SELECT` picks up the existing row.
///
/// Factored out of [`apply_backup_batch`]'s per-record loop (which still does
/// the actual chunk/content resolution by calling this) so other callers that
/// need the same dedup behavior without `apply_backup_batch`'s
/// active-tree-only `tree_entries` insert/replace semantics can reuse it -
/// notably the Scala repository migration tool, which resolves each migrated
/// file's content this way before inserting its own `tree_entries` row via
/// [`crate::insert_historical_tree_entry`].
pub fn resolve_content(
    conn: &Connection,
    chunks: &[ChunkRef],
    content_hash: &[u8],
) -> Result<Option<i64>, Error> {
    if chunks.is_empty() {
        return Ok(None);
    }

    let mut chunk_ids = Vec::with_capacity(chunks.len());
    for chunk_ref in chunks {
        let chunk_id = match chunk_ref {
            ChunkRef::Existing { id, .. } => *id,
            ChunkRef::New {
                length,
                hash,
                extents,
            } => {
                conn.execute(
                    "INSERT INTO chunks (length, hash) VALUES (?1, ?2)
                     ON CONFLICT (length, hash) DO NOTHING",
                    params![*length as i64, hash],
                )?;
                let chunk_id: i64 = conn.query_row(
                    "SELECT id FROM chunks WHERE length = ?1 AND hash = ?2",
                    params![*length as i64, hash],
                    |row| row.get(0),
                )?;
                // INSERT OR IGNORE, not a bare INSERT: on the losing side of
                // the chunks race just resolved above, this chunk_id already
                // has extents from whichever caller's insert won - this
                // caller's own (redundant, harmless) copy of the same bytes
                // elsewhere in the store is simply never referenced.
                for (seq, (start, stop)) in extents.iter().enumerate() {
                    conn.execute(
                        "INSERT OR IGNORE INTO chunk_extents (chunk_id, seq, start, stop)
                         VALUES (?1, ?2, ?3, ?4)",
                        params![chunk_id, seq as i64, *start as i64, *stop as i64],
                    )?;
                }
                chunk_id
            }
        };
        chunk_ids.push(chunk_id);
    }

    let total_length: i64 = chunks.iter().map(|c| c.length() as i64).sum();
    conn.execute(
        "INSERT INTO contents (length, hash) VALUES (?1, ?2)
         ON CONFLICT (hash) DO NOTHING",
        params![total_length, content_hash],
    )?;
    let content_id: i64 = conn.query_row(
        "SELECT id FROM contents WHERE hash = ?1",
        params![content_hash],
        |row| row.get(0),
    )?;
    for (seq, chunk_id) in chunk_ids.iter().enumerate() {
        conn.execute(
            "INSERT OR IGNORE INTO content_chunks (content_id, seq, chunk_id) VALUES (?1, ?2, ?3)",
            params![content_id, seq as i64, chunk_id],
        )?;
    }
    Ok(Some(content_id))
}

/// Applies a batch of [`FileBackupRecord`]s in a single transaction: resolves or
/// inserts each new chunk, resolves or inserts the file's `contents` row
/// (deduplicated by `content_hash`), and inserts or updates each file's
/// `tree_entries` row.
///
/// Re-running a backup for a file at a path that already has an active entry is
/// handled per the immutable-`content_id` invariant the `ref_count` triggers rely
/// on (see `migrations.rs`): if the resolved content is unchanged, only `time` is
/// refreshed in place; if it changed, the old entry is soft-deleted (`deleted_at`
/// set to this record's `time_millis` - a run timestamp isn't threaded through
/// separately) and a new entry is inserted, never mutating `content_id` in place.
/// A name that already exists as a directory is a hard error.
///
/// Uses a plain (`DEFERRED`) transaction. That's fine only because the caller is
/// expected to be the *sole* writer connection for the repository (see the
/// db crate's module-level doc comment): with a single writer, there is no other
/// connection to race with for the write lock, so `DEFERRED` vs. `IMMEDIATE`
/// makes no practical difference here. If a second writer-capable command (e.g.
/// a future GC/maintenance command) is ever run concurrently against the same
/// repository, that assumption breaks: two `DEFERRED` transactions that both
/// read before writing can each acquire a read lock and then contend over the
/// upgrade to a write lock, wasting retries under `busy_timeout` instead of one
/// of them cleanly waiting up front. At that point, switching this transaction
/// to `IMMEDIATE` (`conn.transaction_with_behavior(TransactionBehavior::Immediate)`)
/// would be the fix.
pub fn apply_backup_batch(conn: &mut Connection, batch: &[FileBackupRecord]) -> Result<(), Error> {
    let tx = conn.transaction()?;

    for record in batch {
        let content_id = match &record.content {
            ContentSource::Resolved {
                chunks,
                content_hash,
            } => resolve_content(&tx, chunks, content_hash)?,
            ContentSource::Known(content_id) => *content_id,
        };

        match find_tree_entry(&tx, record.parent_id, &record.name)? {
            None => {
                tx.execute(
                    "INSERT INTO tree_entries (parent_id, name, time, kind, content_id)
                     VALUES (?1, ?2, ?3, 'file', ?4)",
                    params![
                        record.parent_id,
                        record.name,
                        record.time_millis,
                        content_id
                    ],
                )?;
            }
            Some(existing) if existing.kind != EntryKind::File => {
                return Err(Error::NotAFile {
                    parent_id: record.parent_id,
                    name: record.name.clone(),
                });
            }
            Some(existing) if existing.content_id == content_id => {
                tx.execute(
                    "UPDATE tree_entries SET time = ?1 WHERE id = ?2",
                    params![record.time_millis, existing.id],
                )?;
            }
            Some(existing) => {
                tx.execute(
                    "UPDATE tree_entries SET deleted_at = ?1 WHERE id = ?2",
                    params![record.time_millis, existing.id],
                )?;
                tx.execute(
                    "INSERT INTO tree_entries (parent_id, name, time, kind, content_id)
                     VALUES (?1, ?2, ?3, 'file', ?4)",
                    params![
                        record.parent_id,
                        record.name,
                        record.time_millis,
                        content_id
                    ],
                )?;
            }
        }
    }

    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Chunking, RepositorySettings};

    // See tree::tests::test_connection for why the TempDir must outlive the
    // connection (WAL sidecar files need the directory to still exist).
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

    fn one_chunk_record(name: &str, position: u64) -> FileBackupRecord {
        FileBackupRecord {
            parent_id: 0,
            name: name.to_string(),
            time_millis: 1000,
            content: ContentSource::Resolved {
                chunks: vec![ChunkRef::New {
                    length: 5,
                    hash: b"hash1".to_vec(),
                    extents: vec![(position, position + 5)],
                }],
                content_hash: b"content-hash-1".to_vec(),
            },
        }
    }

    #[test]
    fn applies_a_new_file_with_a_new_chunk() {
        let (_temp_dir, mut conn) = test_connection();

        apply_backup_batch(&mut conn, &[one_chunk_record("a.txt", 0)]).unwrap();

        let entry = find_tree_entry(&conn, 0, "a.txt").unwrap().unwrap();
        assert_eq!(entry.kind, EntryKind::File);
        let content_id = entry.content_id.unwrap();
        let (length, ref_count): (i64, i64) = conn
            .query_row(
                "SELECT length, ref_count FROM contents WHERE id = ?1",
                [content_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(length, 5);
        assert_eq!(ref_count, 1);
    }

    #[test]
    fn a_new_chunk_records_all_of_its_extents() {
        let (_temp_dir, mut conn) = test_connection();
        let record = FileBackupRecord {
            parent_id: 0,
            name: "a.txt".to_string(),
            time_millis: 1000,
            content: ContentSource::Resolved {
                chunks: vec![ChunkRef::New {
                    length: 8,
                    hash: b"hash1".to_vec(),
                    extents: vec![(10, 15), (100, 103)],
                }],
                content_hash: b"content-hash-1".to_vec(),
            },
        };

        apply_backup_batch(&mut conn, &[record]).unwrap();

        let chunk_id: i64 = conn
            .query_row(
                "SELECT id FROM chunks WHERE hash = x'6861736831'",
                (),
                |row| row.get(0),
            )
            .unwrap();
        let extents: Vec<(i64, i64, i64)> = conn
            .prepare("SELECT seq, start, stop FROM chunk_extents WHERE chunk_id = ?1 ORDER BY seq")
            .unwrap()
            .query_map([chunk_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(extents, vec![(0, 10, 15), (1, 100, 103)]);
    }

    #[test]
    fn empty_file_gets_no_content_row() {
        let (_temp_dir, mut conn) = test_connection();
        let record = FileBackupRecord {
            parent_id: 0,
            name: "empty.txt".to_string(),
            time_millis: 1000,
            content: ContentSource::Resolved {
                chunks: vec![],
                content_hash: vec![],
            },
        };

        apply_backup_batch(&mut conn, &[record]).unwrap();

        let entry = find_tree_entry(&conn, 0, "empty.txt").unwrap().unwrap();
        assert_eq!(entry.kind, EntryKind::File);
        assert_eq!(entry.content_id, None);
    }

    #[test]
    fn known_content_source_skips_resolve_content_and_reuses_the_id_as_is() {
        let (_temp_dir, mut conn) = test_connection();
        apply_backup_batch(&mut conn, &[one_chunk_record("a.txt", 0)]).unwrap();
        let a = find_tree_entry(&conn, 0, "a.txt").unwrap().unwrap();

        let record = FileBackupRecord {
            parent_id: 0,
            name: "b.txt".to_string(),
            time_millis: 2000,
            content: ContentSource::Known(a.content_id),
        };
        apply_backup_batch(&mut conn, &[record]).unwrap();

        let b = find_tree_entry(&conn, 0, "b.txt").unwrap().unwrap();
        assert_eq!(b.content_id, a.content_id);
        let ref_count: i64 = conn
            .query_row(
                "SELECT ref_count FROM contents WHERE id = ?1",
                [a.content_id.unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ref_count, 2, "both entries now reference the same content");
    }

    #[test]
    fn known_content_source_none_behaves_like_an_empty_file() {
        let (_temp_dir, mut conn) = test_connection();
        let record = FileBackupRecord {
            parent_id: 0,
            name: "empty.txt".to_string(),
            time_millis: 1000,
            content: ContentSource::Known(None),
        };

        apply_backup_batch(&mut conn, &[record]).unwrap();

        let entry = find_tree_entry(&conn, 0, "empty.txt").unwrap().unwrap();
        assert_eq!(entry.kind, EntryKind::File);
        assert_eq!(entry.content_id, None);
    }

    #[test]
    fn two_files_with_the_same_content_hash_share_one_contents_row() {
        let (_temp_dir, mut conn) = test_connection();

        apply_backup_batch(
            &mut conn,
            &[one_chunk_record("a.txt", 0), one_chunk_record("b.txt", 5)],
        )
        .unwrap();

        let a = find_tree_entry(&conn, 0, "a.txt").unwrap().unwrap();
        let b = find_tree_entry(&conn, 0, "b.txt").unwrap().unwrap();
        assert_eq!(a.content_id, b.content_id);
        let ref_count: i64 = conn
            .query_row(
                "SELECT ref_count FROM contents WHERE id = ?1",
                [a.content_id.unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ref_count, 2);
    }

    /// Simulates two workers racing on the same new chunk: both batches try to
    /// insert `(length, hash)` as new. The second must resolve to the first's
    /// row rather than erroring or creating a duplicate. The two files are given
    /// different content (a second, distinct chunk appended to one of them) so
    /// this test isolates chunk-level race resolution from content-level dedup
    /// (covered separately by `two_files_with_the_same_content_hash_...`).
    #[test]
    fn racing_batches_inserting_the_same_new_chunk_resolve_to_one_chunk_row() {
        let (_temp_dir, mut conn) = test_connection();

        apply_backup_batch(&mut conn, &[one_chunk_record("a.txt", 0)]).unwrap();
        // A second, independent worker hashed the same first chunk as part of a
        // different file, also decided it was new (its dedup lookup ran before
        // the first batch committed), and wrote its own (wasted, but harmless)
        // copy of the bytes elsewhere.
        let mut b = one_chunk_record("b.txt", 100);
        let ContentSource::Resolved {
            chunks,
            content_hash,
        } = &mut b.content
        else {
            unreachable!("one_chunk_record always builds a Resolved record")
        };
        chunks.push(ChunkRef::New {
            length: 9,
            hash: b"hash2".to_vec(),
            extents: vec![(105, 114)],
        });
        *content_hash = b"content-hash-2".to_vec();
        apply_backup_batch(&mut conn, &[b]).unwrap();

        let chunk_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM chunks", (), |row| row.get(0))
            .unwrap();
        assert_eq!(
            chunk_count, 2,
            "hash1 shared, hash2 new: two distinct chunks"
        );
        let content_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM contents", (), |row| row.get(0))
            .unwrap();
        assert_eq!(
            content_count, 2,
            "different content_hash: two distinct contents"
        );
        let hash1_ref_count: i64 = conn
            .query_row(
                "SELECT ref_count FROM chunks WHERE hash = ?1",
                [b"hash1".as_slice()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            hash1_ref_count, 2,
            "hash1 is referenced by both distinct contents"
        );
        let hash1_chunk_id: i64 = conn
            .query_row(
                "SELECT id FROM chunks WHERE hash = ?1",
                [b"hash1".as_slice()],
                |row| row.get(0),
            )
            .unwrap();
        let hash1_extents: Vec<(i64, i64)> = conn
            .prepare("SELECT start, stop FROM chunk_extents WHERE chunk_id = ?1")
            .unwrap()
            .query_map([hash1_chunk_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            hash1_extents,
            vec![(0, 5)],
            "the losing worker's own (100, 105) extent must be ignored, not appended"
        );
    }

    #[test]
    fn rerunning_with_unchanged_content_only_refreshes_time() {
        let (_temp_dir, mut conn) = test_connection();
        apply_backup_batch(&mut conn, &[one_chunk_record("a.txt", 0)]).unwrap();
        let first = find_tree_entry(&conn, 0, "a.txt").unwrap().unwrap();

        let mut second = one_chunk_record("a.txt", 0);
        second.time_millis = 2000;
        apply_backup_batch(&mut conn, &[second]).unwrap();

        let after = find_tree_entry(&conn, 0, "a.txt").unwrap().unwrap();
        assert_eq!(after.id, first.id, "same row must be reused, not replaced");
        assert_eq!(after.content_id, first.content_id);
        let time: i64 = conn
            .query_row(
                "SELECT time FROM tree_entries WHERE id = ?1",
                [after.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(time, 2000);
    }

    #[test]
    fn rerunning_with_changed_content_soft_deletes_the_old_entry_and_inserts_a_new_one() {
        let (_temp_dir, mut conn) = test_connection();
        apply_backup_batch(&mut conn, &[one_chunk_record("a.txt", 0)]).unwrap();
        let first = find_tree_entry(&conn, 0, "a.txt").unwrap().unwrap();

        let mut second = FileBackupRecord {
            parent_id: 0,
            name: "a.txt".to_string(),
            time_millis: 2000,
            content: ContentSource::Resolved {
                chunks: vec![ChunkRef::New {
                    length: 9,
                    hash: b"hash2".to_vec(),
                    extents: vec![(100, 109)],
                }],
                content_hash: b"content-hash-2".to_vec(),
            },
        };
        apply_backup_batch(&mut conn, std::slice::from_mut(&mut second)).unwrap();

        let after = find_tree_entry(&conn, 0, "a.txt").unwrap().unwrap();
        assert_ne!(
            after.id, first.id,
            "content changed: a new row must be created"
        );
        assert_ne!(after.content_id, first.content_id);

        let deleted_at: Option<i64> = conn
            .query_row(
                "SELECT deleted_at FROM tree_entries WHERE id = ?1",
                [first.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(deleted_at, Some(2000));
        // The old content is still referenced by the soft-deleted entry.
        let old_ref_count: i64 = conn
            .query_row(
                "SELECT ref_count FROM contents WHERE id = ?1",
                [first.content_id.unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(old_ref_count, 1);
    }

    #[test]
    fn inserting_a_file_where_a_directory_exists_errors() {
        let (_temp_dir, mut conn) = test_connection();
        crate::insert_directory(&conn, 0, "a.txt", 0).unwrap();

        let result = apply_backup_batch(&mut conn, &[one_chunk_record("a.txt", 0)]);

        assert!(matches!(result, Err(Error::NotAFile { parent_id: 0, name }) if name == "a.txt"));
    }
}
