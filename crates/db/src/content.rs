//! Deduplication-index bookkeeping - `contents`/`chunks`/`content_chunks` (REQ-STORAGE-001/002 in
//! `requirements/functional/storage.md`): find-or-create lookups by `(length, hash)`, and
//! recording a newly-resolved chunk's reserved byte range(s). Byte positions for a new chunk come
//! from [`crate::allocation`]; this module never touches `crates/store` itself, only records where
//! a chunk's bytes belong (DESIGN-STORE-002/003 in `docs/design/byte-store.md`) - actually writing
//! them there is the caller's job, using the positions returned here.
//!
//! `pub(crate)` only, reached exclusively through [`crate::Repository`] (DESIGN-METADATA-006).

use rusqlite::{Connection, OptionalExtension, params};

use crate::Error;
use crate::allocation;

/// Looks up an existing content by its whole-content `(length, hash)` - DESIGN-METADATA-007's
/// hash-of-chunk-hashes, not a hash of the content's raw bytes.
pub(crate) fn find_content(
    conn: &Connection,
    length: i64,
    hash: &[u8],
) -> Result<Option<i64>, Error> {
    conn.query_row(
        "SELECT id FROM contents WHERE length = ?1 AND hash = ?2",
        params![length, hash],
        |row| row.get(0),
    )
    .optional()
    .map_err(Error::from)
}

/// Looks up an existing chunk by its own `(length, hash)`.
pub(crate) fn find_chunk(
    conn: &Connection,
    length: i64,
    hash: &[u8],
) -> Result<Option<i64>, Error> {
    conn.query_row(
        "SELECT id FROM chunks WHERE length = ?1 AND hash = ?2",
        params![length, hash],
        |row| row.get(0),
    )
    .optional()
    .map_err(Error::from)
}

/// Reserves storage for a chunk not already known (caller already checked [`find_chunk`] returned
/// `None`) and records it: a fresh `chunks` row plus the `chunk_extents` row(s) covering the
/// reserved byte range(s). Returns the new chunk id and the exact `(start, stop)` ranges to write
/// `length` bytes into through `crates/store`, in order.
///
/// Recording the reservation before the caller has actually written the bytes through `store` is
/// safe: the new chunk's `ref_count` stays `0` (nothing links it into any content yet via
/// `content_chunks`) until [`find_or_create_content`] does so, and nothing else can reserve the
/// same range in the meantime - the only operation that could (REQ-STORAGE-004's reclaim) is
/// itself a mutating operation and so cannot run at the same time as this one
/// (DESIGN-MOUNT-008 in `docs/design/mount-write-path.md`).
pub(crate) fn reserve_and_insert_chunk(
    conn: &Connection,
    length: i64,
    hash: &[u8],
) -> Result<(i64, Vec<(u64, u64)>), Error> {
    let ranges = allocation::reserve(conn, length as u64)?;
    conn.execute(
        "INSERT INTO chunks (length, hash) VALUES (?1, ?2)",
        params![length, hash],
    )?;
    let chunk_id = conn.last_insert_rowid();
    for (seq, &(start, stop)) in ranges.iter().enumerate() {
        conn.execute(
            "INSERT INTO chunk_extents (chunk_id, seq, start, stop) VALUES (?1, ?2, ?3, ?4)",
            params![chunk_id, seq as i64, start as i64, stop as i64],
        )?;
    }
    Ok((chunk_id, ranges))
}

/// Returns `content_id`'s complete physical layout: every `chunk_extents` `(start, stop)` range
/// backing it, in logical order - first by the content's own chunk sequence
/// (`content_chunks.seq`), then by each chunk's own extent sequence
/// (`chunk_extents.seq`, for a chunk split across more than one reserved range). Concatenating the
/// bytes at these ranges, in this order, reproduces the content's own bytes exactly; their total
/// length equals the content's own `length`. An unknown `content_id` returns an empty `Vec`, the
/// same as a content genuinely made of zero chunks (a zero-length content) - callers that need to
/// tell the two apart already have the content's own row (e.g. via [`find_content`]).
pub(crate) fn resolve_extents(
    conn: &Connection,
    content_id: i64,
) -> Result<Vec<(u64, u64)>, Error> {
    let mut stmt = conn.prepare(
        "SELECT ce.start, ce.stop \
         FROM content_chunks cc \
         JOIN chunk_extents ce ON ce.chunk_id = cc.chunk_id \
         WHERE cc.content_id = ?1 \
         ORDER BY cc.seq, ce.seq",
    )?;
    stmt.query_map([content_id], |row| {
        let start: i64 = row.get(0)?;
        let stop: i64 = row.get(1)?;
        Ok((start as u64, stop as u64))
    })?
    .collect::<Result<Vec<_>, _>>()
    .map_err(Error::from)
}

/// One occurrence of a chunk within a content's own chunk sequence - its recorded `(length,
/// hash)`, the chunk's own primary key in `chunks`, plus every `chunk_extents` range backing it
/// (more than one if the chunk is split across separate reserved ranges).
/// Concatenating the bytes at `extents`, in order, reproduces exactly `length` bytes - the same
/// bytes `hash` (BLAKE3 of the chunk's own raw bytes, truncated the same way `chunks.hash` is)
/// was computed over when the chunk was first written.
pub struct ChunkLocation {
    pub hash: Vec<u8>,
    pub length: u64,
    pub extents: Vec<(u64, u64)>,
}

/// Like [`resolve_extents`], but grouped back into the individual chunk occurrences that make up
/// `content_id`'s own chunk sequence, each carrying its own recorded hash - what a caller
/// verifying restored bytes against their recorded hash needs that a flat extent list alone does
/// not provide. The same chunk can appear more than once (a content repeating identical chunk
/// content internally); each occurrence is its own [`ChunkLocation`], not merged with the others,
/// since `content_chunks.seq` - not chunk identity - is what this groups by.
pub(crate) fn resolve_chunks(
    conn: &Connection,
    content_id: i64,
) -> Result<Vec<ChunkLocation>, Error> {
    let mut stmt = conn.prepare(
        "SELECT cc.seq, c.hash, c.length, ce.start, ce.stop \
         FROM content_chunks cc \
         JOIN chunks c ON c.id = cc.chunk_id \
         JOIN chunk_extents ce ON ce.chunk_id = cc.chunk_id \
         WHERE cc.content_id = ?1 \
         ORDER BY cc.seq, ce.seq",
    )?;
    let rows: Vec<(i64, Vec<u8>, i64, i64, i64)> = stmt
        .query_map([content_id], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?
        .collect::<Result<_, _>>()?;

    let mut chunks: Vec<ChunkLocation> = Vec::new();
    let mut current_seq: Option<i64> = None;
    for (seq, hash, length, start, stop) in rows {
        if current_seq != Some(seq) {
            chunks.push(ChunkLocation {
                hash,
                length: length as u64,
                extents: Vec::new(),
            });
            current_seq = Some(seq);
        }
        chunks
            .last_mut()
            .expect("just pushed a fresh ChunkLocation for this seq, above")
            .extents
            .push((start as u64, stop as u64));
    }
    Ok(chunks)
}

/// Finds or creates the `contents` row for the whole-content `(length, hash)`, linking
/// `chunk_ids` (in order) via `content_chunks` if it did not already exist - an empty `chunk_ids`
/// is valid (a zero-length content). Returns the content id.
pub(crate) fn find_or_create_content(
    conn: &Connection,
    length: i64,
    hash: &[u8],
    chunk_ids: &[i64],
) -> Result<i64, Error> {
    if let Some(id) = find_content(conn, length, hash)? {
        return Ok(id);
    }
    conn.execute(
        "INSERT INTO contents (length, hash) VALUES (?1, ?2)",
        params![length, hash],
    )?;
    let content_id = conn.last_insert_rowid();
    for (seq, &chunk_id) in chunk_ids.iter().enumerate() {
        conn.execute(
            "INSERT INTO content_chunks (content_id, seq, chunk_id) VALUES (?1, ?2, ?3)",
            params![content_id, seq as i64, chunk_id],
        )?;
    }
    Ok(content_id)
}

#[cfg(test)]
mod tests {
    use crate::{RepositorySettings, init_repository, open_repository};

    const HASH_A: &[u8] = &[0xAAu8; 20];
    const HASH_B: &[u8] = &[0xBBu8; 20];
    const CONTENT_HASH: &[u8] = &[0xCCu8; 20];

    fn repo() -> (crate::Repository, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        let settings = RepositorySettings::new(Some(20), 1_700_000_000_000);
        init_repository(&repo_root, settings).expect("init must succeed");
        let repo = open_repository(&repo_root).expect("open must succeed");
        (repo, dir)
    }

    #[test]
    fn find_chunk_and_find_content_return_none_when_nothing_is_stored() {
        let (repo, _dir) = repo();
        let found = repo
            .with_connection(|conn, _cache| super::find_chunk(conn, 100, HASH_A))
            .unwrap();
        assert_eq!(found, None);
        let found = repo
            .with_connection(|conn, _cache| super::find_content(conn, 100, CONTENT_HASH))
            .unwrap();
        assert_eq!(found, None);
    }

    #[test]
    fn reserve_and_insert_chunk_creates_a_findable_chunk_with_extents_at_position_zero() {
        let (repo, _dir) = repo();
        let (chunk_id, ranges) = repo
            .with_connection(|conn, _cache| super::reserve_and_insert_chunk(conn, 100, HASH_A))
            .unwrap();
        assert_eq!(ranges, vec![(0, 100)]);

        let found = repo
            .with_connection(|conn, _cache| super::find_chunk(conn, 100, HASH_A))
            .unwrap();
        assert_eq!(found, Some(chunk_id));
    }

    #[test]
    fn reserve_and_insert_chunk_a_second_time_extends_past_the_first() {
        let (repo, _dir) = repo();
        repo.with_connection(|conn, _cache| super::reserve_and_insert_chunk(conn, 100, HASH_A))
            .unwrap();
        let (_id, ranges) = repo
            .with_connection(|conn, _cache| super::reserve_and_insert_chunk(conn, 50, HASH_B))
            .unwrap();
        assert_eq!(ranges, vec![(100, 150)]);
    }

    #[test]
    fn find_or_create_content_creates_a_new_content_linking_its_chunks_in_order() {
        let (repo, _dir) = repo();
        let (chunk_a, _) = repo
            .with_connection(|conn, _cache| super::reserve_and_insert_chunk(conn, 100, HASH_A))
            .unwrap();
        let (chunk_b, _) = repo
            .with_connection(|conn, _cache| super::reserve_and_insert_chunk(conn, 50, HASH_B))
            .unwrap();

        let content_id = repo
            .with_connection(|conn, _cache| {
                super::find_or_create_content(conn, 150, CONTENT_HASH, &[chunk_a, chunk_b])
            })
            .unwrap();

        let seq_and_chunk: Vec<(i64, i64)> = repo
            .with_connection(|conn, _cache| {
                let mut stmt = conn.prepare(
                    "SELECT seq, chunk_id FROM content_chunks WHERE content_id = ?1 ORDER BY seq",
                )?;
                let rows = stmt
                    .query_map([content_id], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .unwrap();
        assert_eq!(seq_and_chunk, vec![(0, chunk_a), (1, chunk_b)]);
    }

    #[test]
    fn resolve_extents_returns_ranges_in_content_chunk_order_not_insertion_order() {
        let (repo, _dir) = repo();
        let (chunk_a, ranges_a) = repo
            .with_connection(|conn, _cache| super::reserve_and_insert_chunk(conn, 100, HASH_A))
            .unwrap();
        let (chunk_b, ranges_b) = repo
            .with_connection(|conn, _cache| super::reserve_and_insert_chunk(conn, 50, HASH_B))
            .unwrap();

        // Content order is deliberately the reverse of chunk-creation order, so this only passes
        // if resolve_extents actually follows content_chunks.seq rather than chunk id/insertion
        // order.
        let content_id = repo
            .with_connection(|conn, _cache| {
                super::find_or_create_content(conn, 150, CONTENT_HASH, &[chunk_b, chunk_a])
            })
            .unwrap();

        let extents = repo
            .with_connection(|conn, _cache| super::resolve_extents(conn, content_id))
            .unwrap();
        let mut expected = ranges_b;
        expected.extend(ranges_a);
        assert_eq!(extents, expected);
    }

    #[test]
    fn resolve_extents_follows_a_chunk_split_across_multiple_ranges_in_its_own_order() {
        let (repo, _dir) = repo();
        // Force chunk_a to spill into two ranges by reserving and freeing a small gap first is
        // not available here (no reclaim yet) - instead exercise the seq ordering directly via
        // raw rows, since that is what resolve_extents must actually respect.
        repo.with_connection(|conn, _cache| {
            conn.execute(
                "INSERT INTO chunks (id, length, hash) VALUES (1, 30, X'0102030405060708090A0B0C0D0E0F1011121314')",
                (),
            )?;
            conn.execute(
                "INSERT INTO chunk_extents (chunk_id, seq, start, stop) VALUES (1, 1, 1000, 1010)",
                (),
            )?;
            conn.execute(
                "INSERT INTO chunk_extents (chunk_id, seq, start, stop) VALUES (1, 0, 0, 20)",
                (),
            )?;
            conn.execute(
                "INSERT INTO contents (id, length, hash) VALUES (1, 30, X'2122232425262728292A2B2C2D2E2F3031323334')",
                (),
            )?;
            conn.execute(
                "INSERT INTO content_chunks (content_id, seq, chunk_id) VALUES (1, 0, 1)",
                (),
            )?;
            Ok(())
        })
        .unwrap();

        let extents = repo
            .with_connection(|conn, _cache| super::resolve_extents(conn, 1))
            .unwrap();
        // seq 0 (0..20) before seq 1 (1000..1010), even though it was inserted second.
        assert_eq!(extents, vec![(0, 20), (1000, 1010)]);
    }

    #[test]
    fn resolve_extents_returns_empty_for_an_unknown_content_id() {
        let (repo, _dir) = repo();
        let extents = repo
            .with_connection(|conn, _cache| super::resolve_extents(conn, 999))
            .unwrap();
        assert_eq!(extents, Vec::<(u64, u64)>::new());
    }

    #[test]
    fn find_or_create_content_returns_the_same_id_for_an_already_known_content() {
        let (repo, _dir) = repo();
        let (chunk_a, _) = repo
            .with_connection(|conn, _cache| super::reserve_and_insert_chunk(conn, 100, HASH_A))
            .unwrap();

        let first = repo
            .with_connection(|conn, _cache| {
                super::find_or_create_content(conn, 100, CONTENT_HASH, &[chunk_a])
            })
            .unwrap();
        let second = repo
            .with_connection(|conn, _cache| {
                super::find_or_create_content(conn, 100, CONTENT_HASH, &[chunk_a])
            })
            .unwrap();
        assert_eq!(first, second);

        // Only one content_chunks link exists - the second call did not insert a duplicate.
        let link_count: i64 = repo
            .with_connection(|conn, _cache| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM content_chunks WHERE content_id = ?1",
                    [first],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(link_count, 1);
    }

    #[test]
    fn find_or_create_content_supports_a_zero_length_content_with_no_chunks() {
        let (repo, _dir) = repo();
        let content_id = repo
            .with_connection(|conn, _cache| {
                super::find_or_create_content(conn, 0, CONTENT_HASH, &[])
            })
            .unwrap();

        let found = repo
            .with_connection(|conn, _cache| super::find_content(conn, 0, CONTENT_HASH))
            .unwrap();
        assert_eq!(found, Some(content_id));
    }

    #[test]
    fn reserve_and_insert_chunk_leaves_ref_count_at_zero_until_linked() {
        let (repo, _dir) = repo();
        let (chunk_id, _) = repo
            .with_connection(|conn, _cache| super::reserve_and_insert_chunk(conn, 100, HASH_A))
            .unwrap();

        let ref_count: i64 = repo
            .with_connection(|conn, _cache| {
                Ok(conn.query_row(
                    "SELECT ref_count FROM chunks WHERE id = ?1",
                    [chunk_id],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(ref_count, 0);

        repo.with_connection(|conn, _cache| {
            super::find_or_create_content(conn, 100, CONTENT_HASH, &[chunk_id])
        })
        .unwrap();

        let ref_count: i64 = repo
            .with_connection(|conn, _cache| {
                Ok(conn.query_row(
                    "SELECT ref_count FROM chunks WHERE id = ?1",
                    [chunk_id],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(
            ref_count, 1,
            "content_chunks_ref_count_ins must have fired once linked"
        );
    }
}
