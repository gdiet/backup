//! Resolves a content's logical byte range against `crates/store`, via `crates/db`'s physical
//! layout (DESIGN-MOUNT-012 in `docs/design/mount-write-path.md`) - the read path an ordinary
//! read-only open needs, and the same one `crate::write_cache`'s original-content fallback
//! reuses.

use mountfs::Errno;
use store::ReadIntegrity;

/// Reads up to `size` bytes of `content_id`'s logical content starting at `offset`. Returns fewer
/// bytes than `size` if `offset + size` reaches past the content's own length - an ordinary short
/// read, not an error. Fails visibly (`Errno::EIO`) rather than returning wrong bytes if any of
/// the backing store data is missing or short (REQ-MOUNT-005's fail-visibly default - no
/// best-effort opt-in exists yet).
pub fn read_content(
    repo: &db::Repository,
    store: &store::ByteStore,
    content_id: i64,
    offset: u64,
    size: u32,
) -> Result<Vec<u8>, Errno> {
    let extents = repo.resolve_extents(content_id).map_err(|_| Errno::EIO)?;

    let mut result = Vec::with_capacity(size as usize);
    let mut skip = offset;
    let mut remaining = u64::from(size);

    for (start, stop) in extents {
        if remaining == 0 {
            break;
        }
        let extent_len = stop - start;
        if skip >= extent_len {
            skip -= extent_len;
            continue;
        }
        let to_read = (extent_len - skip).min(remaining);
        let mut buf = vec![0u8; to_read as usize];
        match store.read(start + skip, &mut buf) {
            Ok(ReadIntegrity::Complete) => {}
            Ok(ReadIntegrity::Incomplete { .. }) | Err(_) => return Err(Errno::EIO),
        }
        result.extend_from_slice(&buf);
        skip = 0;
        remaining -= to_read;
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (
        db::Repository,
        tempfile::TempDir,
        store::ByteStore,
        tempfile::TempDir,
    ) {
        let repo_dir = tempfile::tempdir().unwrap();
        let repo_root = repo_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            db::RepositorySettings::new(Some(20), 1_700_000_000_000),
        )
        .unwrap();
        let repo = db::open_repository(&repo_root).unwrap();

        let store_dir = tempfile::tempdir().unwrap();
        let store = store::ByteStore::new(store_dir.path(), false);

        (repo, repo_dir, store, store_dir)
    }

    #[test]
    fn reads_a_single_chunk_content_at_an_offset() {
        let (repo, _repo_dir, store, _store_dir) = setup();
        let (chunk_id, ranges) = repo
            .reserve_and_insert_chunk(11, b"01234567890123456789")
            .unwrap();
        store.write(ranges[0].0, b"hello world").unwrap();
        let content_id = repo
            .find_or_create_content(11, b"ABCDEFGHIJKLMNOPQRST", &[chunk_id])
            .unwrap();

        let data = read_content(&repo, &store, content_id, 6, 5).unwrap();
        assert_eq!(data, b"world");
    }

    #[test]
    fn reads_across_a_chunk_boundary() {
        let (repo, _repo_dir, store, _store_dir) = setup();
        let (chunk_a, ranges_a) = repo
            .reserve_and_insert_chunk(5, b"aaaaaaaaaaaaaaaaaaaa")
            .unwrap();
        store.write(ranges_a[0].0, b"hello").unwrap();
        let (chunk_b, ranges_b) = repo
            .reserve_and_insert_chunk(6, b"bbbbbbbbbbbbbbbbbbbb")
            .unwrap();
        store.write(ranges_b[0].0, b" world").unwrap();
        let content_id = repo
            .find_or_create_content(11, b"CCCCCCCCCCCCCCCCCCCC", &[chunk_a, chunk_b])
            .unwrap();

        let data = read_content(&repo, &store, content_id, 0, 11).unwrap();
        assert_eq!(data, b"hello world");

        // Straddling the boundary exactly (last 2 bytes of chunk a, first 3 of chunk b).
        let data = read_content(&repo, &store, content_id, 3, 5).unwrap();
        assert_eq!(data, b"lo wo");
    }

    #[test]
    fn a_read_reaching_past_the_end_returns_a_short_result() {
        let (repo, _repo_dir, store, _store_dir) = setup();
        let (chunk_id, ranges) = repo
            .reserve_and_insert_chunk(5, b"aaaaaaaaaaaaaaaaaaaa")
            .unwrap();
        store.write(ranges[0].0, b"hello").unwrap();
        let content_id = repo
            .find_or_create_content(5, b"BBBBBBBBBBBBBBBBBBBB", &[chunk_id])
            .unwrap();

        let data = read_content(&repo, &store, content_id, 3, 100).unwrap();
        assert_eq!(data, b"lo");
    }

    #[test]
    fn reading_a_zero_length_content_returns_empty() {
        let (repo, _repo_dir, store, _store_dir) = setup();
        let content_id = repo
            .find_or_create_content(0, b"AAAAAAAAAAAAAAAAAAAA", &[])
            .unwrap();

        let data = read_content(&repo, &store, content_id, 0, 10).unwrap();
        assert_eq!(data, Vec::<u8>::new());
    }

    #[test]
    fn missing_backing_store_data_fails_visibly_instead_of_returning_wrong_bytes() {
        let (repo, _repo_dir, store, _store_dir) = setup();
        // Reserved and recorded in the metadata, but never actually written to the store.
        let (chunk_id, _ranges) = repo
            .reserve_and_insert_chunk(5, b"aaaaaaaaaaaaaaaaaaaa")
            .unwrap();
        let content_id = repo
            .find_or_create_content(5, b"BBBBBBBBBBBBBBBBBBBB", &[chunk_id])
            .unwrap();

        let err = read_content(&repo, &store, content_id, 0, 5).unwrap_err();
        assert_eq!(err, Errno::EIO);
    }
}
