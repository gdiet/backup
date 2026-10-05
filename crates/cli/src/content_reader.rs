//! Resolves a content's logical byte range against `crates/store`, via `crates/db`'s physical
//! layout (DESIGN-MOUNT-012 in `docs/design/mount-write-path.md`) - the read path an ordinary
//! read-only open needs, and the same one `crate::write_cache`'s original-content fallback
//! reuses.

use std::sync::Arc;

use mountfs::Errno;
use store::ReadIntegrity;

use crate::zero_fill_report::{ReadOrigin, ZeroFillReport};

/// What a read does when the store reports missing or short backing data
/// (REQ-MOUNT-005, DESIGN-MOUNT-027 in `docs/design/mount-write-path.md`).
#[derive(Clone)]
pub enum MissingDataPolicy {
    /// The read fails with `EIO`.
    Fail,
    /// The affected range reads as zero-value bytes, and `report` is told.
    ZeroFill {
        report: Arc<ZeroFillReport>,
        origin: ReadOrigin,
    },
}

impl MissingDataPolicy {
    /// `Fail` without a report, `ZeroFill` with one.
    pub fn for_report(report: Option<&Arc<ZeroFillReport>>, origin: ReadOrigin) -> Self {
        match report {
            Some(report) => Self::ZeroFill {
                report: Arc::clone(report),
                origin,
            },
            None => Self::Fail,
        }
    }
}

/// Reads up to `size` bytes of `content_id`'s logical content starting at `offset`. Returns fewer
/// bytes than `size` if `offset + size` reaches past the content's own length - an ordinary short
/// read, not an error. Fails visibly (`Errno::EIO`) rather than returning wrong bytes if any of
/// the backing store data is missing or short (REQ-MOUNT-005's fail-visibly default), unless
/// `policy` opts into zero-value bytes for that range instead. That covers every failure to read
/// stored data, whether the store reports it as missing or short or as an I/O error. A failure to
/// resolve the content's layout in the metadata is not stored data and still fails.
pub fn read_content(
    repo: &db::Repository,
    store: &store::ByteStore,
    content_id: i64,
    offset: u64,
    size: u32,
    policy: &MissingDataPolicy,
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
            Ok(ReadIntegrity::Incomplete { missing_or_short }) => match policy {
                MissingDataPolicy::Fail => return Err(Errno::EIO),
                // The store already left zero-value bytes in `buf` for the missing part.
                MissingDataPolicy::ZeroFill { report, origin } => {
                    report.note(*origin, &missing_or_short)
                }
            },
            Err(error) => match policy {
                MissingDataPolicy::Fail => return Err(Errno::EIO),
                // `buf` holds whatever was read before the failure, and zero-value bytes after.
                MissingDataPolicy::ZeroFill { report, origin } => {
                    report.note_read_error(*origin, &error)
                }
            },
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
            db::RepositorySettings::new(20, 1_700_000_000_000),
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

        let data = read_content(&repo, &store, content_id, 6, 5, &MissingDataPolicy::Fail).unwrap();
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

        let data =
            read_content(&repo, &store, content_id, 0, 11, &MissingDataPolicy::Fail).unwrap();
        assert_eq!(data, b"hello world");

        // Straddling the boundary exactly (last 2 bytes of chunk a, first 3 of chunk b).
        let data = read_content(&repo, &store, content_id, 3, 5, &MissingDataPolicy::Fail).unwrap();
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

        let data =
            read_content(&repo, &store, content_id, 3, 100, &MissingDataPolicy::Fail).unwrap();
        assert_eq!(data, b"lo");
    }

    #[test]
    fn reading_a_zero_length_content_returns_empty() {
        let (repo, _repo_dir, store, _store_dir) = setup();
        let content_id = repo
            .find_or_create_content(0, b"AAAAAAAAAAAAAAAAAAAA", &[])
            .unwrap();

        let data =
            read_content(&repo, &store, content_id, 0, 10, &MissingDataPolicy::Fail).unwrap();
        assert_eq!(data, Vec::<u8>::new());
    }

    fn zero_fill() -> (MissingDataPolicy, Arc<ZeroFillReport>) {
        let report = Arc::new(ZeroFillReport::with_output(Box::new(std::io::sink())));
        (
            MissingDataPolicy::for_report(Some(&report), ReadOrigin::Visible),
            report,
        )
    }

    #[test]
    fn zero_fill_policy_returns_zero_value_bytes_for_never_written_store_data() {
        let (repo, _repo_dir, store, _store_dir) = setup();
        let (chunk_id, _ranges) = repo
            .reserve_and_insert_chunk(5, b"aaaaaaaaaaaaaaaaaaaa")
            .unwrap();
        let content_id = repo
            .find_or_create_content(5, b"BBBBBBBBBBBBBBBBBBBB", &[chunk_id])
            .unwrap();
        let (policy, report) = zero_fill();

        let data = read_content(&repo, &store, content_id, 0, 5, &policy).unwrap();

        assert_eq!(data, vec![0u8; 5]);
        assert!(report.summary().unwrap().starts_with("1 read(s)"));
    }

    #[test]
    fn zero_fill_policy_keeps_the_real_bytes_and_zero_fills_only_the_short_part() {
        let (repo, _repo_dir, store, _store_dir) = setup();
        let (chunk_a, ranges_a) = repo
            .reserve_and_insert_chunk(5, b"aaaaaaaaaaaaaaaaaaaa")
            .unwrap();
        store.write(ranges_a[0].0, b"hello").unwrap();
        // Reserved right after the first chunk in the same data file, but never written, so the
        // file is too short for it.
        let (chunk_b, _ranges_b) = repo
            .reserve_and_insert_chunk(5, b"bbbbbbbbbbbbbbbbbbbb")
            .unwrap();
        let content_id = repo
            .find_or_create_content(10, b"CCCCCCCCCCCCCCCCCCCC", &[chunk_a, chunk_b])
            .unwrap();
        let (policy, _report) = zero_fill();

        let data = read_content(&repo, &store, content_id, 0, 10, &policy).unwrap();

        assert_eq!(data, b"hello\0\0\0\0\0");
    }

    /// A directory where the store expects a data file: opening it succeeds, reading from it
    /// fails with an I/O error that is not "not found".
    fn content_with_unreadable_store_data(
        repo: &db::Repository,
        store: &store::ByteStore,
        store_dir: &tempfile::TempDir,
    ) -> i64 {
        let (chunk_id, ranges) = repo
            .reserve_and_insert_chunk(11, b"01234567890123456789")
            .unwrap();
        store.write(ranges[0].0, b"hello world").unwrap();
        let data_file = store_dir.path().join("00/00/0000000000");
        std::fs::remove_file(&data_file).unwrap();
        std::fs::create_dir(&data_file).unwrap();
        repo.find_or_create_content(11, b"ABCDEFGHIJKLMNOPQRST", &[chunk_id])
            .unwrap()
    }

    #[test]
    fn an_unreadable_store_file_fails_by_default() {
        let (repo, _repo_dir, store, store_dir) = setup();
        let content_id = content_with_unreadable_store_data(&repo, &store, &store_dir);

        let result = read_content(&repo, &store, content_id, 0, 11, &MissingDataPolicy::Fail);

        assert_eq!(result, Err(Errno::EIO));
    }

    #[test]
    fn zero_fill_policy_treats_an_unreadable_store_file_as_missing_data() {
        let (repo, _repo_dir, store, store_dir) = setup();
        let content_id = content_with_unreadable_store_data(&repo, &store, &store_dir);
        let (policy, report) = zero_fill();

        let data = read_content(&repo, &store, content_id, 0, 11, &policy).unwrap();

        assert_eq!(data, vec![0u8; 11]);
        assert!(
            report
                .summary()
                .unwrap()
                .contains("kind(s) of read error treated as missing data")
        );
    }

    #[test]
    fn zero_fill_policy_leaves_complete_reads_alone_and_unreported() {
        let (repo, _repo_dir, store, _store_dir) = setup();
        let (chunk_id, ranges) = repo
            .reserve_and_insert_chunk(11, b"01234567890123456789")
            .unwrap();
        store.write(ranges[0].0, b"hello world").unwrap();
        let content_id = repo
            .find_or_create_content(11, b"ABCDEFGHIJKLMNOPQRST", &[chunk_id])
            .unwrap();
        let (policy, report) = zero_fill();

        let data = read_content(&repo, &store, content_id, 0, 11, &policy).unwrap();

        assert_eq!(data, b"hello world");
        assert_eq!(report.summary(), None);
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

        let err =
            read_content(&repo, &store, content_id, 0, 5, &MissingDataPolicy::Fail).unwrap_err();
        assert_eq!(err, Errno::EIO);
    }
}
