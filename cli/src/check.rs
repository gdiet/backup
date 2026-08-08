use std::collections::HashSet;
use std::path::Path;
use std::process::ExitCode;

use clap::Args;
use rusqlite::Connection;
use store::{LongTermStore, ReadIntegrity};

use db::ChunkInfo;

use crate::chunk_store::read_chunk_bytes;

#[derive(Args)]
pub struct CheckArgs {
    /// Path within the repository to check. Defaults to the whole repository.
    path: Option<String>,
}

/// Verifies chunk data physically matches what's recorded in the metadata
/// database (missing/short store data, length mismatches, hash mismatches),
/// and - always for the whole repository, regardless of `path`, since this is
/// a cheap, repository-wide invariant rather than something that benefits
/// from scoping - that `ref_count` on `chunks`/`contents` matches a live count
/// of what actually references them.
///
/// Unlike the Scala tool this replaces (which can only check one named file,
/// refuses to check a directory at all, and always exits 0 regardless of
/// what it found), `path` is optional (omitted = whole repository), a
/// directory recurses through everything under it, and the exit code
/// reflects the result.
pub fn run_check(repo: &Path, args: CheckArgs) -> ExitCode {
    let repository = match db::open_repository(repo) {
        Ok(r) => r,
        Err(err) => {
            eprintln!(
                "error: failed to open repository at {}: {err}",
                repo.display()
            );
            return ExitCode::FAILURE;
        }
    };
    let conn = match repository.open_read_connection() {
        Ok(c) => c,
        Err(err) => {
            eprintln!("error: failed to open the metadata database: {err}");
            return ExitCode::FAILURE;
        }
    };

    let chunks = match &args.path {
        None => match db::all_chunks(&conn) {
            Ok(chunks) => chunks,
            Err(err) => {
                eprintln!("error: {err}");
                return ExitCode::FAILURE;
            }
        },
        Some(path) => match scoped_chunks(&conn, path) {
            Ok(Some(chunks)) => chunks,
            Ok(None) => {
                println!("The path '{path}' does not exist.");
                return ExitCode::FAILURE;
            }
            Err(err) => {
                eprintln!("error: {err}");
                return ExitCode::FAILURE;
            }
        },
    };

    let data_store = LongTermStore::new(repository.data_dir(), true);
    let mut problems = 0u64;
    println!("Checking {} chunk(s)...", chunks.len());
    for chunk in &chunks {
        problems += check_chunk(&conn, &data_store, chunk);
    }

    println!("Checking ref_count consistency...");
    match check_ref_counts(&conn) {
        Ok(n) => problems += n,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    }

    if problems == 0 {
        println!("OK - no problems found.");
        ExitCode::SUCCESS
    } else {
        println!("{problems} problem(s) found.");
        ExitCode::FAILURE
    }
}

/// Every distinct chunk reachable from the active files under `path`, or
/// `None` if `path` doesn't exist. A file's own chunks if `path` names a
/// file, or every active descendant file's chunks (deduplicated - a chunk or
/// a whole content may be shared by more than one file) if it names a
/// directory.
fn scoped_chunks(conn: &Connection, path: &str) -> Result<Option<Vec<ChunkInfo>>, db::Error> {
    let Some(entry) = db::resolve_path(conn, path)? else {
        return Ok(None);
    };

    let content_ids: Vec<i64> = match entry.kind {
        db::EntryKind::File => entry.content_id.into_iter().collect(),
        db::EntryKind::Dir => db::subtree_entries_with_paths(conn, entry.id)?
            .into_iter()
            .filter_map(|e| e.entry.content_id)
            .collect(),
    };

    let mut seen_content = HashSet::new();
    let mut seen_chunk = HashSet::new();
    let mut chunks = Vec::new();
    for content_id in content_ids {
        if !seen_content.insert(content_id) {
            continue;
        }
        for chunk in db::ordered_content_chunks(conn, content_id)? {
            if seen_chunk.insert(chunk.chunk_id) {
                chunks.push(chunk);
            }
        }
    }
    Ok(Some(chunks))
}

/// Checks one chunk's physical data against its recorded length and hash.
/// Returns `1` and prints a description if anything is wrong, `0` if it's fine.
fn check_chunk(conn: &Connection, data_store: &LongTermStore, chunk: &ChunkInfo) -> u64 {
    let extents = match db::chunk_extents(conn, chunk.chunk_id) {
        Ok(extents) => extents,
        Err(err) => {
            println!(
                "ERROR chunk {}: failed to look up its extents: {err}",
                chunk.chunk_id
            );
            return 1;
        }
    };
    let extents_len: i64 = extents.iter().map(|(start, stop)| stop - start).sum();
    if extents_len != chunk.length {
        println!(
            "BAD chunk {}: stored length {} does not match the sum of its extents ({})",
            chunk.chunk_id, chunk.length, extents_len
        );
        return 1;
    }

    let (buf, integrity) =
        match read_chunk_bytes(conn, data_store, chunk.chunk_id, chunk.length as u64) {
            Ok(result) => result,
            Err(err) => {
                println!(
                    "ERROR chunk {}: failed to read store data: {err}",
                    chunk.chunk_id
                );
                return 1;
            }
        };
    if let ReadIntegrity::Incomplete { missing_or_short } = integrity {
        println!(
            "MISSING chunk {}: data file(s) missing or shorter than expected: {}",
            chunk.chunk_id,
            missing_or_short.join(", ")
        );
        return 1;
    }

    let mut hash = [0u8; 20];
    blake3::Hasher::new()
        .update(&buf)
        .finalize_xof()
        .fill(&mut hash);
    if hash.as_slice() != chunk.hash.as_slice() {
        println!("BAD chunk {}: content hash does not match", chunk.chunk_id);
        return 1;
    }
    0
}

/// Compares `chunks.ref_count`/`contents.ref_count` against a live count of
/// what actually references them, in two aggregate queries rather than one
/// query per row - this should never find anything (the triggers documented
/// in `migrations.rs` keep these in sync), so it's a cheap sanity check
/// against a trigger bug or manual tampering rather than an expected source
/// of problems.
fn check_ref_counts(conn: &Connection) -> rusqlite::Result<u64> {
    let mut problems = 0u64;

    let mut stmt = conn.prepare(
        "SELECT ch.id, ch.ref_count, COUNT(cc.content_id)
         FROM chunks ch LEFT JOIN content_chunks cc ON cc.chunk_id = ch.id
         GROUP BY ch.id
         HAVING ch.ref_count != COUNT(cc.content_id)",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let (id, stored, actual): (i64, i64, i64) = (row.get(0)?, row.get(1)?, row.get(2)?);
        println!("BAD chunk {id}: ref_count is {stored}, should be {actual}");
        problems += 1;
    }
    drop(rows);
    drop(stmt);

    let mut stmt = conn.prepare(
        "SELECT c.id, c.ref_count, COUNT(t.id)
         FROM contents c LEFT JOIN tree_entries t ON t.content_id = c.id
         GROUP BY c.id
         HAVING c.ref_count != COUNT(t.id)",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let (id, stored, actual): (i64, i64, i64) = (row.get(0)?, row.get(1)?, row.get(2)?);
        println!("BAD content {id}: ref_count is {stored}, should be {actual}");
        problems += 1;
    }

    Ok(problems)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn init_repo() -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            &db::RepositorySettings::new(12, db::Chunking::Cdc).unwrap(),
        )
        .unwrap();
        (temp_dir, repo_root)
    }

    /// Writes `bytes` to the store and records a matching chunk/content/file
    /// in the metadata database - a minimal, self-contained stand-in for a
    /// real `store` run, so these tests don't need to depend on the `store`
    /// module's internals.
    fn seed_file(repo_root: &Path, name: &str, bytes: &[u8]) {
        let data_store = store::LongTermStore::new(repo_root.join("data"), false);
        data_store.write(0, bytes).unwrap();

        let mut hash = [0u8; 20];
        blake3::Hasher::new()
            .update(bytes)
            .finalize_xof()
            .fill(&mut hash);

        let repository = db::open_repository(repo_root).unwrap();
        let mut conn = repository.open_write_connection().unwrap();
        db::apply_backup_batch(
            &mut conn,
            &[db::FileBackupRecord {
                parent_id: 0,
                name: name.to_string(),
                time_millis: 0,
                content: db::ContentSource::Resolved {
                    chunks: vec![db::ChunkRef::New {
                        length: bytes.len() as u64,
                        hash: hash.to_vec(),
                        extents: vec![(0, bytes.len() as u64)],
                    }],
                    content_hash: b"content-hash".to_vec(),
                },
            }],
        )
        .unwrap();
    }

    #[test]
    fn run_check_fails_for_a_missing_path() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_check(
                &repo_root,
                CheckArgs {
                    path: Some("missing".to_string())
                }
            ),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn run_check_succeeds_on_an_empty_repository() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_check(&repo_root, CheckArgs { path: None }),
            ExitCode::SUCCESS
        );
    }

    #[test]
    fn run_check_passes_for_intact_data_and_fails_after_corruption() {
        let (_temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, "a.txt", b"hello world");

        assert_eq!(
            run_check(&repo_root, CheckArgs { path: None }),
            ExitCode::SUCCESS
        );

        // Corrupt the single stored chunk's bytes on disk directly, keeping
        // the same length so this specifically exercises the hash-mismatch
        // path rather than the length-mismatch or missing-data paths.
        let data_file = repo_root
            .join("data")
            .join("00")
            .join("00")
            .join("0000000000");
        std::fs::write(&data_file, b"corrupted!!").unwrap();

        assert_eq!(
            run_check(&repo_root, CheckArgs { path: None }),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn run_check_scoped_to_a_path_only_checks_reachable_chunks() {
        let (_temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, "a.txt", b"hello world");

        assert_eq!(
            run_check(
                &repo_root,
                CheckArgs {
                    path: Some("a.txt".to_string())
                }
            ),
            ExitCode::SUCCESS
        );
    }

    #[test]
    fn check_ref_counts_finds_nothing_wrong_by_default() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let mut conn = repository.open_write_connection().unwrap();
        db::apply_backup_batch(
            &mut conn,
            &[db::FileBackupRecord {
                parent_id: 0,
                name: "a.txt".to_string(),
                time_millis: 0,
                content: db::ContentSource::Resolved {
                    chunks: vec![db::ChunkRef::New {
                        length: 5,
                        hash: b"h".to_vec(),
                        extents: vec![(0, 5)],
                    }],
                    content_hash: b"c".to_vec(),
                },
            }],
        )
        .unwrap();

        assert_eq!(check_ref_counts(&conn).unwrap(), 0);
    }

    #[test]
    fn check_ref_counts_detects_a_tampered_ref_count() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        conn.execute(
            "INSERT INTO chunks (id, length, hash, ref_count) VALUES (1, 5, x'AA', 3)",
            (),
        )
        .unwrap();

        assert_eq!(check_ref_counts(&conn).unwrap(), 1);
    }
}
