use std::collections::HashSet;
use std::path::Path;
use std::process::ExitCode;

use clap::Args;
use rusqlite::Connection;
use store::{LongTermStore, ReadIntegrity};

use crate::check::scoped_chunks;
use crate::chunk_store::read_chunk_bytes;
use crate::format::readable_bytes;
use crate::progress::Progress;

#[derive(Args)]
pub struct ProblemsArgs {
    /// Path within the repository to check. Defaults to the whole repository.
    path: Option<String>,
}

/// One active file affected by missing or short store data - see
/// [`find_problem_files`].
pub(crate) struct ProblemFile {
    pub path: String,
    pub entry: db::TreeEntryRow,
    /// How many of this file's chunks are affected, out of how many total -
    /// shown to give a sense of how much of the file is actually gone
    /// (`backup restore --deleted`, after `fix-problems`, would still fail
    /// on any file with `affected == total_chunks`, but might recover
    /// something for a partially-affected one - not that `fix-problems`
    /// makes that distinction itself, it treats every problem file the
    /// same).
    pub affected_chunks: usize,
    pub total_chunks: usize,
}

/// Verifies stored chunk data against the metadata database (see `check`'s
/// own doc comment for what "missing or short" means), lists every active
/// file affected, at file rather than chunk granularity.
pub fn run_problems(repo: &Path, args: ProblemsArgs) -> ExitCode {
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
    let data_store = LongTermStore::new(repository.data_dir(), true);

    let problems = match find_problem_files(&conn, &data_store, args.path.as_deref()) {
        Ok(Some(problems)) => problems,
        Ok(None) => {
            let path = args.path.as_deref().unwrap_or("");
            println!("The path '{path}' does not exist.");
            return ExitCode::FAILURE;
        }
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    };

    if problems.is_empty() {
        println!("OK - no files with missing or short store data found.");
        return ExitCode::SUCCESS;
    }

    for problem in &problems {
        let size = db::file_size(&conn, &problem.entry).unwrap_or(0);
        println!(
            "{}   {}   {}/{} chunk(s) affected",
            problem.path,
            readable_bytes(size as u64),
            problem.affected_chunks,
            problem.total_chunks
        );
    }
    println!("{} file(s) affected.", problems.len());
    ExitCode::FAILURE
}

/// Finds every active file affected by missing or short store data, scoped
/// to `path` (or the whole repository if `None`), or `None` if `path`
/// doesn't exist.
///
/// Reuses `check`'s exact chunk-scoping (`scoped_chunks`/`all_chunks`) and
/// per-chunk `ReadIntegrity` check, then walks from broken chunks back to
/// the active files they affect: broken chunk -> content(s) built from it
/// (`db::contents_for_chunk`) -> active file(s) referencing that content
/// (`db::entries_for_content`) -> that file's path (`db::path_of`). A
/// broken chunk found while scanning `path` may turn up a file *outside*
/// `path` this way, if that file shares the same (broken) content via
/// dedup - intentional: that file genuinely has the same problem.
pub(crate) fn find_problem_files(
    conn: &Connection,
    data_store: &LongTermStore,
    path: Option<&str>,
) -> Result<Option<Vec<ProblemFile>>, db::Error> {
    let chunks = match path {
        None => db::all_chunks(conn)?,
        Some(path) => match scoped_chunks(conn, path)? {
            Some(chunks) => chunks,
            None => return Ok(None),
        },
    };

    let mut broken_chunk_ids = HashSet::new();
    let total_bytes: u64 = chunks.iter().map(|c| c.length as u64).sum();
    let mut progress = Progress::new(total_bytes);
    for chunk in &chunks {
        let (_, integrity) =
            read_chunk_bytes(conn, data_store, chunk.chunk_id, chunk.length as u64)?;
        if let ReadIntegrity::Incomplete { .. } = integrity {
            broken_chunk_ids.insert(chunk.chunk_id);
        }
        progress.add(chunk.length as u64);
    }
    progress.finish();

    let mut broken_content_ids = HashSet::new();
    for &chunk_id in &broken_chunk_ids {
        for content_id in db::contents_for_chunk(conn, chunk_id)? {
            broken_content_ids.insert(content_id);
        }
    }

    let mut seen_entries = HashSet::new();
    let mut problems = Vec::new();
    for content_id in broken_content_ids {
        let content_chunks = db::ordered_content_chunks(conn, content_id)?;
        let total_chunks = content_chunks.len();
        let affected_chunks = content_chunks
            .iter()
            .filter(|c| broken_chunk_ids.contains(&c.chunk_id))
            .count();
        for entry in db::entries_for_content(conn, content_id)? {
            if !seen_entries.insert(entry.id) {
                continue;
            }
            let path = db::path_of(conn, entry.id)?
                .unwrap_or_else(|| format!("<unknown path, id {}>", entry.id));
            problems.push(ProblemFile {
                path,
                entry,
                affected_chunks,
                total_chunks,
            });
        }
    }
    problems.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(Some(problems))
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
    fn run_problems_fails_for_a_missing_path() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_problems(
                &repo_root,
                ProblemsArgs {
                    path: Some("missing".to_string())
                }
            ),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn run_problems_succeeds_when_nothing_is_missing() {
        let (_temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, "a.txt", b"hello world");
        assert_eq!(
            run_problems(&repo_root, ProblemsArgs { path: None }),
            ExitCode::SUCCESS
        );
    }

    #[test]
    fn run_problems_finds_a_file_whose_data_file_was_deleted() {
        let (temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, "a.txt", b"hello world");
        std::fs::remove_file(
            temp_dir
                .path()
                .join("repo")
                .join("data")
                .join("00")
                .join("00")
                .join("0000000000"),
        )
        .unwrap();

        assert_eq!(
            run_problems(&repo_root, ProblemsArgs { path: None }),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn find_problem_files_reports_two_files_sharing_the_same_broken_content() {
        let (temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, "a.txt", b"same bytes");
        // Second file with identical content shares the same content_id via
        // dedup - apply_backup_batch resolves it to the existing content.
        seed_file(&repo_root, "b.txt", b"same bytes");
        std::fs::remove_file(
            temp_dir
                .path()
                .join("repo")
                .join("data")
                .join("00")
                .join("00")
                .join("0000000000"),
        )
        .unwrap();

        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        let data_store = store::LongTermStore::new(repository.data_dir(), true);
        let mut problems = find_problem_files(&conn, &data_store, None)
            .unwrap()
            .unwrap();
        problems.sort_by(|a, b| a.path.cmp(&b.path));

        assert_eq!(problems.len(), 2);
        assert_eq!(problems[0].path, "a.txt");
        assert_eq!(problems[1].path, "b.txt");
        assert_eq!(problems[0].affected_chunks, 1);
        assert_eq!(problems[0].total_chunks, 1);
    }
}
