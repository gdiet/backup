use std::path::Path;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Args;
use store::LongTermStore;

use crate::problems::find_problem_files;

#[derive(Args)]
pub struct FixProblemsArgs {
    /// Path within the repository to check. Defaults to the whole repository.
    path: Option<String>,

    /// After soft-deleting a problem file, also insert a fresh 0-byte file
    /// at the same path, keeping the original's last-modified time - so a
    /// listing still shows something there instead of a hole. Without this,
    /// the path is simply gone (until/unless recovered via `undelete`).
    #[arg(long)]
    replace_empty: bool,
}

/// Soft-deletes every file `problems` currently finds under `path` (or the
/// whole repository) - i.e. every active file with at least one chunk whose
/// store data is missing or shorter than recorded. With `--replace-empty`,
/// also inserts a fresh, empty file at the same path, so the tree keeps a
/// (harmless, 0-byte) placeholder there rather than a gap - its
/// last-modified time is copied from the file it replaces, not set to "now".
///
/// Detection is re-run fresh, not read back from a prior `problems`
/// invocation - there's no id to go stale between listing and fixing here,
/// unlike `undelete`, since this always acts on whatever is currently
/// broken.
pub fn run_fix_problems(repo: &Path, args: FixProblemsArgs) -> ExitCode {
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
    let mut conn = match repository.open_write_connection() {
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

    let now = now_millis();
    for problem in &problems {
        if args.replace_empty {
            let parent_id = match db::parent_id(&conn, problem.entry.id) {
                Ok(Some(id)) => id,
                Ok(None) | Err(_) => {
                    eprintln!("error: failed to look up the parent of '{}'", problem.path);
                    return ExitCode::FAILURE;
                }
            };
            if let Err(err) = db::soft_delete_and_replace_with_empty(
                &mut conn,
                problem.entry.id,
                now,
                parent_id,
                &problem.entry.name,
                problem.entry.time_millis,
            ) {
                eprintln!(
                    "error: failed to soft-delete and replace '{}': {err}",
                    problem.path
                );
                return ExitCode::FAILURE;
            }
        } else if let Err(err) = db::soft_delete(&conn, problem.entry.id, now) {
            eprintln!("error: failed to soft-delete '{}': {err}", problem.path);
            return ExitCode::FAILURE;
        }
        println!("fixed: {}", problem.path);
    }

    println!("{} file(s) fixed.", problems.len());
    ExitCode::SUCCESS
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
        .as_millis() as i64
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

    fn seed_file(repo_root: &Path, name: &str, bytes: &[u8], time_millis: i64) {
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
                time_millis,
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

    fn break_data_file(repo_root: &Path) {
        std::fs::remove_file(
            repo_root
                .join("data")
                .join("00")
                .join("00")
                .join("0000000000"),
        )
        .unwrap();
    }

    #[test]
    fn run_fix_problems_succeeds_when_nothing_is_broken() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_fix_problems(
                &repo_root,
                FixProblemsArgs {
                    path: None,
                    replace_empty: false
                }
            ),
            ExitCode::SUCCESS
        );
    }

    #[test]
    fn run_fix_problems_soft_deletes_without_replace_empty() {
        let (_temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, "a.txt", b"hello world", 1_000);
        break_data_file(&repo_root);

        let exit = run_fix_problems(
            &repo_root,
            FixProblemsArgs {
                path: None,
                replace_empty: false,
            },
        );
        assert_eq!(exit, ExitCode::SUCCESS);

        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        assert_eq!(db::resolve_path(&conn, "a.txt").unwrap(), None);
    }

    #[test]
    fn run_fix_problems_with_replace_empty_leaves_a_zero_byte_file_with_the_same_mtime() {
        let (_temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, "a.txt", b"hello world", 1_704_067_200_000);
        break_data_file(&repo_root);

        let exit = run_fix_problems(
            &repo_root,
            FixProblemsArgs {
                path: None,
                replace_empty: true,
            },
        );
        assert_eq!(exit, ExitCode::SUCCESS);

        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        let replacement = db::resolve_path(&conn, "a.txt").unwrap().unwrap();
        assert_eq!(replacement.content_id, None);
        assert_eq!(replacement.time_millis, 1_704_067_200_000);
        assert_eq!(db::file_size(&conn, &replacement).unwrap(), 0);
    }

    #[test]
    fn run_fix_problems_fails_for_a_missing_path() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_fix_problems(
                &repo_root,
                FixProblemsArgs {
                    path: Some("missing".to_string()),
                    replace_empty: false
                }
            ),
            ExitCode::FAILURE
        );
    }
}
