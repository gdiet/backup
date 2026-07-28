use std::path::Path;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Args;

#[derive(Args)]
pub struct DelArgs {
    /// Required to delete a directory (and everything under it), even an
    /// empty one - refusing without it is a deliberate safety net the Scala
    /// tool this replaces doesn't have (it recurses unconditionally, with no
    /// flag to opt in and only a README warning as a safety net). No short
    /// form: `-r` is already the global `--repo` flag.
    #[arg(long)]
    recursive: bool,

    /// Path within the repository to delete.
    path: String,
}

/// Soft-deletes a file or directory. Recoverable until a future
/// `reclaim-space` run actually purges it - itself already a meaningful
/// safety improvement over the Scala tool this replaces, which has no grace
/// period built in at all (a deleted entry is immediately eligible for
/// cleanup by its `reclaim-space` since it has no reference counting to tell
/// live and deleted entries apart other than the deletion timestamp filter).
pub fn run_del(repo: &Path, args: DelArgs) -> ExitCode {
    if args.path.trim_matches('/').is_empty() {
        eprintln!("error: refusing to delete the repository root");
        return ExitCode::FAILURE;
    }

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
    let conn = match repository.open_write_connection() {
        Ok(c) => c,
        Err(err) => {
            eprintln!("error: failed to open the metadata database: {err}");
            return ExitCode::FAILURE;
        }
    };

    let entry = match db::resolve_path(&conn, &args.path) {
        Ok(Some(entry)) => entry,
        Ok(None) => {
            println!("The path '{}' does not exist.", args.path);
            return ExitCode::FAILURE;
        }
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    };

    if entry.kind == db::EntryKind::Dir && !args.recursive {
        eprintln!(
            "error: '{}' is a directory; pass --recursive to delete it and everything under it",
            args.path
        );
        return ExitCode::FAILURE;
    }

    match db::soft_delete(&conn, entry.id, now_millis()) {
        Ok(count) => {
            let entries = if count == 1 { "entry" } else { "entries" };
            println!("Deleted {count} {entries}.");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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

    #[test]
    fn refuses_to_delete_the_root() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_del(
                &repo_root,
                DelArgs {
                    recursive: true,
                    path: "".to_string()
                }
            ),
            ExitCode::FAILURE
        );
        assert_eq!(
            run_del(
                &repo_root,
                DelArgs {
                    recursive: true,
                    path: "/".to_string()
                }
            ),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn fails_for_a_missing_path() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_del(
                &repo_root,
                DelArgs {
                    recursive: false,
                    path: "missing".to_string()
                }
            ),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn deletes_a_file_without_the_recursive_flag() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (0, 'a.txt', 0, 'file')",
            (),
        )
        .unwrap();
        drop(conn);

        assert_eq!(
            run_del(
                &repo_root,
                DelArgs {
                    recursive: false,
                    path: "a.txt".to_string()
                }
            ),
            ExitCode::SUCCESS
        );
        let conn = db::open_repository(&repo_root)
            .unwrap()
            .open_read_connection()
            .unwrap();
        assert_eq!(db::resolve_path(&conn, "a.txt").unwrap(), None);
    }

    #[test]
    fn refuses_a_directory_without_the_recursive_flag() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        db::insert_directory(&conn, 0, "sub", 0).unwrap();
        drop(conn);

        assert_eq!(
            run_del(
                &repo_root,
                DelArgs {
                    recursive: false,
                    path: "sub".to_string()
                }
            ),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn deletes_a_directory_with_the_recursive_flag() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        let sub_id = db::insert_directory(&conn, 0, "sub", 0).unwrap();
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (?1, 'a.txt', 0, 'file')",
            [sub_id],
        )
        .unwrap();
        drop(conn);

        assert_eq!(
            run_del(
                &repo_root,
                DelArgs {
                    recursive: true,
                    path: "sub".to_string()
                }
            ),
            ExitCode::SUCCESS
        );
        let conn = db::open_repository(&repo_root)
            .unwrap()
            .open_read_connection()
            .unwrap();
        assert_eq!(db::resolve_path(&conn, "sub").unwrap(), None);
        assert_eq!(db::resolve_path(&conn, "sub/a.txt").unwrap(), None);
    }
}
