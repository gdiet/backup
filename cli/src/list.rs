use std::path::Path;
use std::process::ExitCode;

use clap::Args;

use crate::format::{format_timestamp_millis, print_file_info, readable_bytes};

#[derive(Args)]
pub struct ListArgs {
    /// Path within the repository to list.
    path: String,
}

pub fn run_list(repo: &Path, args: ListArgs) -> ExitCode {
    let repository = match db::open_repository_read_only(repo) {
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

    match entry.kind {
        db::EntryKind::File => match db::file_size(&conn, &entry) {
            Ok(size) => {
                print_file_info(&args.path, &entry.name, size as u64, entry.time_millis);
                ExitCode::SUCCESS
            }
            Err(err) => {
                eprintln!("error: {err}");
                ExitCode::FAILURE
            }
        },
        db::EntryKind::Dir => match db::list_children(&conn, entry.id) {
            Ok(children) => {
                println!("Listing of '{}':", args.path);
                for child in children {
                    print_entry(&conn, &child);
                }
                ExitCode::SUCCESS
            }
            Err(err) => {
                eprintln!("error: {err}");
                ExitCode::FAILURE
            }
        },
    }
}

fn print_entry(conn: &rusqlite::Connection, entry: &db::TreeEntryRow) {
    match entry.kind {
        db::EntryKind::Dir => println!("> {}", entry.name),
        db::EntryKind::File => {
            let size = db::file_size(conn, entry).unwrap_or(0);
            println!(
                "- {}  {}  {}",
                entry.name,
                readable_bytes(size as u64),
                format_timestamp_millis(entry.time_millis)
            );
        }
    }
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
    fn run_list_fails_for_a_missing_path() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_list(
                &repo_root,
                ListArgs {
                    path: "missing".to_string()
                }
            ),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn run_list_succeeds_for_a_directory_and_a_file() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        db::insert_directory(&conn, 0, "sub", 0).unwrap();
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (0, 'a.txt', 0, 'file')",
            (),
        )
        .unwrap();
        drop(conn);

        assert_eq!(
            run_list(
                &repo_root,
                ListArgs {
                    path: "".to_string()
                }
            ),
            ExitCode::SUCCESS
        );
        assert_eq!(
            run_list(
                &repo_root,
                ListArgs {
                    path: "a.txt".to_string()
                }
            ),
            ExitCode::SUCCESS
        );
    }

    #[test]
    fn list_children_are_sorted_dirs_first_then_alphabetical() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (0, 'b.txt', 0, 'file')",
            (),
        )
        .unwrap();
        db::insert_directory(&conn, 0, "a-dir", 0).unwrap();

        let children = db::list_children(&conn, 0).unwrap();
        let names: Vec<&str> = children.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["a-dir", "b.txt"]);
    }
}
