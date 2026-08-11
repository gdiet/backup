use std::path::Path;
use std::process::ExitCode;

use clap::Args;
use rusqlite::Connection;

use crate::format::{format_timestamp_millis, readable_bytes};

#[derive(Args)]
pub struct DeletedArgs {
    /// Only list deleted entries under this (currently active) directory.
    /// Omit to search the whole repository. An already-deleted directory
    /// can't be used here (its own path no longer resolves) - list from a
    /// live ancestor, or omit `path` entirely, to find it.
    path: Option<String>,
}

/// Lists soft-deleted entries - recoverable via `backup undelete` until a
/// future `reclaim-space` run actually purges them. Every deleted row is
/// listed independently, not just the most recent one per path: a path
/// created, deleted, re-created, and deleted again has two rows here, each
/// with its own `id` - `[id]` is what `backup undelete` takes to say which
/// one.
pub fn run_deleted(repo: &Path, args: DeletedArgs) -> ExitCode {
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

    let root_id = match &args.path {
        None => 0,
        Some(path) => match db::resolve_path(&conn, path) {
            Ok(Some(entry)) => entry.id,
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

    match db::deleted_entries(&conn, root_id) {
        Ok(entries) => {
            if entries.is_empty() {
                println!("No deleted entries found.");
            }
            for entry in &entries {
                print_entry(&conn, entry);
            }
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn print_entry(conn: &Connection, entry: &db::DeletedEntry) {
    let deleted = format_timestamp_millis(entry.deleted_at_millis);
    match entry.entry.kind {
        db::EntryKind::Dir => println!("[{}] > {}  deleted {deleted}", entry.entry.id, entry.path),
        db::EntryKind::File => {
            let size = db::file_size(conn, &entry.entry).unwrap_or(0);
            println!(
                "[{}] - {}  {}  deleted {deleted}",
                entry.entry.id,
                entry.path,
                readable_bytes(size as u64)
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
    fn fails_for_a_missing_scope_path() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_deleted(
                &repo_root,
                DeletedArgs {
                    path: Some("missing".to_string())
                }
            ),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn succeeds_with_no_deleted_entries() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_deleted(&repo_root, DeletedArgs { path: None }),
            ExitCode::SUCCESS
        );
    }

    #[test]
    fn lists_a_deleted_entry_scoped_and_unscoped() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        let sub_id = db::insert_directory(&conn, 0, "sub", 0).unwrap();
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind, deleted_at) VALUES (?1, 'a.txt', 0, 'file', 1000)",
            [sub_id],
        )
        .unwrap();
        drop(conn);

        assert_eq!(
            run_deleted(
                &repo_root,
                DeletedArgs {
                    path: Some("sub".to_string())
                }
            ),
            ExitCode::SUCCESS
        );
        assert_eq!(
            run_deleted(&repo_root, DeletedArgs { path: None }),
            ExitCode::SUCCESS
        );
    }
}
