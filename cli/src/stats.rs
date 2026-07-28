use std::path::Path;
use std::process::ExitCode;

use clap::Args;
use rusqlite::Connection;

use crate::format::{print_file_info, readable_bytes};

#[derive(Args)]
pub struct StatsArgs {
    /// Path within the repository to show statistics for. Defaults to the
    /// whole repository.
    path: Option<String>,
}

pub fn run_stats(repo: &Path, args: StatsArgs) -> ExitCode {
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

    match args.path {
        None => match repo_wide_stats(&conn) {
            Ok(stats) => {
                print_repo_stats(&stats);
                ExitCode::SUCCESS
            }
            Err(err) => {
                eprintln!("error: {err}");
                ExitCode::FAILURE
            }
        },
        Some(path) => path_stats(&conn, &path),
    }
}

struct RepoStats {
    live_files: i64,
    deleted_files: i64,
    live_dirs: i64,
    deleted_dirs: i64,
    chunks: i64,
    contents: i64,
    physical_bytes: i64,
    /// Sum of every live file's content length, counting content shared by
    /// several files once per referencing file - i.e. the total size as the
    /// user would see it without any deduplication.
    logical_bytes: i64,
}

fn repo_wide_stats(conn: &Connection) -> rusqlite::Result<RepoStats> {
    conn.query_row(
        "SELECT
            (SELECT COUNT(*) FROM tree_entries WHERE kind = 'file' AND deleted_at IS NULL),
            (SELECT COUNT(*) FROM tree_entries WHERE kind = 'file' AND deleted_at IS NOT NULL),
            (SELECT COUNT(*) FROM tree_entries WHERE kind = 'dir' AND deleted_at IS NULL),
            (SELECT COUNT(*) FROM tree_entries WHERE kind = 'dir' AND deleted_at IS NOT NULL),
            (SELECT COUNT(*) FROM chunks),
            (SELECT COUNT(*) FROM contents),
            (SELECT COALESCE(MAX(stop), 0) FROM chunks),
            (SELECT COALESCE(SUM(c.length), 0) FROM tree_entries t
                JOIN contents c ON c.id = t.content_id WHERE t.deleted_at IS NULL)",
        (),
        |row| {
            Ok(RepoStats {
                live_files: row.get(0)?,
                deleted_files: row.get(1)?,
                live_dirs: row.get(2)?,
                deleted_dirs: row.get(3)?,
                chunks: row.get(4)?,
                contents: row.get(5)?,
                physical_bytes: row.get(6)?,
                logical_bytes: row.get(7)?,
            })
        },
    )
}

fn print_repo_stats(stats: &RepoStats) {
    println!("Repository statistics");
    println!(
        "Files: {} (deleted: {})",
        stats.live_files, stats.deleted_files
    );
    println!(
        "Directories: {} (deleted: {})",
        stats.live_dirs, stats.deleted_dirs
    );
    println!("Distinct chunks: {}", stats.chunks);
    println!("Distinct contents: {}", stats.contents);
    println!(
        "Physical storage: {}",
        readable_bytes(stats.physical_bytes as u64)
    );
    println!(
        "Logical size (undeduplicated): {}",
        readable_bytes(stats.logical_bytes as u64)
    );
    if stats.physical_bytes > 0 {
        let ratio = stats.logical_bytes as f64 / stats.physical_bytes as f64;
        println!("Dedup ratio: {ratio:.2}x");
    }
}

fn path_stats(conn: &Connection, path: &str) -> ExitCode {
    let entry = match db::resolve_path(conn, path) {
        Ok(Some(entry)) => entry,
        Ok(None) => {
            println!("The path '{path}' does not exist.");
            return ExitCode::FAILURE;
        }
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    };

    match entry.kind {
        db::EntryKind::File => match db::file_size(conn, &entry) {
            Ok(size) => {
                print_file_info(path, &entry.name, size as u64, entry.time_millis);
                ExitCode::SUCCESS
            }
            Err(err) => {
                eprintln!("error: {err}");
                ExitCode::FAILURE
            }
        },
        db::EntryKind::Dir => match db::subtree_stats(conn, entry.id) {
            Ok(stats) => {
                println!("Directory information for '{path}':");
                println!(
                    "Files: {}, directories: {}, total size: {}",
                    stats.files,
                    stats.dirs,
                    readable_bytes(stats.total_logical_bytes as u64)
                );
                ExitCode::SUCCESS
            }
            Err(err) => {
                eprintln!("error: {err}");
                ExitCode::FAILURE
            }
        },
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
    fn run_stats_with_no_path_succeeds_on_an_empty_repository() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_stats(&repo_root, StatsArgs { path: None }),
            ExitCode::SUCCESS
        );
    }

    #[test]
    fn run_stats_fails_for_a_missing_path() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(
            run_stats(
                &repo_root,
                StatsArgs {
                    path: Some("missing".to_string())
                }
            ),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn run_stats_succeeds_for_a_file_and_a_directory_path() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        let dir_id = db::insert_directory(&conn, 0, "sub", 0).unwrap();
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (?1, 'a.txt', 0, 'file')",
            [dir_id],
        )
        .unwrap();
        drop(conn);

        assert_eq!(
            run_stats(
                &repo_root,
                StatsArgs {
                    path: Some("sub".to_string())
                }
            ),
            ExitCode::SUCCESS
        );
        assert_eq!(
            run_stats(
                &repo_root,
                StatsArgs {
                    path: Some("sub/a.txt".to_string())
                }
            ),
            ExitCode::SUCCESS
        );
    }

    #[test]
    fn repo_wide_stats_computes_counts_and_dedup_ratio() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let mut conn = repository.open_write_connection().unwrap();
        let record = |name: &str| db::FileBackupRecord {
            parent_id: 0,
            name: name.to_string(),
            time_millis: 0,
            chunks: vec![db::ChunkRef::New {
                length: 10,
                hash: b"h".to_vec(),
                position: 0,
            }],
            content_hash: b"c".to_vec(),
        };
        db::apply_backup_batch(&mut conn, &[record("a.txt"), record("b.txt")]).unwrap();

        let stats = repo_wide_stats(&conn).unwrap();
        assert_eq!(stats.live_files, 2);
        assert_eq!(stats.deleted_files, 0);
        assert_eq!(stats.chunks, 1, "both files share one chunk");
        assert_eq!(stats.contents, 1, "both files share one content");
        assert_eq!(stats.physical_bytes, 10);
        assert_eq!(stats.logical_bytes, 20, "counted once per referencing file");
    }
}
