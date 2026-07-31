//! Permanently excluding specific content from the dedup store while keeping
//! a record of it in the tree - e.g. installer caches, `Thumbs.db`, virus-
//! scanner quarantine files: content the user wants gone but doesn't want
//! silently reappearing on a later `store` run.
//!
//! Two independent subcommands, mirroring the Scala tool this replaces
//! (`dedup blacklist`) but split apart rather than always running both in one
//! shot, since either half is useful on its own:
//! - `blacklist add`: hashes and backs up an external directory's contents
//!   into the tree under a timestamped subdirectory of the repository's
//!   blacklist directory - see [`run_add`].
//! - `blacklist process`: soft-deletes every active tree entry currently
//!   under the blacklist directory (and optionally every other entry sharing
//!   its content) - see [`run_process`].
//!
//! This deliberately does not reproduce the Scala tool's "reads as zeros,
//! entry stays visible" semantic for processed blacklist entries (Scala's
//! `db.removeStorageAllocation`): that operation dedupes whole files, so
//! "remove this content's storage but keep the tree entry showing zeros" is
//! natural there. This project dedupes at the CDC chunk level, where a
//! blacklisted file's chunks may be shared with unrelated, non-blacklisted
//! files - "zero out this file's storage" has no clean chunk-level
//! equivalent, since a chunk still referenced elsewhere can't be zeroed
//! without corrupting whatever else references it. Instead, `blacklist
//! process` just runs the same [`db::soft_delete`] any other deletion in this
//! codebase already goes through (see `del.rs`), letting a later
//! `reclaim-space` run free any chunks that end up unreferenced - see
//! `docs/plans/implemented/blacklist.md` for the full reasoning.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Args, Subcommand};
use rusqlite::Connection;

use crate::format::timestamp_for_filename;
use crate::store;

#[derive(Args)]
pub struct BlacklistArgs {
    #[command(subcommand)]
    command: BlacklistCommand,
}

#[derive(Subcommand)]
enum BlacklistCommand {
    /// Back up an external directory's contents into the repository's
    /// blacklist directory.
    Add(AddArgs),
    /// Soft-delete active tree entries under the repository's blacklist
    /// directory.
    Process(ProcessArgs),
}

pub fn run_blacklist(repo: &Path, args: BlacklistArgs) -> ExitCode {
    match args.command {
        BlacklistCommand::Add(add_args) => run_add(repo, add_args),
        BlacklistCommand::Process(process_args) => run_process(repo, process_args),
    }
}

#[derive(Args)]
pub struct AddArgs {
    /// Directory containing files to add to the blacklist. Its direct
    /// contents (files and subdirectories) are hashed and backed up into the
    /// repository tree, each keeping its own name and structure, under a new
    /// timestamped subdirectory of `--dfs-blacklist`.
    blacklist_dir: PathBuf,

    /// Name of the blacklist directory at the repository root that added
    /// files are backed up under.
    #[arg(long, default_value = "blacklist")]
    dfs_blacklist: String,

    /// Delete each original file under `blacklist_dir` once it has been
    /// confirmed backed up, and remove now-empty source directories
    /// afterward. Off by default - unlike the Scala tool this replaces,
    /// where the equivalent `deleteFiles` option defaults to true; deleting
    /// source data by default felt too easy to trigger by accident, so this
    /// makes it explicit opt-in instead.
    #[arg(long)]
    delete_files: bool,
}

/// Backs up `args.blacklist_dir`'s direct contents into the tree under
/// `<dfs_blacklist>/<timestamp>`, reusing [`store::run_store`] (via
/// [`store::BackupArgs::for_paths`]) for the actual hash-and-store pipeline -
/// each direct entry of `blacklist_dir` is passed as its own source so it
/// lands directly under the timestamped directory, rather than nested one
/// level deeper under `blacklist_dir`'s own name (which is how a plain
/// single-source `store` run would place it).
///
/// If `--delete-files` is given, an original is only deleted once its
/// corresponding tree entry has actually been confirmed present after the
/// backup - not merely inferred from `run_store`'s overall exit code, which
/// can be a success even though individual files were skipped with a logged
/// warning (an unreadable file, say). This is intentionally more
/// conservative than the Scala tool this replaces, which deletes every
/// source file unconditionally once the pass over it completes.
pub fn run_add(repo: &Path, args: AddArgs) -> ExitCode {
    let children: Vec<PathBuf> = match fs::read_dir(&args.blacklist_dir) {
        Ok(read_dir) => read_dir.filter_map(|e| e.ok()).map(|e| e.path()).collect(),
        Err(err) => {
            eprintln!(
                "error: cannot read blacklist source directory '{}': {err}",
                args.blacklist_dir.display()
            );
            return ExitCode::FAILURE;
        }
    };
    if children.is_empty() {
        println!(
            "'{}' is empty - nothing to add to the blacklist",
            args.blacklist_dir.display()
        );
        return ExitCode::SUCCESS;
    }

    let dir_name = timestamp_for_filename(now_millis());
    let dfs_blacklist = args.dfs_blacklist.trim_matches('/');
    let target = PathBuf::from(dfs_blacklist).join(&dir_name);
    let target_tree_path = format!("{dfs_blacklist}/{dir_name}");

    let mut paths = children.clone();
    paths.push(target);
    let exit = store::run_store(repo, store::BackupArgs::for_paths(paths));
    if exit != ExitCode::SUCCESS {
        return exit;
    }

    if args.delete_files {
        let repository = match db::open_repository(repo) {
            Ok(r) => r,
            Err(err) => {
                eprintln!(
                    "error: backup succeeded, but failed to reopen the repository to verify \
                     originals before deleting them: {err}"
                );
                return ExitCode::FAILURE;
            }
        };
        let conn = match repository.open_read_connection() {
            Ok(c) => c,
            Err(err) => {
                eprintln!(
                    "error: backup succeeded, but failed to open the metadata database to \
                     verify originals before deleting them: {err}"
                );
                return ExitCode::FAILURE;
            }
        };
        for child in &children {
            let Some(name) = child.file_name().map(|n| n.to_string_lossy().into_owned()) else {
                continue;
            };
            let child_tree_path = format!("{target_tree_path}/{name}");
            delete_confirmed_original(&conn, child, &child_tree_path);
        }
    }

    println!("blacklist add complete: backed up under '{target_tree_path}'");
    ExitCode::SUCCESS
}

/// Recursively deletes `path` (a direct or nested entry originally listed
/// under `blacklist_dir`) once `tree_path` is confirmed to resolve to an
/// active file entry in the tree, mirroring the Scala tool's own recursive,
/// bottom-up "delete file, then remove now-empty parent directories" walk.
/// Access and verification failures are logged as warnings and skipped
/// rather than aborting the whole pass - consistent with how `store` itself
/// already treats per-file problems during a backup.
fn delete_confirmed_original(conn: &Connection, path: &Path, tree_path: &str) {
    let is_dir = match fs::metadata(path) {
        Ok(m) => m.is_dir(),
        Err(err) => {
            eprintln!(
                "warning: failed to access '{}' while deleting blacklisted originals: {err}",
                path.display()
            );
            return;
        }
    };

    if is_dir {
        let entries: Vec<PathBuf> = match fs::read_dir(path) {
            Ok(read_dir) => read_dir.filter_map(|e| e.ok()).map(|e| e.path()).collect(),
            Err(err) => {
                eprintln!(
                    "warning: failed to read directory '{}' while deleting blacklisted \
                     originals: {err}",
                    path.display()
                );
                return;
            }
        };
        for entry in &entries {
            let Some(name) = entry.file_name().map(|n| n.to_string_lossy().into_owned()) else {
                continue;
            };
            delete_confirmed_original(conn, entry, &format!("{tree_path}/{name}"));
        }
        match fs::read_dir(path) {
            Ok(mut remaining) => {
                if remaining.next().is_none() {
                    match fs::remove_dir(path) {
                        Ok(()) => println!(
                            "deleted empty blacklist source directory: {}",
                            path.display()
                        ),
                        Err(err) => eprintln!(
                            "warning: failed to delete empty blacklist source directory '{}': {err}",
                            path.display()
                        ),
                    }
                } else {
                    eprintln!(
                        "warning: blacklist source directory '{}' not empty after processing",
                        path.display()
                    );
                }
            }
            Err(err) => eprintln!(
                "warning: failed to check blacklist source directory '{}': {err}",
                path.display()
            ),
        }
        return;
    }

    match db::resolve_path(conn, tree_path) {
        Ok(Some(entry)) if entry.kind == db::EntryKind::File => match fs::remove_file(path) {
            Ok(()) => println!("moved to blacklist: {}", path.display()),
            Err(err) => eprintln!(
                "warning: failed to delete blacklisted original '{}': {err}",
                path.display()
            ),
        },
        Ok(_) => eprintln!(
            "warning: not deleting '{}' - '{tree_path}' was not found among the entries just \
             backed up",
            path.display()
        ),
        Err(err) => eprintln!(
            "warning: failed to verify backup of '{}' via '{tree_path}': {err}",
            path.display()
        ),
    }
}

#[derive(Args)]
pub struct ProcessArgs {
    /// Name of the blacklist directory at the repository root to process -
    /// must match whatever `--dfs-blacklist` was used when adding files.
    #[arg(long, default_value = "blacklist")]
    dfs_blacklist: String,

    /// Also soft-delete every other active tree entry that shares content
    /// with a processed blacklist entry, so nothing outside the blacklist
    /// directory still references that content.
    #[arg(long)]
    delete_copies: bool,

    /// Take a database backup (see `db backup`) before processing. Off by
    /// default: unlike the Scala tool this replaces (whose equivalent
    /// `dbBackup` option defaults to true), this command only ever
    /// soft-deletes tree entries - the exact same, already-reversible-until-
    /// `reclaim-space` mechanism `del` already uses without an automatic
    /// backup - rather than Scala's harder-to-reverse "zeroed but present"
    /// content mutation that motivated defaulting it on there.
    #[arg(long)]
    backup: bool,
}

/// Soft-deletes every active tree entry under `<dfs_blacklist>` (files and
/// directories alike - a directory's own soft-delete already covers
/// everything under it, see [`db::soft_delete`]), and, with
/// `--delete-copies`, every other active entry anywhere in the tree that
/// shares a processed file's `content_id`.
pub fn run_process(repo: &Path, args: ProcessArgs) -> ExitCode {
    if args.backup {
        let exit = crate::db_maintenance::run_backup(repo);
        if exit != ExitCode::SUCCESS {
            eprintln!("error: aborting blacklist processing: backup failed");
            return exit;
        }
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

    let dfs_blacklist = args.dfs_blacklist.trim_matches('/');
    let blacklist_root = match db::resolve_path(&conn, dfs_blacklist) {
        Ok(Some(entry)) if entry.kind == db::EntryKind::Dir => entry,
        Ok(Some(_)) => {
            eprintln!("error: '{dfs_blacklist}' is a file, not a directory");
            return ExitCode::FAILURE;
        }
        Ok(None) => {
            println!("'{dfs_blacklist}' does not exist - nothing to process");
            return ExitCode::SUCCESS;
        }
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    };

    let entries = match db::subtree_entries_with_paths(&conn, blacklist_root.id) {
        Ok(entries) => entries,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    };

    let now = now_millis();
    let mut processed = 0usize;
    let mut copies_deleted = 0usize;
    for path_entry in &entries {
        if path_entry.entry.kind != db::EntryKind::File {
            continue;
        }
        match db::soft_delete(&conn, path_entry.entry.id, now) {
            Ok(count) => processed += count,
            Err(err) => {
                eprintln!(
                    "error: failed to soft-delete '{dfs_blacklist}/{}': {err}",
                    path_entry.path
                );
                return ExitCode::FAILURE;
            }
        }

        if args.delete_copies
            && let Some(content_id) = path_entry.entry.content_id
        {
            let copies = match db::entries_for_content(&conn, content_id) {
                Ok(copies) => copies,
                Err(err) => {
                    eprintln!("error: {err}");
                    return ExitCode::FAILURE;
                }
            };
            for copy in copies {
                if copy.id == path_entry.entry.id {
                    continue;
                }
                match db::soft_delete(&conn, copy.id, now) {
                    Ok(count) => copies_deleted += count,
                    Err(err) => {
                        eprintln!(
                            "error: failed to soft-delete a copy (id {}): {err}",
                            copy.id
                        );
                        return ExitCode::FAILURE;
                    }
                }
            }
        }
    }

    if args.delete_copies {
        println!(
            "blacklist processing complete: {processed} blacklist entr{} soft-deleted, \
             {copies_deleted} other cop{} of their content also soft-deleted",
            if processed == 1 { "y" } else { "ies" },
            if copies_deleted == 1 { "y" } else { "ies" },
        );
    } else {
        println!(
            "blacklist processing complete: {processed} blacklist entr{} soft-deleted",
            if processed == 1 { "y" } else { "ies" },
        );
    }
    ExitCode::SUCCESS
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

    fn add_args(blacklist_dir: PathBuf, delete_files: bool) -> AddArgs {
        AddArgs {
            blacklist_dir,
            dfs_blacklist: "blacklist".to_string(),
            delete_files,
        }
    }

    fn process_args(delete_copies: bool, backup: bool) -> ProcessArgs {
        ProcessArgs {
            dfs_blacklist: "blacklist".to_string(),
            delete_copies,
            backup,
        }
    }

    fn read_conn(repo_root: &Path) -> Connection {
        db::open_repository(repo_root)
            .unwrap()
            .open_read_connection()
            .unwrap()
    }

    #[test]
    fn run_add_with_an_empty_source_dir_succeeds_without_touching_the_tree() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();

        let exit = run_add(&repo_root, add_args(source_dir.path().to_path_buf(), false));

        assert_eq!(exit, ExitCode::SUCCESS);
        let conn = read_conn(&repo_root);
        assert_eq!(db::resolve_path(&conn, "blacklist").unwrap(), None);
    }

    #[test]
    fn run_add_backs_up_files_flat_under_a_timestamped_blacklist_subdirectory_and_dedupes() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("a.txt"), b"same content").unwrap();
        std::fs::create_dir(source_dir.path().join("sub")).unwrap();
        std::fs::write(source_dir.path().join("sub").join("b.txt"), b"same content").unwrap();

        let exit = run_add(&repo_root, add_args(source_dir.path().to_path_buf(), false));
        assert_eq!(exit, ExitCode::SUCCESS);

        let conn = read_conn(&repo_root);
        let blacklist = db::resolve_path(&conn, "blacklist").unwrap().unwrap();
        let children = db::list_children(&conn, blacklist.id).unwrap();
        assert_eq!(children.len(), 1, "one timestamped subdirectory");
        let timestamp_dir = &children[0];
        assert_eq!(timestamp_dir.kind, db::EntryKind::Dir);

        let a = db::resolve_path(&conn, &format!("blacklist/{}/a.txt", timestamp_dir.name))
            .unwrap()
            .unwrap();
        let b = db::resolve_path(
            &conn,
            &format!("blacklist/{}/sub/b.txt", timestamp_dir.name),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            a.content_id, b.content_id,
            "identical content must dedupe to the same content row, \
             just like a normal store run"
        );

        // Originals must still be present - delete_files was off.
        assert!(source_dir.path().join("a.txt").is_file());
        assert!(source_dir.path().join("sub").join("b.txt").is_file());
    }

    #[test]
    fn run_add_with_delete_files_removes_confirmed_originals_and_empty_directories() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("a.txt"), b"content a").unwrap();
        std::fs::create_dir(source_dir.path().join("sub")).unwrap();
        std::fs::write(source_dir.path().join("sub").join("b.txt"), b"content b").unwrap();

        let exit = run_add(&repo_root, add_args(source_dir.path().to_path_buf(), true));
        assert_eq!(exit, ExitCode::SUCCESS);

        assert!(!source_dir.path().join("a.txt").exists());
        assert!(
            !source_dir.path().join("sub").exists(),
            "the now-empty 'sub' directory must be removed too"
        );
    }

    #[test]
    fn run_process_soft_deletes_active_entries_under_the_blacklist_directory() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("a.txt"), b"blacklisted content").unwrap();
        assert_eq!(
            run_add(&repo_root, add_args(source_dir.path().to_path_buf(), false)),
            ExitCode::SUCCESS
        );

        let exit = run_process(&repo_root, process_args(false, false));
        assert_eq!(exit, ExitCode::SUCCESS);

        let conn = read_conn(&repo_root);
        let blacklist = db::resolve_path(&conn, "blacklist").unwrap().unwrap();
        let timestamp_dir = &db::list_children(&conn, blacklist.id).unwrap()[0];
        assert_eq!(
            db::resolve_path(&conn, &format!("blacklist/{}/a.txt", timestamp_dir.name)).unwrap(),
            None,
            "the blacklisted file entry must now be soft-deleted (inactive)"
        );
    }

    #[test]
    fn run_process_without_delete_copies_leaves_other_entries_with_the_same_content_alone() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("a.txt"), b"shared content").unwrap();
        assert_eq!(
            run_add(&repo_root, add_args(source_dir.path().to_path_buf(), false)),
            ExitCode::SUCCESS
        );
        // A normal backup elsewhere in the tree, sharing the same content.
        let other_source = tempfile::tempdir().unwrap();
        std::fs::write(other_source.path().join("copy.txt"), b"shared content").unwrap();
        assert_eq!(
            store::run_store(
                &repo_root,
                store::BackupArgs::for_paths(vec![
                    other_source.path().join("copy.txt"),
                    PathBuf::from("elsewhere"),
                ]),
            ),
            ExitCode::SUCCESS
        );

        assert_eq!(
            run_process(&repo_root, process_args(false, false)),
            ExitCode::SUCCESS
        );

        let conn = read_conn(&repo_root);
        assert!(
            db::resolve_path(&conn, "elsewhere/copy.txt")
                .unwrap()
                .is_some(),
            "without --delete-copies, the unrelated copy must stay active"
        );
    }

    #[test]
    fn run_process_with_delete_copies_also_soft_deletes_other_entries_sharing_content() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("a.txt"), b"shared content").unwrap();
        assert_eq!(
            run_add(&repo_root, add_args(source_dir.path().to_path_buf(), false)),
            ExitCode::SUCCESS
        );
        let other_source = tempfile::tempdir().unwrap();
        std::fs::write(other_source.path().join("copy.txt"), b"shared content").unwrap();
        std::fs::write(
            other_source.path().join("unrelated.txt"),
            b"unrelated content",
        )
        .unwrap();
        assert_eq!(
            store::run_store(
                &repo_root,
                store::BackupArgs::for_paths(vec![
                    other_source.path().join("copy.txt"),
                    other_source.path().join("unrelated.txt"),
                    PathBuf::from("elsewhere"),
                ]),
            ),
            ExitCode::SUCCESS
        );

        assert_eq!(
            run_process(&repo_root, process_args(true, false)),
            ExitCode::SUCCESS
        );

        let conn = read_conn(&repo_root);
        assert_eq!(
            db::resolve_path(&conn, "elsewhere/copy.txt").unwrap(),
            None,
            "--delete-copies must soft-delete the copy sharing content with the blacklist entry"
        );
        assert!(
            db::resolve_path(&conn, "elsewhere/unrelated.txt")
                .unwrap()
                .is_some(),
            "a file with unrelated content must not be touched"
        );
    }

    #[test]
    fn run_process_is_a_no_op_when_the_blacklist_directory_does_not_exist() {
        let (_temp_dir, repo_root) = init_repo();

        let exit = run_process(&repo_root, process_args(false, false));

        assert_eq!(exit, ExitCode::SUCCESS);
    }

    #[test]
    fn run_process_with_backup_creates_a_database_backup_first() {
        let (_temp_dir, repo_root) = init_repo();

        let exit = run_process(&repo_root, process_args(false, true));

        assert_eq!(exit, ExitCode::SUCCESS);
        let backups_dir = db::meta_dir(&repo_root).join("backups");
        assert_eq!(
            std::fs::read_dir(&backups_dir).unwrap().count(),
            1,
            "one backup file must have been created"
        );
    }

    #[test]
    fn end_to_end_add_then_process_leaves_only_soft_deleted_blacklist_entries_and_reclaimable_content()
     {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("cache.tmp"), b"installer cache junk").unwrap();

        assert_eq!(
            run_add(&repo_root, add_args(source_dir.path().to_path_buf(), true)),
            ExitCode::SUCCESS
        );
        assert!(
            !source_dir.path().join("cache.tmp").exists(),
            "delete_files must have removed the original"
        );

        assert_eq!(
            run_process(&repo_root, process_args(false, false)),
            ExitCode::SUCCESS
        );

        let conn = read_conn(&repo_root);
        let blacklist = db::resolve_path(&conn, "blacklist").unwrap().unwrap();
        let timestamp_dir = &db::list_children(&conn, blacklist.id).unwrap()[0];
        assert_eq!(
            db::resolve_path(
                &conn,
                &format!("blacklist/{}/cache.tmp", timestamp_dir.name)
            )
            .unwrap(),
            None,
            "processed blacklist entry must be inactive"
        );
    }
}
