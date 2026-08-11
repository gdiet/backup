#[cfg(not(windows))]
use std::fs::File;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, UNIX_EPOCH};

use clap::Args;
use rusqlite::Connection;
use store::{LongTermStore, ReadIntegrity};

use crate::chunk_store::read_chunk_bytes;

#[derive(Args)]
pub struct RestoreArgs {
    /// Allow overwriting existing files at the target. Without this, an
    /// existing file at a restore destination is left untouched and reported
    /// as a warning.
    #[arg(long)]
    overwrite: bool,

    /// Restore a soft-deleted entry (its id, as shown by `backup deleted`)
    /// instead of one or more active repository paths - the entry stays
    /// deleted in the repository; this only copies its content out. With
    /// this, `PATH` takes just the target directory, no source paths.
    #[arg(long)]
    deleted: Option<i64>,

    /// With `--deleted` naming a directory, also restore its descendants
    /// that were deleted together with it (the same scope `backup undelete
    /// --recursive` would reactivate) - see that flag's own doc comment.
    /// Ignored (and meaningless) without `--deleted`.
    #[arg(long, requires = "deleted")]
    recursive: bool,

    /// Without `--deleted`: one or more source paths in the repository
    /// followed by the target directory. With `--deleted <id>`: just the
    /// target directory.
    #[arg(required = true, num_args = 1.., value_name = "PATH")]
    paths: Vec<PathBuf>,
}

/// Restores one or more repository paths to a real filesystem directory.
///
/// Each source keeps its own name as a child of `target` (mirroring `store`'s
/// convention for how sources land under a target). Per-file and
/// per-directory errors are logged and the affected entry is skipped, not
/// fatal - matching `store`'s error-handling philosophy and fixing the Scala
/// tool this replaces, where an uncaught exception (a permission error, a
/// `mkdir` that silently failed) aborts the entire restore with no partial-
/// completion tracking.
pub fn run_restore(repo: &Path, args: RestoreArgs) -> ExitCode {
    let target = match (args.deleted, args.paths.as_slice()) {
        (Some(_), [target]) => target,
        (Some(_), _) => {
            eprintln!("error: --deleted takes just a target directory, no source paths");
            return ExitCode::FAILURE;
        }
        (None, paths) if paths.len() >= 2 => &paths[paths.len() - 1],
        (None, _) => {
            eprintln!(
                "error: at least one source path and a target directory are required \
                 (or --deleted <id> and just a target directory)"
            );
            return ExitCode::FAILURE;
        }
    };

    if !target.is_dir() {
        eprintln!(
            "error: target '{}' is not an existing directory",
            target.display()
        );
        return ExitCode::FAILURE;
    }

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
    let data_store = LongTermStore::new(repository.data_dir(), true);

    let mut warnings = 0u64;
    if let Some(id) = args.deleted {
        if let Err(msg) = restore_deleted(
            &conn,
            &data_store,
            id,
            args.recursive,
            target,
            args.overwrite,
            &mut warnings,
        ) {
            eprintln!("error: {msg}");
            return ExitCode::FAILURE;
        }
    } else {
        let sources = &args.paths[..args.paths.len() - 1];
        for source in sources {
            let source_label = source.to_string_lossy();
            match db::resolve_path(&conn, &source_label) {
                Ok(Some(entry)) => match entry.kind {
                    db::EntryKind::Dir => restore_dir(
                        &conn,
                        &data_store,
                        &entry,
                        target,
                        args.overwrite,
                        &mut warnings,
                    ),
                    db::EntryKind::File => restore_file(
                        &conn,
                        &data_store,
                        &entry,
                        target,
                        args.overwrite,
                        &mut warnings,
                    ),
                },
                Ok(None) => {
                    eprintln!("warning: source path '{source_label}' does not exist");
                    warnings += 1;
                }
                Err(err) => {
                    eprintln!("error: {err}");
                    return ExitCode::FAILURE;
                }
            }
        }
    }

    if warnings > 0 {
        println!("restore completed with {warnings} warning(s)");
    } else {
        println!("restore completed successfully");
    }
    ExitCode::SUCCESS
}

/// Restores a single soft-deleted entry (found by `id`, e.g. via `backup
/// deleted`) - a file, or a directory and (if `recursive`) its descendants
/// that were deleted together with it, per [`db::undelete`]'s identical
/// "same `deleted_at`" scoping (see its own doc comment). The entry stays
/// deleted in the repository; this only ever reads, never touches
/// `deleted_at`. Nested directories' own mtimes aren't restored in this
/// path (unlike [`restore_dir`]'s active-tree walk) - a minor scope cut,
/// not a correctness issue.
fn restore_deleted(
    conn: &Connection,
    data_store: &LongTermStore,
    id: i64,
    recursive: bool,
    target: &Path,
    overwrite: bool,
    warnings: &mut u64,
) -> Result<(), String> {
    let entry = db::get_tree_entry(conn, id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no entry with id {id}"))?;
    if db::is_deleted(conn, id).map_err(|e| e.to_string())? != Some(true) {
        return Err(format!("entry {id} exists but is not currently deleted"));
    }

    match entry.kind {
        db::EntryKind::File => {
            restore_file_at(
                conn,
                data_store,
                &entry,
                &target.join(&entry.name),
                overwrite,
                warnings,
            );
        }
        db::EntryKind::Dir => {
            let dir_target = target.join(&entry.name);
            if let Err(err) = fs::create_dir(&dir_target)
                && !dir_target.is_dir()
            {
                eprintln!(
                    "warning: failed to create directory '{}': {err}",
                    dir_target.display()
                );
                *warnings += 1;
                return Ok(());
            }
            if recursive {
                let descendants = db::deleted_entries(conn, id).map_err(|e| e.to_string())?;
                for descendant in &descendants {
                    let dest = dir_target.join(&descendant.path);
                    match descendant.entry.kind {
                        db::EntryKind::Dir => {
                            if let Err(err) = fs::create_dir(&dest)
                                && !dest.is_dir()
                            {
                                eprintln!(
                                    "warning: failed to create directory '{}': {err}",
                                    dest.display()
                                );
                                *warnings += 1;
                            }
                        }
                        db::EntryKind::File => {
                            restore_file_at(
                                conn,
                                data_store,
                                &descendant.entry,
                                &dest,
                                overwrite,
                                warnings,
                            );
                        }
                    }
                }
            }
            set_mtime(&dir_target, entry.time_millis);
        }
    }
    Ok(())
}

fn restore_dir(
    conn: &Connection,
    data_store: &LongTermStore,
    entry: &db::TreeEntryRow,
    parent_target: &Path,
    overwrite: bool,
    warnings: &mut u64,
) {
    let dir_target = parent_target.join(&entry.name);
    // Directories are always create-if-missing/reuse-if-present - unlike
    // files, an existing directory isn't a content conflict, just a
    // container to merge into; only its individual children are subject to
    // the overwrite check.
    if let Err(err) = fs::create_dir(&dir_target)
        && !dir_target.is_dir()
    {
        eprintln!(
            "warning: failed to create directory '{}': {err}",
            dir_target.display()
        );
        *warnings += 1;
        return;
    }

    match db::list_children(conn, entry.id) {
        Ok(children) => {
            for child in children {
                match child.kind {
                    db::EntryKind::Dir => {
                        restore_dir(conn, data_store, &child, &dir_target, overwrite, warnings)
                    }
                    db::EntryKind::File => {
                        restore_file(conn, data_store, &child, &dir_target, overwrite, warnings)
                    }
                }
            }
        }
        Err(err) => {
            eprintln!("warning: failed to list '{}': {err}", dir_target.display());
            *warnings += 1;
        }
    }

    // Set the directory's own mtime last, after writing its children (which
    // would otherwise bump it again).
    set_mtime(&dir_target, entry.time_millis);
}

fn restore_file(
    conn: &Connection,
    data_store: &LongTermStore,
    entry: &db::TreeEntryRow,
    parent_target: &Path,
    overwrite: bool,
    warnings: &mut u64,
) {
    let file_target = parent_target.join(&entry.name);
    restore_file_at(conn, data_store, entry, &file_target, overwrite, warnings);
}

fn restore_file_at(
    conn: &Connection,
    data_store: &LongTermStore,
    entry: &db::TreeEntryRow,
    file_target: &Path,
    overwrite: bool,
    warnings: &mut u64,
) {
    let mut open_options = OpenOptions::new();
    open_options.write(true);
    if overwrite {
        open_options.create(true).truncate(true);
    } else {
        open_options.create_new(true);
    }

    let mut file = match open_options.open(file_target) {
        Ok(file) => file,
        Err(err) => {
            eprintln!(
                "warning: failed to create '{}': {err}",
                file_target.display()
            );
            *warnings += 1;
            return;
        }
    };

    let chunks = match entry.content_id {
        None => Vec::new(),
        Some(content_id) => match db::ordered_content_chunks(conn, content_id) {
            Ok(chunks) => chunks,
            Err(err) => {
                eprintln!(
                    "warning: failed to read chunk list for '{}': {err}",
                    file_target.display()
                );
                *warnings += 1;
                return;
            }
        },
    };

    for chunk in chunks {
        let buf = match read_chunk_bytes(conn, data_store, chunk.chunk_id, chunk.length as u64) {
            Ok((buf, ReadIntegrity::Complete)) => buf,
            Ok((_, ReadIntegrity::Incomplete { missing_or_short })) => {
                eprintln!(
                    "warning: incomplete store data for '{}': {}",
                    file_target.display(),
                    missing_or_short.join(", ")
                );
                *warnings += 1;
                drop(file);
                let _ = fs::remove_file(file_target);
                return;
            }
            Err(err) => {
                eprintln!(
                    "warning: failed to read store data for '{}': {err}",
                    file_target.display()
                );
                *warnings += 1;
                drop(file);
                let _ = fs::remove_file(file_target);
                return;
            }
        };
        if let Err(err) = file.write_all(&buf) {
            eprintln!(
                "warning: failed to write '{}': {err}",
                file_target.display()
            );
            *warnings += 1;
            return;
        }
    }

    // Set mtime on the handle that's already open for writing, rather than
    // reopening the path: a fresh read-only open (`File::open`) doesn't carry
    // FILE_WRITE_ATTRIBUTES on Windows, which makes `set_times` a silent
    // no-op there.
    let _ = file
        .set_times(std::fs::FileTimes::new().set_modified(mtime_from_millis(entry.time_millis)));
    drop(file);
}

fn mtime_from_millis(time_millis: i64) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::from_millis(time_millis.max(0) as u64)
}

/// Restores a directory's modified time from a stored epoch-millis
/// timestamp. Best-effort: a failure here isn't worth treating as a restore
/// warning on its own.
fn set_mtime(path: &Path, time_millis: i64) {
    let times = std::fs::FileTimes::new().set_modified(mtime_from_millis(time_millis));

    // Plain read access is enough to change timestamps on Unix, but on
    // Windows a read-only handle doesn't carry FILE_WRITE_ATTRIBUTES, so
    // `set_times` would silently fail. Opening a directory for write on
    // Windows additionally requires FILE_FLAG_BACKUP_SEMANTICS, since
    // `CreateFile` otherwise refuses directory handles outright.
    #[cfg(windows)]
    let opened = {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
        OpenOptions::new()
            .write(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)
    };
    #[cfg(not(windows))]
    let opened = File::open(path);

    let Ok(file) = opened else {
        return;
    };
    let _ = file.set_times(times);
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

    fn seed_file(repo_root: &Path, parent_id: i64, name: &str, bytes: &[u8], time_millis: i64) {
        let data_store = LongTermStore::new(repo_root.join("data"), false);
        let start: i64 = {
            let repository = db::open_repository(repo_root).unwrap();
            let conn = repository.open_read_connection().unwrap();
            conn.query_row(
                "SELECT COALESCE(MAX(stop), 0) FROM chunk_extents",
                (),
                |row| row.get(0),
            )
            .unwrap()
        };
        data_store.write(start as u64, bytes).unwrap();

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
                parent_id,
                name: name.to_string(),
                time_millis,
                content: db::ContentSource::Resolved {
                    chunks: if bytes.is_empty() {
                        vec![]
                    } else {
                        vec![db::ChunkRef::New {
                            length: bytes.len() as u64,
                            hash: hash.to_vec(),
                            extents: vec![(start as u64, start as u64 + bytes.len() as u64)],
                        }]
                    },
                    content_hash: hash.to_vec(),
                },
            }],
        )
        .unwrap();
    }

    #[test]
    fn run_restore_fails_if_target_is_not_a_directory() {
        let (temp_dir, repo_root) = init_repo();
        let missing_target = temp_dir.path().join("does-not-exist");
        assert_eq!(
            run_restore(
                &repo_root,
                RestoreArgs {
                    overwrite: false,
                    deleted: None,
                    recursive: false,
                    paths: vec![PathBuf::from("a.txt"), missing_target],
                }
            ),
            ExitCode::FAILURE
        );
    }

    #[test]
    fn restores_a_file_with_content_and_mtime() {
        let (temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, 0, "a.txt", b"hello world", 1_704_067_200_000);
        let target = temp_dir.path().join("out");
        fs::create_dir(&target).unwrap();

        let exit = run_restore(
            &repo_root,
            RestoreArgs {
                overwrite: false,
                deleted: None,
                recursive: false,
                paths: vec![PathBuf::from("a.txt"), target.clone()],
            },
        );

        assert_eq!(exit, ExitCode::SUCCESS);
        let restored = target.join("a.txt");
        assert_eq!(fs::read(&restored).unwrap(), b"hello world");
        let mtime = fs::metadata(&restored).unwrap().modified().unwrap();
        assert_eq!(
            mtime.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
            1_704_067_200_000
        );
    }

    #[test]
    fn restores_an_empty_file() {
        let (temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, 0, "empty.txt", b"", 0);
        let target = temp_dir.path().join("out");
        fs::create_dir(&target).unwrap();

        let exit = run_restore(
            &repo_root,
            RestoreArgs {
                overwrite: false,
                deleted: None,
                recursive: false,
                paths: vec![PathBuf::from("empty.txt"), target.clone()],
            },
        );

        assert_eq!(exit, ExitCode::SUCCESS);
        assert_eq!(fs::read(target.join("empty.txt")).unwrap(), b"");
    }

    #[test]
    fn restores_a_directorys_own_mtime() {
        let (temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        db::insert_directory(&conn, 0, "sub", 1_704_067_200_000).unwrap();
        drop(conn);
        let target = temp_dir.path().join("out");
        fs::create_dir(&target).unwrap();

        let exit = run_restore(
            &repo_root,
            RestoreArgs {
                overwrite: false,
                deleted: None,
                recursive: false,
                paths: vec![PathBuf::from(""), target.clone()],
            },
        );

        assert_eq!(exit, ExitCode::SUCCESS);
        let mtime = fs::metadata(target.join("sub"))
            .unwrap()
            .modified()
            .unwrap();
        assert_eq!(
            mtime.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
            1_704_067_200_000
        );
    }

    #[test]
    fn restores_a_directory_recursively() {
        let (temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        let sub_id = db::insert_directory(&conn, 0, "sub", 0).unwrap();
        drop(conn);
        seed_file(&repo_root, sub_id, "b.txt", b"nested", 0);
        seed_file(&repo_root, 0, "a.txt", b"top", 0);
        let target = temp_dir.path().join("out");
        fs::create_dir(&target).unwrap();

        let exit = run_restore(
            &repo_root,
            RestoreArgs {
                overwrite: false,
                deleted: None,
                recursive: false,
                paths: vec![PathBuf::from(""), target.clone()],
            },
        );

        assert_eq!(exit, ExitCode::SUCCESS);
    }

    #[test]
    fn without_overwrite_an_existing_file_is_left_untouched_and_warned_about() {
        let (temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, 0, "a.txt", b"new content", 0);
        let target = temp_dir.path().join("out");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("a.txt"), b"original").unwrap();

        let exit = run_restore(
            &repo_root,
            RestoreArgs {
                overwrite: false,
                deleted: None,
                recursive: false,
                paths: vec![PathBuf::from("a.txt"), target.clone()],
            },
        );

        assert_eq!(exit, ExitCode::SUCCESS, "warnings don't fail the run");
        assert_eq!(fs::read(target.join("a.txt")).unwrap(), b"original");
    }

    #[test]
    fn with_overwrite_an_existing_file_is_replaced() {
        let (temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, 0, "a.txt", b"new content", 0);
        let target = temp_dir.path().join("out");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("a.txt"), b"original").unwrap();

        let exit = run_restore(
            &repo_root,
            RestoreArgs {
                overwrite: true,
                deleted: None,
                recursive: false,
                paths: vec![PathBuf::from("a.txt"), target.clone()],
            },
        );

        assert_eq!(exit, ExitCode::SUCCESS);
        assert_eq!(fs::read(target.join("a.txt")).unwrap(), b"new content");
    }

    #[test]
    fn warns_and_continues_for_a_missing_source_path() {
        let (temp_dir, repo_root) = init_repo();
        let target = temp_dir.path().join("out");
        fs::create_dir(&target).unwrap();

        let exit = run_restore(
            &repo_root,
            RestoreArgs {
                overwrite: false,
                deleted: None,
                recursive: false,
                paths: vec![PathBuf::from("missing.txt"), target],
            },
        );

        assert_eq!(exit, ExitCode::SUCCESS);
    }

    fn mark_deleted(repo_root: &Path, id: i64, deleted_at_millis: i64) {
        let repository = db::open_repository(repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        conn.execute(
            "UPDATE tree_entries SET deleted_at = ?1 WHERE id = ?2",
            rusqlite::params![deleted_at_millis, id],
        )
        .unwrap();
    }

    #[test]
    fn restores_a_deleted_file_by_id_without_undeleting_it() {
        let (temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, 0, "a.txt", b"hello world", 0);
        let id = {
            let repository = db::open_repository(&repo_root).unwrap();
            let conn = repository.open_read_connection().unwrap();
            db::resolve_path(&conn, "a.txt").unwrap().unwrap().id
        };
        mark_deleted(&repo_root, id, 1_704_067_200_000);
        let target = temp_dir.path().join("out");
        fs::create_dir(&target).unwrap();

        let exit = run_restore(
            &repo_root,
            RestoreArgs {
                overwrite: false,
                deleted: Some(id),
                recursive: false,
                paths: vec![target.clone()],
            },
        );

        assert_eq!(exit, ExitCode::SUCCESS);
        assert_eq!(fs::read(target.join("a.txt")).unwrap(), b"hello world");

        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        assert_eq!(db::is_deleted(&conn, id).unwrap(), Some(true));
    }

    #[test]
    fn restores_a_deleted_directory_recursively_by_id() {
        let (temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        let sub_id = db::insert_directory(&conn, 0, "sub", 0).unwrap();
        drop(conn);
        seed_file(&repo_root, sub_id, "b.txt", b"nested", 0);
        db::soft_delete(
            &db::open_repository(&repo_root)
                .unwrap()
                .open_write_connection()
                .unwrap(),
            sub_id,
            1_704_067_200_000,
        )
        .unwrap();
        let target = temp_dir.path().join("out");
        fs::create_dir(&target).unwrap();

        let exit = run_restore(
            &repo_root,
            RestoreArgs {
                overwrite: false,
                deleted: Some(sub_id),
                recursive: true,
                paths: vec![target.clone()],
            },
        );

        assert_eq!(exit, ExitCode::SUCCESS);
        assert_eq!(
            fs::read(target.join("sub").join("b.txt")).unwrap(),
            b"nested"
        );
    }

    #[test]
    fn fails_to_restore_an_id_that_is_not_deleted() {
        let (temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, 0, "a.txt", b"hello", 0);
        let id = {
            let repository = db::open_repository(&repo_root).unwrap();
            let conn = repository.open_read_connection().unwrap();
            db::resolve_path(&conn, "a.txt").unwrap().unwrap().id
        };
        let target = temp_dir.path().join("out");
        fs::create_dir(&target).unwrap();

        let exit = run_restore(
            &repo_root,
            RestoreArgs {
                overwrite: false,
                deleted: Some(id),
                recursive: false,
                paths: vec![target],
            },
        );

        assert_eq!(exit, ExitCode::FAILURE);
    }

    #[test]
    fn deleted_with_more_than_one_path_is_rejected() {
        let (temp_dir, repo_root) = init_repo();
        let target = temp_dir.path().join("out");
        fs::create_dir(&target).unwrap();

        let exit = run_restore(
            &repo_root,
            RestoreArgs {
                overwrite: false,
                deleted: Some(1),
                recursive: false,
                paths: vec![PathBuf::from("extra"), target],
            },
        );

        assert_eq!(exit, ExitCode::FAILURE);
    }
}
