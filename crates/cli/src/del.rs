//! `dfs del` - REQ-CLI-003 in `requirements/functional/cli-commands.md`. Deletes a tree entry
//! directly against the repository, without mounting. An ordinary live path is soft-deleted, the
//! same outcome as REQ-TREE-008's mount-side delete; a path reached through REQ-TREE-009's
//! `[deleted]` segment (`requirements/functional/tree.md`) instead names a specific soft-deleted
//! entry, permanently removed only when `--purge` is also given.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::deleted::{self, Resolved};

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_millis() as i64
}

fn try_run(
    repo_path: &Path,
    default_path_used: bool,
    path: &str,
    recursive: bool,
    purge: bool,
) -> Result<String, String> {
    let repo = match db::open_repository(repo_path) {
        Ok(repo) => repo,
        Err(db::Error::NoRepositoryHere(_)) if default_path_used => {
            return Err(format!(
                "error: no repository found at the default location ({}).\n\
                 Pass a repository path explicitly instead.",
                repo_path.display()
            ));
        }
        Err(err) => return Err(format!("error: {err}")),
    };
    // Held for the rest of this run (DESIGN-MAINTENANCE-001 in
    // `docs/design/repository-locking.md`), same as ingest/a read-write mount session -
    // REQ-MAINTENANCE-004's exclusivity applies here too.
    let _write_lock = db::acquire_write_lock(repo_path).map_err(|err| format!("error: {err}"))?;

    match deleted::resolve(&repo, path).map_err(|err| format!("error: {err}"))? {
        Some(Resolved::Live(entry)) => {
            if purge {
                return Err(format!(
                    "{path} is a live entry - --purge only applies to an entry already reached \
                     through `[deleted]` (see `dfs list --show-deleted`)"
                ));
            }
            delete_live(&repo, path, &entry, recursive)
        }
        Some(Resolved::Deleted(entry)) => {
            if recursive {
                return Err(format!(
                    "{path} already names one specific soft-deleted entry - --recursive only \
                     applies to a live directory"
                ));
            }
            delete_deleted(&repo, path, &entry, purge)
        }
        Some(Resolved::DeletedChildren { .. }) => Err(format!(
            "{path} names every soft-deleted entry at this location, not one specific entry - \
             address one by its own name instead (see `dfs list --show-deleted`)"
        )),
        None => Err(format!("no such repository path: {path}")),
    }
}

/// Soft-deletes `entry`, a live tree entry - REQ-CLI-003's ordinary case, the same outcome as
/// REQ-TREE-008's mount-side delete. A directory with live children is refused unless `recursive`
/// is given, in which case its own live descendants are soft-deleted first, deepest first, before
/// the directory itself.
fn delete_live(
    repo: &db::Repository,
    path: &str,
    entry: &db::Entry,
    recursive: bool,
) -> Result<String, String> {
    match entry.kind {
        db::EntryKind::File => {
            repo.unlink_file(entry.id, now_millis())
                .map_err(|err| format!("error: {err}"))?;
            Ok(format!("deleted {path}{}", recoverable_note(path)))
        }
        db::EntryKind::Dir => {
            let descendants = if recursive {
                delete_live_children(repo, entry.id)?
            } else {
                0
            };
            match repo.rmdir(entry.id, now_millis()) {
                Ok(()) if descendants == 0 => {
                    Ok(format!("deleted {path}{}", recoverable_note(path)))
                }
                Ok(()) => Ok(format!(
                    "deleted {path} and {descendants} descendant(s){}",
                    recoverable_note(path)
                )),
                Err(db::Error::DirectoryNotEmpty(_)) => Err(format!(
                    "{path} still has live children - pass --recursive to delete them too"
                )),
                Err(err) => Err(format!("error: {err}")),
            }
        }
    }
}

/// REQ-TREE-009's own `[deleted]` address for `path`, right after [`delete_live`] has just
/// soft-deleted it there - `/photos/one.jpg` -> `/photos/[deleted]/one.jpg`, `/a.txt` ->
/// `/[deleted]/a.txt`. Appended to a soft-delete's own success message, so a caller is told both
/// that the removal is recoverable and exactly where to address it next - via `dfs restore`, or
/// `dfs del --purge` there to remove it for good instead.
fn recoverable_note(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    let deleted_path = match trimmed.rfind('/') {
        Some(slash) => format!("{}/[deleted]/{}", &trimmed[..slash], &trimmed[slash + 1..]),
        None => format!("[deleted]/{trimmed}"),
    };
    format!(" (recoverable at {deleted_path} - pass --purge there to remove it for good)")
}

/// Soft-deletes every live child of `parent_id`, recursively, deepest first (so a directory is
/// never `rmdir`-ed while it still has live children) - REQ-CLI-003's `--recursive` opt-in.
/// Returns how many entries this deleted, for [`delete_live`]'s own summary message.
fn delete_live_children(repo: &db::Repository, parent_id: i64) -> Result<u64, String> {
    let children = repo
        .list_children(parent_id)
        .map_err(|err| format!("error: {err}"))?;
    let mut count = 0u64;
    for (_, child) in children {
        if child.kind == db::EntryKind::Dir {
            count += delete_live_children(repo, child.id)?;
            repo.rmdir(child.id, now_millis())
                .map_err(|err| format!("error: {err}"))?;
        } else {
            repo.unlink_file(child.id, now_millis())
                .map_err(|err| format!("error: {err}"))?;
        }
        count += 1;
    }
    Ok(count)
}

/// Permanently purges `entry`, a soft-deleted tree entry reached through REQ-TREE-009's
/// `[deleted]` addressing - REQ-CLI-003's `--purge` case. Refused unless `purge` is explicitly
/// given, so an irreversible removal never happens just because `path` happened to resolve under
/// `[deleted]` (e.g. pasted from a `dfs list --show-deleted` line).
fn delete_deleted(
    repo: &db::Repository,
    path: &str,
    entry: &db::DeletedEntry,
    purge: bool,
) -> Result<String, String> {
    if !purge {
        return Err(format!(
            "{path} is already soft-deleted - pass --purge to remove it permanently"
        ));
    }
    let result = repo
        .purge_deleted_entry(entry.entry.id)
        .map_err(|err| format!("error: {err}"))?;
    Ok(format!(
        "permanently purged {path}{} ({} bytes reclaimed)",
        if result.descendants == 0 {
            String::new()
        } else {
            format!(" and {} descendant(s)", result.descendants)
        },
        result.reclaimed_bytes
    ))
}

pub fn run(repo_path: &Path, default_path_used: bool, path: &str, recursive: bool, purge: bool) {
    match try_run(repo_path, default_path_used, path, recursive, purge) {
        Ok(message) => println!("{message}"),
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (db::Repository, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        db::init_repository(
            &repo_root,
            db::RepositorySettings::new(Some(20), 1_700_000_000_000),
        )
        .unwrap();
        let repo = db::open_repository(&repo_root).unwrap();
        (repo, dir)
    }

    fn create_file(repo: &db::Repository, parent: i64, name: &str, hash_byte: u8) {
        let content_id = repo
            .find_or_create_content(0, &[hash_byte; 20], &[])
            .unwrap();
        repo.settle_file(parent, name, 1_700_000_000_000, content_id)
            .unwrap();
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-del-test-no-default-repository-here");

        let message = try_run(&repo_path, true, "/a", false, false)
            .expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the actionable default-path message, got: {message}"
        );
    }

    #[test]
    fn try_run_soft_deletes_a_live_file() {
        let (repo, dir) = setup();
        create_file(&repo, 0, "a.txt", 0xAA);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/a.txt", false, false).expect("must succeed");
        assert!(message.contains("deleted /a.txt"));
        assert!(
            message.contains("recoverable at /[deleted]/a.txt"),
            "the success message must name where the soft-deleted file is now addressable: {message}"
        );
        assert!(message.contains("--purge"));

        let repo = db::open_repository(&repo_root).unwrap();
        assert!(repo.resolve_path("/a.txt").unwrap().is_none());
        assert!(
            matches!(
                deleted::resolve(&repo, "/[deleted]/a.txt").unwrap(),
                Some(Resolved::Deleted(_))
            ),
            "a soft-deleted file must be reachable through REQ-TREE-009's addressing afterward"
        );
    }

    #[test]
    fn try_run_soft_delete_names_the_recoverable_path_correctly_below_a_directory() {
        let (repo, dir) = setup();
        let dir_id = repo.mkdir(0, "photos", 1_700_000_000_000).unwrap();
        create_file(&repo, dir_id, "one.jpg", 0xAA);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message =
            try_run(&repo_root, false, "/photos/one.jpg", false, false).expect("must succeed");
        assert!(
            message.contains("recoverable at /photos/[deleted]/one.jpg"),
            "got: {message}"
        );
    }

    #[test]
    fn try_run_soft_deletes_an_empty_live_directory() {
        let (repo, dir) = setup();
        repo.mkdir(0, "empty", 1_700_000_000_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/empty", false, false).expect("must succeed");
        assert!(message.contains("deleted /empty"));
    }

    #[test]
    fn try_run_refuses_a_live_directory_with_children_without_recursive() {
        let (repo, dir) = setup();
        let dir_id = repo.mkdir(0, "a", 1_700_000_000_000).unwrap();
        create_file(&repo, dir_id, "f.txt", 0xAA);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/a", false, false)
            .expect_err("must fail - the directory still has a live child");
        assert!(message.contains("--recursive"));

        let repo = db::open_repository(&repo_root).unwrap();
        assert!(
            repo.resolve_path("/a").unwrap().is_some(),
            "a refused delete must leave the directory untouched"
        );
    }

    #[test]
    fn try_run_recursively_deletes_a_live_directory_with_recursive() {
        let (repo, dir) = setup();
        let dir_id = repo.mkdir(0, "a", 1_700_000_000_000).unwrap();
        create_file(&repo, dir_id, "f.txt", 0xAA);
        let nested_id = repo.mkdir(dir_id, "nested", 1_700_000_000_000).unwrap();
        create_file(&repo, nested_id, "g.txt", 0xBB);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message =
            try_run(&repo_root, false, "/a", true, false).expect("must succeed with --recursive");
        assert!(message.contains("deleted /a"));
        assert!(message.contains("3 descendant"), "got: {message}");

        let repo = db::open_repository(&repo_root).unwrap();
        assert!(repo.resolve_path("/a").unwrap().is_none());
    }

    #[test]
    fn try_run_refuses_purge_on_a_live_target() {
        let (repo, dir) = setup();
        create_file(&repo, 0, "a.txt", 0xAA);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/a.txt", false, true)
            .expect_err("must fail - --purge does not apply to a live path");
        assert!(message.contains("--purge"));

        let repo = db::open_repository(&repo_root).unwrap();
        assert!(
            repo.resolve_path("/a.txt").unwrap().is_some(),
            "a refused --purge must leave the live file untouched, not silently soft-delete it"
        );
    }

    #[test]
    fn try_run_refuses_recursive_on_an_already_deleted_target() {
        let (repo, dir) = setup();
        create_file(&repo, 0, "a.txt", 0xAA);
        let live = repo.resolve_path("/a.txt").unwrap().unwrap();
        repo.unlink_file(live.id, 1_700_000_100_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/[deleted]/a.txt", true, true)
            .expect_err("must fail - --recursive does not apply to an already-deleted target");
        assert!(message.contains("--recursive"));

        let repo = db::open_repository(&repo_root).unwrap();
        assert!(
            matches!(
                deleted::resolve(&repo, "/[deleted]/a.txt").unwrap(),
                Some(Resolved::Deleted(_))
            ),
            "a refused delete must leave the soft-deleted entry untouched"
        );
    }

    #[test]
    fn try_run_reports_a_missing_path_as_a_failure() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/nope", false, false)
            .expect_err("must fail - the path does not exist");
        assert!(message.contains("no such repository path"));
    }

    #[test]
    fn try_run_refuses_to_purge_a_soft_deleted_entry_without_the_purge_flag() {
        let (repo, dir) = setup();
        create_file(&repo, 0, "a.txt", 0xAA);
        let live = repo.resolve_path("/a.txt").unwrap().unwrap();
        repo.unlink_file(live.id, 1_700_000_100_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/[deleted]/a.txt", false, false)
            .expect_err("must fail - --purge was not given");
        assert!(message.contains("--purge"));

        let repo = db::open_repository(&repo_root).unwrap();
        assert!(
            matches!(
                deleted::resolve(&repo, "/[deleted]/a.txt").unwrap(),
                Some(Resolved::Deleted(_))
            ),
            "a refused purge must leave the soft-deleted entry addressable"
        );
    }

    #[test]
    fn try_run_permanently_purges_a_soft_deleted_entry_with_the_purge_flag() {
        let (repo, dir) = setup();
        create_file(&repo, 0, "a.txt", 0xAA);
        let live = repo.resolve_path("/a.txt").unwrap().unwrap();
        repo.unlink_file(live.id, 1_700_000_100_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/[deleted]/a.txt", false, true)
            .expect("must succeed - --purge was given");
        assert!(message.contains("permanently purged"));
        assert!(
            !message.contains("descendant"),
            "a plain file has no descendants to mention: {message}"
        );

        let repo = db::open_repository(&repo_root).unwrap();
        assert!(
            deleted::resolve(&repo, "/[deleted]/a.txt")
                .unwrap()
                .is_none(),
            "a purged entry must no longer be addressable at all"
        );
    }

    #[test]
    fn try_run_purges_a_soft_deleted_directory_together_with_its_own_deleted_children() {
        let (repo, dir) = setup();
        let dir_id = repo.mkdir(0, "photos", 1_700_000_000_000).unwrap();
        create_file(&repo, dir_id, "one.jpg", 0xAA);
        let one_id = repo.resolve_path("/photos/one.jpg").unwrap().unwrap().id;
        repo.unlink_file(one_id, 1_700_000_050_000).unwrap();
        repo.rmdir(dir_id, 1_700_000_100_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/[deleted]/photos", false, true)
            .expect("must succeed - purging a deleted directory also purges its own children");
        assert!(message.contains("permanently purged"));
        assert!(
            message.contains("1 descendant"),
            "the purged child must be counted: {message}"
        );

        let repo = db::open_repository(&repo_root).unwrap();
        assert!(
            deleted::resolve(&repo, "/[deleted]/photos")
                .unwrap()
                .is_none(),
            "the purged directory must no longer be addressable"
        );
    }

    #[test]
    fn try_run_refuses_to_purge_the_bare_deleted_segment_itself() {
        let (repo, dir) = setup();
        create_file(&repo, 0, "a.txt", 0xAA);
        let live = repo.resolve_path("/a.txt").unwrap().unwrap();
        repo.unlink_file(live.id, 1_700_000_100_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/[deleted]", false, true)
            .expect_err("must fail - [deleted] alone does not name one specific entry");
        assert!(message.contains("not one specific entry"));
    }
}
