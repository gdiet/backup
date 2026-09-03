//! `dfs restore` - REQ-RESTORE-001/003/004 in requirements/functional/restore.md. Restores one or
//! more repository paths to a real directory on disk, without mounting.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use crate::content_reader;

fn try_run(
    repo_path: &Path,
    default_path_used: bool,
    sources: &[String],
    target: &Path,
    overwrite: bool,
) -> Result<String, String> {
    if !target.is_dir() {
        return Err(format!(
            "error: target {} is not a directory",
            target.display()
        ));
    }

    let repo = match db::open_repository_read_only(repo_path) {
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
    let store = store::ByteStore::new(db::data_dir(repo_path), true);

    let mut restored = 0u64;
    let mut failures: Vec<String> = Vec::new();
    for source in sources {
        match repo.resolve_path(source) {
            Ok(Some(entry)) => {
                let (count, entry_failures) = restore_entry(
                    &repo,
                    &store,
                    &entry,
                    target,
                    source_basename(source),
                    overwrite,
                );
                restored += count;
                failures.extend(entry_failures);
            }
            Ok(None) => failures.push(format!("no such repository path: {source}")),
            Err(err) => failures.push(format!("{source}: {err}")),
        }
    }

    let summary = format!("restored {restored} file(s) to {}", target.display());
    if failures.is_empty() {
        Ok(summary)
    } else {
        Err(format!(
            "{summary}\n{} item(s) failed:\n{}",
            failures.len(),
            failures.join("\n")
        ))
    }
}

/// The last non-empty `/`-separated component of a repository path - what a restored top-level
/// entry is named under the target directory (e.g. restoring `/backups/2024/photos` creates
/// `<target>/photos`, preserving whatever is nested beneath it).
fn source_basename(source: &str) -> &str {
    source
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(source)
}

/// Restores `entry` (and, if a directory, everything beneath it) into `target_parent/name`.
/// Continues past a failure on one item rather than aborting the whole restore (REQ-RESTORE-003
/// fails "that item", not necessarily the run) - returns the count of files actually restored,
/// plus one message per item that failed.
fn restore_entry(
    repo: &db::Repository,
    store: &store::ByteStore,
    entry: &db::Entry,
    target_parent: &Path,
    name: &str,
    overwrite: bool,
) -> (u64, Vec<String>) {
    let target_path = target_parent.join(name);
    match entry.kind {
        db::EntryKind::Dir => restore_dir(repo, store, entry.id, &target_path, overwrite),
        db::EntryKind::File => {
            if target_path.exists() && !overwrite {
                return (
                    0,
                    vec![format!(
                        "{} already exists - pass --overwrite to replace it",
                        target_path.display()
                    )],
                );
            }
            let content_id = entry
                .content_id
                .expect("a file entry always has a content_id (chk_tree_entries_kind_content_id)");
            match write_file(
                repo,
                store,
                content_id,
                entry.size,
                entry.time_millis,
                &target_path,
            ) {
                Ok(()) => (1, Vec::new()),
                Err(err) => (0, vec![format!("{}: {err}", target_path.display())]),
            }
        }
    }
}

fn restore_dir(
    repo: &db::Repository,
    store: &store::ByteStore,
    dir_id: i64,
    target_path: &Path,
    overwrite: bool,
) -> (u64, Vec<String>) {
    if !target_path.exists() {
        if let Err(err) = fs::create_dir(target_path) {
            return (0, vec![format!("{}: {err}", target_path.display())]);
        }
    } else if !target_path.is_dir() {
        return (
            0,
            vec![format!(
                "{} already exists and is not a directory",
                target_path.display()
            )],
        );
    }
    let children = match repo.list_children(dir_id) {
        Ok(children) => children,
        Err(err) => return (0, vec![format!("{}: {err}", target_path.display())]),
    };
    let mut count = 0u64;
    let mut failures = Vec::new();
    for (child_name, child_entry) in children {
        let (child_count, child_failures) = restore_entry(
            repo,
            store,
            &child_entry,
            target_path,
            &child_name,
            overwrite,
        );
        count += child_count;
        failures.extend(child_failures);
    }
    (count, failures)
}

fn write_file(
    repo: &db::Repository,
    store: &store::ByteStore,
    content_id: i64,
    size: u64,
    time_millis: i64,
    target_path: &Path,
) -> Result<(), String> {
    const CHUNK: u32 = 1 << 20; // 1 MiB read/write chunks

    let mut file = fs::File::create(target_path).map_err(|err| err.to_string())?;
    let mut offset = 0u64;
    while offset < size {
        let to_read = (size - offset).min(u64::from(CHUNK)) as u32;
        let data = content_reader::read_content(repo, store, content_id, offset, to_read)
            .map_err(|err| format!("could not read stored content (errno {})", err.0))?;
        file.write_all(&data).map_err(|err| err.to_string())?;
        offset += u64::from(to_read);
    }
    file.set_modified(UNIX_EPOCH + Duration::from_millis(time_millis as u64))
        .map_err(|err| err.to_string())?;
    Ok(())
}

pub fn run(
    repo_path: &Path,
    default_path_used: bool,
    sources: &[String],
    target: &Path,
    overwrite: bool,
) {
    match try_run(repo_path, default_path_used, sources, target, overwrite) {
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

    fn setup() -> (
        db::Repository,
        tempfile::TempDir,
        store::ByteStore,
        tempfile::TempDir,
    ) {
        let repo_dir = tempfile::tempdir().unwrap();
        let repo_root = repo_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            db::RepositorySettings::new(Some(20), 1_700_000_000_000),
        )
        .unwrap();
        let repo = db::open_repository(&repo_root).unwrap();
        let store = store::ByteStore::new(db::data_dir(&repo_root), false);
        (repo, repo_dir, store, tempfile::tempdir().unwrap())
    }

    /// A cheap, content-derived stand-in for a real chunk/content hash - just distinct enough
    /// across the small, hand-picked byte strings these tests use so `chunks`/`contents`' own
    /// `UNIQUE (length, hash)` constraint does not collide between two genuinely different files.
    fn fake_hash(content: &[u8], salt: u8) -> [u8; 20] {
        let mut hash = [salt; 20];
        for (byte, source) in hash.iter_mut().zip(content.iter().cycle()) {
            *byte ^= source;
        }
        hash
    }

    fn create_file(
        repo: &db::Repository,
        store: &store::ByteStore,
        parent: i64,
        name: &str,
        content: &[u8],
    ) {
        let (chunk_id, ranges) = repo
            .reserve_and_insert_chunk(content.len() as i64, &fake_hash(content, 0xAA))
            .unwrap();
        store.write(ranges[0].0, content).unwrap();
        let content_id = repo
            .find_or_create_content(content.len() as i64, &fake_hash(content, 0xBB), &[chunk_id])
            .unwrap();
        repo.settle_file(parent, name, 1_700_000_000_000, content_id)
            .unwrap();
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-restore-test-no-default-repository-here");
        let target = tempfile::tempdir().unwrap();

        let message = try_run(&repo_path, true, &["/a".to_string()], target.path(), false)
            .expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the actionable default-path message, got: {message}"
        );
    }

    #[test]
    fn try_run_restores_a_single_file_preserving_bytes_and_mtime() {
        let (repo, repo_dir, store, target_dir) = setup();
        create_file(&repo, &store, 0, "a.txt", b"hello world");
        drop(repo);
        drop(store);
        let repo_root = repo_dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            &["/a.txt".to_string()],
            target_dir.path(),
            false,
        )
        .expect("must succeed");
        assert!(message.contains("restored 1 file"));

        let restored_path = target_dir.path().join("a.txt");
        assert_eq!(fs::read(&restored_path).unwrap(), b"hello world");
        let mtime = fs::metadata(&restored_path).unwrap().modified().unwrap();
        assert_eq!(
            mtime.duration_since(UNIX_EPOCH).unwrap().as_millis(),
            1_700_000_000_000
        );
    }

    #[test]
    fn try_run_restores_a_directory_recursively_preserving_structure() {
        let (repo, repo_dir, store, target_dir) = setup();
        let dir_id = repo.mkdir(0, "photos", 1_700_000_000_000).unwrap();
        create_file(&repo, &store, dir_id, "one.jpg", b"one");
        let nested_id = repo.mkdir(dir_id, "nested", 1_700_000_000_000).unwrap();
        create_file(&repo, &store, nested_id, "two.jpg", b"two");
        drop(repo);
        drop(store);
        let repo_root = repo_dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            &["/photos".to_string()],
            target_dir.path(),
            false,
        )
        .expect("must succeed");
        assert!(message.contains("restored 2 file"));

        assert_eq!(
            fs::read(target_dir.path().join("photos/one.jpg")).unwrap(),
            b"one"
        );
        assert_eq!(
            fs::read(target_dir.path().join("photos/nested/two.jpg")).unwrap(),
            b"two"
        );
    }

    #[test]
    fn try_run_refuses_to_overwrite_an_existing_file_by_default() {
        let (repo, repo_dir, store, target_dir) = setup();
        create_file(&repo, &store, 0, "a.txt", b"new content");
        drop(repo);
        drop(store);
        let repo_root = repo_dir.path().join("repo");
        fs::write(target_dir.path().join("a.txt"), b"local content").unwrap();

        let message = try_run(
            &repo_root,
            false,
            &["/a.txt".to_string()],
            target_dir.path(),
            false,
        )
        .expect_err("must fail - the target file already exists");
        assert!(
            message.contains("already exists"),
            "expected an already-exists message, got: {message}"
        );
        assert_eq!(
            fs::read(target_dir.path().join("a.txt")).unwrap(),
            b"local content",
            "the existing local file must be left untouched"
        );
    }

    #[test]
    fn try_run_overwrites_when_explicitly_told_to() {
        let (repo, repo_dir, store, target_dir) = setup();
        create_file(&repo, &store, 0, "a.txt", b"new content");
        drop(repo);
        drop(store);
        let repo_root = repo_dir.path().join("repo");
        fs::write(target_dir.path().join("a.txt"), b"local content").unwrap();

        let message = try_run(
            &repo_root,
            false,
            &["/a.txt".to_string()],
            target_dir.path(),
            true,
        )
        .expect("must succeed - overwrite was requested");
        assert!(message.contains("restored 1 file"));
        assert_eq!(
            fs::read(target_dir.path().join("a.txt")).unwrap(),
            b"new content"
        );
    }

    #[test]
    fn try_run_continues_past_one_failed_item_and_reports_it() {
        let (repo, repo_dir, store, target_dir) = setup();
        create_file(&repo, &store, 0, "a.txt", b"a");
        create_file(&repo, &store, 0, "b.txt", b"b");
        drop(repo);
        drop(store);
        let repo_root = repo_dir.path().join("repo");
        // a.txt already exists locally and overwrite is not requested - that one item must fail,
        // but b.txt must still be restored.
        fs::write(target_dir.path().join("a.txt"), b"local").unwrap();

        let message = try_run(
            &repo_root,
            false,
            &["/a.txt".to_string(), "/b.txt".to_string()],
            target_dir.path(),
            false,
        )
        .expect_err("must fail overall - one item failed");
        assert!(message.contains("restored 1 file"));
        assert!(message.contains("1 item(s) failed"));
        assert_eq!(fs::read(target_dir.path().join("b.txt")).unwrap(), b"b");
    }

    #[test]
    fn try_run_reports_a_missing_source_path_as_a_failed_item_not_an_abort() {
        let (repo, repo_dir, store, target_dir) = setup();
        create_file(&repo, &store, 0, "a.txt", b"a");
        drop(repo);
        drop(store);
        let repo_root = repo_dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            &["/does-not-exist".to_string(), "/a.txt".to_string()],
            target_dir.path(),
            false,
        )
        .expect_err("must fail overall - one item failed");
        assert!(message.contains("no such repository path"));
        assert!(message.contains("restored 1 file"));
    }

    #[test]
    fn source_basename_takes_the_last_path_component_ignoring_a_trailing_slash() {
        assert_eq!(source_basename("/backups/2024/photos"), "photos");
        assert_eq!(source_basename("/backups/2024/photos/"), "photos");
        assert_eq!(source_basename("/a"), "a");
    }
}
