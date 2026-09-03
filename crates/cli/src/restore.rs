//! `dfs restore` - REQ-RESTORE-001/003/004 in requirements/functional/restore.md. Restores one or
//! more repository paths to a real directory on disk, without mounting.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use crate::settle::HASH_WIDTH;

/// The three independent restore behaviors a caller can opt into - bundled together since
/// [`restore_entry`]/[`restore_dir`] thread them unchanged through every level of recursion.
#[derive(Clone, Copy)]
struct RestoreOptions {
    /// REQ-RESTORE-004: replace a file that already exists at the destination, instead of
    /// failing that one item.
    overwrite: bool,
    /// REQ-RESTORE-003: additionally check each chunk's bytes against its own recorded hash: not
    /// requested at all, a mismatch is never even detected.
    verify: bool,
    /// REQ-RESTORE-003: zero-fill missing/incomplete stored data, or keep content that fails
    /// `verify`, instead of failing that item.
    best_effort: bool,
}

fn try_run(
    repo_path: &Path,
    default_path_used: bool,
    sources: &[String],
    target: &Path,
    options: RestoreOptions,
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

    let mut outcome = Outcome::default();
    for source in sources {
        match repo.resolve_path(source) {
            Ok(Some(entry)) => outcome.merge(restore_entry(
                &repo,
                &store,
                &entry,
                target,
                source_basename(source),
                options,
            )),
            Ok(None) => outcome
                .failures
                .push(format!("no such repository path: {source}")),
            Err(err) => outcome.failures.push(format!("{source}: {err}")),
        }
    }

    let mut message = format!(
        "restored {} file(s) to {}",
        outcome.restored,
        target.display()
    );
    if !outcome.warnings.is_empty() {
        message.push_str(&format!(
            "\n{} warning(s):\n{}",
            outcome.warnings.len(),
            outcome.warnings.join("\n")
        ));
    }
    if outcome.failures.is_empty() {
        Ok(message)
    } else {
        message.push_str(&format!(
            "\n{} item(s) failed:\n{}",
            outcome.failures.len(),
            outcome.failures.join("\n")
        ));
        Err(message)
    }
}

/// Accumulates one restore run's result across every item attempted - how many files actually
/// landed on disk, plus a message per item that succeeded only with a caveat (REQ-RESTORE-003's
/// best-effort opt-in) or did not succeed at all.
#[derive(Default)]
struct Outcome {
    restored: u64,
    warnings: Vec<String>,
    failures: Vec<String>,
}

impl Outcome {
    fn merge(&mut self, other: Outcome) {
        self.restored += other.restored;
        self.warnings.extend(other.warnings);
        self.failures.extend(other.failures);
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
/// fails "that item", not necessarily the run).
fn restore_entry(
    repo: &db::Repository,
    store: &store::ByteStore,
    entry: &db::Entry,
    target_parent: &Path,
    name: &str,
    options: RestoreOptions,
) -> Outcome {
    let target_path = target_parent.join(name);
    match entry.kind {
        db::EntryKind::Dir => restore_dir(repo, store, entry.id, &target_path, options),
        db::EntryKind::File => {
            if target_path.exists() && !options.overwrite {
                return Outcome {
                    failures: vec![format!(
                        "{} already exists - pass --overwrite to replace it",
                        target_path.display()
                    )],
                    ..Default::default()
                };
            }
            let content_id = entry
                .content_id
                .expect("a file entry always has a content_id (chk_tree_entries_kind_content_id)");
            match write_file(
                repo,
                store,
                content_id,
                entry.time_millis,
                &target_path,
                options,
            ) {
                Ok(warnings) => Outcome {
                    restored: 1,
                    warnings: warnings
                        .into_iter()
                        .map(|warning| format!("{}: {warning}", target_path.display()))
                        .collect(),
                    failures: Vec::new(),
                },
                Err(err) => Outcome {
                    failures: vec![format!("{}: {err}", target_path.display())],
                    ..Default::default()
                },
            }
        }
    }
}

fn restore_dir(
    repo: &db::Repository,
    store: &store::ByteStore,
    dir_id: i64,
    target_path: &Path,
    options: RestoreOptions,
) -> Outcome {
    if !target_path.exists() {
        if let Err(err) = fs::create_dir(target_path) {
            return Outcome {
                failures: vec![format!("{}: {err}", target_path.display())],
                ..Default::default()
            };
        }
    } else if !target_path.is_dir() {
        return Outcome {
            failures: vec![format!(
                "{} already exists and is not a directory",
                target_path.display()
            )],
            ..Default::default()
        };
    }
    let children = match repo.list_children(dir_id) {
        Ok(children) => children,
        Err(err) => {
            return Outcome {
                failures: vec![format!("{}: {err}", target_path.display())],
                ..Default::default()
            };
        }
    };
    let mut outcome = Outcome::default();
    for (child_name, child_entry) in children {
        outcome.merge(restore_entry(
            repo,
            store,
            &child_entry,
            target_path,
            &child_name,
            options,
        ));
    }
    outcome
}

/// Writes `content_id`'s bytes to a fresh temporary file next to `target_path`, then renames it
/// into place only once every chunk has actually landed - so a failure partway (a chunk that
/// fails outright, a write error) never leaves a partial, corrupted-looking file at `target_path`
/// itself. `fs::rename` replaces an existing `target_path` atomically on both Unix and Windows,
/// so this also implements REQ-RESTORE-004's `--overwrite` correctly without extra handling here
/// (the caller already checked `target_path`'s existence against `--overwrite` before this runs).
fn write_file(
    repo: &db::Repository,
    store: &store::ByteStore,
    content_id: i64,
    time_millis: i64,
    target_path: &Path,
    options: RestoreOptions,
) -> Result<Vec<String>, String> {
    let chunks = repo
        .resolve_chunks(content_id)
        .map_err(|err| err.to_string())?;
    let temp_path = temp_path_for(target_path);
    match write_chunks_to(&temp_path, store, &chunks, time_millis, options) {
        Ok(warnings) => {
            fs::rename(&temp_path, target_path).map_err(|err| err.to_string())?;
            Ok(warnings)
        }
        Err(err) => {
            let _ = fs::remove_file(&temp_path);
            Err(err)
        }
    }
}

/// `target_path`'s own file name with a suffix appended - never a bare extension replacement
/// (`Path::with_extension`), which would mishandle a dotfile or an already-extensionless name.
fn temp_path_for(target_path: &Path) -> std::path::PathBuf {
    let mut file_name = target_path
        .file_name()
        .expect("target_path is always a restored entry's own path, which always has a file name")
        .to_os_string();
    file_name.push(".dfs-restore-tmp");
    target_path.with_file_name(file_name)
}

fn write_chunks_to(
    temp_path: &Path,
    store: &store::ByteStore,
    chunks: &[db::ChunkLocation],
    time_millis: i64,
    options: RestoreOptions,
) -> Result<Vec<String>, String> {
    let mut file = fs::File::create(temp_path).map_err(|err| err.to_string())?;
    let mut warnings = Vec::new();
    for chunk in chunks {
        let (data, warning) = read_chunk(store, chunk, options.verify, options.best_effort)?;
        file.write_all(&data).map_err(|err| err.to_string())?;
        warnings.extend(warning);
    }
    file.set_modified(UNIX_EPOCH + Duration::from_millis(time_millis as u64))
        .map_err(|err| err.to_string())?;
    Ok(warnings)
}

/// Reads one chunk's full bytes from the store, honoring `verify` (compare against the chunk's
/// own recorded hash) and `best_effort` (zero-fill missing/incomplete data, or keep content that
/// fails verification, instead of failing the whole file) - REQ-RESTORE-003's two independent
/// opt-ins. Returns the bytes, plus a warning message if something was wrong but `best_effort`
/// let restoring continue anyway.
fn read_chunk(
    store: &store::ByteStore,
    chunk: &db::ChunkLocation,
    verify: bool,
    best_effort: bool,
) -> Result<(Vec<u8>, Option<String>), String> {
    let mut data = Vec::with_capacity(chunk.length as usize);
    let mut incomplete = false;
    for &(start, stop) in &chunk.extents {
        let mut buf = vec![0u8; (stop - start) as usize];
        match store.read(start, &mut buf) {
            Ok(store::ReadIntegrity::Complete) => {}
            Ok(store::ReadIntegrity::Incomplete { .. }) | Err(_) => incomplete = true,
        }
        data.extend_from_slice(&buf);
    }

    if incomplete {
        return if best_effort {
            Ok((
                data,
                Some("missing or incomplete stored data - zero-filled".to_string()),
            ))
        } else {
            Err("missing or incomplete stored data".to_string())
        };
    }

    if verify {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&data);
        let mut computed = [0u8; HASH_WIDTH];
        hasher.finalize_xof().fill(&mut computed);
        if computed.as_slice() != chunk.hash.as_slice() {
            return if best_effort {
                Ok((
                    data,
                    Some("content does not match its recorded hash - kept anyway".to_string()),
                ))
            } else {
                Err("content does not match its recorded hash".to_string())
            };
        }
    }

    Ok((data, None))
}

pub fn run(
    repo_path: &Path,
    default_path_used: bool,
    sources: &[String],
    target: &Path,
    overwrite: bool,
    verify: bool,
    best_effort: bool,
) {
    let options = RestoreOptions {
        overwrite,
        verify,
        best_effort,
    };
    match try_run(repo_path, default_path_used, sources, target, options) {
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

    fn opts(overwrite: bool, verify: bool, best_effort: bool) -> RestoreOptions {
        RestoreOptions {
            overwrite,
            verify,
            best_effort,
        }
    }

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

    /// A real chunk hash (BLAKE3 of `content`, truncated the same way the real chunker does) -
    /// tests that exercise `--verify` need this to actually match, unlike a fake placeholder.
    fn real_hash(content: &[u8]) -> [u8; HASH_WIDTH] {
        let hash = blake3::hash(content);
        let mut truncated = [0u8; HASH_WIDTH];
        truncated.copy_from_slice(&hash.as_bytes()[..HASH_WIDTH]);
        truncated
    }

    fn create_file(
        repo: &db::Repository,
        store: &store::ByteStore,
        parent: i64,
        name: &str,
        content: &[u8],
    ) {
        let (chunk_id, ranges) = repo
            .reserve_and_insert_chunk(content.len() as i64, &real_hash(content))
            .unwrap();
        store.write(ranges[0].0, content).unwrap();
        // The content-level hash is never verified by restore (only chunk hashes are - see
        // read_chunk), so a cheap, distinct placeholder is fine here.
        let content_hash = real_hash(&[content, b"-content"].concat());
        let content_id = repo
            .find_or_create_content(content.len() as i64, &content_hash, &[chunk_id])
            .unwrap();
        repo.settle_file(parent, name, 1_700_000_000_000, content_id)
            .unwrap();
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-restore-test-no-default-repository-here");
        let target = tempfile::tempdir().unwrap();

        let message = try_run(
            &repo_path,
            true,
            &["/a".to_string()],
            target.path(),
            opts(false, false, false),
        )
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
            opts(false, false, false),
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
            opts(false, false, false),
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
            opts(false, false, false),
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
            opts(true, false, false),
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
            opts(false, false, false),
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
            opts(false, false, false),
        )
        .expect_err("must fail overall - one item failed");
        assert!(message.contains("no such repository path"));
        assert!(message.contains("restored 1 file"));
    }

    #[test]
    fn try_run_verifies_correct_content_without_complaint() {
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
            opts(false, true, false),
        )
        .expect("must succeed - the content genuinely matches its recorded hash");
        assert!(message.contains("restored 1 file"));
        assert!(!message.contains("warning"));
    }

    #[test]
    fn try_run_fails_that_item_on_a_hash_mismatch_when_verify_is_requested() {
        let (repo, repo_dir, store, target_dir) = setup();
        create_file(&repo, &store, 0, "a.txt", b"hello world");
        // Corrupt the stored bytes directly, bypassing the metadata (which still records the
        // original, correct hash) - simulates on-disk bit rot or a truncated/overwritten file.
        let ranges = repo.resolve_extents(1).unwrap();
        // Same length as "hello world" (11 bytes) - the store's own extent for this chunk is
        // sized exactly for the original content, so a same-length corruption is what a restore
        // read actually sees; a longer write here would just get read back truncated.
        store.write(ranges[0].0, b"CORRUPTED!!").unwrap();
        drop(repo);
        drop(store);
        let repo_root = repo_dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            &["/a.txt".to_string()],
            target_dir.path(),
            opts(false, true, false),
        )
        .expect_err("must fail - the stored bytes no longer match the recorded hash");
        assert!(message.contains("does not match its recorded hash"));
        assert!(
            !target_dir.path().join("a.txt").exists(),
            "a failed item must not leave a partial file behind"
        );
    }

    #[test]
    fn try_run_keeps_mismatched_content_under_best_effort_and_warns() {
        let (repo, repo_dir, store, target_dir) = setup();
        create_file(&repo, &store, 0, "a.txt", b"hello world");
        let ranges = repo.resolve_extents(1).unwrap();
        // Same length as "hello world" (11 bytes) - the store's own extent for this chunk is
        // sized exactly for the original content, so a same-length corruption is what a restore
        // read actually sees; a longer write here would just get read back truncated.
        store.write(ranges[0].0, b"CORRUPTED!!").unwrap();
        drop(repo);
        drop(store);
        let repo_root = repo_dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            &["/a.txt".to_string()],
            target_dir.path(),
            opts(false, true, true),
        )
        .expect("must succeed - best-effort keeps mismatched content instead of failing");
        assert!(message.contains("restored 1 file"));
        assert!(message.contains("does not match its recorded hash"));
        assert_eq!(
            fs::read(target_dir.path().join("a.txt")).unwrap(),
            b"CORRUPTED!!",
            "best-effort keeps the mismatched content as-is, not what the hash implies"
        );
    }

    #[test]
    fn try_run_fails_that_item_on_missing_data_by_default() {
        let (repo, repo_dir, store, target_dir) = setup();
        // Reserved and recorded in the metadata, but never actually written to the store.
        let (chunk_id, _ranges) = repo
            .reserve_and_insert_chunk(5, &real_hash(b"hello"))
            .unwrap();
        let content_id = repo
            .find_or_create_content(5, &real_hash(b"content-hash"), &[chunk_id])
            .unwrap();
        repo.settle_file(0, "a.txt", 1_700_000_000_000, content_id)
            .unwrap();
        drop(repo);
        drop(store);
        let repo_root = repo_dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            &["/a.txt".to_string()],
            target_dir.path(),
            opts(false, false, false),
        )
        .expect_err("must fail - the stored data was never actually written");
        assert!(message.contains("missing or incomplete"));
        assert!(!target_dir.path().join("a.txt").exists());
    }

    #[test]
    fn try_run_zero_fills_missing_data_under_best_effort_and_warns() {
        let (repo, repo_dir, store, target_dir) = setup();
        let (chunk_id, _ranges) = repo
            .reserve_and_insert_chunk(5, &real_hash(b"hello"))
            .unwrap();
        let content_id = repo
            .find_or_create_content(5, &real_hash(b"content-hash"), &[chunk_id])
            .unwrap();
        repo.settle_file(0, "a.txt", 1_700_000_000_000, content_id)
            .unwrap();
        drop(repo);
        drop(store);
        let repo_root = repo_dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            &["/a.txt".to_string()],
            target_dir.path(),
            opts(false, false, true),
        )
        .expect("must succeed - best-effort zero-fills missing data instead of failing");
        assert!(message.contains("restored 1 file"));
        assert!(message.contains("zero-filled"));
        assert_eq!(
            fs::read(target_dir.path().join("a.txt")).unwrap(),
            vec![0u8; 5],
            "best-effort zero-fills exactly the missing chunk's own recorded length"
        );
    }

    #[test]
    fn source_basename_takes_the_last_path_component_ignoring_a_trailing_slash() {
        assert_eq!(source_basename("/backups/2024/photos"), "photos");
        assert_eq!(source_basename("/backups/2024/photos/"), "photos");
        assert_eq!(source_basename("/a"), "a");
    }
}
