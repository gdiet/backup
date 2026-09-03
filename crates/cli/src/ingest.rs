//! `dfs ingest` - REQ-INGEST-001/002/003/004/005/006/007 in `requirements/functional/ingest.md`.
//! Recursively imports one or more real filesystem paths into a repository target directory,
//! deduplicating their content along the way. The target path is resolved via `crate::target_path`
//! (REQ-INGEST-007's templated/creatable segments, DESIGN-CLI-006).

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use time::OffsetDateTime;

use crate::ignore_rules::{self, BackupIgnore, EffectiveRules};
use crate::settle::{self, SettleError};
use crate::target_path;

/// The handles one ingest run needs, threaded unchanged through every level of its recursion.
struct Ctx<'a> {
    repo: &'a db::Repository,
    store: &'a store::ByteStore,
    cdc_target_size_bits: Option<u32>,
}

/// Accumulates one ingest run's result: how many files actually landed in the repository, plus a
/// message per source-side item that was skipped rather than imported (REQ-INGEST-004) - a
/// backend failure instead aborts the run outright via `Result::Err`, so it never reaches here.
#[derive(Default)]
struct Outcome {
    imported: u64,
    warnings: Vec<String>,
}

impl Outcome {
    fn warning(message: String) -> Self {
        Self {
            imported: 0,
            warnings: vec![message],
        }
    }

    fn merge(&mut self, other: Outcome) {
        self.imported += other.imported;
        self.warnings.extend(other.warnings);
    }
}

fn try_run(
    repo_path: &Path,
    default_path_used: bool,
    sources: &[String],
    target: &str,
    reference: Option<&str>,
    force_reference: bool,
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
    // `docs/design/repository-locking.md`), same as a read-write mount session.
    let _write_lock = db::acquire_write_lock(repo_path).map_err(|err| format!("error: {err}"))?;
    let store = store::ByteStore::new(db::data_dir(repo_path), false);
    let ctx = Ctx {
        repo: &repo,
        store: &store,
        cdc_target_size_bits: repo.settings().cdc_target_size_bits(),
    };

    // REQ-INGEST-007's own "current date/time at run start" - captured once, so every
    // placeholder across the whole target path resolves against the same moment.
    let resolved_target = target_path::resolve(&repo, target, OffsetDateTime::now_utc())?;

    let source_paths: Vec<PathBuf> = sources.iter().map(PathBuf::from).collect();

    let reference_dir_id = match reference {
        Some(reference_path) => {
            let entry = match repo.resolve_path(reference_path) {
                Ok(Some(entry)) if entry.kind == db::EntryKind::Dir => entry,
                Ok(Some(_)) => {
                    return Err(format!(
                        "error: reference {reference_path} is not a directory"
                    ));
                }
                Ok(None) => {
                    return Err(format!("error: no such repository path: {reference_path}"));
                }
                Err(err) => return Err(format!("error: {err}")),
            };
            if !force_reference {
                validate_reference(&repo, entry.id, &source_paths)
                    .map_err(|err| format!("error: {err}"))?;
            }
            Some(entry.id)
        }
        None => None,
    };

    let mut outcome = Outcome::default();
    for source_path in &source_paths {
        let Some(name) = source_path.file_name().and_then(|n| n.to_str()) else {
            outcome.warnings.push(format!(
                "{}: has no usable file name - skipped",
                source_path.display()
            ));
            continue;
        };
        outcome.merge(
            ingest_entry(
                &ctx,
                source_path,
                resolved_target.id,
                name,
                &EffectiveRules::none(),
                reference_dir_id,
            )
            .map_err(|err| format!("error: {err}"))?,
        );
    }

    let mut message = format!(
        "imported {} file(s) into {}",
        outcome.imported, resolved_target.path
    );
    if !outcome.warnings.is_empty() {
        message.push_str(&format!(
            "\n{} warning(s):\n{}",
            outcome.warnings.len(),
            outcome.warnings.join("\n")
        ));
    }
    Ok(message)
}

/// Imports `source_path` (and, if a directory, everything beneath it that survives REQ-INGEST-002
/// exclusion) into `target_parent_id/name`. Returns `Ok` with a per-item warning for a
/// source-*read* failure (REQ-INGEST-004: that item is skipped, the run continues); returns `Err`
/// for a *backend* failure (a `crates/store`/metadata write), which aborts the whole run.
fn ingest_entry(
    ctx: &Ctx,
    source_path: &Path,
    target_parent_id: i64,
    name: &str,
    rules: &EffectiveRules,
    reference_dir_id: Option<i64>,
) -> Result<Outcome, String> {
    // Never follows a symlink - REQ-INGEST-001 covers files and directories only, and silently
    // resolving a symlink target risks an unbounded loop through the source tree.
    let metadata = match fs::symlink_metadata(source_path) {
        Ok(m) => m,
        Err(err) => {
            return Ok(Outcome::warning(format!(
                "{}: {err}",
                source_path.display()
            )));
        }
    };
    if metadata.is_dir() {
        ingest_dir(
            ctx,
            source_path,
            &metadata,
            target_parent_id,
            name,
            rules,
            reference_dir_id,
        )
    } else if metadata.is_file() {
        let reference = reference_children(ctx.repo, reference_dir_id)?;
        ingest_file(
            ctx,
            source_path,
            &metadata,
            target_parent_id,
            name,
            reference.as_ref(),
        )
    } else {
        Ok(Outcome::warning(format!(
            "{}: not a regular file or directory - skipped",
            source_path.display()
        )))
    }
}

fn ingest_dir(
    ctx: &Ctx,
    source_dir: &Path,
    dir_metadata: &fs::Metadata,
    target_parent_id: i64,
    name: &str,
    rules: &EffectiveRules,
    reference_dir_id: Option<i64>,
) -> Result<Outcome, String> {
    let mut outcome = Outcome::default();

    let backupignore_path = source_dir.join(".backupignore");
    let own_ignore = match fs::read_to_string(&backupignore_path) {
        Ok(content) => Some(ignore_rules::parse(&content)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => None,
        Err(err) => {
            outcome
                .warnings
                .push(format!("{}: {err}", backupignore_path.display()));
            None
        }
    };
    if matches!(own_ignore, Some(BackupIgnore::ExcludeWholeDirectory)) {
        return Ok(outcome);
    }
    let effective = match &own_ignore {
        Some(BackupIgnore::Rules(own_rules)) => rules.combined_with(own_rules),
        Some(BackupIgnore::ExcludeWholeDirectory) => {
            unreachable!("returned above already")
        }
        None => rules.clone(),
    };

    let dir_mtime = match mtime_millis(dir_metadata) {
        Ok(m) => m,
        Err(err) => {
            outcome
                .warnings
                .push(format!("{}: {err}", source_dir.display()));
            return Ok(outcome);
        }
    };

    let target_id = match ctx.repo.mkdir(target_parent_id, name, dir_mtime) {
        Ok(id) => id,
        Err(db::Error::EntryAlreadyExists { .. }) => {
            match find_child(ctx.repo, target_parent_id, name)? {
                Some(entry) if entry.kind == db::EntryKind::Dir => entry.id,
                _ => {
                    outcome.warnings.push(format!(
                        "{}: a non-directory entry already exists at the target - skipped",
                        source_dir.display()
                    ));
                    return Ok(outcome);
                }
            }
        }
        Err(err) => return Err(format!("{}: {err}", source_dir.display())),
    };

    let reference = reference_children(ctx.repo, reference_dir_id)?;

    let entries = match fs::read_dir(source_dir) {
        Ok(entries) => entries,
        Err(err) => {
            outcome
                .warnings
                .push(format!("{}: {err}", source_dir.display()));
            return Ok(outcome);
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                outcome
                    .warnings
                    .push(format!("{}: {err}", source_dir.display()));
                continue;
            }
        };
        let child_name_os = entry.file_name();
        let Some(child_name) = child_name_os.to_str() else {
            outcome.warnings.push(format!(
                "{}: non-UTF-8 file name - skipped",
                entry.path().display()
            ));
            continue;
        };
        let child_metadata = match entry.metadata() {
            Ok(m) => m,
            Err(err) => {
                outcome
                    .warnings
                    .push(format!("{}: {err}", entry.path().display()));
                continue;
            }
        };
        let is_dir = child_metadata.is_dir();
        if effective.excludes(child_name, is_dir) {
            continue;
        }

        let child_outcome = if is_dir {
            let child_reference_dir_id = reference
                .as_ref()
                .and_then(|children| children.get(child_name))
                .filter(|entry| entry.kind == db::EntryKind::Dir)
                .map(|entry| entry.id);
            ingest_dir(
                ctx,
                &entry.path(),
                &child_metadata,
                target_id,
                child_name,
                &effective.propagate_into(child_name),
                child_reference_dir_id,
            )
        } else if child_metadata.is_file() {
            ingest_file(
                ctx,
                &entry.path(),
                &child_metadata,
                target_id,
                child_name,
                reference.as_ref(),
            )
        } else {
            Ok(Outcome::warning(format!(
                "{}: not a regular file or directory - skipped",
                entry.path().display()
            )))
        };
        outcome.merge(child_outcome?);
    }

    // REQ-INGEST-005: overrides every per-child `touch()` bump `mkdir`/`settle_file` above already
    // applied while populating this directory, so it ends up carrying the source directory's own
    // modification time rather than "when the import happened" - `docs/design/
    // directory-mtime-touch.md`'s directed-import case. Applied unconditionally, even for a
    // directory with no children at all.
    ctx.repo
        .set_mtime(target_id, dir_mtime)
        .map_err(|err| format!("{}: {err}", source_dir.display()))?;

    Ok(outcome)
}

/// Imports one source file. Reuses a matching `reference` entry's already-settled content without
/// reading this file again if its name, size, and modification time all match (REQ-INGEST-003);
/// otherwise reads, chunks, and hashes it via [`settle::settle`], the same engine a mounted
/// write's own background settle job uses.
fn ingest_file(
    ctx: &Ctx,
    source_path: &Path,
    metadata: &fs::Metadata,
    target_parent_id: i64,
    name: &str,
    reference: Option<&HashMap<String, db::Entry>>,
) -> Result<Outcome, String> {
    let size = metadata.len();
    let mtime_millis = match mtime_millis(metadata) {
        Ok(m) => m,
        Err(err) => {
            return Ok(Outcome::warning(format!(
                "{}: {err}",
                source_path.display()
            )));
        }
    };

    if let Some(reference_entry) = reference.and_then(|children| children.get(name))
        && reference_entry.kind == db::EntryKind::File
        && reference_entry.size == size
        && reference_entry.time_millis == mtime_millis
    {
        let content_id = reference_entry
            .content_id
            .expect("a file entry always has a content_id (chk_tree_entries_kind_content_id)");
        return match ctx
            .repo
            .settle_file(target_parent_id, name, mtime_millis, content_id)
        {
            Ok(_) => Ok(Outcome {
                imported: 1,
                warnings: Vec::new(),
            }),
            Err(err) => Err(format!("{}: {err}", source_path.display())),
        };
    }

    let mut file = match fs::File::open(source_path) {
        Ok(file) => file,
        Err(err) => {
            return Ok(Outcome::warning(format!(
                "{}: {err}",
                source_path.display()
            )));
        }
    };
    let content_id = settle::settle(ctx.repo, ctx.store, ctx.cdc_target_size_bits, size, {
        use std::io::Read;
        move |_pos, len| {
            let mut buf = vec![0u8; len as usize];
            file.read_exact(&mut buf)?;
            Ok(buf)
        }
    });
    match content_id {
        Ok(content_id) => {
            match ctx
                .repo
                .settle_file(target_parent_id, name, mtime_millis, content_id)
            {
                Ok(_) => Ok(Outcome {
                    imported: 1,
                    warnings: Vec::new(),
                }),
                Err(err) => Err(format!("{}: {err}", source_path.display())),
            }
        }
        Err(SettleError::Read(err)) => Ok(Outcome::warning(format!(
            "{}: {err}",
            source_path.display()
        ))),
        Err(err @ (SettleError::Write(_) | SettleError::Db(_))) => {
            Err(format!("{}: {err}", source_path.display()))
        }
    }
}

/// `metadata`'s modification time as Unix epoch milliseconds - negative for a source predating
/// 1970, which `tree_entries.time` (a plain `INTEGER`) accepts without complaint.
fn mtime_millis(metadata: &fs::Metadata) -> io::Result<i64> {
    let modified = metadata.modified()?;
    Ok(match modified.duration_since(UNIX_EPOCH) {
        Ok(since_epoch) => since_epoch.as_millis() as i64,
        Err(before_epoch) => -(before_epoch.duration().as_millis() as i64),
    })
}

/// The live entry named `name` directly inside `parent_id`, if any - used to recover from
/// `db::Error::EntryAlreadyExists` and decide whether the existing entry is itself a directory
/// ingest can descend into.
fn find_child(
    repo: &db::Repository,
    parent_id: i64,
    name: &str,
) -> Result<Option<db::Entry>, String> {
    repo.list_children(parent_id)
        .map(|children| {
            children
                .into_iter()
                .find(|(child_name, _)| child_name == name)
                .map(|(_, entry)| entry)
        })
        .map_err(|err| err.to_string())
}

/// `reference_dir_id`'s own direct children, by name - `None` if there is no reference directory
/// at this level (either no `--reference` was given, or an ancestor's reference lookup already
/// missed). Looked up once per directory rather than once per file, since both REQ-INGEST-003's
/// file-content matching and this same directory's own children's REQ-INGEST-003 lookups one
/// level down need it.
fn reference_children(
    repo: &db::Repository,
    reference_dir_id: Option<i64>,
) -> Result<Option<HashMap<String, db::Entry>>, String> {
    match reference_dir_id {
        None => Ok(None),
        Some(id) => repo
            .list_children(id)
            .map(|children| Some(children.into_iter().collect()))
            .map_err(|err| err.to_string()),
    }
}

/// REQ-INGEST-006: checks that `reference_dir_id`'s own contents roughly correspond to what
/// `sources` are about to import, before [`ingest_file`] is trusted to use it for REQ-INGEST-003's
/// accelerated matching. The comparison covers `reference_dir_id`'s and `sources`'s own top-level
/// entries, plus - for any name present as a directory on both sides - that directory's own
/// immediate children one level deeper. Fails if the larger side's entry count exceeds 1.6 times
/// the number of entries actually shared between the two sides, plus 1.
fn validate_reference(
    repo: &db::Repository,
    reference_dir_id: i64,
    sources: &[PathBuf],
) -> Result<(), String> {
    let reference_top: HashMap<String, db::Entry> = repo
        .list_children(reference_dir_id)
        .map_err(|err| err.to_string())?
        .into_iter()
        .collect();

    let mut total_reference = reference_top.len();
    let mut total_sources = 0usize;
    let mut total_shared = 0usize;

    for source in sources {
        let Some(name) = source.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Ok(source_metadata) = fs::symlink_metadata(source) else {
            continue;
        };
        let source_is_dir = source_metadata.is_dir();
        total_sources += 1;

        let Some(reference_entry) = reference_top.get(name) else {
            continue;
        };
        let reference_is_dir = reference_entry.kind == db::EntryKind::Dir;
        if reference_is_dir != source_is_dir {
            continue;
        }
        total_shared += 1;
        if source_is_dir {
            let (deeper_sources, deeper_reference, deeper_shared) =
                compare_one_level_deeper(repo, source, reference_entry.id);
            total_sources += deeper_sources;
            total_reference += deeper_reference;
            total_shared += deeper_shared;
        }
    }

    // max(reference, sources) > shared * 1.6 + 1, in exact integer arithmetic (1.6 = 8/5):
    // multiplying both sides by 5 avoids floating-point rounding for what is otherwise a purely
    // integer comparison.
    let max_count = total_reference.max(total_sources) as u64;
    let shared_count = total_shared as u64;
    if 5 * max_count > 8 * shared_count + 5 {
        return Err(format!(
            "reference does not correspond closely enough to the sources being ingested ({total_shared} \
             of {total_reference} reference / {total_sources} source entries considered match) - \
             pass --force-reference to use it anyway"
        ));
    }
    Ok(())
}

/// One directory's own immediate children on both sides, for [`validate_reference`]'s one-level-
/// deeper comparison - `(source_count, reference_count, shared_count)`. A listing failure on
/// either side (the source directory disappeared, a transient database error) contributes nothing
/// rather than aborting validation outright: this is a best-effort correspondence check, not part
/// of the ingest write path itself.
fn compare_one_level_deeper(
    repo: &db::Repository,
    source_dir: &Path,
    reference_dir_id: i64,
) -> (usize, usize, usize) {
    let Ok(reference_children) = repo.list_children(reference_dir_id) else {
        return (0, 0, 0);
    };
    let reference_children: HashMap<String, db::Entry> = reference_children.into_iter().collect();
    let Ok(source_entries) = fs::read_dir(source_dir) else {
        return (0, reference_children.len(), 0);
    };

    let mut source_count = 0usize;
    let mut shared_count = 0usize;
    for entry in source_entries.flatten() {
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Ok(source_is_dir) = entry.metadata().map(|m| m.is_dir()) else {
            continue;
        };
        source_count += 1;
        if let Some(reference_entry) = reference_children.get(&name)
            && (reference_entry.kind == db::EntryKind::Dir) == source_is_dir
        {
            shared_count += 1;
        }
    }
    (source_count, reference_children.len(), shared_count)
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    repo_path: &Path,
    default_path_used: bool,
    sources: &[String],
    target: &str,
    reference: Option<&str>,
    force_reference: bool,
) {
    match try_run(
        repo_path,
        default_path_used,
        sources,
        target,
        reference,
        force_reference,
    ) {
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
    use std::time::{Duration, SystemTime};

    fn setup() -> (db::Repository, tempfile::TempDir, tempfile::TempDir) {
        let repo_dir = tempfile::tempdir().unwrap();
        let repo_root = repo_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            db::RepositorySettings::new(Some(20), 1_700_000_000_000),
        )
        .unwrap();
        let repo = db::open_repository(&repo_root).unwrap();
        (repo, repo_dir, tempfile::tempdir().unwrap())
    }

    fn set_mtime(path: &Path, millis: i64) {
        let time = UNIX_EPOCH + Duration::from_millis(millis as u64);
        filetime_set(path, time);
    }

    /// Sets a path's modification time without adding a new dependency - `std::fs::File::
    /// set_modified` covers both files and directories on every platform this project targets.
    fn filetime_set(path: &Path, time: SystemTime) {
        if path.is_dir() {
            // There is no directory-mtime setter in `std`; open it (read-only is enough on Unix)
            // and use the same `set_modified` call a file uses.
            fs::File::open(path).unwrap().set_modified(time).unwrap();
        } else {
            fs::File::options()
                .write(true)
                .open(path)
                .unwrap()
                .set_modified(time)
                .unwrap();
        }
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-ingest-test-no-default-repository-here");
        let source_dir = tempfile::tempdir().unwrap();

        let message = try_run(
            &repo_path,
            true,
            &[source_dir.path().display().to_string()],
            "/",
            None,
            false,
        )
        .expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the actionable default-path message, got: {message}"
        );
    }

    #[test]
    fn try_run_refuses_a_nonexistent_target() {
        let (repo, repo_dir, source_root) = setup();
        drop(repo);
        let repo_root = repo_dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            &[source_root.path().display().to_string()],
            "/no-such-target",
            None,
            false,
        )
        .expect_err("must fail - the target does not exist");
        assert!(message.contains("no such repository path"));
    }

    #[test]
    fn try_run_imports_into_a_templated_creatable_target_path() {
        let (repo, repo_dir, source_root) = setup();
        drop(repo);
        let repo_root = repo_dir.path().join("repo");
        fs::write(source_root.path().join("a.txt"), b"hello").unwrap();

        // REQ-INGEST-007: neither /backups nor its dated subdirectory exist yet - the leading `+`
        // creates both, since marking one segment creatable cascades to the segments below it.
        let message = try_run(
            &repo_root,
            false,
            &[source_root.path().display().to_string()],
            "/+backups/[yyyy]",
            None,
            false,
        )
        .expect("must succeed - the target path is creatable");
        assert!(message.contains("imported"));

        let repo = db::open_repository(&repo_root).unwrap();
        let backups = repo.resolve_path("/backups").unwrap().unwrap();
        assert_eq!(backups.kind, db::EntryKind::Dir);
        let year = time::OffsetDateTime::now_utc().year();
        let dated = repo
            .resolve_path(&format!("/backups/{year}"))
            .unwrap()
            .unwrap();
        assert_eq!(dated.kind, db::EntryKind::Dir);
    }

    #[test]
    fn try_run_imports_a_single_file_preserving_bytes_and_mtime() {
        let (repo, repo_dir, source_root) = setup();
        drop(repo);
        let repo_root = repo_dir.path().join("repo");
        let file_path = source_root.path().join("a.txt");
        fs::write(&file_path, b"hello world").unwrap();
        set_mtime(&file_path, 1_650_000_000_000);

        let message = try_run(
            &repo_root,
            false,
            &[file_path.display().to_string()],
            "/",
            None,
            false,
        )
        .expect("must succeed");
        assert!(message.contains("imported 1 file"));

        let repo = db::open_repository_read_only(&repo_root).unwrap();
        let entry = repo.resolve_path("/a.txt").unwrap().unwrap();
        assert_eq!(entry.size, 11);
        assert_eq!(entry.time_millis, 1_650_000_000_000);
        let content_id = entry.content_id.unwrap();
        let mut bytes = Vec::new();
        for (start, stop) in repo.resolve_extents(content_id).unwrap() {
            let mut buf = vec![0u8; (stop - start) as usize];
            let store = store::ByteStore::new(db::data_dir(&repo_root), true);
            store.read(start, &mut buf).unwrap();
            bytes.extend(buf);
        }
        assert_eq!(bytes, b"hello world");
    }

    #[test]
    fn try_run_imports_a_directory_recursively_preserving_structure_and_mtimes() {
        let (repo, repo_dir, source_root) = setup();
        drop(repo);
        let repo_root = repo_dir.path().join("repo");
        let photos = source_root.path().join("photos");
        fs::create_dir(&photos).unwrap();
        fs::write(photos.join("one.jpg"), b"one").unwrap();
        let nested = photos.join("nested");
        fs::create_dir(&nested).unwrap();
        fs::write(nested.join("two.jpg"), b"two").unwrap();
        set_mtime(&nested, 1_600_000_000_000);
        set_mtime(&photos, 1_600_000_000_000);

        let message = try_run(
            &repo_root,
            false,
            &[photos.display().to_string()],
            "/",
            None,
            false,
        )
        .expect("must succeed");
        assert!(message.contains("imported 2 file"));

        let repo = db::open_repository_read_only(&repo_root).unwrap();
        assert!(repo.resolve_path("/photos/one.jpg").unwrap().is_some());
        assert!(
            repo.resolve_path("/photos/nested/two.jpg")
                .unwrap()
                .is_some()
        );
        let dir_entry = repo.resolve_path("/photos").unwrap().unwrap();
        assert_eq!(
            dir_entry.time_millis, 1_600_000_000_000,
            "the directory's own mtime must be the source's, not just whatever the last child \
             settle happened to bump it to"
        );
    }

    #[test]
    fn try_run_excludes_entries_matched_by_a_backupignore_rule() {
        let (repo, repo_dir, source_root) = setup();
        drop(repo);
        let repo_root = repo_dir.path().join("repo");
        let src = source_root.path().join("src");
        fs::create_dir(&src).unwrap();
        fs::write(src.join(".backupignore"), "*.log\n").unwrap();
        fs::write(src.join("keep.txt"), b"keep").unwrap();
        fs::write(src.join("drop.log"), b"drop").unwrap();

        try_run(
            &repo_root,
            false,
            &[src.display().to_string()],
            "/",
            None,
            false,
        )
        .expect("must succeed");

        let repo = db::open_repository_read_only(&repo_root).unwrap();
        assert!(repo.resolve_path("/src/keep.txt").unwrap().is_some());
        assert!(repo.resolve_path("/src/drop.log").unwrap().is_none());
    }

    #[test]
    fn try_run_excludes_a_whole_directory_holding_an_empty_backupignore() {
        let (repo, repo_dir, source_root) = setup();
        drop(repo);
        let repo_root = repo_dir.path().join("repo");
        let src = source_root.path().join("src");
        fs::create_dir(&src).unwrap();
        let excluded = src.join("excluded");
        fs::create_dir(&excluded).unwrap();
        fs::write(excluded.join(".backupignore"), "").unwrap();
        fs::write(excluded.join("inside.txt"), b"never imported").unwrap();
        fs::write(src.join("kept.txt"), b"kept").unwrap();

        try_run(
            &repo_root,
            false,
            &[src.display().to_string()],
            "/",
            None,
            false,
        )
        .expect("must succeed");

        let repo = db::open_repository_read_only(&repo_root).unwrap();
        assert!(repo.resolve_path("/src/kept.txt").unwrap().is_some());
        assert!(repo.resolve_path("/src/excluded").unwrap().is_none());
    }

    #[test]
    fn try_run_propagates_a_multi_segment_backupignore_rule_into_the_named_subdirectory_only() {
        let (repo, repo_dir, source_root) = setup();
        drop(repo);
        let repo_root = repo_dir.path().join("repo");
        let src = source_root.path().join("src");
        fs::create_dir(&src).unwrap();
        fs::write(src.join(".backupignore"), "log/*.log\n").unwrap();
        let log_dir = src.join("log");
        fs::create_dir(&log_dir).unwrap();
        fs::write(log_dir.join("a.log"), b"excluded").unwrap();
        fs::write(log_dir.join("a.txt"), b"kept").unwrap();
        let other_dir = src.join("other");
        fs::create_dir(&other_dir).unwrap();
        fs::write(
            other_dir.join("a.log"),
            b"kept - rule only applies inside log/",
        )
        .unwrap();

        try_run(
            &repo_root,
            false,
            &[src.display().to_string()],
            "/",
            None,
            false,
        )
        .expect("must succeed");

        let repo = db::open_repository_read_only(&repo_root).unwrap();
        assert!(repo.resolve_path("/src/log/a.log").unwrap().is_none());
        assert!(repo.resolve_path("/src/log/a.txt").unwrap().is_some());
        assert!(repo.resolve_path("/src/other/a.log").unwrap().is_some());
    }

    #[test]
    fn try_run_skips_a_source_that_disappeared_with_a_warning_and_continues() {
        // A source that cannot be read at all (REQ-INGEST-004's "something that disappears
        // mid-run") - simulated here as a path that was simply never there, which fails the very
        // first `fs::symlink_metadata` call the same way a permission error or a mid-run deletion
        // would. Unlike a permission bit, this is reliable regardless of which user runs the
        // test suite (in particular, root - which this sandboxed environment always runs as -
        // bypasses ordinary file permission checks entirely, so a chmod-based test would not
        // actually exercise this path here).
        let (repo, repo_dir, source_root) = setup();
        drop(repo);
        let repo_root = repo_dir.path().join("repo");
        let missing = source_root.path().join("does-not-exist.txt");
        fs::write(source_root.path().join("ok.txt"), b"ok").unwrap();

        let message = try_run(
            &repo_root,
            false,
            &[
                missing.display().to_string(),
                source_root.path().join("ok.txt").display().to_string(),
            ],
            "/",
            None,
            false,
        )
        .expect("must succeed overall - only one item was unreadable");
        assert!(message.contains("imported 1 file"));
        assert!(message.contains("1 warning"));

        let repo = db::open_repository_read_only(&repo_root).unwrap();
        assert!(repo.resolve_path("/ok.txt").unwrap().is_some());
        assert!(repo.resolve_path("/does-not-exist.txt").unwrap().is_none());
    }

    #[test]
    fn try_run_accelerates_a_matching_file_via_reference_without_rereading_it() {
        let (repo, repo_dir, source_root) = setup();
        let repo_root = repo_dir.path().join("repo");
        let store = store::ByteStore::new(db::data_dir(&repo_root), false);

        // Build a reference tree directly, with content that deliberately differs from what the
        // real source file holds below - REQ-INGEST-003 says a size/mtime match reuses the
        // reference's own content "without re-reading" the source, so if acceleration actually
        // happened, the reference's bytes (not the source's real, different bytes) end up stored.
        let (chunk_id, ranges) = repo.reserve_and_insert_chunk(3, &[1u8; 20]).unwrap();
        store.write(ranges[0].0, b"REF").unwrap();
        let reference_content_id = repo
            .find_or_create_content(3, &[2u8; 20], &[chunk_id])
            .unwrap();
        repo.settle_file(0, "a.txt", 1_650_000_000_000, reference_content_id)
            .unwrap();
        let reference_dir_id = repo.mkdir(0, "reference", 1_650_000_000_000).unwrap();
        repo.settle_file(
            reference_dir_id,
            "a.txt",
            1_650_000_000_000,
            reference_content_id,
        )
        .unwrap();
        let target_dir_id = repo.mkdir(0, "target", 1_650_000_000_000).unwrap();
        drop(repo);
        drop(store);

        let file_path = source_root.path().join("a.txt");
        fs::write(&file_path, b"NEW").unwrap();
        set_mtime(&file_path, 1_650_000_000_000);
        let _ = target_dir_id;

        try_run(
            &repo_root,
            false,
            &[file_path.display().to_string()],
            "/target",
            Some("/reference"),
            false,
        )
        .expect("must succeed");

        let repo = db::open_repository_read_only(&repo_root).unwrap();
        let entry = repo.resolve_path("/target/a.txt").unwrap().unwrap();
        assert_eq!(
            entry.content_id,
            Some(reference_content_id),
            "a size/mtime match must link the reference's content id directly"
        );
    }

    #[test]
    fn try_run_reads_a_file_normally_when_reference_mtime_does_not_match() {
        let (repo, repo_dir, source_root) = setup();
        let repo_root = repo_dir.path().join("repo");
        let (chunk_id, ranges) = repo.reserve_and_insert_chunk(3, &[1u8; 20]).unwrap();
        let store = store::ByteStore::new(db::data_dir(&repo_root), false);
        store.write(ranges[0].0, b"REF").unwrap();
        let reference_content_id = repo
            .find_or_create_content(3, &[2u8; 20], &[chunk_id])
            .unwrap();
        let reference_dir_id = repo.mkdir(0, "reference", 1_650_000_000_000).unwrap();
        repo.settle_file(
            reference_dir_id,
            "a.txt",
            1_650_000_000_000,
            reference_content_id,
        )
        .unwrap();
        drop(repo);
        drop(store);

        let file_path = source_root.path().join("a.txt");
        fs::write(&file_path, b"NEW").unwrap();
        // Deliberately not matching the reference's recorded mtime (1_650_000_000_000).
        set_mtime(&file_path, 1_651_000_000_000);

        try_run(
            &repo_root,
            false,
            &[file_path.display().to_string()],
            "/",
            Some("/reference"),
            false,
        )
        .expect("must succeed");

        let repo = db::open_repository_read_only(&repo_root).unwrap();
        let entry = repo.resolve_path("/a.txt").unwrap().unwrap();
        assert_ne!(
            entry.content_id,
            Some(reference_content_id),
            "an mtime mismatch must fall back to actually reading the source"
        );
    }

    #[test]
    fn try_run_aborts_when_the_reference_barely_corresponds_to_the_sources() {
        let (repo, repo_dir, source_root) = setup();
        let repo_root = repo_dir.path().join("repo");
        // A reference with entries that share almost nothing in common with the sources below.
        for i in 0..10 {
            repo.mkdir(0, &format!("unrelated-{i}"), 1_650_000_000_000)
                .unwrap();
        }
        drop(repo);

        fs::write(source_root.path().join("a.txt"), b"a").unwrap();

        let message = try_run(
            &repo_root,
            false,
            &[source_root.path().join("a.txt").display().to_string()],
            "/",
            Some("/"),
            false,
        )
        .expect_err("must fail - the reference does not correspond to the sources");
        assert!(message.contains("does not correspond closely enough"));
    }

    #[test]
    fn try_run_force_reference_bypasses_the_correspondence_check() {
        let (repo, repo_dir, source_root) = setup();
        let repo_root = repo_dir.path().join("repo");
        for i in 0..10 {
            repo.mkdir(0, &format!("unrelated-{i}"), 1_650_000_000_000)
                .unwrap();
        }
        drop(repo);

        fs::write(source_root.path().join("a.txt"), b"a").unwrap();

        let message = try_run(
            &repo_root,
            false,
            &[source_root.path().join("a.txt").display().to_string()],
            "/",
            Some("/"),
            true,
        );
        assert!(
            message.is_ok(),
            "must succeed once --force-reference bypasses the check: {message:?}"
        );
    }
}
