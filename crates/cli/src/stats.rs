//! `dfs stats` - REQ-QUERY-003 in `requirements/functional/query.md`. Repository-wide or
//! path-scoped item counts and size statistics, without mounting. Only the repository-wide report
//! has the facts that belong to the repository as a whole: its age, its chunking target size, the
//! stored chunks and extents, the extent of `data/`, the metadata size, and the soft-deleted
//! entries.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::time_format::{TimeDisplay, format_time};

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_millis() as i64
}

fn try_run(
    repo_path: &Path,
    default_path_used: bool,
    target_path: &str,
    assume_read_only_medium: bool,
    display: TimeDisplay,
) -> Result<String, String> {
    // DESIGN-METADATA-013: validates an explicit --assume-read-only-medium against what actually
    // happens, rather than trusting it blindly - see the called function's own doc comment.
    let repo = match db::open_repository_read_only_with_medium_assertion(
        repo_path,
        assume_read_only_medium,
    ) {
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

    let entry = match repo.resolve_path(target_path) {
        Ok(Some(entry)) if entry.kind == db::EntryKind::Dir => entry,
        Ok(Some(_)) => return Err(format!("error: {target_path} is not a directory")),
        Ok(None) => return Err(format!("error: no such repository path: {target_path}")),
        Err(err) => return Err(format!("error: {err}")),
    };

    // REQ-QUERY-003: the storage and metadata facts are repository-wide only - id 0 is the only
    // entry that is "the whole repository", never merely a directory that happens to be empty of
    // ancestors.
    if entry.id == 0 {
        let stats = repo.stats().map_err(|err| format!("error: {err}"))?;
        let repository = repo
            .repository_stats()
            .map_err(|err| format!("error: {err}"))?;
        Ok(format_stats(
            target_path,
            &stats,
            Some(&RepositoryFacts {
                creation_time_millis: repo.settings().creation_time_millis(),
                cdc_target_size_bits: repo.settings().cdc_target_size_bits(),
                stats: repository,
            }),
            display,
        ))
    } else {
        let stats = repo
            .stats_for(entry.id)
            .map_err(|err| format!("error: {err}"))?;
        Ok(format_stats(target_path, &stats, None, display))
    }
}

/// What only the repository-wide report has, besides [`db::Stats`].
struct RepositoryFacts {
    creation_time_millis: i64,
    cdc_target_size_bits: u32,
    stats: db::RepositoryStats,
}

/// The width of a row's label column, colon included, plus one separating space - wide enough for
/// the longest label.
const LABEL_WIDTH: usize = 16;

fn row(label: &str, value: impl std::fmt::Display) -> String {
    format!("{:<LABEL_WIDTH$}{value}", format!("{label}:"))
}

fn format_stats(
    target_path: &str,
    stats: &db::Stats,
    repository: Option<&RepositoryFacts>,
    display: TimeDisplay,
) -> String {
    let mut lines = vec![
        format!(
            "{target_path}: {} dir(s), {} file(s)",
            stats.dirs, stats.files
        ),
        row("logical size", size_label(stats.logical_size)),
        row("physical size", size_label(stats.physical_size)),
        row(
            "dedup ratio",
            dedup_ratio_label(stats.logical_size, stats.physical_size),
        ),
    ];
    // Repository-wide, the chunk row counts every stored chunk instead (see below).
    if repository.is_none() {
        lines.push(row(
            "chunks",
            chunks_label(stats.chunks, stats.physical_size),
        ));
    }
    lines.push(row("empty files", stats.empty_files));
    if let Some(average) = stats.logical_size.checked_div(stats.files) {
        lines.push(row("avg file size", size_label(average)));
    }
    if let (Some(oldest), Some(newest)) = (stats.oldest_file_time, stats.newest_file_time) {
        lines.push(row(
            "file mtimes",
            format!(
                "{} to {}",
                format_time(oldest, display),
                format_time(newest, display)
            ),
        ));
    }
    if let Some(repository) = repository {
        let stored = &repository.stats;
        lines.push(row(
            "repository age",
            format!(
                "{} (created {})",
                age_label(repository.creation_time_millis),
                format_time(repository.creation_time_millis, display)
            ),
        ));
        lines.push(row(
            "cdc target",
            format!(
                "{} bits ({})",
                repository.cdc_target_size_bits,
                human_size(1u64 << repository.cdc_target_size_bits)
            ),
        ));
        lines.push(row(
            "chunks",
            chunks_label(stored.chunks, stored.chunk_bytes),
        ));
        lines.push(row(
            "chunk extents",
            extents_label(stored.chunk_extents, stored.chunks),
        ));
        lines.push(row("data end", size_label(stored.data_end)));
        lines.push(row("unused in data", size_label(stored.data_unused)));
        lines.push(row("metadata size", size_label(stored.metadata_size)));
        lines.push(row(
            "soft-deleted",
            format!(
                "{} dir(s), {} file(s), not counted above",
                stored.deleted_dirs, stored.deleted_files
            ),
        ));
    }
    lines.join("\n")
}

/// `logical_size / physical_size` and the share of space that saves, e.g. `"2.70x (63.0 % saved)"`.
/// The label is `"n/a"` when `physical_size` is `0` (an empty scope), since the ratio is
/// meaningless with nothing actually stored.
fn dedup_ratio_label(logical_size: u64, physical_size: u64) -> String {
    if physical_size == 0 {
        return "n/a".to_string();
    }
    let ratio = logical_size as f64 / physical_size as f64;
    let saved_percent =
        100.0 * logical_size.saturating_sub(physical_size) as f64 / logical_size.max(1) as f64;
    format!("{ratio:.2}x ({saved_percent:.1} % saved)")
}

/// A byte count in full, followed by its human-readable form from one KiB on.
fn size_label(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} bytes")
    } else {
        format!("{bytes} bytes ({})", human_size(bytes))
    }
}

/// `bytes` in binary units with one decimal, e.g. `"1.4 MiB"`.
fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["bytes", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} bytes")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// `chunks`, with the average chunk size once there is at least one chunk.
fn chunks_label(chunks: u64, chunk_bytes: u64) -> String {
    if chunks == 0 {
        return "0".to_string();
    }
    format!("{chunks} (average {})", human_size(chunk_bytes / chunks))
}

/// `extents`, with how many each chunk has on average once there is at least one chunk.
fn extents_label(extents: u64, chunks: u64) -> String {
    if chunks == 0 {
        return extents.to_string();
    }
    format!(
        "{extents} ({:.2} per chunk)",
        extents as f64 / chunks as f64
    )
}

/// How long ago `creation_time_millis` was, as a whole number of days - REQ-STORAGE-008's
/// millisecond precision is not itself meaningful to a human reading a repository's age.
fn age_label(creation_time_millis: i64) -> String {
    let age_days = (now_millis() - creation_time_millis).max(0) / 86_400_000;
    format!("{age_days} day(s)")
}

pub fn run(
    repo_path: &Path,
    default_path_used: bool,
    target_path: &str,
    assume_read_only_medium: bool,
    display: TimeDisplay,
) {
    match try_run(
        repo_path,
        default_path_used,
        target_path,
        assume_read_only_medium,
        display,
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

    fn setup() -> (db::Repository, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        db::init_repository(
            &repo_root,
            db::RepositorySettings::new(20, 1_700_000_000_000),
        )
        .unwrap();
        let repo = db::open_repository(&repo_root).unwrap();
        (repo, dir)
    }

    fn create_file(repo: &db::Repository, parent: i64, name: &str, length: i64, hash_byte: u8) {
        let (chunk_id, _ranges) = repo
            .reserve_and_insert_chunk(length, &[hash_byte; 20])
            .unwrap();
        let content_id = repo
            .find_or_create_content(length, &[hash_byte.wrapping_add(1); 20], &[chunk_id])
            .unwrap();
        repo.settle_file(parent, name, 1_700_000_000_000, content_id)
            .unwrap();
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-stats-test-no-default-repository-here");

        let message = try_run(&repo_path, true, "/", false, TimeDisplay::Utc)
            .expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the actionable default-path message, got: {message}"
        );
    }

    #[test]
    fn try_run_reports_repository_wide_stats_including_age() {
        let (repo, dir) = setup();
        create_file(&repo, 0, "a.txt", 10, 0xAA);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message =
            try_run(&repo_root, false, "/", false, TimeDisplay::Utc).expect("must succeed");
        assert!(message.contains("1 file(s)"));
        assert!(message.contains("logical size:   10 bytes"));
        assert!(message.contains("repository age"));
        assert!(
            message.contains("created 2023-11-14T22:13:20Z"),
            "got: {message}"
        );
    }

    #[test]
    fn try_run_reports_the_storage_facts_repository_wide() {
        let (repo, dir) = setup();
        create_file(&repo, 0, "a.txt", 10, 0xAA);
        create_file(&repo, 0, "b.txt", 30, 0xBB);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message =
            try_run(&repo_root, false, "/", false, TimeDisplay::Utc).expect("must succeed");
        assert!(
            message.contains("cdc target:     20 bits (1.0 MiB)"),
            "got: {message}"
        );
        assert!(
            message.contains("chunks:         2 (average 20 bytes)"),
            "got: {message}"
        );
        assert!(
            message.contains("chunk extents:  2 (1.00 per chunk)"),
            "got: {message}"
        );
        assert!(
            message.contains("data end:       40 bytes"),
            "got: {message}"
        );
        assert!(
            message.contains("unused in data: 0 bytes"),
            "got: {message}"
        );
        assert!(message.contains("metadata size:  "), "got: {message}");
        assert!(
            message.contains("soft-deleted:   0 dir(s), 0 file(s), not counted above"),
            "got: {message}"
        );
        assert!(message.contains("empty files:    0"), "got: {message}");
        assert!(
            message.contains("avg file size:  20 bytes"),
            "got: {message}"
        );
        assert!(
            message.contains("file mtimes:    2023-11-14T22:13:20Z to 2023-11-14T22:13:20Z"),
            "got: {message}"
        );
    }

    #[test]
    fn try_run_reports_chunks_and_file_facts_for_a_path_but_no_repository_facts() {
        let (repo, dir) = setup();
        let a_id = repo.mkdir(0, "a", 1_700_000_000_000).unwrap();
        create_file(&repo, a_id, "in-a.txt", 10, 0xAA);
        create_file(&repo, 0, "outside.txt", 99, 0xBB);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message =
            try_run(&repo_root, false, "/a", false, TimeDisplay::Utc).expect("must succeed");
        assert!(
            message.contains("chunks:         1 (average 10 bytes)"),
            "got: {message}"
        );
        assert!(
            message.contains("file mtimes:    2023-11-14T22:13:20Z to 2023-11-14T22:13:20Z"),
            "--utc applies to a path's file times: {message}"
        );
        for repository_wide in [
            "cdc target",
            "chunk extents",
            "data end",
            "unused in data",
            "metadata size",
            "soft-deleted",
            "repository age",
        ] {
            assert!(
                !message.contains(repository_wide),
                "path-scoped stats must not report {repository_wide}: {message}"
            );
        }
    }

    #[test]
    fn dedup_ratio_label_gives_the_ratio_and_the_share_of_space_saved() {
        assert_eq!(dedup_ratio_label(270, 100), "2.70x (63.0 % saved)");
        assert_eq!(dedup_ratio_label(100, 100), "1.00x (0.0 % saved)");
        assert_eq!(dedup_ratio_label(0, 0), "n/a");
    }

    #[test]
    fn human_size_uses_binary_units_with_one_decimal() {
        assert_eq!(human_size(1023), "1023 bytes");
        assert_eq!(human_size(1024), "1.0 KiB");
        assert_eq!(human_size(1_468_006), "1.4 MiB");
        assert_eq!(human_size(2_234_840_141_596), "2.0 TiB");
    }

    #[test]
    fn size_label_adds_the_human_form_from_one_kib_on() {
        assert_eq!(size_label(10), "10 bytes");
        assert_eq!(size_label(2048), "2048 bytes (2.0 KiB)");
    }

    #[test]
    fn try_run_reports_path_scoped_stats_without_age() {
        let (repo, dir) = setup();
        let a_id = repo.mkdir(0, "a", 1_700_000_000_000).unwrap();
        create_file(&repo, a_id, "in-a.txt", 10, 0xAA);
        create_file(&repo, 0, "outside.txt", 99, 0xBB);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message =
            try_run(&repo_root, false, "/a", false, TimeDisplay::Local).expect("must succeed");
        assert!(message.contains("1 file(s)"));
        assert!(message.contains("logical size:   10 bytes"));
        assert!(
            !message.contains("repository age"),
            "path-scoped stats must not report repository age: {message}"
        );
    }

    #[test]
    fn try_run_reports_a_dedup_ratio_reflecting_shared_content() {
        let (repo, dir) = setup();
        let (chunk_id, _ranges) = repo.reserve_and_insert_chunk(10, &[0xAA; 20]).unwrap();
        let content_id = repo
            .find_or_create_content(10, &[0xBB; 20], &[chunk_id])
            .unwrap();
        repo.settle_file(0, "one.txt", 1_700_000_000_000, content_id)
            .unwrap();
        repo.settle_file(0, "two.txt", 1_700_000_000_000, content_id)
            .unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message =
            try_run(&repo_root, false, "/", false, TimeDisplay::Utc).expect("must succeed");
        assert!(message.contains("logical size:   20 bytes"));
        assert!(message.contains("physical size:  10 bytes"));
        assert!(message.contains("2.00x"), "got: {message}");
    }

    #[test]
    fn try_run_reports_an_empty_repository_with_an_na_dedup_ratio() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message =
            try_run(&repo_root, false, "/", false, TimeDisplay::Utc).expect("must succeed");
        assert!(message.contains("0 dir(s), 0 file(s)"));
        assert!(message.contains("dedup ratio:    n/a"));
    }

    #[test]
    fn try_run_reports_a_missing_path_clearly() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            "/does-not-exist",
            false,
            TimeDisplay::Local,
        )
        .expect_err("must fail - the path does not exist");
        assert!(message.contains("no such repository path"));
    }

    #[test]
    fn try_run_refuses_a_file() {
        let (repo, dir) = setup();
        create_file(&repo, 0, "a.txt", 10, 0xAA);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/a.txt", false, TimeDisplay::Local)
            .expect_err("must fail - a.txt is a file, not a directory");
        assert!(message.contains("not a directory"));
    }
}
