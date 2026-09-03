//! `dfs list` - REQ-QUERY-001 in requirements/functional/query.md. Lists a directory's live,
//! direct children without mounting.

use std::path::Path;

fn try_run(repo_path: &Path, default_path_used: bool, target_path: &str) -> Result<String, String> {
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

    let entry = match repo.resolve_path(target_path) {
        Ok(Some(entry)) => entry,
        Ok(None) => return Err(format!("error: no such repository path: {target_path}")),
        Err(err) => return Err(format!("error: {err}")),
    };

    let mut children = match repo.list_children(entry.id) {
        Ok(children) => children,
        Err(db::Error::WrongKind(_)) => {
            return Err(format!("error: {target_path} is not a directory"));
        }
        Err(err) => return Err(format!("error: {err}")),
    };
    if children.is_empty() {
        return Ok(format!("{target_path}: empty"));
    }
    children.sort_by(|(a, _), (b, _)| a.cmp(b));

    let lines: Vec<String> = children
        .into_iter()
        .map(|(name, entry)| {
            let kind = match entry.kind {
                db::EntryKind::Dir => "dir",
                db::EntryKind::File => "file",
            };
            format!(
                "{kind:<4} {:>12} {} {name}",
                entry.size,
                format_time(entry.time_millis)
            )
        })
        .collect();
    Ok(lines.join("\n"))
}

/// Formats `time_millis` (Unix epoch milliseconds, always non-negative for a repository-created
/// timestamp) as a UTC `YYYY-MM-DDTHH:MM:SSZ` string, using only `std` - no timezone database, and
/// no dependency added just for this. `civil_from_days` is Howard Hinnant's public-domain
/// days-since-epoch-to-civil-date algorithm (<https://howardhinnant.github.io/date_algorithms.html>).
fn format_time(time_millis: i64) -> String {
    let total_seconds = time_millis.div_euclid(1000);
    let days = total_seconds.div_euclid(86_400);
    let seconds_of_day = total_seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3600;
    let minute = (seconds_of_day % 3600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

pub fn run(repo_path: &Path, default_path_used: bool, target_path: &str) {
    match try_run(repo_path, default_path_used, target_path) {
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

    #[test]
    fn civil_from_days_matches_the_unix_epoch() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }

    #[test]
    fn civil_from_days_matches_a_well_known_reference_date() {
        // 2000-01-01T00:00:00Z is Unix time 946_684_800 - a widely cited reference value.
        assert_eq!(civil_from_days(946_684_800 / 86_400), (2000, 1, 1));
    }

    #[test]
    fn civil_from_days_handles_a_leap_day() {
        // The year 2000 is a leap year (divisible by 400) - Feb 29 exists and this algorithm's
        // century/400-year corrections must get that right, not just an ordinary leap year.
        assert_eq!(civil_from_days(946_684_800 / 86_400 + 59), (2000, 2, 29));
    }

    #[test]
    fn format_time_renders_the_full_timestamp() {
        let millis = 946_684_800_000 + 59 * 86_400_000 + 3_661_000;
        assert_eq!(format_time(millis), "2000-02-29T01:01:01Z");
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-list-test-no-default-repository-here");

        let message =
            try_run(&repo_path, true, "/").expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the actionable default-path message, got: {message}"
        );
        assert!(
            message.contains("explicitly"),
            "expected a hint to pass the path explicitly, got: {message}"
        );
    }

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

    #[test]
    fn try_run_reports_an_empty_root() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/").expect("must succeed - root exists");
        assert_eq!(message, "/: empty");
    }

    #[test]
    fn try_run_lists_direct_children_sorted_by_name_with_kind_size_and_mtime() {
        let (repo, dir) = setup();
        repo.mkdir(0, "b-dir", 1_700_000_000_000).unwrap();
        let content_id = repo
            .find_or_create_content(3, b"AAAAAAAAAAAAAAAAAAAA", &[])
            .unwrap();
        repo.settle_file(0, "a-file.txt", 1_700_000_000_000, content_id)
            .unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/").expect("must succeed");
        let lines: Vec<&str> = message.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(
            lines[0].contains("a-file.txt") && lines[0].starts_with("file"),
            "expected the file entry first (alphabetical), got: {}",
            lines[0]
        );
        assert!(
            lines[0].contains(" 3 "),
            "expected the file's logical size (3 bytes), got: {}",
            lines[0]
        );
        assert!(
            lines[1].contains("b-dir") && lines[1].starts_with("dir"),
            "expected the directory entry second (alphabetical), got: {}",
            lines[1]
        );
    }

    #[test]
    fn try_run_reports_a_missing_path_clearly() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/does-not-exist")
            .expect_err("must fail - the path does not exist");
        assert!(
            message.contains("no such repository path"),
            "expected a no-such-path message, got: {message}"
        );
    }

    #[test]
    fn try_run_refuses_to_list_a_file_as_if_it_were_a_directory() {
        let (repo, dir) = setup();
        let content_id = repo
            .find_or_create_content(0, b"BBBBBBBBBBBBBBBBBBBB", &[])
            .unwrap();
        repo.settle_file(0, "a.txt", 1_700_000_000_000, content_id)
            .unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/a.txt")
            .expect_err("must fail - a.txt is a file, not a directory");
        assert!(
            message.contains("not a directory"),
            "expected a not-a-directory message, got: {message}"
        );
    }
}
