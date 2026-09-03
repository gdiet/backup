//! REQ-INGEST-007's templated/creatable target-path syntax for `dfs ingest` - concrete syntax
//! decided in DESIGN-CLI-006 (`docs/design/ingest-target-template-syntax.md`).
//!
//! A `/`-separated segment may contain `[...]` date/time placeholders (resolved against a single
//! captured "now", REQ-INGEST-007's "current date/time at run start") and may be prefixed with `+`
//! (create on demand, otherwise reuse) or `!` (must not already exist, always freshly created) -
//! neither prefix is REQ-INGEST-007's own default, "must already exist". Marking one segment
//! creatable makes every segment below it default to creatable too.

use time::OffsetDateTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Existence {
    MustExist,
    CreateIfMissing,
    MustBeFresh,
}

/// [`resolve`]'s own result: the final segment's directory id, paired with the fully resolved
/// absolute path (every placeholder substituted, every marker stripped) - so a caller can report
/// exactly where a templated target actually landed, not just echo the raw, unresolved argument
/// back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub id: i64,
    pub path: String,
}

/// Resolves `target` against `repo`, walking it one segment at a time from the root. `now` is
/// REQ-INGEST-007's single "current date/time at run start" - callers resolving more than one path
/// in the same run must reuse the same `now` so every placeholder in that run resolves identically.
pub fn resolve(
    repo: &db::Repository,
    target: &str,
    now: OffsetDateTime,
) -> Result<Resolved, String> {
    let now_millis = now.unix_timestamp() * 1000;
    let mut current_path = String::new();
    let mut current_id = 0i64;
    let mut cascade_creatable = false;

    for raw_segment in target.split('/').filter(|s| !s.is_empty()) {
        let (existence, marked) = classify(raw_segment, cascade_creatable);
        if existence != Existence::MustExist {
            cascade_creatable = true;
        }
        let name = resolve_placeholders(marked, now)?;
        current_path.push('/');
        current_path.push_str(&name);

        current_id = resolve_segment(
            repo,
            &current_path,
            current_id,
            &name,
            existence,
            now_millis,
        )?;
    }
    Ok(Resolved {
        id: current_id,
        path: if current_path.is_empty() {
            "/".to_string()
        } else {
            current_path
        },
    })
}

/// Splits a raw `/`-separated segment into its existence marker (defaulting to `cascade_creatable`
/// when unmarked) and the remainder still needing placeholder resolution.
fn classify(raw_segment: &str, cascade_creatable: bool) -> (Existence, &str) {
    if let Some(rest) = raw_segment.strip_prefix('!') {
        (Existence::MustBeFresh, rest)
    } else if let Some(rest) = raw_segment.strip_prefix('+') {
        (Existence::CreateIfMissing, rest)
    } else if cascade_creatable {
        (Existence::CreateIfMissing, raw_segment)
    } else {
        (Existence::MustExist, raw_segment)
    }
}

fn resolve_segment(
    repo: &db::Repository,
    current_path: &str,
    parent_id: i64,
    name: &str,
    existence: Existence,
    now_millis: i64,
) -> Result<i64, String> {
    let existing = repo
        .resolve_path(current_path)
        .map_err(|err| format!("error: {err}"))?;
    match (existing, existence) {
        (Some(_), Existence::MustBeFresh) => Err(format!(
            "{current_path} already exists - required to be freshly created (leading `!`)"
        )),
        (Some(entry), _) if entry.kind != db::EntryKind::Dir => {
            Err(format!("{current_path} exists and is not a directory"))
        }
        (Some(entry), _) => Ok(entry.id),
        (None, Existence::MustExist) => Err(format!(
            "no such repository path: {current_path} (pass a leading `+`/`!` on this segment to \
             create it)"
        )),
        (None, Existence::CreateIfMissing | Existence::MustBeFresh) => repo
            .mkdir(parent_id, name, now_millis)
            .map_err(|err| format!("error: {err}")),
    }
}

/// Resolves every `[...]` date/time placeholder in `segment` against `now`, left to right. A
/// segment with no `[...]` at all is returned unchanged.
fn resolve_placeholders(segment: &str, now: OffsetDateTime) -> Result<String, String> {
    let mut result = String::with_capacity(segment.len());
    let mut rest = segment;
    while let Some(start) = rest.find('[') {
        result.push_str(&rest[..start]);
        let after_bracket = &rest[start + 1..];
        let end = after_bracket
            .find(']')
            .ok_or_else(|| format!("{segment}: unterminated '[' placeholder"))?;
        result.push_str(&resolve_pattern(&after_bracket[..end], now));
        rest = &after_bracket[end + 1..];
    }
    result.push_str(rest);
    Ok(result)
}

/// One `resolve_pattern` token: its literal spelling, paired with how to render `now` for it.
type Token = (&'static str, fn(OffsetDateTime) -> String);

/// Substitutes every recognized token (`yyyy`, `MM`, `dd`, `HH`, `mm`, `ss` - REQ-INGEST-007's own
/// example vocabulary) in `pattern` with `now`'s own zero-padded value; any other character is
/// kept verbatim, so a literal separator (`-`, `_`, ...) can sit between tokens. No token is a
/// prefix of another, so this scan is unambiguous without needing a longest-match rule.
fn resolve_pattern(pattern: &str, now: OffsetDateTime) -> String {
    const TOKENS: &[Token] = &[
        ("yyyy", |t| format!("{:04}", t.year())),
        ("MM", |t| format!("{:02}", t.month() as u8)),
        ("dd", |t| format!("{:02}", t.day())),
        ("HH", |t| format!("{:02}", t.hour())),
        ("mm", |t| format!("{:02}", t.minute())),
        ("ss", |t| format!("{:02}", t.second())),
    ];
    let mut result = String::with_capacity(pattern.len());
    let mut rest = pattern;
    'outer: while !rest.is_empty() {
        for (token, render) in TOKENS {
            if let Some(remainder) = rest.strip_prefix(token) {
                result.push_str(&render(now));
                rest = remainder;
                continue 'outer;
            }
        }
        let mut chars = rest.chars();
        let ch = chars.next().expect("rest is non-empty here");
        result.push(ch);
        rest = chars.as_str();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::{Date, Month, Time};

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

    /// A fixed, well-known moment (a leap day) for deterministic placeholder-resolution tests -
    /// built from `time`'s own constructors rather than the `macros` feature's `datetime!`, which
    /// this crate does not otherwise need.
    fn now() -> OffsetDateTime {
        Date::from_calendar_date(2024, Month::February, 29)
            .unwrap()
            .with_time(Time::from_hms(13, 5, 7).unwrap())
            .assume_utc()
    }

    #[test]
    fn resolve_placeholders_substitutes_every_known_token() {
        assert_eq!(
            resolve_placeholders("[yyyy-MM-dd]", now()).unwrap(),
            "2024-02-29"
        );
        assert_eq!(
            resolve_placeholders("[HH]-[mm]-[ss]", now()).unwrap(),
            "13-05-07"
        );
    }

    #[test]
    fn resolve_placeholders_keeps_unrecognized_characters_verbatim() {
        assert_eq!(
            resolve_placeholders("backup-[yyyy]-full", now()).unwrap(),
            "backup-2024-full"
        );
    }

    #[test]
    fn resolve_placeholders_leaves_a_plain_segment_unchanged() {
        assert_eq!(resolve_placeholders("backups", now()).unwrap(), "backups");
    }

    #[test]
    fn resolve_placeholders_reports_an_unterminated_bracket() {
        let err = resolve_placeholders("[yyyy", now()).unwrap_err();
        assert!(err.contains("unterminated"));
    }

    #[test]
    fn resolve_requires_an_existing_target_by_default() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");
        let repo = db::open_repository(&repo_root).unwrap();

        let err = resolve(&repo, "/does-not-exist", now()).unwrap_err();
        assert!(err.contains("no such repository path"));
    }

    #[test]
    fn resolve_uses_an_existing_plain_directory_without_marks() {
        let (repo, _dir) = setup();
        let id = repo.mkdir(0, "backups", 100).unwrap();

        let resolved = resolve(&repo, "/backups", now()).unwrap();
        assert_eq!(resolved.id, id);
        assert_eq!(resolved.path, "/backups");
    }

    #[test]
    fn resolve_creates_a_plus_marked_segment_that_is_missing() {
        let (repo, _dir) = setup();

        let resolved = resolve(&repo, "/+backups", now()).unwrap();
        let entry = repo.resolve_path("/backups").unwrap().unwrap();
        assert_eq!(resolved.id, entry.id);
        assert_eq!(
            resolved.path, "/backups",
            "the resolved path must not still carry the + marker"
        );
        assert_eq!(entry.kind, db::EntryKind::Dir);
    }

    #[test]
    fn resolve_reuses_a_plus_marked_segment_that_already_exists() {
        let (repo, _dir) = setup();
        let id = repo.mkdir(0, "backups", 100).unwrap();

        let resolved = resolve(&repo, "/+backups", now()).unwrap();
        assert_eq!(
            resolved.id, id,
            "must reuse the existing directory, not fail or recreate it"
        );
    }

    #[test]
    fn resolve_creates_a_bang_marked_segment_that_is_missing() {
        let (repo, _dir) = setup();

        let resolved = resolve(&repo, "/!backups", now()).unwrap();
        let entry = repo.resolve_path("/backups").unwrap().unwrap();
        assert_eq!(resolved.id, entry.id);
    }

    #[test]
    fn resolve_refuses_a_bang_marked_segment_that_already_exists() {
        let (repo, _dir) = setup();
        repo.mkdir(0, "backups", 100).unwrap();

        let err = resolve(&repo, "/!backups", now()).unwrap_err();
        assert!(err.contains("already exists"));
    }

    #[test]
    fn resolve_cascades_creatable_to_segments_below_a_marked_one() {
        let (repo, _dir) = setup();

        // Only the first segment is marked - "year" and "month" below it must still be created.
        let resolved = resolve(&repo, "/+backups/year/month", now()).unwrap();
        let entry = repo.resolve_path("/backups/year/month").unwrap().unwrap();
        assert_eq!(resolved.id, entry.id);
    }

    #[test]
    fn resolve_combines_placeholders_and_markers_across_segments() {
        let (repo, _dir) = setup();

        let resolved = resolve(&repo, "/+backups/+[yyyy]/![MM-dd]", now()).unwrap();
        let entry = repo.resolve_path("/backups/2024/02-29").unwrap().unwrap();
        assert_eq!(resolved.id, entry.id);
        assert_eq!(resolved.path, "/backups/2024/02-29");
    }

    #[test]
    fn resolve_refuses_a_target_that_is_a_file() {
        let (repo, _dir) = setup();
        let content_id = repo.find_or_create_content(0, &[0xAA; 20], &[]).unwrap();
        repo.settle_file(0, "a.txt", 100, content_id).unwrap();

        let err = resolve(&repo, "/a.txt", now()).unwrap_err();
        assert!(err.contains("not a directory"));
    }
}
