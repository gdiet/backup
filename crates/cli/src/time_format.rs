//! A timestamp formatter shared by every CLI command that prints or logs a repository timestamp -
//! `dfs list`, `dfs find`, `dfs stats`, and REQ-TREE-009's `[deleted]`/REQ-MOUNT-008's `[time]`
//! suffixes all go through this module.
//!
//! User-facing timestamps ([`format_time`], and `[time]`'s own
//! [`format_deletion_suffix_for_display`]) render in the operator's local timezone by default,
//! switchable to UTC via each command's own `--utc` flag ([`TimeDisplay`]) - a personal backup
//! tool's own operator is the one person who ever looks at these, so their own wall clock is the
//! more useful default; `--utc` stays available for an unambiguous, portable reading regardless of
//! where it is read (REQ-OPERABILITY-008; DESIGN-CLI-007 in
//! `docs/design/user-facing-timezone-display.md` for why calling into `time`'s `local-offset`
//! feature from this project's own multithreaded mount session is safe). Machine-facing output
//! ([`format_utc_time`] for
//! `usage.log`; [`format_deletion_suffix`] for REQ-TREE-009's own `[deleted]` addressing and `dfs
//! db-backup`'s backup-file naming) stays UTC unconditionally: REQ-TREE-009's addressing is a
//! stable identity pasted between commands and sessions, not a place for a display preference to
//! leak into, and a log/backup filename is read by tooling, not eyeballed for a specific timezone.

use time::{OffsetDateTime, UtcOffset};

/// Which timezone a user-facing timestamp renders in - each command's own `--utc` flag chooses
/// between them, defaulting to [`Local`](TimeDisplay::Local).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeDisplay {
    /// The process's own local timezone (REQ-OPERABILITY-008's default) - resolved fresh for
    /// every timestamp via [`UtcOffset::local_offset_at`], not cached once, so a timestamp from
    /// before a daylight-saving transition renders under the offset that genuinely applied to it,
    /// not whatever offset happens to apply "now". Falls back to UTC (rendered exactly like
    /// [`Utc`](TimeDisplay::Utc), including its own "Z" marker - never silently mislabeled as
    /// local) on the rare platform/environment where the local offset cannot be determined at all
    /// (e.g. a container missing its timezone database).
    Local,
    /// UTC, unconditionally.
    Utc,
}

/// `time_millis` (Unix epoch milliseconds) resolved under `display` - truncated to whole seconds,
/// since nothing in this project needs sub-second display precision. The returned `bool` is
/// whether the result ended up UTC (either because `display` asked for it, or as
/// [`TimeDisplay::Local`]'s own indeterminate-offset fallback) - callers use it to decide whether
/// to append the "Z" marker.
fn resolve(time_millis: i64, display: TimeDisplay) -> (OffsetDateTime, bool) {
    let seconds = time_millis.div_euclid(1000);
    let utc = OffsetDateTime::from_unix_timestamp(seconds)
        .expect("a real repository timestamp is always in range for OffsetDateTime");
    match display {
        TimeDisplay::Utc => (utc, true),
        TimeDisplay::Local => match UtcOffset::local_offset_at(utc) {
            Ok(offset) => (utc.to_offset(offset), false),
            Err(_) => (utc, true),
        },
    }
}

/// Formats `time_millis` as `YYYY-MM-DDTHH:MM:SS`, with a trailing `Z` exactly when the result is
/// UTC (`display` asked for it, or [`TimeDisplay::Local`]'s own fallback) - `dfs list`/`dfs
/// find`'s mtime column, `dfs stats`' repository creation time.
pub fn format_time(time_millis: i64, display: TimeDisplay) -> String {
    let (dt, is_utc) = resolve(time_millis, display);
    let marker = if is_utc { "Z" } else { "" };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{marker}",
        dt.year(),
        dt.month() as u8,
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}

/// Formats `time_millis` as a UTC `YYYY-MM-DDTHH:MM:SSZ` string, unconditionally - `usage.log`'s
/// own machine-facing timestamp, which must stay absolute regardless of any operator display
/// preference.
pub fn format_utc_time(time_millis: i64) -> String {
    format_time(time_millis, TimeDisplay::Utc)
}

/// Formats `time_millis` as `YYYY-MM-DD_HH-MM-SS` in UTC, unconditionally - REQ-TREE-009's
/// deletion-timestamp suffix (`requirements/functional/tree.md`), safe to embed directly in a path
/// component (no `:`, unlike [`format_time`]'s ISO 8601 form, which Windows refuses in a file
/// name) - dashes throughout rather than switching to a bare digit run partway through, so the
/// whole timestamp reads the same way at a glance. Also reused by `dfs db-backup` for its own
/// backup-file naming (an unrelated timestamp - when the backup was made, not a deletion time -
/// that just happens to want the same path-safe shape). Stays UTC unconditionally, unlike
/// [`format_deletion_suffix_for_display`]: this is REQ-TREE-009's own addressing scheme, a stable
/// identity pasted between commands and sessions, not a place for a display preference to leak
/// into.
pub fn format_deletion_suffix(time_millis: i64) -> String {
    let (dt, _) = resolve(time_millis, TimeDisplay::Utc);
    format!(
        "{:04}-{:02}-{:02}_{:02}-{:02}-{:02}",
        dt.year(),
        dt.month() as u8,
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}

/// Like [`format_deletion_suffix`], but switchable under `display` - REQ-MOUNT-008's own `[time]`
/// view, which (unlike REQ-TREE-009's `[deleted]`) is a purely interactive chronological-browsing
/// convenience with no stable-identity role, so a display preference is fair game here. Appends a
/// trailing `Z` exactly when the result is UTC, the same as [`format_time`].
pub fn format_deletion_suffix_for_display(time_millis: i64, display: TimeDisplay) -> String {
    let (dt, is_utc) = resolve(time_millis, display);
    let marker = if is_utc { "Z" } else { "" };
    format!(
        "{:04}-{:02}-{:02}_{:02}-{:02}-{:02}{marker}",
        dt.year(),
        dt.month() as u8,
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_time_renders_the_full_timestamp_in_utc() {
        let millis = 946_684_800_000 + 59 * 86_400_000 + 3_661_000;
        assert_eq!(
            format_time(millis, TimeDisplay::Utc),
            "2000-02-29T01:01:01Z"
        );
    }

    #[test]
    fn format_time_matches_the_unix_epoch_in_utc() {
        assert_eq!(format_time(0, TimeDisplay::Utc), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn format_time_local_never_panics_and_always_parses_as_a_plausible_timestamp() {
        // The actual offset depends on the machine running this test, so this cannot assert an
        // exact value - only that TimeDisplay::Local resolves to *something* well-formed, with or
        // without the "Z" fallback marker.
        let rendered = format_time(946_684_800_000, TimeDisplay::Local);
        let digits_and_marker = rendered.trim_end_matches('Z');
        assert_eq!(digits_and_marker.len(), 19, "got {rendered}");
        assert!(digits_and_marker.starts_with("2000-01-0"), "got {rendered}");
    }

    #[test]
    fn format_utc_time_matches_format_time_under_utc() {
        assert_eq!(format_utc_time(0), format_time(0, TimeDisplay::Utc));
    }

    #[test]
    fn format_deletion_suffix_renders_a_path_safe_form_with_no_colons_or_marker() {
        let millis = 946_684_800_000 + 59 * 86_400_000 + 3_661_000;
        assert_eq!(format_deletion_suffix(millis), "2000-02-29_01-01-01");
    }

    #[test]
    fn format_deletion_suffix_for_display_under_utc_matches_the_unconditional_form_plus_a_marker() {
        let millis = 946_684_800_000 + 59 * 86_400_000 + 3_661_000;
        assert_eq!(
            format_deletion_suffix_for_display(millis, TimeDisplay::Utc),
            format!("{}Z", format_deletion_suffix(millis))
        );
    }

    #[test]
    fn format_deletion_suffix_for_display_local_never_panics_and_always_parses_as_plausible() {
        let rendered = format_deletion_suffix_for_display(946_684_800_000, TimeDisplay::Local);
        let digits_and_marker = rendered.trim_end_matches('Z');
        assert_eq!(digits_and_marker.len(), 19, "got {rendered}");
        assert!(digits_and_marker.starts_with("2000-01-0"), "got {rendered}");
    }
}
