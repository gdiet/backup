//! A UTC timestamp formatter shared by every CLI command that prints or logs a repository
//! timestamp - `dfs list`, `dfs find`, REQ-TREE-009's `[deleted]` suffix, and REQ-INGEST-007's own
//! date/time placeholders (`crate::target_path`) all go through this module or `time` directly.

use time::OffsetDateTime;

/// `time_millis` (Unix epoch milliseconds) as a UTC [`OffsetDateTime`] - truncated to whole
/// seconds, since nothing in this project needs sub-second display precision.
fn to_datetime(time_millis: i64) -> OffsetDateTime {
    let seconds = time_millis.div_euclid(1000);
    OffsetDateTime::from_unix_timestamp(seconds)
        .expect("a real repository timestamp is always in range for OffsetDateTime")
}

/// Formats `time_millis` as a UTC `YYYY-MM-DDTHH:MM:SSZ` string.
pub fn format_time(time_millis: i64) -> String {
    let dt = to_datetime(time_millis);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        dt.year(),
        dt.month() as u8,
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}

/// Formats `time_millis` as `YYYY-MM-DD_HHMMSS` in UTC - REQ-TREE-009's deletion-timestamp suffix
/// (`requirements/functional/tree.md`), safe to embed directly in a path component (no `:`,
/// unlike [`format_time`]'s ISO 8601 form, which Windows refuses in a file name).
pub fn format_deletion_suffix(time_millis: i64) -> String {
    let dt = to_datetime(time_millis);
    format!(
        "{:04}-{:02}-{:02}_{:02}{:02}{:02}",
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
    fn format_time_renders_the_full_timestamp() {
        let millis = 946_684_800_000 + 59 * 86_400_000 + 3_661_000;
        assert_eq!(format_time(millis), "2000-02-29T01:01:01Z");
    }

    #[test]
    fn format_time_matches_the_unix_epoch() {
        assert_eq!(format_time(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn format_deletion_suffix_renders_a_path_safe_form_with_no_colons() {
        let millis = 946_684_800_000 + 59 * 86_400_000 + 3_661_000;
        assert_eq!(format_deletion_suffix(millis), "2000-02-29_010101");
    }
}
