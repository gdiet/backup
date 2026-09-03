//! A UTC timestamp formatter shared by every CLI command that prints or logs a repository
//! timestamp - no timezone database, and no dependency added just for this (see
//! `developer-todos/adopt-time-crate-for-ingest-007-and-list.md` for revisiting that choice once
//! REQ-INGEST-007's date-placeholder syntax needs real calendar arithmetic, not just formatting).

/// Formats `time_millis` (Unix epoch milliseconds) as a UTC `YYYY-MM-DDTHH:MM:SSZ` string.
/// `civil_from_days` is Howard Hinnant's public-domain days-since-epoch-to-civil-date algorithm
/// (<https://howardhinnant.github.io/date_algorithms.html>).
pub fn format_time(time_millis: i64) -> String {
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
}
