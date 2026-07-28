//! Output formatting helpers shared by the reporting commands (`stats`, `list`,
//! `find`, `check`).

const KB: f64 = 1_000.0;
const MB: f64 = 1_000_000.0;
const GB: f64 = 1_000_000_000.0;
const TB: f64 = 1_000_000_000_000.0;

/// Formats a byte count as a human-readable string with the smallest unit that
/// keeps the number under 4 significant digits (`B`, `kB`, `MB`, `GB`, `TB`).
pub fn readable_bytes(bytes: u64) -> String {
    if bytes < 10_000 {
        format!("{bytes} B")
    } else if bytes < 1_000_000 {
        format!("{:.2} kB", bytes as f64 / KB)
    } else if bytes < 1_000_000_000 {
        format!("{:.2} MB", bytes as f64 / MB)
    } else if bytes < 1_000_000_000_000 {
        format!("{:.2} GB", bytes as f64 / GB)
    } else {
        format!("{:.2} TB", bytes as f64 / TB)
    }
}

/// Prints the standard two-line "file information" block (`stats <path>` and
/// `list <path>` show identical output for a file - one shared function
/// instead of duplicating the format in both places).
pub fn print_file_info(path_label: &str, name: &str, size: u64) {
    println!("File information for '{path_label}':");
    println!("{name} .. {}", readable_bytes(size));
}

/// Formats a Unix timestamp in milliseconds as `YYYY-MM-DD HH:MM:SS` (UTC).
///
/// Hand-rolled instead of pulling in a date/time crate for one formatting
/// call: this is Howard Hinnant's well-known, widely-verified
/// days-since-epoch <-> civil-date algorithm
/// (<http://howardhinnant.github.io/date_algorithms.html>), good for every
/// year representable in an `i64` count of days, proleptic Gregorian.
pub fn format_timestamp_millis(millis: i64) -> String {
    let total_seconds = millis.div_euclid(1000);
    let days = total_seconds.div_euclid(86400);
    let seconds_of_day = total_seconds.rem_euclid(86400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3600;
    let minute = (seconds_of_day % 3600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
}

fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = (z - era * 146_097) as u64; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365; // [0, 399]
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100); // [0, 365]
    let mp = (5 * day_of_year + 2) / 153; // [0, 11]
    let day = (day_of_year - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let year = if month <= 2 { year + 1 } else { year };
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_each_unit_range() {
        assert_eq!(readable_bytes(0), "0 B");
        assert_eq!(readable_bytes(9_999), "9999 B");
        assert_eq!(readable_bytes(10_000), "10.00 kB");
        assert_eq!(readable_bytes(999_999), "1000.00 kB");
        assert_eq!(readable_bytes(1_000_000), "1.00 MB");
        assert_eq!(readable_bytes(1_000_000_000), "1.00 GB");
        assert_eq!(readable_bytes(1_000_000_000_000), "1.00 TB");
        assert_eq!(readable_bytes(2_500_000_000_000), "2.50 TB");
    }

    #[test]
    fn formats_known_timestamps() {
        assert_eq!(format_timestamp_millis(0), "1970-01-01 00:00:00");
        // 2024-01-01T00:00:00Z, a well-known Unix timestamp.
        assert_eq!(
            format_timestamp_millis(1_704_067_200_000),
            "2024-01-01 00:00:00"
        );
        // 2000-02-29T12:34:56Z - a leap day, and exercises hour/minute/second.
        assert_eq!(
            format_timestamp_millis(951_827_696_000),
            "2000-02-29 12:34:56"
        );
        // Sub-second millis must not affect the formatted seconds.
        assert_eq!(format_timestamp_millis(999), "1970-01-01 00:00:00");
    }
}
