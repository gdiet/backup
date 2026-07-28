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
}
