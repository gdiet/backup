//! Time-throttled, byte-based progress reporting, shared by every command
//! whose work is dominated by reading real data (as opposed to a fast
//! metadata-only pass) - originally written for `migrate_scala_repo`'s
//! chunk/hash walk, now also used by `check`/`problems` (see
//! `docs/plans/implemented/check-problems-progress.md`).

use crate::format::readable_bytes;

/// Prints at most once per [`Progress::INTERVAL`], so a run with many
/// small items doesn't spam the console, but a single huge item being
/// processed still gets periodic updates instead of going silent for as
/// long as that takes - deliberately approximate (byte-based, not
/// item-count-based, since item sizes vary widely), not meant to be exact
/// to the byte.
pub(crate) struct Progress {
    total_bytes: u64,
    done_bytes: u64,
    started: std::time::Instant,
    last_printed: std::time::Instant,
}

impl Progress {
    pub(crate) const INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

    pub(crate) fn new(total_bytes: u64) -> Self {
        let now = std::time::Instant::now();
        Self {
            total_bytes,
            done_bytes: 0,
            started: now,
            last_printed: now,
        }
    }

    /// Counts `bytes` as done, printing a progress line if
    /// [`Progress::INTERVAL`] has elapsed since the last one.
    pub(crate) fn add(&mut self, bytes: u64) {
        self.done_bytes += bytes;
        if self.last_printed.elapsed() >= Self::INTERVAL {
            self.print();
            self.last_printed = std::time::Instant::now();
        }
    }

    /// Prints one final line regardless of the interval - so a run doesn't
    /// end without ever showing 100%, or showing a stale percentage from
    /// partway through the last interval.
    pub(crate) fn finish(&mut self) {
        self.print();
    }

    fn print(&self) {
        if self.total_bytes == 0 {
            return;
        }
        let percent = self.done_bytes as f64 / self.total_bytes as f64 * 100.0;
        let elapsed = self.started.elapsed();
        let eta = if self.done_bytes > 0 && self.done_bytes < self.total_bytes {
            let total_secs =
                elapsed.as_secs_f64() * self.total_bytes as f64 / self.done_bytes as f64;
            format!(
                ", ETA {}",
                format_duration_secs(total_secs - elapsed.as_secs_f64())
            )
        } else {
            String::new()
        };
        println!(
            "progress: {} / {} ({percent:.1}%), elapsed {}{eta}",
            readable_bytes(self.done_bytes),
            readable_bytes(self.total_bytes),
            format_duration_secs(elapsed.as_secs_f64()),
        );
    }
}

pub(crate) fn format_duration_secs(secs: f64) -> String {
    let total = secs.max(0.0).round() as u64;
    let (h, m, s) = (total / 3600, (total % 3600) / 60, total % 60);
    if h > 0 {
        format!("{h}h {m}m {s}s")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}
