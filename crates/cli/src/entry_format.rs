//! A one-line entry formatter (`kind size time name`) shared by every CLI command that prints a
//! flat list of tree entries - `dfs list` and `dfs find` today.

use crate::time_format::format_time;

/// `entry.kind`'s display label - `"dir"` or `"file"`, the vocabulary this project's own listing
/// output uses throughout (never FUSE/WinFSP terms like `S_IFDIR`).
pub fn kind_label(kind: db::EntryKind) -> &'static str {
    match kind {
        db::EntryKind::Dir => "dir",
        db::EntryKind::File => "file",
    }
}

/// One formatted listing line: kind, size, UTC modification time, then `name` (a bare name for
/// `dfs list`, a full repository path for `dfs find`).
pub fn format_line(kind: &str, size: u64, time_millis: i64, name: &str) -> String {
    format!("{kind:<4} {size:>12} {} {name}", format_time(time_millis))
}
