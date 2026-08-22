//! `dfs mount`'s real implementation - REQ-MOUNT-001. Read-only by default (REQ-MOUNT-002);
//! `--read-write` opts into the directory operations REQ-MOUNT-003 requires (mkdir/rmdir/rename/
//! utimens - see `dedup_fs::DedupFs`). No cross-process repository locking yet
//! (REQ-MAINTENANCE-004) - out of scope for this first mount milestone.

use std::path::Path;

use crate::dedup_fs::DedupFs;

pub fn run(repo_path: &Path, mountpoint: &Path, read_write: bool) {
    let repo = match db::open_repository(repo_path) {
        Ok(repo) => repo,
        Err(err) => {
            eprintln!("error: {err}");
            std::process::exit(1);
        }
    };

    if let Err(err) = mountfs::preflight() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
    if let Err(err) = mountfs::mount(DedupFs::new(repo, read_write), mountpoint, !read_write) {
        eprintln!("mount failed: {err}");
        std::process::exit(1);
    }
}
