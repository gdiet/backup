//! REQ-CLI-006's default repository path, shared by every command that needs a repository
//! (`create-repo`, `mount`) - DESIGN-CLI-004 in docs/design/distribution.md.

use std::path::{Path, PathBuf};

const DEFAULT_REPO_DIR_NAME: &str = "dedupfs-repository";

/// Resolves REQ-CLI-006's default repository path: a `dedupfs-repository` directory next to the
/// running executable. A thin wrapper around `std::env::current_exe()` - the actual resolution
/// logic lives in [`default_repo_path_from`], which takes the executable path as a plain
/// parameter so it stays testable without depending on where the test binary itself happens to
/// run from.
pub fn default_repo_path() -> std::io::Result<PathBuf> {
    std::env::current_exe().map(|exe_path| default_repo_path_from(&exe_path))
}

fn default_repo_path_from(exe_path: &Path) -> PathBuf {
    exe_path
        .parent()
        .unwrap_or(exe_path)
        .join(DEFAULT_REPO_DIR_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_repo_path_from_joins_the_dir_name_onto_the_executables_parent() {
        assert_eq!(
            default_repo_path_from(Path::new("/opt/dfs/bin/dfs")),
            Path::new("/opt/dfs/bin/dedupfs-repository")
        );
    }

    #[test]
    fn default_repo_path_from_handles_an_executable_directly_under_root() {
        assert_eq!(
            default_repo_path_from(Path::new("/dfs")),
            Path::new("/dedupfs-repository")
        );
    }

    #[test]
    fn default_repo_path_from_falls_back_to_the_input_itself_when_it_has_no_parent() {
        // Path::new("/").parent() is None - the one case the `unwrap_or(exe_path)` fallback
        // exists for (current_exe() is not expected to ever actually return "/", but the
        // fallback must still behave sensibly rather than panic).
        assert_eq!(
            default_repo_path_from(Path::new("/")),
            Path::new("/dedupfs-repository")
        );
    }
}
