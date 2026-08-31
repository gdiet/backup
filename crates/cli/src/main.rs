mod content_reader;
mod create_repo;
mod dedup_fs;
mod mount;
mod pending_files;
mod repo_path;
mod write_cache;

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// REQ-CLI-005's default when neither `--cdc-target-size-bits` nor `--whole-file` is given -
/// content-defined chunking with an average chunk size a little above 1 MiB.
const DEFAULT_CDC_TARGET_SIZE_BITS: u32 = 20;

#[derive(Args)]
#[group(multiple = false)]
struct ChunkingArgs {
    /// Use content-defined chunking with this target chunk size, in bits (6-30) - average chunk
    /// size ends up slightly above 2^bits. Defaults to 20 if neither this nor --whole-file is
    /// given. This choice is fixed for the repository's lifetime once created - it cannot be
    /// changed later.
    #[arg(long)]
    cdc_target_size_bits: Option<u32>,
    /// Use whole-file chunking - no sub-file deduplication. Fixed for the repository's lifetime
    /// once created, same as --cdc-target-size-bits.
    #[arg(long)]
    whole_file: bool,
}

/// Resolves `ChunkingArgs` into the value `create_repo::run` expects: `None` for whole-file
/// chunking, `Some(bits)` for content-defined chunking at an explicit or defaulted target size
/// (REQ-CLI-005).
fn resolve_cdc_target_size_bits(whole_file: bool, explicit_bits: Option<u32>) -> Option<u32> {
    if whole_file {
        None
    } else {
        Some(explicit_bits.unwrap_or(DEFAULT_CDC_TARGET_SIZE_BITS))
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Creates a new, empty repository at PATH.
    CreateRepo {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable when omitted.
        path: Option<PathBuf>,
        #[command(flatten)]
        chunking: ChunkingArgs,
    },
    /// Mounts a repository as a real filesystem at MOUNTPOINT (REQ-MOUNT-001).
    Mount {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable when omitted.
        // A flag, not a positional like create-repo's `path`: clap does not allow an optional
        // positional ahead of a required one, and MOUNTPOINT below must stay required.
        #[arg(long)]
        repo: Option<PathBuf>,
        /// Directory to mount at. On Linux, this directory must already exist - fuse3 refuses to
        /// mount onto a path that does not. On Windows, it does not need to already exist - WinFSP
        /// creates it itself and removes it again on unmount.
        mountpoint: PathBuf,
        /// Allow structural changes (currently: directories only - REQ-MOUNT-003) through the
        /// mount. Without this, the mount is read-only (REQ-MOUNT-002).
        #[arg(long)]
        read_write: bool,
    },
}

#[derive(Parser)]
#[command(
    name = "dfs",
    about = "DedupFS: a deduplicating backup filesystem",
    arg_required_else_help = true
)]
// Version token format: DESIGN-CLI-001 in docs/design/cli-version.md.
// WinFSP notice on Windows builds only: DESIGN-CLI-002 in the same file.
#[cfg_attr(
    windows,
    command(version = concat!(
        env!("CARGO_PKG_VERSION"), " ", env!("DFS_VERSION_DATE"), " ", env!("DFS_VERSION_HASH"),
        "\n\nUses WinFSP for Windows mount support:\n",
        "WinFsp - Windows File System Proxy, Copyright (C) Bill Zissimopoulos\n",
        "https://github.com/winfsp/winfsp",
    ))
)]
#[cfg_attr(
    not(windows),
    command(version = concat!(
        env!("CARGO_PKG_VERSION"), " ", env!("DFS_VERSION_DATE"), " ", env!("DFS_VERSION_HASH"),
    ))
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

/// Resolves an optional repository-path argument into `(path, default_path_used)` - the
/// operator's own path when given, or REQ-CLI-006's default when omitted. Exits with an
/// actionable message if the default itself cannot be determined (a `std::env::current_exe()`
/// failure).
fn resolve_repo_path(path: Option<PathBuf>) -> (PathBuf, bool) {
    match path {
        Some(path) => (path, false),
        None => match repo_path::default_repo_path() {
            Ok(path) => (path, true),
            Err(err) => {
                eprintln!(
                    "error: could not determine the default repository location: {err}\n\
                     Pass the path explicitly instead."
                );
                std::process::exit(1);
            }
        },
    }
}

fn main() {
    match Cli::parse().command {
        Commands::CreateRepo { path, chunking } => {
            let cdc_target_size_bits =
                resolve_cdc_target_size_bits(chunking.whole_file, chunking.cdc_target_size_bits);
            let (path, default_path_used) = resolve_repo_path(path);
            create_repo::run(&path, cdc_target_size_bits, default_path_used);
        }
        Commands::Mount {
            repo,
            mountpoint,
            read_write,
        } => {
            let (repo, default_path_used) = resolve_repo_path(repo);
            mount::run(&repo, &mountpoint, read_write, default_path_used);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_cdc_target_size_bits_defaults_to_20_when_neither_flag_given() {
        assert_eq!(resolve_cdc_target_size_bits(false, None), Some(20));
    }

    #[test]
    fn resolve_cdc_target_size_bits_respects_an_explicit_target_size() {
        assert_eq!(resolve_cdc_target_size_bits(false, Some(24)), Some(24));
    }

    #[test]
    fn resolve_cdc_target_size_bits_whole_file_overrides_to_none() {
        assert_eq!(resolve_cdc_target_size_bits(true, None), None);
    }
}
