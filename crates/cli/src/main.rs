mod backpressure;
mod content_reader;
mod create_repo;
mod dedup_fs;
mod failure_log;
mod ignore_rules;
mod ingest;
mod list;
mod mount;
mod pending_files;
mod repo_path;
mod restore;
mod settle;
mod settle_pool;
mod time_format;
mod unlock;
mod usage_log;
mod write_cache;

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};

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
    // REQ-CLI-005.
    /// Creates a new, empty repository.
    CreateRepo {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable when omitted.
        path: Option<PathBuf>,
        #[command(flatten)]
        chunking: ChunkingArgs,
    },
    // REQ-MOUNT-001.
    /// Mounts a repository as a real filesystem.
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
        // REQ-MOUNT-002/003.
        /// Allow structural changes, content writes, and modification-time updates through the
        /// mount. Without this, the mount is read-only.
        #[arg(long)]
        read_write: bool,
    },
    // REQ-MAINTENANCE-008, DESIGN-MAINTENANCE-003.
    /// Checks whether a repository's write lock is stale (nothing currently holds it) and clears
    /// it if so. Never removes an actively held lock.
    Unlock {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable when omitted.
        path: Option<PathBuf>,
    },
    // REQ-QUERY-001.
    /// Lists a directory's live, direct contents, without mounting.
    List {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable when omitted.
        #[arg(long)]
        repo: Option<PathBuf>,
        /// Repository path to list.
        #[arg(default_value = "/")]
        path: String,
    },
    // REQ-RESTORE-001/003/004.
    /// Restores one or more repository paths to a real directory on disk, without mounting.
    Restore {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable when omitted.
        #[arg(long)]
        repo: Option<PathBuf>,
        /// Overwrite a file that already exists at the destination. Off by default
        /// (REQ-RESTORE-004): restoring never overwrites a file that is already there unless
        /// told to.
        #[arg(long)]
        overwrite: bool,
        /// Check each restored file's content against its recorded hash. Off by default
        /// (REQ-RESTORE-003): a mismatch is never even detected unless this is given.
        #[arg(long)]
        verify: bool,
        /// Restore what can be restored instead of failing an item outright: zero-fill missing
        /// or incomplete stored data, and keep content that fails --verify anyway. Off by
        /// default (REQ-RESTORE-003).
        #[arg(long)]
        best_effort: bool,
        /// One or more repository paths to restore, followed by the target directory on disk
        /// (which must already exist).
        #[arg(required = true, num_args = 2..)]
        paths: Vec<String>,
    },
    // REQ-INGEST-001/002/003/004/005/006.
    /// Imports one or more real filesystem paths into the repository, deduplicating their
    /// content along the way.
    Ingest {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable when omitted.
        #[arg(long)]
        repo: Option<PathBuf>,
        /// An earlier ingest's target repository path to accelerate this run against
        /// (REQ-INGEST-003): a source file matching a same-named, same-size, same-modified-time
        /// file under it is linked to that existing content without being read again.
        #[arg(long)]
        reference: Option<String>,
        /// Skip REQ-INGEST-006's check that --reference actually corresponds to the sources being
        /// ingested, and use it regardless.
        #[arg(long, requires = "reference")]
        force_reference: bool,
        /// One or more real filesystem paths to import, followed by the target repository
        /// directory (which must already exist).
        #[arg(required = true, num_args = 2..)]
        paths: Vec<String>,
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

/// Unix epoch milliseconds for right now - used only for [`usage_log::log_invocation`]'s own
/// timestamp column, one per process invocation, so a fresh call per command (rather than a
/// shared helper) is not worth factoring out.
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_millis() as i64
}

fn main() {
    // The two-step equivalent of `Cli::parse()`, kept apart so `matches` (which flags were
    // actually passed on the command line) survives past parsing for `usage_log::log_invocation`
    // - `Parser::parse()` itself does exactly this internally, just without exposing `matches`.
    let matches = Cli::command().get_matches();
    let cli = Cli::from_arg_matches(&matches)
        .expect("matches were parsed against exactly this Cli::command(), so this cannot fail");
    // A second, distinct, unconsumed `Command` tree for the log call below - `get_matches` above
    // already consumed its own.
    let top = Cli::command();
    let time_millis = now_millis();

    match cli.command {
        Commands::CreateRepo { path, chunking } => {
            let cdc_target_size_bits =
                resolve_cdc_target_size_bits(chunking.whole_file, chunking.cdc_target_size_bits);
            let (path, default_path_used) = resolve_repo_path(path);
            create_repo::run(&path, cdc_target_size_bits, default_path_used);
            // Only reached once create_repo::run has actually succeeded (it exits the process on
            // failure) - meta/ does not exist yet beforehand, unlike every other command below.
            usage_log::log_invocation(&db::meta_dir(&path), &top, &matches, time_millis);
        }
        Commands::Mount {
            repo,
            mountpoint,
            read_write,
        } => {
            let (repo, default_path_used) = resolve_repo_path(repo);
            usage_log::log_invocation(&db::meta_dir(&repo), &top, &matches, time_millis);
            mount::run(&repo, &mountpoint, read_write, default_path_used);
        }
        Commands::Unlock { path } => {
            let (path, default_path_used) = resolve_repo_path(path);
            usage_log::log_invocation(&db::meta_dir(&path), &top, &matches, time_millis);
            unlock::run(&path, default_path_used);
        }
        Commands::List { repo, path } => {
            let (repo, default_path_used) = resolve_repo_path(repo);
            usage_log::log_invocation(&db::meta_dir(&repo), &top, &matches, time_millis);
            list::run(&repo, default_path_used, &path);
        }
        Commands::Restore {
            repo,
            overwrite,
            verify,
            best_effort,
            paths,
        } => {
            let (repo, default_path_used) = resolve_repo_path(repo);
            usage_log::log_invocation(&db::meta_dir(&repo), &top, &matches, time_millis);
            let (sources, target) = paths.split_at(paths.len() - 1);
            restore::run(
                &repo,
                default_path_used,
                sources,
                Path::new(&target[0]),
                overwrite,
                verify,
                best_effort,
            );
        }
        Commands::Ingest {
            repo,
            reference,
            force_reference,
            paths,
        } => {
            let (repo, default_path_used) = resolve_repo_path(repo);
            usage_log::log_invocation(&db::meta_dir(&repo), &top, &matches, time_millis);
            let (sources, target) = paths.split_at(paths.len() - 1);
            ingest::run(
                &repo,
                default_path_used,
                sources,
                &target[0],
                reference.as_deref(),
                force_reference,
            );
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
