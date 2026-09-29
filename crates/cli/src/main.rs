mod backpressure;
mod content_reader;
mod create_repo;
mod db_backup;
mod db_compact;
mod db_restore;
mod dedup_fs;
mod del;
mod deleted;
mod entry_format;
mod failure_log;
mod find;
mod ignore_rules;
mod ingest;
mod list;
mod migrate_content;
mod migrate_scala_repo;
mod mount;
mod pending_files;
mod ram_budget;
mod reclaim;
mod repo_path;
mod restore;
mod scala_import;
mod settle;
mod settle_pool;
mod stats;
mod target_path;
mod time_format;
mod unlock;
mod usage_log;
mod write_cache;

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand, parser::ValueSource};

/// REQ-CLI-005's default when `--cdc-target-size-bits` is not given - content-defined chunking
/// with an average chunk size a little above 1 MiB.
const DEFAULT_CDC_TARGET_SIZE_BITS: u32 = 20;

#[derive(Args)]
struct ChunkingArgs {
    /// Content-defined chunking's target chunk size, in bits (6-23) - average chunk size ends up
    /// slightly above 2^bits. Defaults to 20 if not given. This choice is fixed for the
    /// repository's lifetime once created - it cannot be changed later.
    #[arg(long)]
    cdc_target_size_bits: Option<u32>,
}

/// Resolves `ChunkingArgs` into the value `create_repo::run` expects: an explicit or defaulted
/// target size (REQ-CLI-005) - REQ-STORAGE-003 no longer offers a separate whole-file mode.
fn resolve_cdc_target_size_bits(explicit_bits: Option<u32>) -> u32 {
    explicit_bits.unwrap_or(DEFAULT_CDC_TARGET_SIZE_BITS)
}

#[derive(Args)]
struct RamBudgetArgs {
    /// The memory budget this process aims to stay within, in megabytes. A higher budget allows
    /// more caching, which can improve performance.
    #[arg(long, default_value_t = ram_budget::DEFAULT_GROSS_BUDGET_BYTES / (1024 * 1024))]
    ram_budget_mb: u64,
}

#[derive(Args)]
struct ReadOnlyMediumArgs {
    /// Allows opening a repository that lives on genuinely read-only media, which would
    /// otherwise fail to open. If the repository is nonetheless modified by another process
    /// while this command has it open, the result is undefined behavior and can include data
    /// corruption.
    #[arg(long)]
    assume_read_only_medium: bool,
}

/// REQ-OPERABILITY-008's own opt-in, shared by every command that shows a timestamp: local
/// timezone by default, this command's own escape hatch into UTC.
#[derive(Args)]
struct UtcArgs {
    /// Shows timestamps in UTC instead of the local timezone (the default). Does not affect
    /// `[deleted]`'s own disambiguated names (`dfs list`/`dfs restore`'s output, or a mounted
    /// read-write session's own `[deleted]` view) - that addressing stays UTC unconditionally,
    /// since it is a stable identity meant to be pasted between commands and sessions, not a
    /// place for a display preference to leak into.
    #[arg(long)]
    utc: bool,
}

/// [`UtcArgs::utc`] resolved into the enum [`time_format`] itself works with.
fn time_display(utc: bool) -> time_format::TimeDisplay {
    if utc {
        time_format::TimeDisplay::Utc
    } else {
        time_format::TimeDisplay::Local
    }
}

#[derive(Args)]
struct BackpressureArgs {
    /// Slows writes down once too much data is still waiting to be durably saved, so a slow or
    /// busy repository cannot build up an unbounded backlog of not-yet-saved content. Below this
    /// much backlog, in bytes, writes are never slowed down at all.
    #[arg(long, default_value_t = crate::backpressure::DEFAULT_FREE_ZONE_BYTES)]
    backpressure_free_zone_bytes: u64,
    /// How quickly writes get slowed down further as the backlog grows past
    /// `--backpressure-free-zone-bytes` - a smaller value slows writes down faster for the same
    /// backlog.
    #[arg(long, default_value_t = crate::backpressure::DEFAULT_SLOPE_DIVISOR)]
    backpressure_slope_divisor: u128,
}

#[derive(Subcommand)]
enum Commands {
    // REQ-CLI-005.
    /// Creates a new, empty repository.
    CreateRepo {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable.
        path: Option<PathBuf>,
        #[command(flatten)]
        chunking: ChunkingArgs,
    },
    // REQ-INGEST-001/002/003/004/005/006.
    /// Imports one or more directories or files into the repository, deduplicating their
    /// content along the way.
    Ingest {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable.
        #[arg(long)]
        repository: Option<PathBuf>,
        /// An earlier ingest's target repository path to accelerate this run against:
        /// a source file matching a same-named, same-size, same-modified-time
        /// file under it is linked to that existing content without being read again.
        #[arg(long)]
        reference: Option<String>,
        /// Skip the likeness check that --reference actually corresponds to the sources being
        /// ingested, and use it regardless.
        #[arg(long, requires = "reference")]
        force_reference: bool,
        /// One or more directories or files to import, followed by the target repository path.
        /// Each `/`-separated segment must already exist by default; prefix a
        /// segment with `+` to create it on demand, or with `!` to require it be
        /// freshly created. Marking a segment either way makes every segment below it default to
        /// `+`. A segment may also contain date/time placeholders in square brackets (`yyyy`/`MM`/`dd`/`HH`/
        /// `mm`/`ss`, e.g. `[yyyy-MM-dd]`), which will be resolved against this run's start time.
        #[arg(required = true, num_args = 2.., value_name = "PATH")]
        paths: Vec<String>,
        #[command(flatten)]
        ram_budget: RamBudgetArgs,
    },
    // REQ-MOUNT-001.
    /// Mounts a repository as a real filesystem.
    Mount {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable.
        // A flag, not a positional like create-repo's `path`: clap does not allow an optional
        // positional ahead of a required one, and MOUNTPOINT below must stay required.
        #[arg(long)]
        repository: Option<PathBuf>,
        /// Directory to mount at. On Linux, this directory must already exist - fuse3 refuses to
        /// mount onto a path that does not. On Windows, it does not need to already exist - WinFSP
        /// creates it itself and removes it again on unmount.
        mountpoint: PathBuf,
        // REQ-MOUNT-002/003.
        /// Allow structural changes, content writes, and modification-time updates through the
        /// mount. Without this, the mount is read-only.
        #[arg(long)]
        read_write: bool,
        // REQ-MOUNT-004/007.
        /// Reveal and make browsable the `[deleted]` view (and its `[time]` presentation) through
        /// the mount, at the same locations `dfs list --show-deleted` reveals them. Off by
        /// default, so an ordinary recursive tool walking the mount never descends into deletion
        /// history without asking for it. Available on a read-only mount too.
        #[arg(long)]
        show_deleted: bool,
        /// A second, escalating opt-in: additionally allows permanently purging an
        /// entry from inside the `[deleted]` view (deleting it there, rather than only recovering
        /// it by moving it out). Meaningless without `--show-deleted`, and without `--read-write`
        /// - nothing mutating is ever allowed on a read-only mount regardless of this flag.
        #[arg(long)]
        purge: bool,
        #[command(flatten)]
        ram_budget: RamBudgetArgs,
        // DESIGN-MOUNT-018.
        /// Directory content gets cached to on disk when needed, instead of the OS temp
        /// directory (the default). Useful e.g. when the temp directory is on a slow disk or
        /// doesn't have enough free space. Should be a local disk, not a network share - ideally
        /// not the repository's own disk either. Must already exist.
        #[arg(long)]
        spill_directory: Option<PathBuf>,
        #[command(flatten)]
        backpressure: BackpressureArgs,
        // Only meaningful without --read-write - a read-write mount already needs, and holds, the
        // repository-wide write lock (REQ-MAINTENANCE-004), which already rules out a concurrent
        // writer for the reason DESIGN-METADATA-013's assertion cares about.
        #[command(flatten)]
        read_only_medium: ReadOnlyMediumArgs,
        // Affects only the [time] view (REQ-MOUNT-008) - the base [deleted] view is unaffected
        // (UtcArgs's own doc comment).
        #[command(flatten)]
        utc: UtcArgs,
        /// Logs every call this mount session's own filesystem implementation receives (path,
        /// arguments, result) to this file, one line per call - a diagnostic aid for working out
        /// what a file manager or `rm -rf` actually does against the mount, not something an
        /// ordinary session needs. Overwritten fresh on each `dfs mount` invocation. Off by
        /// default: this can grow large fast, and unlike --show-deleted/--purge it has no
        /// bearing on repository content, only on this one session's own observability.
        #[arg(long)]
        debug_log: Option<PathBuf>,
    },
    // REQ-RESTORE-001/003/004.
    /// Restores one or more repository paths to a real directory on disk, without mounting.
    Restore {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable.
        #[arg(long)]
        repository: Option<PathBuf>,
        /// Overwrite a file that already exists at the destination. Off by default.
        #[arg(long)]
        overwrite: bool,
        /// Check each restored file's content against its recorded hash. Off by default: a
        /// mismatch is never even detected unless this is given.
        #[arg(long)]
        verify: bool,
        /// Restore what can be restored instead of failing an item outright: zero-fill missing
        /// or incomplete stored data, and keep content that fails --verify anyway. Off by
        /// default.
        #[arg(long)]
        best_effort: bool,
        /// One or more repository paths to restore, followed by the target directory on disk
        /// (which must already exist).
        #[arg(required = true, num_args = 2.., value_name = "PATH")]
        paths: Vec<String>,
        #[command(flatten)]
        read_only_medium: ReadOnlyMediumArgs,
    },
    // REQ-QUERY-001, REQ-CLI-007.
    /// Lists a directory's live, direct contents, without mounting.
    List {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable.
        #[arg(long)]
        repository: Option<PathBuf>,
        /// Reveal the `[deleted]` marker in the listing wherever the target directory
        /// has soft-deleted children - off by default, so a script parsing plain `dfs list`
        /// output is never surprised by an extra entry. A path that already names `[deleted]`
        /// explicitly works regardless of this flag.
        #[arg(long)]
        show_deleted: bool,
        /// Repository path to list.
        #[arg(default_value = "/")]
        path: String,
        #[command(flatten)]
        read_only_medium: ReadOnlyMediumArgs,
        #[command(flatten)]
        utc: UtcArgs,
    },
    // REQ-QUERY-002.
    /// Searches live entries anywhere in the repository by name, without mounting.
    Find {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable.
        #[arg(long)]
        repository: Option<PathBuf>,
        /// Name pattern to search for - case-insensitive, `*` matches any run of characters and
        /// `?` matches exactly one.
        pattern: String,
        #[command(flatten)]
        read_only_medium: ReadOnlyMediumArgs,
        #[command(flatten)]
        utc: UtcArgs,
    },
    // REQ-QUERY-003.
    /// Reports item counts and size statistics, repository-wide or for one directory's own
    /// subtree, without mounting.
    Stats {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable.
        #[arg(long)]
        repository: Option<PathBuf>,
        /// Repository path to report on. Repository age is only reported for the default, `/`.
        #[arg(default_value = "/")]
        path: String,
        #[command(flatten)]
        read_only_medium: ReadOnlyMediumArgs,
        #[command(flatten)]
        utc: UtcArgs,
    },
    // REQ-CLI-003.
    /// Deletes a tree entry directly against the repository, without mounting.
    Del {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable.
        #[arg(long)]
        repository: Option<PathBuf>,
        /// When the target is a live directory that still has live children, delete them too
        /// (deepest first) instead of refusing. Refused as an error if the target instead
        /// resolves to a soft-deleted entry, where it would have no effect.
        #[arg(long)]
        recursive: bool,
        /// When the target is a specific soft-deleted entry (reached through the `[deleted]`
        /// segment - see `dfs list --show-deleted`), permanently remove it instead of
        /// refusing. Without this, such a target is left untouched: an irreversible removal never
        /// happens just because the given path happened to resolve under `[deleted]`. Refused as
        /// an error if the target instead resolves to a live path, where it would have no effect.
        #[arg(long)]
        purge: bool,
        /// Repository path to delete - a live path, or one reached through `[deleted]`.
        path: String,
    },
    // REQ-MAINTENANCE-008, DESIGN-MAINTENANCE-003.
    /// Checks whether a repository's write lock is stale (nothing currently holds it) and clears
    /// it if so. Never removes an actively held lock.
    Unlock {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable.
        path: Option<PathBuf>,
    },
    // REQ-STORAGE-004.
    /// Bulk-purges every soft-deleted entry that has stayed soft-deleted for at least a
    /// caller-chosen minimum age, reclaiming the storage each purge frees along the way.
    Reclaim {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable.
        #[arg(long)]
        repository: Option<PathBuf>,
        /// Only purge an entry that has stayed soft-deleted for at least this many days.
        /// Defaults to 0 (purged the next time this command runs at all).
        #[arg(long, default_value_t = 0)]
        min_age_days: u32,
    },
    // REQ-MAINTENANCE-001.
    /// Backs up a repository's metadata to a fresh, timestamped, self-contained file.
    DbBackup {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable.
        #[arg(long)]
        repository: Option<PathBuf>,
        /// Directory to write the timestamped backup file into (which must already exist).
        target: PathBuf,
        #[command(flatten)]
        read_only_medium: ReadOnlyMediumArgs,
    },
    // REQ-MAINTENANCE-002.
    /// Restores a repository's metadata from a prior `db-backup` file, wholesale-replacing the
    /// live metadata store.
    DbRestore {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable.
        #[arg(long)]
        repository: Option<PathBuf>,
        /// The backup file to restore from (produced by `dfs db-backup`).
        backup: PathBuf,
    },
    // REQ-MAINTENANCE-003.
    /// Compacts a repository's metadata store, reclaiming space freed by past deletions.
    DbCompact {
        /// Repository path. Defaults to a `dedupfs-repository` directory next to the dfs
        /// executable.
        #[arg(long)]
        repository: Option<PathBuf>,
    },
    // REQ-MIGRATION-001 through 005.
    /// Adopts an existing Scala-DedupFS repository in place: imports its `fsc db-backup` SQL
    /// export's metadata into a small, durable staging database, adopts (or reuses) one destination
    /// metadata database per `--cdc-target-size-bits` value against the repository's own, unchanged
    /// `data/` directory, then migrates the whole tree and content into every destination
    /// (docs/design/scala-migration-tool.md). Safely resumable: an interrupted run picks up where
    /// it left off on the next invocation with the same arguments.
    MigrateScalaRepo {
        /// The existing Scala-DedupFS repository to adopt in place. Its `data/` directory must
        /// already exist (REQ-MIGRATION-002) and is only ever read from, never written to
        /// (REQ-MIGRATION-005) - unlike every other command's `--repository`, this is not defaulted,
        /// since guessing wrong here would point the migration at the wrong repository entirely.
        #[arg(long)]
        repository: PathBuf,
        /// Path to the H2 SQL script export produced by the Scala tool's `fsc db-backup` command -
        /// either the zipped script as produced directly, or an already-unzipped `.sql` file.
        #[arg(long)]
        script: PathBuf,
        /// Where to keep the imported metadata. An ordinary file, persisted across runs rather
        /// than rebuilt each time (DESIGN-MIGRATION-001) - reused as-is on a later invocation
        /// unless it is missing or was left behind by an interrupted import. Defaults to
        /// `migrate-staging.db` inside `--repository` itself, removed automatically once every
        /// requested target size has been fully migrated.
        #[arg(long)]
        staging: Option<PathBuf>,
        /// The candidate content-defined-chunking target size(s), in bits, to migrate into - one
        /// destination metadata database per value (DESIGN-MIGRATION-003). Repeat this flag to
        /// compare more than one value from a single read of the source; migrating into more than
        /// one leaves none of the resulting databases immediately usable until one is chosen and
        /// renamed (DESIGN-MIGRATION-004).
        #[arg(long, required = true)]
        cdc_target_size_bits: Vec<u32>,
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

/// Whether `id` was actually typed on the command line within `matches`, as opposed to a
/// `default_value_t` that is present either way - REQ-OPERABILITY-007's distinction for `mount`'s
/// read-write-only tuning flags (see [`mount::RepoOpenOptions`]'s `_given` fields).
fn explicitly_given(matches: &clap::ArgMatches, id: &str) -> bool {
    matches.value_source(id) == Some(ValueSource::CommandLine)
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
            let cdc_target_size_bits = resolve_cdc_target_size_bits(chunking.cdc_target_size_bits);
            let (path, default_path_used) = resolve_repo_path(path);
            create_repo::run(&path, cdc_target_size_bits, default_path_used);
            // Only reached once create_repo::run has actually succeeded (it exits the process on
            // failure) - meta/ does not exist yet beforehand, unlike every other command below.
            usage_log::log_invocation(&db::meta_dir(&path), &top, &matches, time_millis);
        }
        Commands::Ingest {
            repository,
            reference,
            force_reference,
            paths,
            ram_budget,
        } => {
            let (repository, default_path_used) = resolve_repo_path(repository);
            usage_log::log_invocation(&db::meta_dir(&repository), &top, &matches, time_millis);
            let (sources, target) = paths.split_at(paths.len() - 1);
            ingest::run(
                &repository,
                default_path_used,
                sources,
                &target[0],
                reference.as_deref(),
                force_reference,
                ram_budget.ram_budget_mb * 1024 * 1024,
            );
        }
        Commands::Mount {
            repository,
            mountpoint,
            read_write,
            read_only_medium,
            show_deleted,
            purge,
            ram_budget,
            spill_directory,
            backpressure,
            utc,
            debug_log,
        } => {
            let (repository, default_path_used) = resolve_repo_path(repository);
            usage_log::log_invocation(&db::meta_dir(&repository), &top, &matches, time_millis);
            let mount_matches = matches
                .subcommand_matches("mount")
                .expect("cli.command matched Commands::Mount, so its own matches must exist");
            mount::run(
                &repository,
                &mountpoint,
                read_write,
                default_path_used,
                spill_directory.as_deref(),
                mount::RepoOpenOptions {
                    assume_read_only_medium: read_only_medium.assume_read_only_medium,
                    ram_budget_mb_given: explicitly_given(mount_matches, "ram_budget_mb"),
                    backpressure_free_zone_bytes_given: explicitly_given(
                        mount_matches,
                        "backpressure_free_zone_bytes",
                    ),
                    backpressure_slope_divisor_given: explicitly_given(
                        mount_matches,
                        "backpressure_slope_divisor",
                    ),
                },
                dedup_fs::Tuning {
                    ram_budget_gross_bytes: ram_budget.ram_budget_mb * 1024 * 1024,
                    backpressure_free_zone_bytes: backpressure.backpressure_free_zone_bytes,
                    backpressure_slope_divisor: backpressure.backpressure_slope_divisor,
                    show_deleted,
                    allow_purge: purge,
                    time_display: time_display(utc.utc),
                },
                debug_log.as_deref(),
            );
        }
        Commands::Restore {
            repository,
            overwrite,
            verify,
            best_effort,
            paths,
            read_only_medium,
        } => {
            let (repository, default_path_used) = resolve_repo_path(repository);
            usage_log::log_invocation(&db::meta_dir(&repository), &top, &matches, time_millis);
            let (sources, target) = paths.split_at(paths.len() - 1);
            restore::run(
                &repository,
                default_path_used,
                sources,
                Path::new(&target[0]),
                restore::RestoreOptions {
                    overwrite,
                    verify,
                    best_effort,
                },
                read_only_medium.assume_read_only_medium,
            );
        }
        Commands::List {
            repository,
            show_deleted,
            path,
            read_only_medium,
            utc,
        } => {
            let (repository, default_path_used) = resolve_repo_path(repository);
            usage_log::log_invocation(&db::meta_dir(&repository), &top, &matches, time_millis);
            list::run(
                &repository,
                default_path_used,
                &path,
                show_deleted,
                read_only_medium.assume_read_only_medium,
                time_display(utc.utc),
            );
        }
        Commands::Find {
            repository,
            pattern,
            read_only_medium,
            utc,
        } => {
            let (repository, default_path_used) = resolve_repo_path(repository);
            usage_log::log_invocation(&db::meta_dir(&repository), &top, &matches, time_millis);
            find::run(
                &repository,
                default_path_used,
                &pattern,
                read_only_medium.assume_read_only_medium,
                time_display(utc.utc),
            );
        }
        Commands::Stats {
            repository,
            path,
            read_only_medium,
            utc,
        } => {
            let (repository, default_path_used) = resolve_repo_path(repository);
            usage_log::log_invocation(&db::meta_dir(&repository), &top, &matches, time_millis);
            stats::run(
                &repository,
                default_path_used,
                &path,
                read_only_medium.assume_read_only_medium,
                time_display(utc.utc),
            );
        }
        Commands::Del {
            repository,
            recursive,
            purge,
            path,
        } => {
            let (repository, default_path_used) = resolve_repo_path(repository);
            usage_log::log_invocation(&db::meta_dir(&repository), &top, &matches, time_millis);
            del::run(&repository, default_path_used, &path, recursive, purge);
        }
        Commands::Unlock { path } => {
            let (path, default_path_used) = resolve_repo_path(path);
            usage_log::log_invocation(&db::meta_dir(&path), &top, &matches, time_millis);
            unlock::run(&path, default_path_used);
        }
        Commands::Reclaim {
            repository,
            min_age_days,
        } => {
            let (repository, default_path_used) = resolve_repo_path(repository);
            usage_log::log_invocation(&db::meta_dir(&repository), &top, &matches, time_millis);
            reclaim::run(&repository, default_path_used, min_age_days, time_millis);
        }
        Commands::DbBackup {
            repository,
            target,
            read_only_medium,
        } => {
            let (repository, default_path_used) = resolve_repo_path(repository);
            usage_log::log_invocation(&db::meta_dir(&repository), &top, &matches, time_millis);
            db_backup::run(
                &repository,
                default_path_used,
                &target,
                time_millis,
                read_only_medium.assume_read_only_medium,
            );
        }
        Commands::DbRestore { repository, backup } => {
            let (repository, default_path_used) = resolve_repo_path(repository);
            usage_log::log_invocation(&db::meta_dir(&repository), &top, &matches, time_millis);
            db_restore::run(&repository, default_path_used, &backup);
        }
        Commands::DbCompact { repository } => {
            let (repository, default_path_used) = resolve_repo_path(repository);
            usage_log::log_invocation(&db::meta_dir(&repository), &top, &matches, time_millis);
            db_compact::run(&repository, default_path_used);
        }
        Commands::MigrateScalaRepo {
            repository,
            script,
            staging,
            cdc_target_size_bits,
        } => {
            migrate_scala_repo::run(
                &repository,
                &script,
                staging.as_deref(),
                &cdc_target_size_bits,
            );
            // Only reached once migrate_scala_repo::run has actually succeeded (it exits the
            // process on failure) - meta/ may not exist yet beforehand (or ever, if more than one
            // target size was requested), the same reasoning as Commands::CreateRepo above.
            usage_log::log_invocation(&db::meta_dir(&repository), &top, &matches, time_millis);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_cdc_target_size_bits_defaults_to_20_when_not_given() {
        assert_eq!(resolve_cdc_target_size_bits(None), 20);
    }

    #[test]
    fn resolve_cdc_target_size_bits_respects_an_explicit_target_size() {
        assert_eq!(resolve_cdc_target_size_bits(Some(22)), 22);
    }
}
