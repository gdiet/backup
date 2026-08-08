use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};

mod backup_ignore;
mod check;
mod chunk_store;
mod db_maintenance;
mod del;
mod find;
mod format;
mod io_limiter;
mod list;
mod migrate_scala_repo;
mod mount;
mod ram_budget_check;
mod reclaim_space;
mod restore;
mod spilling_chunker;
mod stats;
mod store;
mod temp_dir;

/// Deduplicating backup application.
#[derive(Parser)]
#[command(name = "backup", about = "Deduplicating backup application.")]
struct Cli {
    /// Repository directory.
    #[arg(
        short = 'r',
        long,
        default_value = "../backup-repository",
        global = true
    )]
    repo: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Initialize a new repository.
    Init(InitArgs),
    /// Back up one or more sources to a target path in the repository.
    Store(store::BackupArgs),
    /// Restore one or more sources from the repository to a target directory.
    Restore(restore::RestoreArgs),
    /// Show repository statistics, or statistics for a specific path.
    Stats(stats::StatsArgs),
    /// List a directory's contents, or show info for a single file.
    List(list::ListArgs),
    /// Search the whole repository for entries matching a name pattern.
    Find(find::FindArgs),
    /// Verify stored chunk data against the metadata database.
    Check(check::CheckArgs),
    /// Soft-delete a file, or a directory and everything under it.
    Del(del::DelArgs),
    /// Database maintenance: backup, restore, compact.
    Db(db_maintenance::DbArgs),
    /// Hard-delete old soft-deleted entries and orphaned chunks/contents.
    ReclaimSpace(reclaim_space::ReclaimSpaceArgs),
    /// Mount the repository's file tree read-only via FUSE.
    Mount(mount::MountArgs),
    /// Migrate an old Scala repository (its `fsc db-backup` SQL export plus
    /// its `data/` directory) into this, already-initialized but empty,
    /// repository - re-chunking and re-hashing all content along the way.
    MigrateScalaRepo(migrate_scala_repo::MigrateScalaRepoArgs),
}

#[derive(Args)]
struct InitArgs {
    /// Average CDC chunk size target, as `2^N` bytes.
    #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u32).range(
        *db::CDC_TARGET_SIZE_BITS_RANGE.start() as i64..=*db::CDC_TARGET_SIZE_BITS_RANGE.end() as i64
    ))]
    cdc_target_size_bits: u32,

    /// Chunking method used when splitting file content for deduplication.
    #[arg(long, value_enum, default_value_t = ChunkingArg::Cdc)]
    chunking: ChunkingArg,
}

/// CLI-facing mirror of [`db::Chunking`] (`clap::ValueEnum` can't be derived for a
/// type defined in another crate).
#[derive(Clone, Copy, ValueEnum)]
enum ChunkingArg {
    Cdc,
    None,
}

impl From<ChunkingArg> for db::Chunking {
    fn from(value: ChunkingArg) -> Self {
        match value {
            ChunkingArg::Cdc => db::Chunking::Cdc,
            ChunkingArg::None => db::Chunking::None,
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Command::Init(args) => match run_init(&cli.repo, args) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("error: {err}");
                ExitCode::FAILURE
            }
        },
        Command::Store(args) => store::run_store(&cli.repo, args),
        Command::Restore(args) => restore::run_restore(&cli.repo, args),
        Command::Stats(args) => stats::run_stats(&cli.repo, args),
        Command::List(args) => list::run_list(&cli.repo, args),
        Command::Find(args) => find::run_find(&cli.repo, args),
        Command::Check(args) => check::run_check(&cli.repo, args),
        Command::Del(args) => del::run_del(&cli.repo, args),
        Command::Db(args) => db_maintenance::run_db(&cli.repo, args),
        Command::ReclaimSpace(args) => reclaim_space::run_reclaim_space(&cli.repo, args),
        Command::Mount(args) => mount::run_mount(&cli.repo, args),
        Command::MigrateScalaRepo(args) => {
            migrate_scala_repo::run_migrate_scala_repo(&cli.repo, args)
        }
    }
}

fn run_init(repo: &Path, args: InitArgs) -> Result<(), db::Error> {
    let settings = db::RepositorySettings::new(args.cdc_target_size_bits, args.chunking.into())?;
    db::init_repository(repo, &settings)?;
    println!("repository initialized: {}", repo.display());
    Ok(())
}
