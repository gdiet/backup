use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};

mod check;
mod find;
mod format;
mod list;
mod stats;
mod store;

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
    Restore(RestoreArgs),
    /// Show repository statistics, or statistics for a specific path.
    Stats(stats::StatsArgs),
    /// List a directory's contents, or show info for a single file.
    List(list::ListArgs),
    /// Search the whole repository for entries matching a name pattern.
    Find(find::FindArgs),
    /// Verify stored chunk data against the metadata database.
    Check(check::CheckArgs),
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

#[derive(Args)]
struct RestoreArgs {
    /// One or more source paths in the repository followed by the target directory.
    #[arg(required = true, num_args = 2.., value_name = "PATH")]
    paths: Vec<PathBuf>,
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
        Command::Restore(args) => {
            run_restore(&cli.repo, args);
            ExitCode::SUCCESS
        }
        Command::Stats(args) => stats::run_stats(&cli.repo, args),
        Command::List(args) => list::run_list(&cli.repo, args),
        Command::Find(args) => find::run_find(&cli.repo, args),
        Command::Check(args) => check::run_check(&cli.repo, args),
    }
}

fn run_init(repo: &Path, args: InitArgs) -> Result<(), db::Error> {
    let settings = db::RepositorySettings::new(args.cdc_target_size_bits, args.chunking.into())?;
    db::init_repository(repo, &settings)?;
    println!("repository initialized: {}", repo.display());
    Ok(())
}

fn run_restore(repo: &Path, args: RestoreArgs) {
    let (sources, target) = args.paths.split_at(args.paths.len() - 1);
    let target = &target[0];

    // TODO: replace with actual restore implementation.
    println!("repo: {repo:?}");
    println!("sources: {sources:?}");
    println!("target: {target:?}");
}
