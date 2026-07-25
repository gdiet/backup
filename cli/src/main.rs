use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// Deduplicating backup application.
#[derive(Parser)]
#[command(name = "backup", about = "Deduplicating backup application.")]
struct Cli {
    /// Repository directory.
    #[arg(short = 'r', long, default_value = "../backup-repository", global = true)]
    repo: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Initialize a new repository.
    Init(InitArgs),
    /// Back up one or more sources to a target path in the repository.
    Store(BackupArgs),
    /// Restore one or more sources from the repository to a target directory.
    Restore(RestoreArgs),
}

#[derive(Args)]
struct InitArgs {}

#[derive(Args)]
struct BackupArgs {
    /// Create missing target directories.
    #[arg(short = 'p', long = "create-dirs")]
    create_dirs: bool,

    /// Require target to be an existing directory.
    #[arg(short = 't', long = "target-exists")]
    target_exists: bool,

    /// Number of concurrent backup processes (1-32).
    #[arg(short = 'c', long, default_value_t = 4, value_parser = clap::value_parser!(u32).range(1..=32))]
    concurrency: u32,

    /// One or more source paths followed by the target path in the repository.
    #[arg(required = true, num_args = 2.., value_name = "PATH")]
    paths: Vec<PathBuf>,
}

#[derive(Args)]
struct RestoreArgs {
    /// One or more source paths in the repository followed by the target directory.
    #[arg(required = true, num_args = 2.., value_name = "PATH")]
    paths: Vec<PathBuf>,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Init(args) => run_init(&cli.repo, args),
        Command::Store(args) => run_backup(&cli.repo, args),
        Command::Restore(args) => run_restore(&cli.repo, args),
    }
}

fn run_init(repo: &PathBuf, _args: InitArgs) {
    // TODO: implement repository initialization.
    println!("repo: {repo:?}");
    println!("init: not yet implemented");
}

fn run_backup(repo: &PathBuf, args: BackupArgs) {
    let (sources, target) = args.paths.split_at(args.paths.len() - 1);
    let target = &target[0];

    // TODO: replace with actual backup implementation.
    println!("repo: {repo:?}");
    println!("sources: {sources:?}");
    println!("target: {target:?}");
    println!("create_dirs: {}", args.create_dirs);
    println!("target_exists: {}", args.target_exists);
    println!("concurrency: {}", args.concurrency);
}

fn run_restore(repo: &PathBuf, args: RestoreArgs) {
    let (sources, target) = args.paths.split_at(args.paths.len() - 1);
    let target = &target[0];

    // TODO: replace with actual restore implementation.
    println!("repo: {repo:?}");
    println!("sources: {sources:?}");
    println!("target: {target:?}");
}
