mod create_repo;
mod mount;

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Args)]
#[group(required = true, multiple = false)]
struct ChunkingArgs {
    /// Use content-defined chunking with this target chunk size, in bits (6-30) - average chunk
    /// size ends up slightly above 2^bits.
    #[arg(long)]
    cdc_target_size_bits: Option<u32>,
    /// Use whole-file chunking - no sub-file deduplication.
    #[arg(long)]
    whole_file: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Creates a new, empty repository at PATH.
    CreateRepo {
        path: PathBuf,
        #[command(flatten)]
        chunking: ChunkingArgs,
    },
    /// Mounts a temporary in-memory playground filesystem - not a DedupFS repository yet.
    Mount { mountpoint: PathBuf },
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

fn main() {
    match Cli::parse().command {
        Commands::CreateRepo { path, chunking } => {
            let cdc_target_size_bits = if chunking.whole_file {
                None
            } else {
                chunking.cdc_target_size_bits
            };
            create_repo::run(&path, cdc_target_size_bits);
        }
        Commands::Mount { mountpoint } => mount::run(&mountpoint),
    }
}
