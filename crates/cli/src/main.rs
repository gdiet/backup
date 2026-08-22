mod mount;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Subcommand)]
enum Commands {
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
        Commands::Mount { mountpoint } => mount::run(&mountpoint),
    }
}
