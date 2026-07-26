use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use cdc::{CdcConfig, ChunkHasher, HashingChunker, LengthHash};
use clap::Args;
use rayon::ThreadPoolBuilder;
use rayon::prelude::*;

/// Average chunk size target for content-defined chunking, as `2^CDC_TARGET_SIZE_BITS` bytes.
const CDC_TARGET_SIZE_BITS: u32 = 20;

/// Number of bytes read from a file at a time.
const READ_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Args)]
pub struct BackupArgs {
    /// Create missing target directories.
    #[arg(short = 'p', long = "create-dirs")]
    create_dirs: bool,

    /// Require target to be an existing directory.
    #[arg(short = 't', long = "target-exists")]
    target_exists: bool,

    /// Number of concurrent chunking threads (1-32). Default: rayon's global thread
    /// pool (one thread per CPU core).
    #[arg(short = 'c', long, value_parser = clap::value_parser!(u32).range(1..=32))]
    concurrency: Option<u32>,

    /// One or more source paths followed by the target path in the repository.
    #[arg(required = true, num_args = 2.., value_name = "PATH")]
    paths: Vec<PathBuf>,
}

/// A [`ChunkHasher`] backed by [`blake3::Hasher`].
///
/// A local newtype is required because Rust's orphan rule forbids implementing a
/// foreign trait (`cdc::ChunkHasher`) for a foreign type (`blake3::Hasher`) directly.
struct Blake3Hasher(blake3::Hasher);

/// Number of hash bytes taken from blake3's extendable output.
const HASH_LENGTH: usize = 20;

impl ChunkHasher for Blake3Hasher {
    fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    fn finalize_reset(&mut self) -> Vec<u8> {
        let mut hash = [0u8; HASH_LENGTH];
        self.0.finalize_xof().fill(&mut hash);
        self.0.reset();
        hash.to_vec()
    }
}

pub fn run_store(repo: &Path, args: BackupArgs) {
    let (sources, target) = args.paths.split_at(args.paths.len() - 1);
    let target = &target[0];

    // TODO: use target/create_dirs/target_exists once the repository metadata store exists.
    println!("repo: {repo:?}");
    println!("target: {target:?}");

    // Validate the CDC config once, up front, instead of on every call to chunk_and_print.
    let cdc_config =
        CdcConfig::new(CDC_TARGET_SIZE_BITS).expect("CDC_TARGET_SIZE_BITS is a valid constant");

    // Traversal (single-threaded, via the underlying iterator) and chunking (parallel,
    // via rayon) run concurrently: chunking of already-found files overlaps with
    // discovering the rest of the tree, instead of waiting for the full walk to finish.
    let run = || {
        walk_files(sources)
            .par_bridge()
            .for_each(|path| chunk_and_print(&path, &cdc_config))
    };

    match args.concurrency {
        Some(concurrency) => {
            let pool = ThreadPoolBuilder::new()
                .num_threads(concurrency as usize)
                .build()
                .expect("failed to build thread pool");
            pool.install(run);
        }
        None => run(),
    }
}

/// Sequentially traverses all `sources`, one after another, yielding the path of each
/// regular file found. Errors accessing individual entries are logged to stderr and skipped.
fn walk_files(sources: &[PathBuf]) -> impl Iterator<Item = PathBuf> + '_ {
    sources.iter().flat_map(|source| {
        walkdir::WalkDir::new(source)
            .into_iter()
            .filter_map(|entry| match entry {
                Ok(entry) if entry.file_type().is_file() => Some(entry.into_path()),
                Ok(_) => None, // skip directories, symlinks, etc.
                Err(err) => {
                    eprintln!("warning: failed to access entry: {err}");
                    None
                }
            })
    })
}

/// Reads, chunks and hashes the file at `path`, then prints its chunks (length and hash)
/// to stdout. Errors reading the file are logged to stderr and the file is skipped.
fn chunk_and_print(path: &Path, cdc_config: &CdcConfig) {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: failed to open {}: {err}", path.display());
            return;
        }
    };

    let mut hasher = HashingChunker::new(Blake3Hasher(blake3::Hasher::new()), cdc_config.chunker());
    let mut chunks = Vec::new();
    let mut buf = [0u8; READ_BUFFER_SIZE];

    loop {
        let n = match file.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(err) => {
                eprintln!("warning: failed to read {}: {err}", path.display());
                return;
            }
        };
        chunks.extend(hasher.next(&buf[..n]));
    }
    chunks.extend(hasher.flush());

    println!("{}: {}", path.display(), format_chunks(&chunks));
}

fn format_chunks(chunks: &[LengthHash]) -> String {
    chunks
        .iter()
        .map(|chunk| format!("{}:{}", hex(&chunk.hash), chunk.length))
        .collect::<Vec<_>>()
        .join(", ")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
