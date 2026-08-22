//! `dfs create-repo` - REQ-CLI-005 in requirements/functional/cli-commands.md.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn run(path: &Path, cdc_target_size_bits: Option<u32>) {
    // Validate against cdc's own bounds before touching the filesystem at all -
    // DESIGN-METADATA-009's "CLI validation" section: reuse cdc::ChunkerConfig::new
    // rather than duplicating its bounds here.
    if let Err(err) = cdc::ChunkerConfig::new(cdc_target_size_bits) {
        eprintln!("error: {err}");
        std::process::exit(1);
    }

    let creation_time_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_millis() as i64;
    let settings = db::RepositorySettings::new(cdc_target_size_bits, creation_time_millis);

    match db::init_repository(path, settings) {
        Ok(()) => println!("created repository at {}", path.display()),
        Err(err) => {
            eprintln!("error: {err}");
            std::process::exit(1);
        }
    }
}
