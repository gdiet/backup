//! Minimal binary for manually verifying `mountfs::preflight()`'s
//! documented graceful-absence behavior against a real environment
//! genuinely missing the platform mount library - see
//! `scripts/test-linux-without-libfuse.sh` and `docs/design/mount-abstraction.md`'s "Why runtime
//! dynamic loading, not build-time linking".
//!
//! Prints whether the platform mount library was found, and exits 0
//! either way - the driving script asserts on the printed message, not
//! the exit code, since "library missing" is an expected, successful
//! result here, not a failure of this binary itself.

fn main() {
    match mountfs::preflight() {
        Ok(()) => println!("preflight: library found and usable"),
        Err(err) => println!("preflight: library not available ({err})"),
    }
}
