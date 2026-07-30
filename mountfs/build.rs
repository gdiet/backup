//! Links against the platform FUSE-compatible implementation. See
//! `docs/plans/cross-platform-mount-crate.md` for why: Linux links real
//! system libfuse3 via pkg-config (requires `libfuse3-dev`/`fuse3-devel` at
//! build time, `libfuse3` at runtime); Windows will vendor WinFSP's
//! `fuse3/fuse.h` headers instead (not implemented yet - no build-time
//! WinFSP install required for that path).

fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
    if target_os == "linux" {
        pkg_config::Config::new()
            .atleast_version("3.10")
            .probe("fuse3")
            .expect(
                "libfuse3 development headers not found - install libfuse3-dev (Debian/Ubuntu) \
                 or fuse3-devel (Fedora)",
            );
    }
    // Windows (WinFSP) backend: not implemented yet.
}
