//! Platform-abstracted repository mounting. See
//! `docs/plans/cross-platform-mount-crate.md` for the design and current
//! status - as of this commit, still just the sequencing plan's step-1
//! Linux spike (`linux` module): no public cross-platform API yet.

#[cfg(target_os = "linux")]
pub mod linux;
