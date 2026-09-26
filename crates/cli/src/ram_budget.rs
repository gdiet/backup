//! DESIGN-MEMORY-001's startup RAM budget (`docs/design/ram-budget.md`): computed once, from a
//! gross operator-configurable limit minus the SQLite connection's own `cache_size` and a
//! per-thread stack reserve for the CDC/hash/persist pool and, for a real mount session, the
//! FUSE/WinFSP dispatch pool.

/// DESIGN-MEMORY-001's default gross limit - the operator-configurable total this budget starts
/// from, before any reserve is subtracted.
pub const DEFAULT_GROSS_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// The stack size the CDC/hash/persist pool's own worker threads (`ingest::FileJobPool`,
/// `settle_pool::JobPool`) are actually spawned with, via `std::thread::Builder::stack_size` -
/// not a guess at Rust's own default. That default is documented as "subject to change", and can
/// be overridden process-wide by an operator's `RUST_MIN_STACK`, neither of which this budget
/// would have any visibility into. An explicitly requested `stack_size` always wins over both,
/// regardless of Rust version or `RUST_MIN_STACK` (verified empirically; see this file's own
/// `stack_size_wins_over_the_default_and_rust_min_stack` test) - so setting it explicitly, to the
/// same value this budget reserves for it, keeps the reserve and the real allocation identical by
/// construction rather than by coincidence. Not exposed as its own `--` flag: `--ram-budget-mb` is
/// the one lever an operator has any real basis to size, this is purely that budget's own internal
/// accounting for it.
pub(crate) const RUST_THREAD_STACK_RESERVE_BYTES: u64 = 2 * 1024 * 1024;

/// DESIGN-MEMORY-001's FUSE/WinFSP dispatch-pool reserve, per real measurement
/// (`docs/design/ram-budget.md`'s "Provisional dispatch-pool reserve"):
///
/// - **Linux/libfuse3**: a fixed 10 threads x 8 MiB = 80 MiB. Reproduced identically on two
///   independent machines with very different logical-processor counts (4 and 12), both landing
///   on pool size 10 and 8 MiB stacks - libfuse3's own dispatch-pool size behaves as a hardcoded
///   fallback here, not something derived from this machine's hardware, so a fixed reserve fits
///   (`agent-todos/done/determine-libfuse3-dispatch-pool-and-stack-size.md`).
/// - **Windows/WinFSP**: `available_parallelism() x 1 MiB`. WinFSP's own measured dispatch
///   concurrency matched that one machine's logical-processor count exactly, so this reserve
///   scales with the machine's own core count instead of using a fixed number
///   (`agent-todos/done/determine-winfsp-dispatch-pool-and-stack-size.md`).
///
/// Neither measurement has been reproduced across more than a couple of machines per platform,
/// so both remain estimates an operator can override via the RAM budget total, not guarantees.
#[cfg(target_os = "linux")]
fn dispatch_pool_reserve_bytes() -> u64 {
    10 * 8 * 1024 * 1024
}

/// See the Linux definition of this function above for the shared doc comment.
#[cfg(target_os = "windows")]
fn dispatch_pool_reserve_bytes() -> u64 {
    let cores = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1) as u64;
    cores * 1024 * 1024
}

/// Neither Linux nor Windows: this project's mount write path is not supported here at all
/// (`crates/mountfs`), so this reserve is never actually charged against a real dispatch pool -
/// kept only so `caching_budget_bytes` below compiles on every target. Falls back to the original
/// conservative pre-measurement estimate (16 threads x 8 MiB).
#[cfg(not(any(target_os = "linux", target_os = "windows")))]
fn dispatch_pool_reserve_bytes() -> u64 {
    16 * 8 * 1024 * 1024
}

/// Whether this process runs a FUSE/WinFSP dispatch pool of its own - only a real mount session
/// does; `dfs ingest`/`dfs create-repo` do not, and reserve nothing for one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchPool {
    None,
    Fuse,
}

impl DispatchPool {
    fn reserve_bytes(self) -> u64 {
        match self {
            DispatchPool::None => 0,
            DispatchPool::Fuse => dispatch_pool_reserve_bytes(),
        }
    }
}

/// DESIGN-MEMORY-001: `gross_bytes` minus `cache_size_bytes` (`db::Repository::cache_size_bytes`)
/// minus a fixed 2 MiB reserve per `worker_threads`, minus `dispatch_pool`'s own reserve if this
/// process runs one. Saturates at `0` rather than underflowing if the reserves alone already
/// exceed `gross_bytes` - the resulting `0` budget is then rejected by
/// [`check_fits_max_chunk_size`] the same way any other too-small budget is, rather than this
/// function failing on its own.
pub fn caching_budget_bytes(
    gross_bytes: u64,
    cache_size_bytes: u64,
    worker_threads: u64,
    dispatch_pool: DispatchPool,
) -> u64 {
    let worker_reserve = worker_threads.saturating_mul(RUST_THREAD_STACK_RESERVE_BYTES);
    let reserves = cache_size_bytes
        .saturating_add(worker_reserve)
        .saturating_add(dispatch_pool.reserve_bytes());
    gross_bytes.saturating_sub(reserves)
}

/// A repository's own configured chunking granularity cannot possibly fit within the computed RAM
/// budget (REQ-OPERABILITY-006 in
/// `requirements/non-functional/operability.md`) - checked once at startup so this surfaces as an
/// actionable error before anything runs, not as a silently exceeded bound once running.
#[derive(Debug, PartialEq, Eq)]
pub struct BudgetTooSmall {
    pub available_bytes: u64,
    pub required_bytes: u64,
}

impl std::fmt::Display for BudgetTooSmall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RAM budget too small for this repository: {} bytes available for caching, but its \
             own chunking configuration needs at least {} bytes for a single chunk - increase the \
             RAM budget or use a repository with a smaller target chunk size",
            self.available_bytes, self.required_bytes
        )
    }
}

impl std::error::Error for BudgetTooSmall {}

/// Checks `caching_budget_bytes` (from [`caching_budget_bytes`] above) against
/// `max_chunk_size_bytes` (a repository's own [`cdc::ChunkerConfig::max_chunk_size`]) - `Err` if
/// the budget cannot possibly hold even one chunk of that repository's own configured size.
pub fn check_fits_max_chunk_size(
    caching_budget_bytes: u64,
    max_chunk_size_bytes: u64,
) -> Result<(), BudgetTooSmall> {
    if caching_budget_bytes >= max_chunk_size_bytes {
        Ok(())
    } else {
        Err(BudgetTooSmall {
            available_bytes: caching_budget_bytes,
            required_bytes: max_chunk_size_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The real OS-reported stack size of the *calling* thread - not a documented default assumed
    /// or read from `RUST_MIN_STACK` - via `pthread_getattr_np`/`pthread_attr_getstack`. Same
    /// mechanism `crates/mountfs/src/linux/mod.rs`'s own dispatch-pool measurement already uses.
    #[cfg(target_os = "linux")]
    fn actual_stack_size_bytes() -> usize {
        unsafe {
            let mut attr: libc::pthread_attr_t = std::mem::zeroed();
            let rc = libc::pthread_getattr_np(libc::pthread_self(), &mut attr);
            assert_eq!(rc, 0, "pthread_getattr_np failed with errno {rc}");
            let mut addr: *mut libc::c_void = std::ptr::null_mut();
            let mut size: libc::size_t = 0;
            let rc = libc::pthread_attr_getstack(&attr, &mut addr, &mut size);
            assert_eq!(rc, 0, "pthread_attr_getstack failed with errno {rc}");
            libc::pthread_attr_destroy(&mut attr);
            size
        }
    }

    /// Same measurement, via `GetCurrentThreadStackLimits` - see
    /// `crates/mountfs/src/disk_space.rs`'s own Windows FFI block for the same call.
    #[cfg(target_os = "windows")]
    fn actual_stack_size_bytes() -> usize {
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn GetCurrentThreadStackLimits(low_limit: *mut usize, high_limit: *mut usize);
        }
        let mut low: usize = 0;
        let mut high: usize = 0;
        unsafe { GetCurrentThreadStackLimits(&mut low, &mut high) };
        high - low
    }

    /// `RUST_THREAD_STACK_RESERVE_BYTES` is only a correct accounting of the CDC/hash/persist
    /// pool's real stack usage as long as an explicitly requested `stack_size` actually wins over
    /// both Rust's own (documented as "subject to change") default and any `RUST_MIN_STACK` an
    /// operator might have set - confirmed here empirically, on the two platforms this project
    /// supports a real mount on, rather than assumed from std's documentation or source. Picks a
    /// deliberately unusual size (not 2 MiB, not a value anyone would plausibly set
    /// `RUST_MIN_STACK` to) so a regression - e.g. a future Rust version clamping a small
    /// `stack_size` up to `RUST_MIN_STACK` - could not coincidentally still pass. Kept a multiple
    /// of 64 KiB (Windows' own `VirtualAlloc` reservation granularity) so the real, OS-reported
    /// size can be asserted exactly, without also asserting anything about platform-specific
    /// rounding behavior that has nothing to do with what this test actually checks.
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    #[test]
    fn stack_size_wins_over_the_default_and_rust_min_stack() {
        let requested_bytes: usize = 3 * 1024 * 1024 + 512 * 1024;
        let handle = std::thread::Builder::new()
            .stack_size(requested_bytes)
            .spawn(actual_stack_size_bytes)
            .expect("spawning this test's one worker thread must succeed");
        let actual_bytes = handle.join().expect("the spawned thread must not panic");
        assert_eq!(actual_bytes, requested_bytes);
    }

    #[test]
    fn caching_budget_subtracts_cache_size_and_worker_stacks() {
        let budget =
            caching_budget_bytes(256 * 1024 * 1024, 4 * 1024 * 1024, 4, DispatchPool::None);
        // 256 MiB - 4 MiB cache_size - 4 * 2 MiB worker stacks = 244 MiB.
        assert_eq!(budget, 244 * 1024 * 1024);
    }

    #[test]
    fn caching_budget_additionally_subtracts_the_dispatch_pool_reserve_when_present() {
        let without_fuse = caching_budget_bytes(256 * 1024 * 1024, 0, 0, DispatchPool::None);
        let with_fuse = caching_budget_bytes(256 * 1024 * 1024, 0, 0, DispatchPool::Fuse);
        assert!(with_fuse < without_fuse);
        assert_eq!(without_fuse - with_fuse, dispatch_pool_reserve_bytes());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn dispatch_pool_reserve_is_the_fixed_libfuse3_measurement_on_linux() {
        assert_eq!(dispatch_pool_reserve_bytes(), 80 * 1024 * 1024);
    }

    #[test]
    fn caching_budget_saturates_at_zero_rather_than_underflowing() {
        let budget = caching_budget_bytes(10, 100, 0, DispatchPool::None);
        assert_eq!(budget, 0);
    }

    #[test]
    fn check_fits_max_chunk_size_accepts_a_budget_at_least_as_large_as_the_max_chunk() {
        assert!(check_fits_max_chunk_size(96 * 1024 * 1024, 96 * 1024 * 1024).is_ok());
        assert!(check_fits_max_chunk_size(200 * 1024 * 1024, 96 * 1024 * 1024).is_ok());
    }

    #[test]
    fn check_fits_max_chunk_size_rejects_a_budget_smaller_than_the_max_chunk() {
        let err = check_fits_max_chunk_size(50 * 1024 * 1024, 96 * 1024 * 1024).unwrap_err();
        assert_eq!(
            err,
            BudgetTooSmall {
                available_bytes: 50 * 1024 * 1024,
                required_bytes: 96 * 1024 * 1024,
            }
        );
    }
}
