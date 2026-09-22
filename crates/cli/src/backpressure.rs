//! DESIGN-MOUNT-006's `write()` backpressure delay in `docs/design/mount-write-path.md`: a small
//! sleep `DedupFs::write` adds after every successful write, scaling smoothly with
//! [`crate::settle_pool::JobPool::bytes_in_persist_queue`] (DESIGN-MOUNT-006's `bytesInPersistQueue`
//! signal) rather than admitting work at full speed until some fixed limit and then blocking
//! outright.

use std::time::Duration;

/// Below this much backlog, `write()` adds no delay at all - DESIGN-MOUNT-006's free zone.
pub const DEFAULT_FREE_ZONE_BYTES: u64 = 1_000_000_000;

/// Tuning constant for [`write_backpressure_delay`]'s formula. Derived from two anchor points: no
/// delay at or below [`DEFAULT_FREE_ZONE_BYTES`], and 500 ms added to a 10 kB write once the
/// backlog reaches 10 GB - `SLOPE_DIVISOR = (10_000_000_000 - 1_000_000_000) * 10_000 / 500ms_in_ns`.
pub const DEFAULT_SLOPE_DIVISOR: u128 = 180_000_000_000;

/// `write()`'s own backpressure delay (DESIGN-MOUNT-006):
///
/// ```text
/// storeDelayMillis = max(0, bytesInPersistQueue - freeZoneBytes) * writeLength / SLOPE_DIVISOR
/// ```
///
/// Grows smoothly with `bytes_in_persist_queue` (DESIGN-MOUNT-006's `bytesInPersistQueue`, the
/// total bytes across every released, not-yet-durably-persisted generation, in memory or spilled)
/// once it exceeds `free_zone_bytes`, and, deliberately, with `data_len` - the size of the write
/// call this delay is being added to.
///
/// Scaling by `data_len` keeps the delay's effective throttle (bytes of `write()` payload allowed
/// per second, above the free zone) independent of whatever write granularity the calling tool
/// happens to use - a caller that flushes every 8 KiB is not throttled 16x harder than one
/// flushing every 128 KiB just because it makes more, smaller calls to move the same total bytes.
/// This was verified as a real, not merely theoretical, concern: a calibration mount session
/// driving `crates/perf-gen`'s own default-buffered output through a real libfuse3 mount observed
/// ~8 KiB as a typical write() length (`std::io::BufWriter`'s default capacity).
///
/// Deliberately has no upper cap, unlike an earlier version of this formula - see DESIGN-MOUNT-006
/// in `docs/design/mount-write-path.md` for why: the tool's default settings are sized for files up
/// to roughly 5 GB, so an operator working within that range experiences an increasingly graceful
/// slowdown rather than a hard "persist pause", with no need for the delay to plateau.
pub fn write_backpressure_delay(
    bytes_in_persist_queue: u64,
    data_len: usize,
    free_zone_bytes: u64,
    slope_divisor: u128,
) -> Duration {
    let over_free_zone = bytes_in_persist_queue.saturating_sub(free_zone_bytes);
    let raw_nanos = (over_free_zone as u128)
        .saturating_mul(data_len as u128)
        .saturating_mul(1_000_000) // milliseconds -> nanoseconds
        .saturating_div(slope_divisor)
        .min(u64::MAX as u128) as u64;
    Duration::from_nanos(raw_nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delay(bytes_in_persist_queue: u64, data_len: usize) -> Duration {
        write_backpressure_delay(
            bytes_in_persist_queue,
            data_len,
            DEFAULT_FREE_ZONE_BYTES,
            DEFAULT_SLOPE_DIVISOR,
        )
    }

    #[test]
    fn no_backlog_means_no_delay() {
        assert_eq!(delay(0, 8192), Duration::ZERO);
    }

    #[test]
    fn backlog_at_or_below_the_free_zone_adds_no_delay() {
        assert_eq!(delay(DEFAULT_FREE_ZONE_BYTES, 128 * 1024), Duration::ZERO);
    }

    #[test]
    fn backlog_meaningfully_above_the_free_zone_adds_a_nonzero_delay() {
        // +1 byte over the free zone would round to zero nanoseconds under integer division at
        // this write size - a small but real margin above it is what actually exercises the
        // "some backlog above the free zone" case meaningfully.
        assert!(delay(DEFAULT_FREE_ZONE_BYTES + 10_000_000, 128 * 1024) > Duration::ZERO);
    }

    #[test]
    fn no_bytes_written_means_no_delay_however_severe_the_backlog() {
        assert_eq!(delay(u64::MAX, 0), Duration::ZERO);
    }

    #[test]
    fn delay_grows_with_backlog_for_a_fixed_write_size() {
        let small = delay(2_000_000_000, 8192);
        let large = delay(8_000_000_000, 8192);
        assert!(small < large, "small={small:?} large={large:?}");
    }

    #[test]
    fn delay_grows_with_data_len_for_a_fixed_backlog() {
        let backlog = 5_000_000_000;
        let small_call = delay(backlog, 8 * 1024);
        let large_call = delay(backlog, 128 * 1024);
        assert!(
            small_call < large_call,
            "small_call={small_call:?} large_call={large_call:?}"
        );
    }

    #[test]
    fn effective_throttle_is_independent_of_write_granularity_above_the_free_zone() {
        // The whole point of scaling by data_len: two callers moving the same total bytes through
        // this backlog level, one via many 8 KiB calls and one via few 128 KiB calls, see the same
        // effective bytes-per-second throttle (not exactly equal call-for-call, since the 8 KiB
        // case makes 16x as many calls, but the same delay-per-byte).
        let backlog = 5_000_000_000;
        let per_byte_delay_small = delay(backlog, 8 * 1024).as_nanos() as f64 / (8.0 * 1024.0);
        let per_byte_delay_large = delay(backlog, 128 * 1024).as_nanos() as f64 / (128.0 * 1024.0);
        let ratio = per_byte_delay_small / per_byte_delay_large;
        assert!(
            (0.99..=1.01).contains(&ratio),
            "per-byte delay should match regardless of call size, got ratio {ratio}"
        );
    }

    #[test]
    fn matches_the_anchor_point_this_default_slope_divisor_was_derived_from() {
        // 500 ms added to a 10 kB write once the backlog reaches 10 GB - the anchor point
        // DEFAULT_SLOPE_DIVISOR's own doc comment derives it from.
        let got = delay(10_000_000_000, 10_000);
        let want = Duration::from_millis(500);
        let diff = got.as_nanos().abs_diff(want.as_nanos());
        assert!(
            diff < 1_000_000,
            "expected ~500ms at the anchor point, got {got:?}"
        );
    }

    #[test]
    fn has_no_upper_cap() {
        // Unlike an earlier version of this formula, an extreme backlog and a large write must be
        // allowed to produce an arbitrarily large delay - DESIGN-MOUNT-006's deliberate choice.
        let extreme = delay(1_000_000_000_000, 10 * 1024 * 1024);
        assert!(
            extreme > Duration::from_secs(60),
            "expected an uncapped, very large delay, got {extreme:?}"
        );
    }
}
