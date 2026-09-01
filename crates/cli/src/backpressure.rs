//! DESIGN-MOUNT-006's `write()` backpressure delay in `docs/design/mount-write-path.md`: a small
//! sleep `DedupFs::write` adds after every successful write, scaling smoothly with
//! [`crate::settle_pool::JobPool::backlog_spilled_bytes`] (DESIGN-MOUNT-010's released-but-not-
//! yet-settled signal) rather than admitting work at full speed until some fixed limit and then
//! blocking outright.

use std::time::Duration;

/// Caps a single `write()` call's added delay, however large the backlog or the call itself -
/// bounds worst-case per-call latency (a FUSE/WinFSP dispatch thread blocked this long is one
/// fewer available for every other concurrent request in the meantime) rather than letting it
/// grow without bound. Anchored to the Scala predecessor's own "severe but working" throttle
/// point (`Backend.scala`'s `cacheLoadDelay_ms`, cited in
/// `agent-todos/done/wire-write-backpressure-delay.md`).
pub const MAX_WRITE_BACKPRESSURE_DELAY: Duration = Duration::from_millis(250);

/// Tuning constant for [`write_backpressure_delay`]'s formula. Chosen so that once
/// `backlog_spilled_bytes` reaches ~1.4 GiB, a write call's own added delay reaches the cap above
/// at a data length of 128 KiB - the same effective-bandwidth floor (~512 KiB/s) a first-cut,
/// call-count-based (not byte-scaled) version of this formula would have given a 128 KiB writer at
/// its own cap-reaching backlog level, now reproduced exactly rather than only for that one
/// arbitrary write size - see `write_backpressure_delay`'s doc comment for why the byte-scaled form
/// matters. 1.4 GiB and 128 KiB are not measured constants of their own; 786_432 = 512 KiB/s x
/// 1_500_000_000 bytes / 1e9 ns/s is the number that reproduces them from the formula below.
pub const SLOPE_DIVISOR: u128 = 786_432;

/// `write()`'s own backpressure delay (DESIGN-MOUNT-006): grows smoothly with
/// `backlog_spilled_bytes` (DESIGN-MOUNT-010) and, deliberately, with `data_len` - the size of the
/// write call this delay is being added to.
///
/// Scaling by `data_len` keeps the delay's effective throttle (bytes of `write()` payload allowed
/// per second, once the delay is not yet at the cap) independent of whatever write granularity the
/// calling tool happens to use - a caller that flushes every 8 KiB is not throttled 16x harder than
/// one flushing every 128 KiB just because it makes more, smaller calls to move the same total
/// bytes. This was verified as a real, not merely theoretical, concern: a calibration mount session
/// driving `crates/perf-gen`'s own default-buffered output through a real libfuse3 mount observed
/// ~8 KiB as a typical write() length (`std::io::BufWriter`'s default capacity), noticeably smaller
/// than the ~128 KiB a flat, call-count-only version of this formula implicitly assumed as "normal".
///
/// `MAX_WRITE_BACKPRESSURE_DELAY` still caps the result, so a single unusually large `write()` call
/// against a severe backlog cannot itself block a dispatch thread far longer than the flat form
/// ever would have.
pub fn write_backpressure_delay(backlog_spilled_bytes: u64, data_len: usize) -> Duration {
    let raw_nanos = (backlog_spilled_bytes as u128)
        .saturating_mul(data_len as u128)
        .saturating_div(SLOPE_DIVISOR)
        .min(u64::MAX as u128) as u64;
    Duration::from_nanos(raw_nanos).min(MAX_WRITE_BACKPRESSURE_DELAY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_backlog_means_no_delay() {
        assert_eq!(write_backpressure_delay(0, 8192), Duration::ZERO);
    }

    #[test]
    fn no_bytes_written_means_no_delay_however_severe_the_backlog() {
        assert_eq!(write_backpressure_delay(u64::MAX, 0), Duration::ZERO);
    }

    #[test]
    fn delay_grows_with_backlog_for_a_fixed_write_size() {
        let small = write_backpressure_delay(64 * 1024 * 1024, 8192);
        let large = write_backpressure_delay(512 * 1024 * 1024, 8192);
        assert!(small < large, "small={small:?} large={large:?}");
    }

    #[test]
    fn delay_grows_with_data_len_for_a_fixed_backlog() {
        let backlog = 512 * 1024 * 1024;
        let small_call = write_backpressure_delay(backlog, 8 * 1024);
        let large_call = write_backpressure_delay(backlog, 128 * 1024);
        assert!(
            small_call < large_call,
            "small_call={small_call:?} large_call={large_call:?}"
        );
    }

    #[test]
    fn effective_throttle_is_independent_of_write_granularity_below_the_cap() {
        // The whole point of scaling by data_len: two callers moving the same total bytes through
        // this backlog level, one via many 8 KiB calls and one via few 128 KiB calls, see the same
        // effective bytes-per-second throttle (not exactly equal call-for-call, since the 8 KiB
        // case makes 16x as many calls, but the same delay-per-byte).
        let backlog = 512 * 1024 * 1024;
        let per_byte_delay_small =
            write_backpressure_delay(backlog, 8 * 1024).as_nanos() as f64 / (8.0 * 1024.0);
        let per_byte_delay_large =
            write_backpressure_delay(backlog, 128 * 1024).as_nanos() as f64 / (128.0 * 1024.0);
        let ratio = per_byte_delay_small / per_byte_delay_large;
        assert!(
            (0.99..=1.01).contains(&ratio),
            "per-byte delay should match regardless of call size, got ratio {ratio}"
        );
    }

    #[test]
    fn delay_is_capped_regardless_of_how_severe_the_backlog_or_call_is() {
        assert_eq!(
            write_backpressure_delay(u64::MAX, usize::MAX),
            MAX_WRITE_BACKPRESSURE_DELAY
        );
    }

    #[test]
    fn cap_is_reached_at_roughly_the_anchor_point_this_constant_was_derived_from() {
        // See SLOPE_DIVISOR's own doc comment: ~1.4 GiB backlog at a 128 KiB write should just
        // reach the cap.
        let just_under = write_backpressure_delay(1_400_000_000, 128 * 1024);
        let at_anchor = write_backpressure_delay(1_500_000_000, 128 * 1024);
        assert!(just_under < MAX_WRITE_BACKPRESSURE_DELAY);
        assert_eq!(at_anchor, MAX_WRITE_BACKPRESSURE_DELAY);
    }
}
