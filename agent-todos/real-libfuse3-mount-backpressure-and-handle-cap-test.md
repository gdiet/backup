# Add a real-libfuse3-mount-backed test for the handle-cap formula and the backpressure signal

**Why parked**: needs an environment with working `/dev/fuse` access. This session's remote
container has a `/dev/fuse` device node present, but real libfuse3 mounts time out becoming ready
(`real_mount_serves_the_full_read_only_op_set_via_libfuse3` fails with "mount did not become ready
within 5s") - see `.local/agent-environment.md`. `AGENTS.md`'s debugging discipline requires
verifying that a regression test actually runs (and, ideally, actually catches a reverted bug) -
not just that it compiles - so this cannot be written here without a way to run it.
**Size**: medium
**Opened**: 2026-09-22, by a Linux remote-execution session (this container's `/dev/fuse` does not
actually work despite the device node existing)
**Context**: `developer-todos/ram-budget-and-backpressure-redesign.md`'s original 11-step plan,
step 10 ("verification"). `DESIGN-MOUNT-019` (per-handle halving formula,
`crates/cli/src/write_cache.rs`) and `DESIGN-MOUNT-006` (backpressure delay formula,
`crates/cli/src/backpressure.rs`) are both already covered by pure-function unit tests that do not
need a real mount (`try_acquire_share_grants_half_the_available_budget_to_a_single_growing_handle`,
`try_acquire_share_gives_a_second_handle_half_of_what_the_first_left_behind`, and 10 tests in
`backpressure.rs` including anchor-point and no-cap verification) - those are solid coverage of the
formulas themselves in isolation.

What is still missing is an end-to-end check, through a real libfuse3 mount (`fuse_main_real`, same
shape as `crates/mountfs/src/linux/mod.rs`'s existing `real_mount_*` tests), that the whole write
path actually converges as designed under real concurrent writes: open multiple file handles
through the mount, write to them concurrently faster than the store can persist, and confirm (a)
each handle's actually-observed RAM cache size trends toward the halving formula's predicted
equilibrium rather than just the isolated unit-level function producing the right number, and (b)
writes visibly slow down (via `write_backpressure_delay`) once `bytesInPersistQueue` exceeds
`DEFAULT_FREE_ZONE_BYTES`, and speed back up once the persist queue drains. This exercises the
actual wiring in `crates/cli/src/dedup_fs.rs::write()`, not just the formulas it calls.

Follow the existing `real_mount_*` naming/skip convention (prefix `real_mount_`, so
`cargo test -- --skip real_mount` continues to exclude it in FUSE-less environments) and place it
either in `crates/cli/src/dedup_fs.rs`'s test module (if it already has real-mount test
infrastructure) or wherever the existing mount-backed CLI tests live. Verify it red/green against a
temporarily-reverted formula before considering it done, per `AGENTS.md`'s debugging discipline.
