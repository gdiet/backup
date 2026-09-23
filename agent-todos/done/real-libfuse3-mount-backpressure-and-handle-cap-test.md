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

## Done

**Completed**: 2026-09-22/23, by WSL2/Linux session on `3327`, branch `memory-design`. Two new
`real_mount_*` tests in `crates/cli/src/dedup_fs.rs`'s test module (real-mount infra added there -
`mount_for_test`/`unmount_and_join`/`timed_synced_write` helpers, mirroring
`crates/mountfs/src/linux/mod.rs`'s `DispatchProbeFs` pattern):

- `real_mount_write_backpressure_delay_grows_then_drains_with_the_persist_queue`: releases a real
  8 MiB generation through the mount, times a write immediately after (clearly slower than
  baseline) and again once `wait_for_settled` confirms the backlog has drained (fast again).
- `real_mount_concurrent_handles_converge_toward_the_per_handle_halving_formula`: opens two real
  handles through the mount against a small shared budget; confirms the first, alone, does not
  spill while the second, competing for what the first already claimed, does.

Both verified red (temporarily neutering `write_backpressure_delay`/`try_acquire_share`) and green
again, per `AGENTS.md`'s debugging discipline; both also re-run reliably (5x isolated, plus the
full `cargo test --workspace` default-parallel run) before considering this done.

### Two real findings along the way

1. **A real request-dispatch race right after mount readiness, not specific to this test.**
   Client operations issued immediately after the existing `real_mount_*` readiness-probe pattern
   (a retried `std::fs::write` until it succeeds) can still return success without ever reaching
   this filesystem's own `create()`/`write()` dispatch at all - confirmed directly with temporary
   server- and client-side tracing: several consecutive client operations "succeeded" with zero
   corresponding dispatch calls, and the very next one (or an unrelated syscall inserted in
   between, e.g. running `mountpoint -q`) would suddenly show every prior operation's dispatch
   firing at once. Not explained further (some libfuse3/kernel warm-up right after the first
   successful request, not this probe's own file specifically) - worked around with a short,
   documented `thread::sleep(200ms)` in the shared `mount_for_test` helper right after the
   readiness probe succeeds. Any future `real_mount_*` test built on this same probe pattern should
   likely go through `mount_for_test` (or apply the same pause) rather than rediscovering this.
2. **`write_cache.rs`'s `spill_to_disk` migrates a handle's *entire* accumulated content to its
   spill file, not just the overflow past its fair share**, and releases the whole prior in-memory
   grant back to the shared budget at that same moment. A naive reading of DESIGN-MOUNT-019's
   formula might expect "spill file size = bytes past the fair share" (a 50%/25% split showing up
   directly in file sizes) - that is not what happens; once a handle spills at all, its spill file
   ends up holding everything it ever writes. The externally-observable, mount-driven signal that
   actually matches the formula's fairness property is binary (does a handle spill *at all* for a
   write sized to fall within a "first, alone" equilibrium but not a "second, after the first
   already claimed its share" one), not a size comparison - the second test above checks that.

No code changes to the formulas themselves - this was pure test-writing plus one small, documented
timing fix in shared real-mount test infrastructure. `libc` added as a `crates/cli` dev-dependency
(`cargo add --dev libc`, already a transitive dependency via `mountfs` at the same version) for
`O_SYNC` (`OpenOptionsExt::custom_flags`) - needed because `fsync` is `Unimplemented` in
`crates/mountfs/src/linux/sys.rs`, so a `sync_all()` call alone does not reliably force this
filesystem's `write()` dispatch to have already run, the way it does for other `real_mount_*` tests
that only need one large write per file, not the ordering guarantee this test's second one needs.

### Correction (2026-09-23)

Finding 1 above was a misdiagnosis, corrected while resolving the follow-up agent-todo it spawned
(`agent-todos/done/mount-readiness-dispatch-race-root-cause.md`). There is no libfuse3/kernel
dispatch race: `mount_path` is a real, already-existing directory (`tempfile::tempdir()`), and the
old readiness probe (a retried `std::fs::write` until it succeeded) could succeed against that
*underlying* directory itself, before libfuse's `mount(2)` call had actually attached over it -
confirmed by comparing `stat(2)`'s `st_dev` before spawning the mount thread and at the moment the
probe reported success: they were still identical, and an immediate `fusermount3 -u` failed with
"entry ... not found in /etc/mtab". Every operation that appeared to "succeed without dispatching"
was simply landing on the plain pre-mount directory, not on FUSE at all. `mount_for_test` now polls
`st_dev` directly instead of write-probing, and the `thread::sleep(200ms)` workaround this file
originally described is gone - once `st_dev` changes, every operation against that path is
necessarily routed through FUSE.
