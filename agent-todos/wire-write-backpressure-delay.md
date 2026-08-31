# Wire DESIGN-MOUNT-006's write() backpressure delay to JobPool::backlog_spilled_bytes

**Why parked**: genuinely out of scope for the "wire create/write/truncate/unlink into
`dedup_fs.rs`" task this came up during - implementing it well needs a deliberately chosen delay
formula/constant, not just a mechanical wiring-together of already-built pieces, and there is no
benchmark data available in this session to calibrate one responsibly.
**Size**: medium (needs a considered formula, ideally checked against some representative
workload, not just guessed constants)
**Opened**: 2026-08-31, by Claude Code on the web session (branch `mount-read-write`)
**Context**: DESIGN-MOUNT-006/010 in
[`../docs/design/mount-write-path.md`](../docs/design/mount-write-path.md);
`crates/cli/src/settle_pool.rs`'s `JobPool::backlog_spilled_bytes()` (currently `#[allow(dead_code)]`
with a pointer to this file); `crates/cli/src/dedup_fs.rs`'s `MountFilesystem::write`.

DESIGN-MOUNT-006 decided that `write()` should add a small delay that scales smoothly with
`JobPool::backlog_spilled_bytes()` - the released-but-not-yet-settled spilled bytes DESIGN-MOUNT-010
narrows the signal to - rather than admitting work at full speed until some fixed limit and then
blocking outright. Neither design doc specifies an exact formula (deliberately: DESIGN-MOUNT-006
says only "the concrete signal this delay scales with... is DESIGN-MOUNT-010", not a constant), so
this is a genuine design choice, not just wiring:

- Pick a delay function (linear in backlog bytes is the obvious starting point, matching "scales
  smoothly ... zero bytes spilled means zero added delay ... no fixed threshold").
- Pick its constant(s) with at least some justification - ideally exercised against a synthetic
  workload (`crates/perf-gen` already exists for generating test data) that can actually spill
  meaningfully, rather than a guessed number.
- Wire the result into `DedupFs::write` (call `self.pool.backlog_spilled_bytes()`, sleep
  accordingly) after `self.pending.write(...)` succeeds.
- Remove the `#[allow(dead_code)]` on `JobPool::backlog_spilled_bytes` once it has a real caller.
