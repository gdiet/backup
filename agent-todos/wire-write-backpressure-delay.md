# Wire DESIGN-MOUNT-006's write() backpressure delay to JobPool::backlog_spilled_bytes

**Why parked**: genuinely out of scope for the "wire create/write/truncate/unlink into
`dedup_fs.rs`" task this came up during - implementing it well needs a deliberately chosen delay
formula/constant, not just a mechanical wiring-together of already-built pieces. Originally also
blocked on having no way to calibrate; that part is now unblocked - see "Calibration" below (a real
read-write libfuse3 mount on WSL2/Linux can drive a spilling workload). Still needs a Rust build
environment, so not doable from a native-Windows-without-Rust session.
**Size**: medium
**Opened**: 2026-08-31, by Claude Code on the web session (branch `mount-read-write`)
**Context**: DESIGN-MOUNT-006/010 in
[`../docs/design/mount-write-path.md`](../docs/design/mount-write-path.md);
`crates/cli/src/settle_pool.rs`'s `JobPool::backlog_spilled_bytes()` (currently `#[allow(dead_code)]`
with a pointer to this file); `crates/cli/src/dedup_fs.rs`'s `DedupFs::write`.

DESIGN-MOUNT-006 decided that `write()` should add a small delay that scales smoothly with
`JobPool::backlog_spilled_bytes()` - the released-but-not-yet-settled spilled bytes DESIGN-MOUNT-010
narrows the signal to - rather than admitting work at full speed until some fixed limit and then
blocking outright. Neither design doc specifies an exact formula (deliberately), so this is a
genuine design choice, not just wiring.

## Research (2026-09-01, native-Windows session)

### The Scala precedent (`.local/scala` = `main`, `dedup/server/Backend.scala`)

```scala
cacheLoadDelay_ms = bytesInPersistQueue * persistQueueSize / 1_000_000_000
// slept per 32-KiB write chunk (data.tapEach), persist runs single-threaded
```

- `bytesInPersistQueue` = summed size of *all* files enqueued for persist (memory + spill, not
  spill-only)
- `persistQueueSize` = *count* of those files - a second, multiplicative factor
- quadratic-ish; ramps hard. Order of magnitude: ~1 GB across ~10 files -> 10 ms per 32 KiB
  (~3 MiB/s ingest cap); 5 GB across 50 files -> 250 ms.

### Why the Scala formula does not port 1:1

| | Scala | Rust |
|---|---|---|
| Signal | all queued bytes | spill-only (`> 0` only after the 256-MiB budget is blown) |
| Free zone | none | yes, deliberate (DESIGN-MOUNT-010: common case never spills) |
| Persist | single-threaded | `available_parallelism()` workers |
| Sleep unit | per 32-KiB chunk | per `write()` call (one FUSE/WinFSP request) |
| File-count factor | yes (`x persistQueueSize`) | `JobPool` tracks no count (would be one AtomicU64) |

A Rust spilled byte is a worse state than a Scala queued byte (budget already exceeded, disk in
play), so the slope per signal-byte should be more aggressive than Scala's - but with the free zone
in front of it.

### Agreed first-cut shape (developer, 2026-09-01): linear + hard cap, nothing else

```rust
fn write_backpressure_delay(backlog_spilled_bytes: u64) -> Duration {
    Duration::from_micros(backlog_spilled_bytes / SLOPE_DIVISOR).min(MAX_WRITE_BACKPRESSURE_DELAY)
}
```

With `SLOPE_DIVISOR = 6_000`, `MAX_WRITE_BACKPRESSURE_DELAY = Duration::from_millis(250)` as the
starting estimate (anchored to Scala's "severe but working" point mapped onto the spill-only
signal):

| spilled | delay per `write()` |
|---|---|
| 0 | 0 |
| 16 MiB | ~2.8 ms |
| 64 MiB | ~11 ms |
| 256 MiB | ~45 ms |
| 1 GiB | ~180 ms |
| >= ~1.4 GiB | 250 ms (cap) |

Deliberately **not** in the first cut: the `x count` factor (needs a `backlog_generation_count` in
`JobPool` - cheap, but scope), and scaling the sleep by `data.len()`.

### Open point raised during review - resolve during calibration

The cap (250 ms) and the whole delay are **flat per `write()` call, independent of `data.len()`**.
Consequence: the effective ingest-bandwidth throttle then varies with the client's write size - a
client doing 128-KiB writes is throttled to ~512 KiB/s at the cap, one doing 1-MiB writes to
~4 MiB/s, for the same backlog. Options: (a) accept it for the first cut (write size is usually
consistent within a session); (b) scale the delay by `data.len()` with its own smaller per-call
cap; (c) hybrid. Pick based on what the calibration workload shows.

## Calibration (WSL2/Linux, libfuse3 - now possible)

`crates/mountfs/src/linux/` + the `real_mount_*` libfuse3 tests + `cli`'s `mount --read-write`
mean a real read-write DedupFS can be FUSE-mounted on WSL2 with `/dev/fuse` access. That removes
the original "no benchmark data" blocker:

1. Mount a repo read-write.
2. Drive a workload that actually spills: enough concurrent / large writers to push
   released-but-unsettled bytes past the 256-MiB `DEFAULT_BUDGET_BYTES` and keep them there
   (`crates/perf-gen` generates the content; several writers, or one writer of many multi-hundred-MiB
   files back to back, faster than the settle pool drains).
3. Measure ingest throughput vs. the settle pool's drain rate at a few backlog levels (log or
   sample `backlog_spilled_bytes()`), with backpressure off, then with the candidate formula.
4. Choose `SLOPE_DIVISOR` / `MAX_WRITE_BACKPRESSURE_DELAY` so the delay meaningfully caps ingest
   once spill is large without throttling a backlog that the pool is comfortably keeping up with,
   and settle the flat-vs-`data.len()` question from step (3)'s numbers.

## Implementation steps

1. `write_backpressure_delay` as a pure function + `pub const SLOPE_DIVISOR` /
   `MAX_WRITE_BACKPRESSURE_DELAY` (in `settle_pool.rs` or a small `backpressure` module), with a
   comment stating the Scala anchor, that the constant is an estimate, and to revisit it against a
   mount benchmark (cf. `developer-todos/performance-baselines-tree-and-content-operations.md`).
2. In `DedupFs::write`, after `self.pending.write(...)` succeeds:
   `thread::sleep(write_backpressure_delay(self.pool.backlog_spilled_bytes()))`.
3. Remove `#[allow(dead_code)]` on `JobPool::backlog_spilled_bytes`.
4. Tests (no mount needed): pure-function behaviour over the range including the cap; `write()`
   actually consults the pool (feed `JobPool` a spilled generation as `settle_pool.rs`'s
   `backlog_spilled_bytes_tracks_generations_currently_in_flight` test does, assert a non-zero
   delay); red/green (drop the sleep line, the second test fails).
5. `docs/design/mount-write-path.md` DESIGN-MOUNT-006: add the concrete formula + anchor under the
   heading (Status stays `implemented`).
6. Full verification suite (`cargo build`/`fmt`/`clippy`/`test`/`doc`), then move this file to
   `agent-todos/done/`.
