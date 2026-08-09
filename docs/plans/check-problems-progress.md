# Progress indicator for `check`/`problems`

**Status**: plan complete, ready to implement. No open questions - the
mechanism this reuses is already proven in production.

## Context

Follow-up identified while investigating `backup deleted`'s performance
(`docs/plans/implemented/deleted-entries-performance.md`): unlike that
command, `backup check`/`backup problems` (unscoped) aren't slow because of
a fixable query-plan defect - they're inherently I/O-bound, since `check`'s
whole job is to read every chunk's actual bytes back from the store (all
2.39 TB of physical data in the real `dedup/` repository) and verify them.
No query fix makes that fast; what's missing is feedback while it runs, per
the general "a CLI command should return a result quickly or signal how
long it'll take" bar from that same investigation.

## Reuse, don't reinvent: `migrate_scala_repo.rs`'s `Progress`

`cli/src/migrate_scala_repo.rs` already has exactly this - a byte-based,
time-throttled progress reporter (`Progress`, `struct` + `impl`, plus its
`format_duration_secs` helper), printing at most once every 2 seconds
(`Progress::INTERVAL`) with bytes-done/bytes-total/percent/elapsed/ETA, and
unconditionally once more at the end (`finish()`) so a run never ends
without showing 100%. It's already proven correct in production use
(that command's own read+chunk+hash pass) and already tested. No new
design needed for the reporting mechanism itself - just reuse it.

## Design

1. Extract `Progress`, `Progress::INTERVAL`, and `format_duration_secs`
   out of `migrate_scala_repo.rs` into a new small shared module (e.g.
   `cli/src/progress.rs`), `pub(crate)`. `migrate_scala_repo.rs` switches to
   importing it instead of defining its own copy - a pure move, not a
   behavior change there.
2. `check.rs`'s `run_check`: before the `for chunk in &chunks` loop,
   compute `total_bytes = chunks.iter().map(|c| c.length as u64).sum()`
   and `let mut progress = Progress::new(total_bytes);`. After each
   `check_chunk(...)` call (regardless of whether it found a problem),
   `progress.add(chunk.length as u64);` - byte-based, so it advances
   correctly even though chunk sizes vary widely (the real repository
   averages ~6.9 MB/chunk but individual chunks range far outside that).
   `progress.finish();` right before the existing "Checking ref_count
   consistency..." line.
3. `problems.rs`'s `find_problem_files`: same pattern around its own
   `for chunk in &chunks` loop (the one calling `read_chunk_bytes` to test
   `ReadIntegrity`). Since `fix_problems.rs`'s `run_fix_problems` calls
   `find_problem_files` too (re-running detection fresh, per that
   command's own design), this covers both commands automatically - no
   separate wiring needed in `fix_problems.rs`.
4. Scoped runs (`check <path>`/`problems <path>`) get progress too, for
   free - `total_bytes` is already computed from whatever chunk set is in
   scope, scoped or not.

## Explicitly not changed

- `deleted`/`stats`/`list`/`find`/`restore` - already fast or already have
  their own appropriate feedback; not touched by this plan.
- No new CLI flag - progress reporting is unconditional, matching
  `migrate-scala-repo`'s own precedent (no `--quiet`/`--no-progress`
  escape hatch exists there either, and nothing suggests a need for one
  here).

## Verification checklist

- `cargo fmt --check && cargo clippy --workspace --all-targets -- -D
  warnings && cargo test --workspace && cargo doc --no-deps --workspace`.
- Existing `check`/`problems` tests keep passing unmodified (progress
  output only adds `println!` lines, doesn't change exit codes or the
  existing problem-report lines).
- Spot-check against the real `dedup/` repository: run `backup check`
  scoped to a moderately-sized directory (not the full 2.39 TB - that's a
  multi-hour read, not a quick verification step) and confirm progress
  lines appear at a sensible cadence.
- Once shipped, move this file under `docs/plans/implemented/`.
