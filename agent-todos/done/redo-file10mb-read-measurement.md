# Redo the 10 MB file-read measurement now that the script's index bug is fixed

**Why parked**: needs a real machine with the multi-GB test tree already built (or time/disk budget
to rebuild it) and enough free disk space to hold it - this session runs in a Linux container with
neither, and is not the target hardware these performance baselines are meant to characterize
anyway.
**Size**: small (a single script re-run plus writing up the result, once on suitable hardware -
same shape as the other real-hardware verification TODOs already in this directory).
**Opened**: 2026-09-01, by Claude Code on the web session (branch `mount-read-write`)
**Context**: `agent-todos/done/file-read-scripts-restart-index-each-run.md` (the fix this
measurement depends on); `performance/scripts/file10mb-read.sh`/`.ps1`;
`performance/measurements/2026-08-28-3327-file10mb-read-wsl2.md` (the confounded measurement to
replace/supersede) and its `.yaml` sibling; `performance/methodology.md` for the report format.

`file10mb-read.{sh,ps1}` used to restart its read index at file 1 on every one of its 5 runs,
producing a monotonic-looking upward throughput trend that was a script artifact (later runs found
more of the same low-index range still page-cache-warm), not a real filesystem effect - see the
`Done` section of the TODO above for the fix (now picks a pseudo-random index per read, like
`dir-lookup.{sh,ps1}` already did). This is the only read rung where the bug has been shown to
matter (the 100 B/30 KB rungs' whole tree already fits in RAM regardless of indexing order, so
those existing measurements do not need redoing).

## What's needed

On a machine that already has (or can rebuild, given disk budget) the 10 MB-file test tree
`file10mb-create.sh`/`.ps1` produces:

1. Run `file10mb-read.sh` (or `.ps1`) following `performance/methodology.md`'s protocol (5 runs,
   ~20 s each).
2. Write up a new measurement report in `performance/measurements/` (both the `.md` narrative and
   its `.yaml` sibling, matching the existing file naming/format convention) - either superseding
   `2026-08-28-3327-file10mb-read-wsl2.md` if run on the same machine/environment, or as an
   additional data point if run elsewhere.
3. Confirm the new numbers no longer show the artifact's telltale monotonic upward trend across the
   5 runs, and note that explicitly in the new report's own Notes section.

## Done

**Completed**: 2026-09-01, by a Claude Code Desktop-App session on `julius` (real hardware, the
existing multi-GB test trees already present - no rebuild needed, read-only re-runs).

Re-ran both `file10mb-read.ps1` (native Windows) and `file10mb-read.sh` (WSL2) against the
existing 10 MB test trees. Results:

- **Native Windows**: flat at ~21 ops/s after run 1, no monotonic trend - confirms the fix. New
  mean (20.4 ops/s) is lower than the old confounded mean (26.3 ops/s), as expected once reads are
  genuinely random rather than accidentally re-warming the same low-index range.
- **WSL2**: still shows *some* upward drift (14.05 → 5.85 → 16.25 → 23.0 → 29.45 ops/s across the 5
  runs), but for a different, legitimate reason than the original bug: WSL2's page cache (~3.8 GiB)
  is small relative to the ~20 GB tree, so cumulative caching from genuinely random sampling across
  5 runs still produces some drift - not the old artifact (which always re-touched the exact same
  fixed low-index range) recurring. Documented explicitly in the new protocol's Notes rather than
  smoothed over.

Also added retroactive addenda to the three original confounded protocols this fix affects
(`2026-08-28-julius-file10mb-read-native.md`, `-wsl2.md`, and `2026-08-28-3327-file10mb-read-wsl2.md`
- the last one not re-run itself, `3327`-specific hardware access needed for that), and corrected
`performance/overview.md`'s "WSL2 wins at 10 MB reads" finding, which the corrected `julius` numbers
show was itself an artifact of the same bug (native Windows is ahead again once fixed, matching
every other size).

New measurement protocols: `2026-09-01-julius-file10mb-read-native.md`/`.yaml` and
`2026-09-01-julius-file10mb-read-wsl2.md`/`.yaml`.
