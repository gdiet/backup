# 10 MB file read - native - julius/native Windows/local SSD

## Setup
- Date: 2026-08-28
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, "Best power efficiency"/Power Saver overlay ("Längste
  Akkulaufzeit") - confirmed via `powercfg /query <guid>` → `GUID-Alias: OVERLAY_SCHEME_MIN`. On AC
  power (developer-confirmed).
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`)
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised
- Isolation: none deliberate - ordinary interactive development machine, this Claude Code Desktop
  session present throughout, nothing else closed or checked beforehand.

## Workload
- Operation: Reading back 10 MB files (analogous to the corresponding write)
- Location: native
- Tool: PowerShell `[System.IO.File]::ReadAllBytes` (`../scripts/file10mb-read.ps1`)
- Mode: sequential
- Window: 20 s
- Scale: 2,630 reads total across the 5 runs, against the 2,642-file (~26.4 GB) tree created by
  `2026-08-28-julius-file10mb-create-native.md` immediately before this run - at this size the
  working set exceeds this machine's 8 GB RAM, so these reads are not page-cache-warm the way the
  smaller sizes' reads are.
- Content: 10 MB, unique per file (as created)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 505 reads, 20.00 s, 25.2 ops/s |
| 2 | 526 reads, 20.03 s, 26.3 ops/s |
| 3 | 536 reads, 20.05 s, 26.7 ops/s |
| 4 | 532 reads, 20.02 s, 26.6 ops/s |
| 5 | 531 reads, 20.02 s, 26.5 ops/s |

Mean: 26.3 ops/s Range: 25.2 - 26.7 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. Unlike the create side
(`2026-08-28-julius-file10mb-create-native.md`), reads are flat across all 5 runs with no
downward trend - consistent with the create-side degradation being specifically a *write*-path
effect (write-amplification/GC-adjacent) rather than a general property of high sustained IO
volume on this device. Read throughput (26.3 ops/s) is almost identical to create throughput
(26.4 ops/s mean) at this size - unlike every smaller size in this ladder, where reads ran
10-16x faster than creates (see the 100 B/30 KB read protocols' Notes) - consistent with this
being the first size where reads are genuinely disk-bound (cold, not cache-served) rather than
page-cache-warm, so read and write converge toward the same underlying SSD throughput ceiling.

**Retroactive addendum** (added 2026-09-01, once a script bug was found and fixed): this
measurement used `file10mb-read.ps1` *before* it was fixed to pick a pseudo-random index per read -
it restarted its read index at file 1 every run instead, which the "flat across all 5 runs"
observation above still held for at this size only because the whole tree happened to fit in page
cache well enough here that the artifact (visible on other machines/environments with a larger
tree relative to RAM, see `2026-08-28-3327-file10mb-read-wsl2.md`) never showed up as a trend. The
absolute throughput number is still confounded, though - the same fixed low-index range was
repeatedly re-touched, so this ran warmer than a genuinely random access pattern would. See
`2026-09-01-julius-file10mb-read-native.md` for the corrected re-run (20.4 vs. this file's
26.3 ops/s mean) and `agent-todos/done/file-read-scripts-restart-index-each-run.md` for the bug.
