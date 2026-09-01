# 10 MB file read - native - julius/native Windows/local SSD

## Setup
- Date: 2026-09-01
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: not captured this session (unrelated bug-fix re-run, not a power-mode comparison -
  see `2026-08-28-julius-file10mb-read-native.md` for the confirmed "Best power efficiency"/Power
  Saver overlay from the original round on this same machine)
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`)
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised
- Isolation: none deliberate - ordinary interactive development machine, this Claude Code Desktop
  session present throughout, nothing else closed or checked beforehand.

## Workload
- Operation: Reading back 10 MB files (analogous to the corresponding write)
- Location: native
- Tool: PowerShell `[System.IO.File]::ReadAllBytes` (`../scripts/file10mb-read.ps1`, **fixed**
  version - pseudo-random index per read, not the sequential-from-file-1-every-run indexing the
  original round used)
- Mode: sequential
- Window: 20 s
- Scale: 2,049 reads total across the 5 runs, against the same 2,642-file (~26.4 GB) tree created
  by `2026-08-28-julius-file10mb-create-native.md` - unchanged, this is a read-only re-run.
- Content: 10 MB, unique per file (as created)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 357 reads, 20.00 s, 17.8 ops/s |
| 2 | 421 reads, 20.02 s, 21.0 ops/s |
| 3 | 420 reads, 20.03 s, 21.0 ops/s |
| 4 | 427 reads, 20.04 s, 21.3 ops/s |
| 5 | 424 reads, 20.05 s, 21.1 ops/s |

Mean: 20.4 ops/s Range: 17.8 - 21.3 ops/s (N=5)

## Notes
**Supersedes `2026-08-28-julius-file10mb-read-native.md`** for interpretation purposes (that
protocol is kept, not deleted, per `../methodology.md`'s convention, with its own retroactive
addendum pointing here) - re-run specifically because `file10mb-read.{ps1,sh}` used to restart its
read index at file 1 every run, producing a page-cache-driven artifact at working-set sizes
exceeding RAM (see `agent-todos/done/file-read-scripts-restart-index-each-run.md`). Fixed to pick a
pseudo-random index per read instead, matching `dir-lookup.{ps1,sh}`'s existing approach.

Run 1 is within the discard threshold, kept. Runs 2-5 are flat (~21 ops/s) with no monotonic trend -
confirms the fix: the old protocol's artifact (a monotonic-looking upward trend from repeatedly
re-touching the same low-index range) is gone. The new mean (20.4 ops/s) is noticeably *lower* than
the old, confounded mean (26.3 ops/s) - expected, since genuinely random sampling across a ~26 GB
tree against this machine's 8 GB RAM is mostly disk-bound, unlike the old protocol's accidental
page-cache-warm low-index range.
