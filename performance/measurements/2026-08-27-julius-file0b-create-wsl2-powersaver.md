# Zero-byte file creation - native - julius/WSL2 Debian 12/local SSD (Power Saver)

## Setup
- Date: 2026-08-27
- Machine: julius
- Execution environment: WSL2, Debian 12 (bookworm), kernel 6.18.33.2-microsoft-standard-WSL2
  (WSL 2.7.12.0). Working directory `~/dedupfs-perf/files0b` - WSL2's own native filesystem, not
  `/mnt/c/...` (see `../methodology.md`'s "Execution environment" note).
- Power profile: base scheme unchanged (`381b4222-...`, "Ausbalanciert"/Balanced), power-mode
  overlay switched to "Energiesparmodus" (Power Saver) just before this run - confirmed via
  registry (`ActiveOverlayAcPowerScheme` = `961cc777-2547-4f9d-8174-7d86181b8a7a`) on the Windows
  host; WSL2 has no separate power-mode setting of its own and shares the host's - see
  `2026-08-27-julius-dir-create-native-powersaver.md`'s Setup for the full explanation.
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA, reached via WSL2's own
  virtual disk, not the DrvFs/9p bridge - see `../machines.md`)
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised (see
  `../scripts/README.md`)
- Isolation: none deliberate - same conditions as the companion same-day measurement (see there);
  `~/dedupfs-perf` was deleted first so this is a fresh tree, not a continuation of that run's.

## Workload
- Operation: Zero-byte file creation, spread across several directories
- Location: native
- Tool: bash `touch` (`../scripts/file0b-create.sh`)
- Mode: sequential
- Window: 20 s
- Scale: 56,132 files total across the 5 runs (10,803-12,010 per run, time-boxed not count-fixed),
  spread round-robin across 20 subdirectories under `~/dedupfs-perf/files0b`, a fresh tree deleted
  before this run.
- Content: zero-byte (empty files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 10,803 files, 20 s, 540 ops/s |
| 2 | 11,170 files, 20 s, 558 ops/s |
| 3 | 12,010 files, 20 s, 600 ops/s |
| 4 | 11,273 files, 20 s, 563 ops/s |
| 5 | 10,876 files, 20 s, 543 ops/s |

Mean: 560.8 ops/s Range: 540 - 600 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept.

Unlike the other three Power Saver runs in this batch, this one *is* slower than its
Best-Performance-overlay companion, in the naively expected direction: 560.8 vs. 667.4 ops/s mean
(~16% slower - see `2026-08-27-julius-file0b-create-wsl2.md`). Runs 2-5 are also noticeably noisier
here (543-600 ops/s) than the tight 897-977 ops/s-scale bands seen elsewhere in this project so far,
proportionally. Combined with the other three Power Saver results in this batch (two faster, one
flat), this measurement alone should not be read as confirming a general "Power Saver slows WSL2
IO" effect - the four results across this batch point in three different directions for what looks
like the same underlying change, which is itself the notable finding; see `../overview.md` for the
side-by-side comparison across all eight measurements taken so far.
