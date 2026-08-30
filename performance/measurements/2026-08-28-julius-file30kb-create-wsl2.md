# 30 KB file creation - native - julius/WSL2 Debian 12/local SSD

## Setup
- Date: 2026-08-28
- Machine: julius
- Execution environment: WSL2, Debian 12 (bookworm), kernel 6.18.33.2-microsoft-standard-WSL2
  (WSL 2.7.12.0). Working directory `~/dedupfs-perf/files30kb` - WSL2's own native filesystem.
- Power profile: Balanced base scheme, "Best power efficiency"/Power Saver overlay ("Längste
  Akkulaufzeit") on the Windows host - confirmed via `powercfg /query <guid>` →
  `GUID-Alias: OVERLAY_SCHEME_MIN`; WSL2 shares the host's setting. On AC power
  (developer-confirmed).
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA, reached via WSL2's own
  virtual disk - see `../machines.md`)
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised
- Isolation: none deliberate - ordinary interactive development machine, this Claude Code Desktop
  session present throughout, nothing else closed or checked beforehand.

## Workload
- Operation: File creation at 30 KB, unique content
- Location: native
- Tool: bash redirection, content from `perf-gen` (`../scripts/file30kb-create.sh`)
- Mode: sequential
- Window: 20 s
- Scale: 9,391 files total across the 5 runs (1,832-1,927 per run, time-boxed not count-fixed),
  spread round-robin across 20 subdirectories under `~/dedupfs-perf/files30kb`, first measurement
  against this tree. ~275 MB total written.
- Content: 30 KB, unique per file (`perf-gen`-generated)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 1,832 files, 20 s, 91 ops/s |
| 2 | 1,880 files, 20 s, 94 ops/s |
| 3 | 1,888 files, 20 s, 94 ops/s |
| 4 | 1,864 files, 20 s, 93 ops/s |
| 5 | 1,927 files, 20 s, 96 ops/s |

Mean: 93.6 ops/s Range: 91 - 96 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. Tight spread, no trend.

Roughly 45% of the equivalent native-Windows throughput (93.6 vs. 209.5 ops/s mean - see
`2026-08-28-julius-file30kb-create-native.md`), essentially the same ratio as the 100 B create
measurement above (44%) - unlike native Windows, this WSL2 measurement does not show a
faster-first-run pattern.
