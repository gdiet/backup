# 10 MB file creation - native - julius/WSL2 Debian 12/local SSD

## Setup
- Date: 2026-08-28
- Machine: julius
- Execution environment: WSL2, Debian 12 (bookworm), kernel 6.18.33.2-microsoft-standard-WSL2
  (WSL 2.7.12.0). Working directory `~/dedupfs-perf/files10mb` - WSL2's own native filesystem.
- Power profile: Balanced base scheme, "Best power efficiency"/Power Saver overlay ("Längste
  Akkulaufzeit") on the Windows host - confirmed via `powercfg /query <guid>` →
  `GUID-Alias: OVERLAY_SCHEME_MIN`; WSL2 shares the host's setting. On AC power
  (developer-confirmed).
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA, reached via WSL2's own
  virtual disk - see `../machines.md`). Free space on WSL2's virtual disk checked after this run:
  925 GB available (`df -h ~`), no concern.
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised
- Isolation: none deliberate - ordinary interactive development machine, this Claude Code Desktop
  session present throughout, nothing else closed or checked beforehand.

## Workload
- Operation: File creation at 10 MB, unique content
- Location: native
- Tool: bash redirection, content from `perf-gen` (`../scripts/file10mb-create.sh`)
- Mode: sequential
- Window: 20 s
- Scale: 2,005 files total across the 5 runs (373-431 per run, time-boxed not count-fixed), spread
  round-robin across 20 subdirectories under `~/dedupfs-perf/files10mb`, first measurement against
  this tree. ~20.05 GB total written.
- Content: 10 MB, unique per file (`perf-gen`-generated)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 431 files, 20 s, 21 ops/s |
| 2 | 396 files, 20 s, 19 ops/s |
| 3 | 425 files, 20 s, 21 ops/s |
| 4 | 380 files, 20 s, 19 ops/s |
| 5 | 373 files, 20 s, 18 ops/s |

Mean: 19.6 ops/s Range: 18 - 21 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. A mild downward drift (21 → 19 → 21 → 19 → 18, not
strictly monotonic but net ~14% peak-to-last) - the same direction as, but noticeably milder than,
the ~32% peak-to-last drop seen in the native-Windows 10 MB create measurement
(`2026-08-28-julius-file10mb-create-native.md`), despite this run writing less total data (~20 GB
vs. ~26 GB there) over the same 5 runs - consistent with (but not proof of) the native-Windows
degradation being at least partly a Windows-filesystem/driver-layer effect rather than purely an
SSD-hardware one, since both share the same underlying device.

Roughly 74% of the equivalent native-Windows throughput (19.6 vs. 26.4 ops/s mean) - much closer
than the 44-45% ratio seen at 100 B/30 KB, plausibly because the native-Windows number itself is
already degraded by the trend above rather than WSL2 gaining ground.
