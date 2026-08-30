# 30 KB file read - native - julius/WSL2 Debian 12/local SSD

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
- Operation: Reading back 30 KB files (analogous to the corresponding write)
- Location: native
- Tool: bash `cat` to `/dev/null` (`../scripts/file30kb-read.sh`)
- Mode: sequential
- Window: 20 s
- Scale: 53,194 reads total across the 5 runs, against the 9,391-file (~275 MB) tree created by
  `2026-08-28-julius-file30kb-create-wsl2.md` immediately before this run.
- Content: 30 KB, unique per file (as created)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 10,047 reads, 20 s, 502 ops/s |
| 2 | 10,547 reads, 20 s, 527 ops/s |
| 3 | 10,953 reads, 20 s, 547 ops/s |
| 4 | 10,838 reads, 20 s, 541 ops/s |
| 5 | 10,809 reads, 20 s, 540 ops/s |

Mean: 531.4 ops/s Range: 502 - 547 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. Mild upward settling from run 1 to run 3, then flat.

Roughly 14% of the equivalent native-Windows read throughput (531.4 vs. 3760.9 ops/s mean - see
`2026-08-28-julius-file30kb-read-native.md`), essentially the same ratio as the 100 B read
measurement above (15%) - both far below the ~44-45% ratio seen on the create side, the same
pattern noted there.
