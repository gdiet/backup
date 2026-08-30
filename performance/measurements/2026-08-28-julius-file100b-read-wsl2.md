# 100 B file read - native - julius/WSL2 Debian 12/local SSD

## Setup
- Date: 2026-08-28
- Machine: julius
- Execution environment: WSL2, Debian 12 (bookworm), kernel 6.18.33.2-microsoft-standard-WSL2
  (WSL 2.7.12.0). Working directory `~/dedupfs-perf/files100b` - WSL2's own native filesystem.
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
- Operation: Reading back 100 B files (analogous to the corresponding write)
- Location: native
- Tool: bash `cat` to `/dev/null` (`../scripts/file100b-read.sh`)
- Mode: sequential
- Window: 20 s
- Scale: 60,393 reads total across the 5 runs, against the 10,766-file tree created by
  `2026-08-28-julius-file100b-create-wsl2.md` immediately before this run.
- Content: 100 B, unique per file (as created)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 8,878 reads, 20 s, 443 ops/s |
| 2 | 12,326 reads, 20 s, 616 ops/s |
| 3 | 13,012 reads, 20 s, 650 ops/s |
| 4 | 13,077 reads, 20 s, 653 ops/s |
| 5 | 13,100 reads, 20 s, 655 ops/s |

Mean: 603.4 ops/s Range: 443 - 655 ops/s (N=5)

## Notes
Run 1 (443 ops/s) is well under the runs-2-5 median (~651.5 ops/s, ~32% lower) but stays above the
fixed 50%-slower discard threshold, so it is kept per `../methodology.md`'s rule rather than a
subjective "looks like warmup" call - worth flagging as a borderline case even though the rule does
not trigger. Runs 2-5 climb and then plateau (616 → 650 → 653 → 655), consistent with a genuine
warmup effect that the fixed threshold just does not happen to catch here.

Roughly 15% of the equivalent native-Windows read throughput (603.4 vs. 3964.4 ops/s mean - see
`2026-08-28-julius-file100b-read-native.md`) - a much larger native-ahead gap than the ~44% seen on
the create side above, plausibly because the native-Windows reads are page-cache-warm .NET
in-process reads while these are individual read syscalls without an equivalent in-process
optimization; not disentangled further here.
