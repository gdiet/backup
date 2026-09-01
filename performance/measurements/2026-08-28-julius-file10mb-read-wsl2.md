# 10 MB file read - native - julius/WSL2 Debian 12/local SSD

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
  virtual disk - see `../machines.md`)
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised
- Isolation: none deliberate - ordinary interactive development machine, this Claude Code Desktop
  session present throughout, nothing else closed or checked beforehand.

## Workload
- Operation: Reading back 10 MB files (analogous to the corresponding write)
- Location: native
- Tool: bash `cat` to `/dev/null` (`../scripts/file10mb-read.sh`)
- Mode: sequential
- Window: 20 s
- Scale: 3,552 reads total across the 5 runs, against the 2,005-file (~20.05 GB) tree created by
  `2026-08-28-julius-file10mb-create-wsl2.md` immediately before this run - exceeds this machine's
  8 GB RAM (and WSL2's own ~3.8 GiB budget within it), so not page-cache-warm.
- Content: 10 MB, unique per file (as created)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 612 reads, 20 s, 30 ops/s |
| 2 | 755 reads, 20 s, 37 ops/s |
| 3 | 749 reads, 20 s, 37 ops/s |
| 4 | 745 reads, 20 s, 37 ops/s |
| 5 | 691 reads, 20 s, 34 ops/s |

Mean: 35.0 ops/s Range: 30 - 37 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept.

**Reversal from every smaller size**: this is the only read measurement in this session where WSL2
is *faster* than native Windows (35.0 vs. 26.3 ops/s mean - see
`2026-08-28-julius-file10mb-read-native.md`), the opposite of the ~15% native-ahead ratio seen at
100 B/30 KB. At this size neither side is page-cache-warm (the working set exceeds RAM on both), so
this is plausibly closer to a genuine disk-bound comparison than the smaller sizes, where cmdlet/
cache effects likely dominated the native-Windows numbers - consistent with the lookup/listing
results above also favoring WSL2 once per-call overhead stops dominating. Not confirmed further
here.

**Retroactive addendum** (added 2026-09-01, once a script bug was found and fixed): this
measurement used `file10mb-read.sh` *before* it was fixed to pick a pseudo-random index per read -
it restarted its read index at file 1 every run instead, so this run's flat-looking numbers were
likely still somewhat page-cache-warm from repeatedly re-touching the same low-index range, not a
genuinely random access pattern. The "reversal from every smaller size" observation above may be
partly an artifact of this rather than purely a real WSL2-vs-native comparison - see
`2026-09-01-julius-file10mb-read-wsl2.md` for the corrected re-run (17.7 vs. this file's 35.0 ops/s
mean, and now the *slower* side again, matching the smaller sizes' native-ahead pattern rather than
reversing it) and `agent-todos/done/file-read-scripts-restart-index-each-run.md` for the bug.
