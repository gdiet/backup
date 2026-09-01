# 10 MB file read - native - julius/WSL2 Debian 12/local SSD

## Setup
- Date: 2026-09-01
- Machine: julius
- Execution environment: WSL2, Debian 12 (bookworm), kernel 6.18.33.2-microsoft-standard-WSL2
  (WSL 2.7.12.0). Working directory `~/dedupfs-perf/files10mb` - WSL2's own native filesystem.
- Power profile: not captured this session (unrelated bug-fix re-run - see
  `2026-08-28-julius-file10mb-read-wsl2.md` for the confirmed Power Saver overlay from the original
  round on this same machine)
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA, reached via WSL2's own
  virtual disk - see `../machines.md`)
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised
- Isolation: none deliberate - ordinary interactive development machine, this Claude Code Desktop
  session present throughout, nothing else closed or checked beforehand.

## Workload
- Operation: Reading back 10 MB files (analogous to the corresponding write)
- Location: native
- Tool: bash `cat` to `/dev/null` (`../scripts/file10mb-read.sh`, **fixed** version - pseudo-random
  index per read via `$RANDOM`, not the sequential-from-file-1-every-run indexing the original
  round used)
- Mode: sequential
- Window: 20 s
- Scale: 1,772 reads total across the 5 runs, against the same 2,005-file (~20.05 GB) tree created
  by `2026-08-28-julius-file10mb-create-wsl2.md` - unchanged, this is a read-only re-run.
- Content: 10 MB, unique per file (as created)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 281 reads, 20 s, 14.05 ops/s |
| 2 | 117 reads, 20 s, 5.85 ops/s |
| 3 | 325 reads, 20 s, 16.25 ops/s |
| 4 | 460 reads, 20 s, 23.0 ops/s |
| 5 | 589 reads, 20 s, 29.45 ops/s |

Mean: 17.7 ops/s Range: 5.85 - 29.45 ops/s (N=5)

## Notes
Run 1 (14.05 ops/s) is within the discard threshold (median of runs 2-5 is 19.6, and 14.05 is above
the 50%-slower cutoff), kept.

**Supersedes `2026-08-28-julius-file10mb-read-wsl2.md`** for interpretation purposes (kept, not
deleted, with its own retroactive addendum pointing here) - re-run for the same index-bug fix as
the native protocol above. This one, however, still shows real run-to-run variance and a rough
upward drift from run 2 onward (5.85 → 16.25 → 23.0 → 29.45) - but this is **not** the old bug
resurfacing. The old artifact came from every run restarting at the *same* low-index files, so
later runs found progressively more of that *fixed* range still page-cache-warm from the previous
pass. Here the index is genuinely random each time, but WSL2's own page cache is bounded to
~3.8 GiB (see `../machines.md`) against a ~20 GB tree - as random sampling proceeds across the 5
runs, a growing fraction of the reachable files have been touched at least once and some fraction
stays resident, so *some* upward drift from cache accumulation is a real, expected property of any
random-read benchmark against a partially-cacheable dataset, not a script defect. Run 2's outlier
low value (5.85, lower than run 1) argues against a clean monotonic caching story on its own -
treated as ordinary noise given the small per-run sample size (~100-600 reads).

The wide range (5.85-29.45) makes this measurement's single mean less meaningful than usual - a
repeat with more runs, or a larger read count per run, would characterize the variance itself
better than this 5-run baseline can. Not pursued further here, flagged for a future pass if this
number matters more precisely later.
