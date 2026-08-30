# 10 MB file creation - native - 3327/native Windows/local NVMe SSD

## Setup
- Date: 2026-08-28
- Machine: 3327
- Execution environment: native Windows (Windows 11 Enterprise, 10.0.26200), Windows PowerShell 5.1
- Power profile: base scheme `aeeed979-846d-4e51-bb46-e7b4f140eb43` ("Thieme Energy Options", a
  custom/renamed Windows scheme, not one of the stock named profiles), AC power-mode overlay
  `961cc777-2547-4f9d-8174-7d86181b8a7a` = `OVERLAY_SCHEME_MIN` ("Best power efficiency" / power
  saver). DC overlay not checked. On AC power (plugged in, battery at 100 %, `Win32_Battery`
  BatteryStatus = 2).
- IO device: local NVMe SSD (SK Hynix HFS001TEJ9X162N per `../machines.md`), NTFS, `C:` drive
- DedupFS build: `3022a2b1` on `rust-performance-test-idea` - the `native` scripts exercise no
  DedupFS code (see `../scripts/README.md`); recorded only to pin which script version was run.
- Isolation: not an isolated machine - corporate-managed Windows 11 with Defender and management
  agents running, not controlled or disabled. No other user-driven foreground load during the runs;
  background activity was not observed or controlled. Scripts were run one at a time, sequentially.

## Workload
- Operation: 10 MB file creation, spread across 20 subdirectories
- Location: native
- Tool: PowerShell `[System.IO.File]::WriteAllBytes` (one random template buffer, per-file random
  bytes poked in at intervals - see `../methodology.md`'s "File-content workloads"), via
  `performance/scripts/file10mb-create.ps1`
- Mode: sequential
- Window: **5 s** (reduced from the standard 20 s - see Notes)
- Scale: ~430-630 files per run, ~2,440 total by the end (~24 GB); fresh start, no prior state
- Content: 10 MB/file, unique per file, non-deduplicating

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 634 files, 5.02 s, 126.4 ops/s - not discarded: it is the fastest run, not a slow warmup (empty tree, cold cache) |
| 2 | 430 files, 5.00 s, 86.0 ops/s |
| 3 | 433 files, 5.02 s, 86.3 ops/s |
| 4 | 505 files, 5.00 s, 101.0 ops/s |
| 5 | 435 files, 5.01 s, 86.8 ops/s |

Mean: 97.3 ops/s (~970 MB/s). Range: 86.0 - 126.4 (N=5)

## Notes

First measurement of this workload anywhere. This is the size-ladder's multi-chunk point (~8-10
chunks at the 20-bit CDC default) for the future native-vs-DedupFS comparison; the DedupFS side is
still blocked on REQ-STORAGE-007's byte store.

**Window reduced to 5 s** rather than the standard 20 s: at ~100 ops/s a full 20-s x 5-run
measurement would accumulate ~7,000+ files x 10 MB ~= 70+ GB on the developer's working disk (the
"state between runs" rule keeps the files). A 5-s window still completes ~430-630 operations per
run - ample for a within-run spread - and bounds the footprint to ~24 GB. `ops/s` normalises for
window length, so the number stays comparable to a 20-s run except that less accumulated state
means slightly *less* of the create-slows-as-the-tree-grows effect than a 20-s run would show.

Run 1 (empty tree, cold cache) is a clear outlier on the fast side; runs 2-5 settle around
~85-100 ops/s. At ~970 MB/s this is finally write-volume-bound rather than per-file-call-bound -
the crossover from the flat ~450-520 ops/s seen at 0 B / 100 B / 30 KB happens somewhere between
30 KB and 10 MB.
