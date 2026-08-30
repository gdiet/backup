# 30 KB file read-back - native - 3327/native Windows/local NVMe SSD

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
- Operation: 30 KB file read-back, cycling over the files `file30kb-create.ps1` left behind
- Location: native
- Tool: PowerShell `[System.IO.File]::ReadAllBytes` (result discarded via `$null =`), via
  `performance/scripts/file30kb-read.ps1`
- Mode: sequential
- Window: 20 s
- Scale: reads cycle over the ~52,000-file dataset from `file30kb-create` (~1.5 GB total, fits in
  RAM cache on this 32 GB machine); ~23,800-46,400 reads per run
- Content: 30 KB/file

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 23767 reads, 20 s, 1188.3 ops/s - not discarded: ~40 % below the run 2-5 median, short of the 50 % threshold, but clearly a cold-cache first run |
| 2 | 27710 reads, 20 s, 1385.4 ops/s |
| 3 | 46388 reads, 20 s, 2319.3 ops/s |
| 4 | 40975 reads, 20 s, 2048.7 ops/s |
| 5 | 39709 reads, 20 s, 1985.4 ops/s |

Mean: 1785.4 ops/s. Range: 1188.3 - 2319.3 (N=5)

## Notes

First measurement of this workload anywhere.

Same shape as `file100b-read`: reads several times faster than the corresponding creates, and a
wide upward-drifting spread (runs 1-2 clearly below runs 3-5 as the 1.5 GB dataset warms into the
page cache). At ~1785 ops/s of 30 KB that is ~52 MB/s of logical throughput, but this is
cache-dominated, not a disk-bandwidth figure. Same non-isolation caveat as the other read
protocols - treat the mean as approximate and worth a quieter repeat.

Note the reads at 100 B (~1562 ops/s) and 30 KB (~1785 ops/s) are within noise of each other:
under warm cache the per-call + loop overhead dominates over the 300x difference in bytes moved.
