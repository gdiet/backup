# 30 KB file creation - native - 3327/native Windows/local NVMe SSD

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
- Operation: 30 KB file creation, spread across 20 subdirectories
- Location: native
- Tool: PowerShell `[System.IO.File]::WriteAllBytes` (one random template buffer, per-file random
  bytes poked in at intervals - see `../methodology.md`'s "File-content workloads"), via
  `performance/scripts/file30kb-create.ps1`
- Mode: sequential
- Window: 20 s
- Scale: ~9,800-11,600 files per run, ~52,000 total by the end (~1.5 GB); fresh start, no prior state
- Content: 30 KB/file, unique per file, non-deduplicating

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 11557 files, 20 s, 577.8 ops/s - not discarded: it is the fastest run |
| 2 | 10367 files, 20 s, 518.3 ops/s |
| 3 | 9859 files, 20 s, 492.9 ops/s |
| 4 | 9802 files, 20 s, 490.1 ops/s |
| 5 | 10371 files, 20 s, 518.4 ops/s |

Mean: 519.5 ops/s. Range: 490.1 - 577.8 (N=5)

## Notes

First measurement of this workload anywhere.

Still within the same ~450-520 ops/s band as the 0 B / 100 B create workloads on this machine -
30 KB per file is not yet enough for the content write to overtake the per-file create + call
overhead. The size-ladder point where write volume starts to dominate is between here and the
10 MB rung (`file10mb-create` measured ~97 ops/s / ~970 MB/s).
