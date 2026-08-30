# 100 B file creation - native - 3327/native Windows/local NVMe SSD

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
- Operation: 100 B file creation, spread across 20 subdirectories
- Location: native
- Tool: PowerShell `[System.IO.File]::WriteAllBytes` (one random template buffer, per-file random
  bytes poked in at intervals - see `../methodology.md`'s "File-content workloads"), via
  `performance/scripts/file100b-create.ps1`
- Mode: sequential
- Window: 20 s
- Scale: ~9,000-10,300 files per run, ~47,700 total by the end; fresh start, no prior state
- Content: 100 B/file, unique per file, non-deduplicating

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 9185 files, 20 s, 459.2 ops/s - not discarded (well within the median) |
| 2 | 9200 files, 20 s, 460.0 ops/s |
| 3 | 9068 files, 20 s, 453.4 ops/s |
| 4 | 9898 files, 20 s, 494.9 ops/s |
| 5 | 10305 files, 20 s, 515.2 ops/s |

Mean: 476.5 ops/s. Range: 453.4 - 515.2 (N=5)

## Notes

First measurement of this workload anywhere.

Tighter spread than the zero-byte and directory create workloads, and a mild *upward* drift over
the 5 runs rather than a decline. Throughput (~477 ops/s) is within noise of `file0b-create`
(~457) and `dir-create` (~480) on this machine: at 100 B the content write is negligible next to
the per-file create + `WriteAllBytes` call + security-filter cost, exactly what `../methodology.md`
expects for small files. The template-and-poke content scheme adds nothing measurable here.
