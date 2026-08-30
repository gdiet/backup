# 10 MB file read-back - native - 3327/native Windows/local NVMe SSD

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
- Operation: 10 MB file read-back, cycling over the files `file10mb-create.ps1` left behind
- Location: native
- Tool: PowerShell `[System.IO.File]::ReadAllBytes` (result discarded via `$null =`), via
  `performance/scripts/file10mb-read.ps1`
- Mode: sequential
- Window: 20 s
- Scale: reads cycle over the ~2,440-file dataset from `file10mb-create` (~24 GB total, larger than
  the ~15 GB of RAM this machine keeps available for cache, so the reads are a mix of cache hits
  and real NVMe reads); ~990-1,400 reads per run
- Content: 10 MB/file

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 989 reads, 20 s, 49.4 ops/s - not discarded: ~27 % below the run 2-5 median, short of the 50 % threshold (cold cache) |
| 2 | 1332 reads, 20 s, 66.6 ops/s |
| 3 | 1115 reads, 20 s, 55.7 ops/s |
| 4 | 1404 reads, 20 s, 70.2 ops/s |
| 5 | 1399 reads, 20 s, 69.9 ops/s |

Mean: 62.4 ops/s (~620 MB/s). Range: 49.4 - 70.2 (N=5)

## Notes

First measurement of this workload anywhere.

Unusually, 10 MB reads (~62 ops/s, ~620 MB/s) come out *slower* than 10 MB creates (~97 ops/s,
~970 MB/s) on this machine - the reverse of the small-file workloads. The likely reason:
`WriteAllBytes` returns once the data is in the OS write-back cache, well before it is physically
on the SSD, whereas the 24 GB read dataset does not fit in cache so a good share of the reads are
real NVMe reads. This is the only file-content workload here whose dataset is large enough for the
disk to actually matter.

Less noisy than the small-file reads (range factor 1.4x vs 1.8x), because the disk component
anchors the number rather than pure cache/interpreter timing. Run 1 is the cold-cache low point.
Same non-isolation caveat as the other protocols.
