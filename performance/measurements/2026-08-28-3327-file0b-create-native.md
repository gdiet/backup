# Zero-byte file creation - native - 3327/native Windows/local NVMe SSD

## Setup
- Date: 2026-08-28
- Machine: 3327
- Execution environment: native Windows (Windows 11 Enterprise, 10.0.26200), Windows PowerShell 5.1
- Power profile: base scheme `aeeed979-846d-4e51-bb46-e7b4f140eb43` ("Thieme Energy Options", a
  custom/renamed Windows scheme, not one of the stock named profiles), AC power-mode overlay
  `961cc777-2547-4f9d-8174-7d86181b8a7a` = `OVERLAY_SCHEME_MIN` ("Besseres Overlay für
  Akkulaufzeit" / "Best power efficiency", i.e. power saver). DC overlay not checked. On AC power
  (plugged in, battery at 100 %, `Win32_Battery` BatteryStatus = 2).
- IO device: local NVMe SSD (SK Hynix HFS001TEJ9X162N per `../machines.md`), NTFS, `C:` drive
- DedupFS build: `3022a2b1` on `rust-performance-test-idea` - the `native` scripts exercise no
  DedupFS code (see `../scripts/README.md`); recorded only to pin which script version was run.
- Isolation: not an isolated machine - corporate-managed Windows 11 with Defender and management
  agents running, not controlled or disabled. No other user-driven foreground load during the runs;
  background activity was not observed or controlled. Scripts were run one at a time, sequentially.

## Workload
- Operation: zero-byte file creation, spread across 20 subdirectories
- Location: native
- Tool: PowerShell `New-Item -ItemType File`, via `performance/scripts/file0b-create.ps1`
- Mode: sequential
- Window: 20 s
- Scale: ~8,400-10,600 files per run, ~45,700 total by the end; fresh start, no prior state
- Content: none (zero-byte files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 10567 files, 20 s, 528.2 ops/s - not discarded: it is the fastest run, not a slow warmup |
| 2 | 9037 files, 20 s, 451.8 ops/s |
| 3 | 8420 files, 20 s, 421.0 ops/s |
| 4 | 8919 files, 20 s, 445.9 ops/s |
| 5 | 8764 files, 20 s, 438.2 ops/s |

Mean: 457.0 ops/s. Range: 421.0 - 528.2 (N=5)

## Notes

First native-Windows measurement on `3327` for this operation, and the first `3327` row for it at
all (previously `julius`-only).

Run 1 is again the fastest (cold cache, empty tree), then the rate settles around ~440 ops/s
without a clear further trend - unlike `dir-create`'s steady decline, because these files are
spread across 20 subdirectories rather than piling into one.

Essentially the same throughput as `dir-create` on this machine (~457 vs ~480) - `New-Item
-ItemType File` and `New-Item -ItemType Directory` cost about the same here, both dominated by
per-call cmdlet + NTFS-create + security-filter overhead.

Much slower than `julius` native Windows (~939 best-performance / ~1101 power-saver). Same
suspected cause as the `dir-create` protocol notes: the corporate security/management stack on this
machine's OS image. Not isolated or measured directly.
