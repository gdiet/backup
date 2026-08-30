# 100 B file creation - native - julius/native Windows/USB stick

## Setup
- Date: 2026-08-28
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, "Best power efficiency"/Power Saver overlay ("Längste
  Akkulaufzeit") - confirmed via `powercfg /query <guid>` → `GUID-Alias: OVERLAY_SCHEME_MIN`. On AC
  power (developer-confirmed).
- IO device: external USB stick, drive `I:`, NTFS, labeled "USB Stick", ~4 GB total capacity.
  Measured write throughput ~8.7 MB/s via a single 100 MB probe before this session - USB2-class
  speed. Free space checked before starting: 2.65 GB (after the companion
  `2026-08-28-julius-file10mb-create-usb.md` run).
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised
- Isolation: none deliberate - ordinary interactive development machine, this Claude Code Desktop
  session present throughout, nothing else closed or checked beforehand.

## Workload
- Operation: File creation at 100 B, unique content
- Location: native
- Tool: PowerShell `[System.IO.File]::WriteAllBytes` (same logic as
  `../scripts/file100b-create.ps1`, run inline against `I:\dedupfs-perf\files100b`)
- Mode: sequential
- Window: 20 s
- Scale: 10,093 files total across the 5 runs (1,786-2,164 per run, time-boxed not count-fixed),
  spread round-robin across 20 subdirectories, first measurement against this tree. ~1 MB total
  content written (negligible against the ~200 MB/run this size ladder normally produces on faster
  storage).
- Content: 100 B, unique per file (random template with fresh random bytes poked in, generator
  outside the timed loop)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 2,070 files, 20.01 s, 103.4 ops/s |
| 2 | 2,164 files, 20.00 s, 108.2 ops/s |
| 3 | 2,149 files, 20.01 s, 107.4 ops/s |
| 4 | 1,786 files, 20.00 s, 89.3 ops/s |
| 5 | 1,924 files, 20.00 s, 96.2 ops/s |

Mean: 100.9 ops/s Range: 89.3 - 108.2 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. Run 4 (89.3 ops/s) is the low point of the whole
series, ~17% below the run-2/3 plateau - not a warmup pattern (it is the fourth run, not the
first), no explanation available given Isolation was not controlled.

Slower than directory creation on the same device (100.9 vs. 158.3 ops/s mean - see
`2026-08-28-julius-dir-create-usb.md`) - expected, since this operation also transfers 100 B of
content and (on NTFS) writes it resident in the MFT record rather than as a separate data run,
still cheaper than an external write but not free. Roughly 2.4x slower than this same content size
on the internal SSD (100.9 vs. 242.6 ops/s - see
`2026-08-28-julius-file100b-create-native.md`) - a much smaller gap than directory creation's
~5.8x, plausibly because per-command USB protocol latency is a larger fraction of a `mkdir`'s total
cost than of a small file write's.
