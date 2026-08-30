# Directory lookup - native - julius/native Windows/local SSD

## Setup
- Date: 2026-08-28
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, "Best power efficiency"/Power Saver overlay ("Längste
  Akkulaufzeit" in the current Settings UI) - confirmed via `powercfg /query <guid>` →
  `GUID-Alias: OVERLAY_SCHEME_MIN` for `961cc777-2547-4f9d-8174-7d86181b8a7a`, per
  `../methodology.md`'s recipe. On AC power (developer-confirmed; not reliably queryable per
  `../methodology.md`).
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`)
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised
- Isolation: none deliberate - ordinary interactive development machine, this Claude Code Desktop
  session present throughout, nothing else closed or checked beforehand.

## Workload
- Operation: Directory lookup
- Location: native
- Tool: PowerShell `Test-Path` (`../scripts/dir-lookup.ps1`)
- Mode: sequential
- Window: 20 s
- Scale: fixed 100,000-directory tree under `C:\dedupfs-perf\lookup`, built once before this
  measurement (not part of the timed runs); 214,635 lookups total across the 5 runs, each a
  pseudo-random existing entry.
- Content: n/a (directory lookups, no file content)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 42,620 lookups, 20.01 s, 2130.3 ops/s |
| 2 | 43,747 lookups, 20.01 s, 2186.7 ops/s |
| 3 | 43,292 lookups, 20.01 s, 2163.8 ops/s |
| 4 | 43,041 lookups, 20.01 s, 2151.2 ops/s |
| 5 | 41,935 lookups, 20.01 s, 2095.6 ops/s |

Mean: 2145.5 ops/s Range: 2095.6 - 2186.7 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. Runs are tightly clustered (~2096-2187 ops/s) with no
trend - expected, since the lookup tree is fixed-size and does not grow across runs.
