# Directory lookup - native - 3327/native Windows/local NVMe SSD

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
- Operation: directory lookup - `Test-Path` on a pseudo-randomly chosen existing entry in a fixed
  100,000-directory tree, built once as setup (~169 s here) and not grown between runs
- Location: native
- Tool: PowerShell `Test-Path` (plus `Get-Random` per iteration), via
  `performance/scripts/dir-lookup.ps1`
- Mode: sequential
- Window: 20 s
- Scale: fixed 100,000-entry tree; ~20,300-34,700 lookups per run
- Content: not applicable

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 20301 lookups, 20 s, 1015.0 ops/s - not discarded: ~35 % below the run 2-5 median, short of the 50 % threshold, but clearly a cold first pass over the 100k-entry tree |
| 2 | 30629 lookups, 20 s, 1531.4 ops/s |
| 3 | 34690 lookups, 20 s, 1734.5 ops/s |
| 4 | 31592 lookups, 20 s, 1579.6 ops/s |
| 5 | 31325 lookups, 20 s, 1566.2 ops/s |

Mean: 1485.3 ops/s. Range: 1015.0 - 1734.5 (N=5)

## Notes

First native-Windows measurement of this workload on `3327` (closes a coverage gap - previously
`julius`-only). `3327` has no WSL2 lookup measurement yet, so the native-vs-WSL2 comparison can
only be made on `julius` for now.

At ~1485 ops/s each `Test-Path` costs ~670 us. That is `Test-Path`'s own cmdlet overhead (provider
resolution, wildcard parsing) plus a `Get-Random` call per iteration, far more than a filesystem
lookup itself - the same effect `julius`'s protocols call out, where WSL2 `test -d` (a shell
builtin) runs ~25-34x faster than native-Windows `Test-Path`.

Slower than `julius` native Windows (~2145 ops/s) despite the newer CPU - consistent with every
other create/lookup workload on this machine, prime suspect the corporate security / management
stack. Run 1 is the cold-tree low point; runs 2-5 sit around ~1550-1735.
