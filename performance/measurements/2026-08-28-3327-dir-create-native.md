# Directory creation - native - 3327/native Windows/local NVMe SSD

## Setup
- Date: 2026-08-28
- Machine: 3327
- Execution environment: native Windows (Windows 11 Enterprise, 10.0.26200), Windows PowerShell 5.1
- Power profile: base scheme `aeeed979-846d-4e51-bb46-e7b4f140eb43` ("Thieme Energy Options", a
  custom/renamed Windows scheme, not one of the stock named profiles), AC power-mode overlay
  `961cc777-2547-4f9d-8174-7d86181b8a7a` = `OVERLAY_SCHEME_MIN` ("Besseres Overlay für
  Akkulaufzeit" / "Best power efficiency", i.e. power saver). DC overlay not checked. On AC power
  (plugged in, battery at 100 %, `Win32_Battery` BatteryStatus = 2), so the AC overlay above is the
  one that applied.
- IO device: local NVMe SSD (SK Hynix HFS001TEJ9X162N per `../machines.md`), NTFS, `C:` drive
- DedupFS build: `3022a2b1` on `rust-performance-test-idea` - the `native` scripts exercise no
  DedupFS code (see `../scripts/README.md`); recorded only to pin which script version was run.
- Isolation: not an isolated machine - corporate-managed Windows 11 with Defender and management
  agents running, not controlled or disabled. No other user-driven foreground load during the runs;
  background activity was not observed or controlled. Scripts were run one at a time, sequentially.

## Workload
- Operation: directory creation
- Location: native
- Tool: PowerShell `New-Item -ItemType Directory`, via `performance/scripts/dir-create.ps1`
- Mode: sequential
- Window: 20 s
- Scale: ~8,600-10,600 directories per run, ~48,000 total by the end; fresh start, no prior state
- Content: not applicable (directories, no file content)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 10564 dirs, 20 s, 528.1 ops/s - not discarded: it is the *fastest* run, not a slow warmup, so the fixed 50%-slower-than-median rule does not apply |
| 2 | 10174 dirs, 20 s, 508.7 ops/s |
| 3 | 9772 dirs, 20 s, 488.6 ops/s |
| 4 | 8868 dirs, 20 s, 443.4 ops/s |
| 5 | 8616 dirs, 20 s, 430.7 ops/s |

Mean: 479.9 ops/s. Range: 430.7 - 528.1 (N=5)

## Notes

First native-Windows measurement on `3327` - closes a coverage gap (previously `3327` had only a
WSL2 row for this operation, and native Windows was `julius`-only).

**Monotonic decline across the 5 runs** (528 -> 431, -18 %), tracking the single directory growing
from empty to ~48,000 entries as the runs accumulate (the "state between runs" rule keeps them in
one directory). This is a real finding about how NTFS directory creation scales with the target
directory's existing size, not run-to-run noise - the file-creation workloads, which spread entries
across 20 subdirectories, do not show it as cleanly.

**Native Windows here (~480 ops/s) is about half of `3327`'s own WSL2 result (~985 ops/s,
`2026-08-28-3327-dir-create-wsl2.md`)** - the opposite direction from `julius`, where WSL2 runs at
roughly half of native Windows. Confounded by power mode (this run: power saver; the `3327` WSL2
run: best-performance overlay) and by the corporate-managed OS image here, so treat it as an open
question, not a settled reversal.

**Also markedly slower than `julius` native Windows** (~822 best-performance / ~929 power-saver)
despite `3327` having the newer, faster CPU. The most likely contributor is the corporate security
/ management stack on this machine - every `New-Item` crosses a filesystem filter that AV hooks -
but this was not isolated or measured directly.
