# Performance Overview

Aggregated, at-a-glance view built from the individual measurement protocols in
[`measurements/`](measurements/) - see [`methodology.md`](methodology.md) for how each number was
obtained. Every number here also lives in the measurement protocol it was taken from; this file has
none of its own.

Grouped by operation - one table per operation, columns for machine/environment/IO device/location
and sequential vs. best-parallel-`N` throughput, split into further files if a single table per
operation gets too wide or long to read at a glance.

## Directory creation

| Machine | Environment | Power mode | IO device | Location | Mode | Throughput (ops/s) | Protocol |
|---|---|---|---|---|---|---|---|
| julius | native Windows | best performance* | local SSD | native | sequential | 821.9 (763.4-857.8) | [2026-08-27](measurements/2026-08-27-julius-dir-create-native.md) |
| julius | native Windows | power saver | local SSD | native | sequential | 929.1 (879.5-959.8) | [2026-08-27](measurements/2026-08-27-julius-dir-create-native-powersaver.md) |
| julius | WSL2, Debian 12 | best performance* | local SSD | native | sequential | 408.0 (395-412) | [2026-08-27](measurements/2026-08-27-julius-dir-create-wsl2.md) |
| julius | WSL2, Debian 12 | power saver | local SSD | native | sequential | 417.4 (412-423) | [2026-08-27](measurements/2026-08-27-julius-dir-create-wsl2-powersaver.md) |
| 3327 | WSL2, Ubuntu 24.04 | best performance (custom base scheme) | local SSD | native | sequential | 984.6 (818-1048) | [2026-08-28](measurements/2026-08-28-3327-dir-create-wsl2.md) |
| 3327 | native Windows | power saver (custom base scheme) | local NVMe SSD | native | sequential | 479.9 (430.7-528.1) | [2026-08-28](measurements/2026-08-28-3327-dir-create-native.md) |
| julius | native Windows | power saver | USB2 stick | native | sequential | 158.3 (153.7-163.3) | [2026-08-28](measurements/2026-08-28-julius-dir-create-usb.md) |
| julius | native Windows | power saver | local SSD | db-direct | sequential | 196.0 (108.5-428.0) | [2026-09-01](measurements/2026-09-01-julius-db-direct-mkdir-native.md) |

The `db-direct` row is this project's first measurement at any location other than `native` - see
that protocol's Notes for a real, monotonic scale-dependent slowdown across its 5 runs (not noise),
not yet root-caused.

## Zero-byte file creation

| Machine | Environment | Power mode | IO device | Location | Mode | Throughput (ops/s) | Protocol |
|---|---|---|---|---|---|---|---|
| julius | native Windows | best performance* | local SSD | native | sequential | 938.8 (890.3-977.3) | [2026-08-27](measurements/2026-08-27-julius-file0b-create-native.md) |
| julius | native Windows | power saver | local SSD | native | sequential | 1101.0 (1070.6-1122.5) | [2026-08-27](measurements/2026-08-27-julius-file0b-create-native-powersaver.md) |
| julius | WSL2, Debian 12 | best performance* | local SSD | native | sequential | 667.4 (655-672) | [2026-08-27](measurements/2026-08-27-julius-file0b-create-wsl2.md) |
| julius | WSL2, Debian 12 | power saver | local SSD | native | sequential | 560.8 (540-600) | [2026-08-27](measurements/2026-08-27-julius-file0b-create-wsl2-powersaver.md) |
| 3327 | WSL2, Ubuntu 24.04 | power saver (custom base scheme) | local SSD | native | sequential | 1018.6 (941-1054) | [2026-08-28](measurements/2026-08-28-3327-file0b-create-wsl2.md) |
| 3327 | native Windows | power saver (custom base scheme) | local NVMe SSD | native | sequential | 457.0 (421.0-528.2) | [2026-08-28](measurements/2026-08-28-3327-file0b-create-native.md) |

\* Developer-reported, not independently captured at the time - see those protocols' retroactive
addenda.

Both operations show the same environment pattern on `julius`: WSL2 runs at roughly half the
native-Windows throughput, for both directory and zero-byte-file creation, staying entirely on
WSL2's own filesystem (not `/mnt/c/...`) in both cases - see each protocol's Notes for what was and
was not investigated about that gap.

The `julius` power-mode comparison is inconsistent rather than a clean "Power Saver is slower"
story: both native-Windows measurements got *faster* under Power Saver (+13%, +17%), WSL2 directory
creation was essentially flat (+2%), and only WSL2 zero-byte-file creation slowed down as naively
expected (-16%). Three of four results point away from the intuitive direction - flagged as an open
question rather than resolved by this single round of measurements; see the `-powersaver`
protocols' Notes for the caveats (uncontrolled isolation, single 5-run samples, no repeated A/B run).

`3327` native Windows is the *reverse* of `julius` for directory creation: it runs at about half of
`3327`'s own WSL2 result (~480 vs ~985 ops/s), whereas on `julius` WSL2 is the slower side.
Confounded by power mode (the `3327` native runs are power-saver, its WSL2 run best-performance) and
by the corporate-managed OS image on `3327`. `3327` native is also well below `julius` native
(~460-520 vs ~820-1100 for dir/file0b create) despite the newer CPU - prime suspect the corporate
security / management stack (every create crosses a filesystem filter that AV hooks). Not isolated
or measured directly. See the `3327` protocols' Notes.

`julius`'s external USB2 stick is, unsurprisingly, the slowest IO device measured yet for directory
creation (~158 vs. ~929 ops/s on its own internal SSD, ~5.8x slower) - notable because directory
creation writes no file content, so this is purely USB2's per-command protocol latency, not its
raw throughput ceiling. See `2026-08-28-julius-dir-create-usb.md`.

`3327`'s zero-byte-file creation follows the same native-slower-than-WSL2 pattern as directory
creation on this machine (457.0 native vs. 1018.6 WSL2 ops/s, ~2.2x) - consistent rather than an
outlier, unlike `julius` where WSL2 is the slower side for both operations.

## Directory lookup

| Machine | Environment | Power mode | IO device | Location | Mode | Throughput (ops/s) | Protocol |
|---|---|---|---|---|---|---|---|
| julius | native Windows | power saver | local SSD | native | sequential | 2145.5 (2095.6-2186.7) | [2026-08-28](measurements/2026-08-28-julius-dir-lookup-native.md) |
| julius | WSL2, Debian 12 | power saver | local SSD | native | sequential | 50350.2 (42929-55676) | [2026-08-28](measurements/2026-08-28-julius-dir-lookup-wsl2.md) |
| 3327 | native Windows | power saver (custom base scheme) | local NVMe SSD | native | sequential | 1485.3 (1015.0-1734.5) | [2026-08-28](measurements/2026-08-28-3327-dir-lookup-native.md) |
| 3327 | WSL2, Ubuntu 24.04 | power saver (custom base scheme) | local SSD | native | sequential | 104351.4 (99535-107170) | [2026-08-28](measurements/2026-08-28-3327-dir-lookup-wsl2.md) |

## Directory listing

| Machine | Environment | Power mode | IO device | Location | Mode | Throughput (ops/s) | Protocol |
|---|---|---|---|---|---|---|---|
| julius | native Windows | power saver | local SSD | native | sequential | 0.51 (0.49-0.52) | [2026-08-28](measurements/2026-08-28-julius-dir-listing-native.md) |
| julius | WSL2, Debian 12 | power saver | local SSD | native | sequential | 33.8 (32-35) | [2026-08-28](measurements/2026-08-28-julius-dir-listing-wsl2.md) |
| 3327 | WSL2, Ubuntu 24.04 | power saver (custom base scheme) | local SSD | native | sequential | 57.2 (56-58) | [2026-08-28](measurements/2026-08-28-3327-dir-listing-wsl2.md) |

Both lookup and listing show a *much* larger native-Windows-vs-WSL2 gap (~25x and ~66x, WSL2 far
ahead this time) than directory/file creation did (~2x, native-Windows ahead) - plausibly a
`Test-Path`/`Get-ChildItem` cmdlet-overhead effect rather than a filesystem-level one; see both
`-wsl2` protocols' Notes for the reasoning and what would be needed to separate the two cleanly.
`3327` has no native-Windows listing measurement yet, so its own environment ratio cannot be
computed, but its WSL2 result (57.2 ops/s) is only ~1.7x `julius`'s WSL2 result - a much smaller
machine-to-machine ratio than lookup's ~2.1x, consistent with per-entry in-process work (not
per-call syscall overhead) dominating this operation, where a faster CPU helps proportionally
less.
`3327` native Windows lookup (~1485 ops/s) is again below `julius` native (~2145), the same
ordering as every create workload on that machine. `3327`'s own WSL2 lookup (104,351.4 ops/s) is
the fastest number in this entire file - ~70x its own native-Windows result and ~2.1x `julius`'s
WSL2 result, the largest environment gap seen for any operation on either machine, consistent with
lookup being almost entirely per-call-overhead-bound (a single cheap `test -d` syscall) rather
than filesystem-bound.

## File creation (100 B / 30 KB / 10 MB)

| Machine | Environment | Power mode | Size | IO device | Location | Mode | Throughput (ops/s) | Protocol |
|---|---|---|---|---|---|---|---|---|
| julius | native Windows | power saver | 100 B | local SSD | native | sequential | 242.6 (225.3-275.5) | [2026-08-28](measurements/2026-08-28-julius-file100b-create-native.md) |
| julius | WSL2, Debian 12 | power saver | 100 B | local SSD | native | sequential | 107.0 (103-109) | [2026-08-28](measurements/2026-08-28-julius-file100b-create-wsl2.md) |
| julius | native Windows | power saver | 100 B | USB2 stick | native | sequential | 100.9 (89.3-108.2) | [2026-08-28](measurements/2026-08-28-julius-file100b-create-usb.md) |
| julius | native Windows | power saver | 30 KB | local SSD | native | sequential | 209.5 (202.2-236.3) | [2026-08-28](measurements/2026-08-28-julius-file30kb-create-native.md) |
| julius | WSL2, Debian 12 | power saver | 30 KB | local SSD | native | sequential | 93.6 (91-96) | [2026-08-28](measurements/2026-08-28-julius-file30kb-create-wsl2.md) |
| julius | native Windows | power saver | 10 MB | local SSD | native | sequential | 26.4 (21.2-31.3) | [2026-08-28](measurements/2026-08-28-julius-file10mb-create-native.md) |
| julius | WSL2, Debian 12 | power saver | 10 MB | local SSD | native | sequential | 19.6 (18-21) | [2026-08-28](measurements/2026-08-28-julius-file10mb-create-wsl2.md) |
| julius | native Windows | power saver | 10 MB | USB2 stick | native | sequential | 1.05 (0.95-1.12) | [2026-08-28](measurements/2026-08-28-julius-file10mb-create-usb.md) |
| 3327 | native Windows | power saver (custom base scheme) | 100 B | local NVMe SSD | native | sequential | 476.5 (453.4-515.2) | [2026-08-28](measurements/2026-08-28-3327-file100b-create-native.md) |
| 3327 | WSL2, Ubuntu 24.04 | power saver (custom base scheme) | 100 B | local SSD | native | sequential | 954.0 (933-979) | [2026-08-28](measurements/2026-08-28-3327-file100b-create-wsl2.md) |
| 3327 | native Windows | power saver (custom base scheme) | 30 KB | local NVMe SSD | native | sequential | 519.5 (490.1-577.8) | [2026-08-28](measurements/2026-08-28-3327-file30kb-create-native.md) |
| 3327 | WSL2, Ubuntu 24.04 | power saver (custom base scheme) | 30 KB | local SSD | native | sequential | 878.0 (784-936) | [2026-08-28](measurements/2026-08-28-3327-file30kb-create-wsl2.md) |
| 3327 | native Windows | power saver (custom base scheme) | 10 MB | local NVMe SSD | native | sequential | 97.3 / ~970 MB/s (86.0-126.4) | [2026-08-28](measurements/2026-08-28-3327-file10mb-create-native.md) |
| 3327 | WSL2, Ubuntu 24.04 | power saver (custom base scheme) | 10 MB | local SSD | native | sequential | 52.0 / ~520 MB/s (47-54) | [2026-08-28](measurements/2026-08-28-3327-file10mb-create-wsl2.md) |

`julius`'s 10 MB run shows a real downward trend across its 5 runs on both environments (milder on
WSL2, ~14% vs. ~32% peak-to-last) as data accumulates on the shared underlying SSD (~26 GB native,
~20 GB WSL2) - not seen at 100 B/30 KB; see each protocol's Notes. On `julius`, native Windows is
ahead of WSL2 at every size (WSL2 at 44-45% of native for 100 B/30 KB, closing to 74% at 10 MB,
plausibly because the native number itself is already degraded by the trend above there) - content
generation differs between the two sides too (`perf-gen` binary per file on WSL2 vs. in-process
template-and-poke on native Windows), a confound these comparisons do not control for.

`3327`'s native-Windows 10 MB run used a reduced 5-s window (to bound the on-disk footprint to
~24 GB); its WSL2 10 MB run used the standard 20 s window instead (745 GB free comfortably absorbs
the ~52.5 GB written). At both sizes below 10 MB `3327` native is flat across sizes (~450-520 ops/s
at 0 B / 100 B / 30 KB), so the per-file call + create cost dominates the content write until
somewhere between 30 KB and 10 MB; `3327` WSL2 is similarly flat (~880-1020 ops/s at 0 B / 100 B /
30 KB) before dropping to ~52 ops/s at 10 MB, the same shape. `3327` native runs faster than
`julius` native Windows here (~477 vs ~243 at 100 B) - the opposite ordering from the
directory/zero-byte creations above, where `3327` was slower; unexplained, and both are single
non-isolated samples.

`3327` WSL2 is consistently ~9x `julius` WSL2 at 100 B/30 KB (954.0 vs. 107.0; 878.0 vs. 93.6) -
a far larger machine-to-machine ratio than either machine's own native-Windows numbers show
against each other (~2x for directory/zero-byte creation), narrowing to ~2.7x at 10 MB (52.0 vs.
19.6) once the workload turns write-volume-bound rather than per-call-bound - the same
overhead-vs-volume-bound distinction the lookup/listing section above draws.

`julius`'s USB2 stick narrows dramatically at 100 B (100.9 ops/s, only ~2.4x behind its own
internal SSD's 242.6, versus ~5.8x behind at directory creation) but falls off a cliff at 10 MB
(1.05 ops/s, ~25x behind the SSD's 26.4) - USB2's raw throughput ceiling (~8.7-10.5 MB/s measured)
dominates once meaningful content has to move, where it barely mattered for a 100 B payload. The
stick's tiny ~4 GB capacity required a free-space check and a throughput probe before running the
10 MB size at all - see that protocol's Notes for the capacity math.

## File read (100 B / 30 KB / 10 MB)

| Machine | Environment | Power mode | Size | IO device | Location | Mode | Throughput (ops/s) | Protocol |
|---|---|---|---|---|---|---|---|---|
| julius | native Windows | power saver | 100 B | local SSD | native | sequential | 3964.4 (3728.5-4226.5) | [2026-08-28](measurements/2026-08-28-julius-file100b-read-native.md) |
| julius | WSL2, Debian 12 | power saver | 100 B | local SSD | native | sequential | 603.4 (443-655) | [2026-08-28](measurements/2026-08-28-julius-file100b-read-wsl2.md) |
| julius | native Windows | power saver | 30 KB | local SSD | native | sequential | 3760.9 (3645.7-3811.1) | [2026-08-28](measurements/2026-08-28-julius-file30kb-read-native.md) |
| julius | WSL2, Debian 12 | power saver | 30 KB | local SSD | native | sequential | 531.4 (502-547) | [2026-08-28](measurements/2026-08-28-julius-file30kb-read-wsl2.md) |
| julius | native Windows | power saver | 10 MB | local SSD | native | sequential | 26.3 (25.2-26.7)\*\* | [2026-08-28](measurements/2026-08-28-julius-file10mb-read-native.md) |
| julius | WSL2, Debian 12 | power saver | 10 MB | local SSD | native | sequential | 35.0 (30-37)\*\* | [2026-08-28](measurements/2026-08-28-julius-file10mb-read-wsl2.md) |
| julius | native Windows | (not captured) | 10 MB | local SSD | native | sequential | 20.4 (17.8-21.3) | [2026-09-01](measurements/2026-09-01-julius-file10mb-read-native.md) |
| julius | WSL2, Debian 12 | (not captured) | 10 MB | local SSD | native | sequential | 17.7 (5.85-29.45) | [2026-09-01](measurements/2026-09-01-julius-file10mb-read-wsl2.md) |
| 3327 | native Windows | power saver (custom base scheme) | 100 B | local NVMe SSD | native | sequential | 1562.0 (1144.4-2076.0) | [2026-08-28](measurements/2026-08-28-3327-file100b-read-native.md) |
| 3327 | WSL2, Ubuntu 24.04 | power saver (custom base scheme) | 100 B | local SSD | native | sequential | 958.4 (901-1019) | [2026-08-28](measurements/2026-08-28-3327-file100b-read-wsl2.md) |
| 3327 | native Windows | power saver (custom base scheme) | 30 KB | local NVMe SSD | native | sequential | 1785.4 (1188.3-2319.3) | [2026-08-28](measurements/2026-08-28-3327-file30kb-read-native.md) |
| 3327 | WSL2, Ubuntu 24.04 | power saver (custom base scheme) | 30 KB | local SSD | native | sequential | 1002.4 (981-1029) | [2026-08-28](measurements/2026-08-28-3327-file30kb-read-wsl2.md) |
| 3327 | native Windows | power saver (custom base scheme) | 10 MB | local NVMe SSD | native | sequential | 62.4 / ~620 MB/s (49.4-70.2) | [2026-08-28](measurements/2026-08-28-3327-file10mb-read-native.md) |
| 3327 | WSL2, Ubuntu 24.04 | power saver (custom base scheme) | 10 MB | local SSD | native | sequential | 92.8 / ~928 MB/s (64-118)\*\* | [2026-08-28](measurements/2026-08-28-3327-file10mb-read-wsl2.md) |

\*\* Confounded by a read-script indexing artifact (now fixed - see the `2026-09-01` rows above for
`julius`'s corrected re-runs, and the `3327` WSL2 paragraph below for the still-outstanding `3327`
case).

On `julius`, reads at 100 B/30 KB run 10-16x faster than the matching creates on native Windows
(page-cache-warm working sets, both well under this machine's 8 GB RAM); at 10 MB the working set
exceeds RAM on both environments. Native Windows is far ahead of WSL2 at 100 B/30 KB (~15% ratio, a
much bigger native-ahead gap than the create side's 44-45%). The original 10 MB round (`2026-08-28`)
showed WSL2 *winning* there (26.3 vs. 35.0 ops/s), the only read comparison that reversed the
pattern - but that round used the read scripts before the indexing-artifact fix, so both numbers
were confounded by page-cache effects the fix specifically targets. The corrected `2026-09-01`
re-run reverses the reversal: native Windows is ahead again at 10 MB too (20.4 vs. 17.7 ops/s),
matching every other size rather than standing out from it - the original "WSL2 wins at 10 MB"
finding does not survive the fix and should not be relied on.

On `3327` native Windows, reads at 100 B/30 KB likewise run several times faster than the matching
creates (page-cache-warm working sets: ~5 MB and ~1.5 GB, well under RAM), and the 100 B and 30 KB
reads land within noise of each other despite ~300x the bytes - the per-call + loop overhead
dominates. At 10 MB `3327`'s working set (~24 GB) exceeds its ~15 GB cache and the read (~62 ops/s,
~620 MB/s) comes out *slower* than the create (~97 ops/s, ~970 MB/s, whose `WriteAllBytes` returns
into the write-back cache) - unlike `julius`, where read and create converge at 10 MB instead.
`3327`'s small-file reads carry a wide upward-drifting spread (cache warming on a non-isolated
machine) - worth a quieter repeat. See each protocol's Notes.

On `3327` WSL2, reads run only slightly ahead of creates at 100 B/30 KB (~0.5%, ~14%) and more
clearly ahead at 10 MB (~78%, but see the caveat below) - a much flatter read/create relationship
than either `julius` or `3327` native Windows show, consistent with `3327` WSL2's create side
already being per-call-overhead-bound rather than IO-bound at these sizes (see the file-creation
section above), so cache-warm reads gain relatively little over it. \*\* The 10 MB WSL2 read
number is confounded by a read-script indexing artifact (`file10mb-read.sh` used to re-read the
same low-numbered files from run 1 every run, so later runs found more of that fixed range already
page-cache-warm from the previous run - see that protocol's Notes) - the fix (pseudo-random
indexing) has since been made and confirmed on `julius` (see the `2026-09-01` rows in the table
above), but `3327` itself has not yet been re-run with it; treat the 92.8 ops/s figure and its wide
range (64-118) as provisional until it is.
