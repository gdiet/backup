# file*-read.{ps1,sh} restart their read index at file 1 every run

**Why parked**: found while running real 5x20s measurements, not while doing the fix - fixing it
would need re-running the affected measurement to get a clean number, more than fits inside the
current time-boxed measurement session.
**Size**: medium - confirm scope before starting (touches all six `file*-read.{ps1,sh}` scripts and
at least one measurement protocol needs redoing).
**Opened**: 2026-08-28, by WSL2/Linux session on `3327`.
**Context**: `performance/measurements/2026-08-28-3327-file10mb-read-wsl2.md`'s Notes - the
measurement that surfaced this.

Every `file*-read.{ps1,sh}` script resets its own `ops`/`$ops` counter to 0 at the start of each of
the 5 runs, then indexes as `idx = (ops % total) + 1` (`.sh`) / `$idx = ($ops % $total) + 1`
(`.ps1`). Because `ops` restarts at 0 every run, every run re-reads starting from file 1, not
continuing where the previous run left off - `dir-lookup.{ps1,sh}}` avoids this by picking a
pseudo-random index instead, but the file-read scripts do not.

This is invisible when the whole tree fits in RAM (true for the 100 B/30 KB rungs on every machine
measured so far), but at 10 MB it produces a real, misleading effect: since each run touches only
the low-index files (however many fit in that run's 20 s), and every run touches the *same*
low-index range, later runs find progressively more of that range still page-cache-warm from the
previous run's pass over it - producing a monotonic-looking upward throughput trend that is a
script artifact, not a filesystem effect. Confirmed on `2026-08-28-3327-file10mb-read-wsl2.md`
(64 -> 74 -> 100 -> 108 -> 118 ops/s across the 5 runs, ~52.5 GB tree vs. 32 GB RAM on that
machine).

Fix options (pick one before starting - this needs a decision, not just typing):

1. Carry a persisted read-position counter across runs/invocations (mirroring the create scripts'
   own counter-file pattern), so each run continues from where the last one left off instead of
   restarting at file 1.
2. Pick a pseudo-random index per read, the same way `dir-lookup.{ps1,sh}` already does, rather
   than a sequential/wrapping one.

Once fixed, re-run `file10mb-read.sh`/`.ps1` (the only rung where this has been shown to matter)
to get a measurement not confounded by this artifact; the 100 B/30 KB read measurements already
taken do not need redoing (the effect could not have shown up there - the whole tree fits in RAM
either way).
