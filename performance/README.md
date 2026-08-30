# Performance Measurements

This directory holds empirical performance measurements for DedupFS - raw per-run measurement
protocols and aggregated overviews derived from them. It exists to give the non-functional
performance requirements in
[`../requirements/non-functional/performance.md`](../requirements/non-functional/performance.md)
something concrete to check against, and to build up comparable numbers across machines,
environments, and storage devices over time instead of losing one-off measurements to chat history
or a developer's own private notes.

## Layout

- [`methodology.md`](methodology.md) - how a measurement is taken: what dimensions get recorded,
  the statistical approach, isolation from interference, and the catalog of workloads and
  locations this project measures.
- [`machines.md`](machines.md) - alias-to-hardware mapping for the machine names measurements
  reference, so "julius" or "3327" stays interpretable without asking the developer each time.
- [`scripts/`](scripts/README.md) - runnable implementations of specific workload-catalog entries,
  producing the 5-runs-of-~20-seconds output the statistical approach asks for; grows as more
  workloads get a ready-to-run script (or, for `location: db-direct`, a Rust benchmark under
  `../crates/db/examples/`) instead of a from-scratch procedure each time.
- `measurements/` - one file per individual measurement run (a "measurement protocol"), named
  `<date>-<machine>-<short-slug>.md`, plus a same-named `.yaml` sidecar carrying the same fields in
  machine-readable form (see `methodology.md`'s "Machine-readable sidecar"). Bounded in size - a
  summary of a run, not a line per operation. Touched again only when the same measurement is
  repeated because a methodological flaw is suspected in the original - append a note explaining
  why, do not silently delete or overwrite the original numbers.
- [`overview.md`](overview.md) (or further files, split by whatever axis helps once one file gets
  unwieldy) - tables built from the individual measurement protocols, organized for spotting
  trends across machines, environments, storage devices, and DedupFS access paths at a glance.
  Update this whenever a measurement protocol is added or changed; the individual protocol files
  remain the source of truth, this is a derived view.

## Adding a measurement

1. Read [`methodology.md`](methodology.md) and use its recording template and statistical approach
   (5 runs, each about 20 seconds unless that is too short for the operation being measured;
   discard run 1 only if it crosses the fixed warmup threshold; report the mean and range, not a
   confidence interval) so different measurements stay comparable to each other.
2. Add the resulting `.md` file and its `.yaml` sidecar to `measurements/`.
3. Add or update a row in the relevant table in `overview.md`, linking back to the new file.

## Running a self-directed measurement session

Given only a time budget (e.g. "run whatever measurements are missing, you have 5 minutes"), work
through this without needing anything beyond what is in this directory:

1. **Identify this machine.** Check the hostname (`hostname` / `$env:COMPUTERNAME`) against the
   roster in [`machines.md`](machines.md). If it matches a known alias, use that. If it does not,
   this is plausibly a machine worth adding - follow `machines.md`'s "Adding a machine", filling in
   `TBD` for anything not cheaply knowable rather than spending the time budget on it.
2. **Find coverage gaps.** Cross-reference [`overview.md`](overview.md)'s per-operation tables
   against `methodology.md`'s workload catalog, the location catalog, and `machines.md`'s roster -
   a (workload, location, machine) combination with no row anywhere is a bigger gap than one
   already measured on the other machine; see `machines.md`'s "Coverage goal" for why both
   machines matter over time.
3. **Prefer what already has runnable tooling - for *this* environment.** Check
   [`scripts/README.md`](scripts/README.md) first - native filesystem workloads with a `.ps1`/`.sh`
   pair, and `location: db-direct` via
   [`../crates/db/examples/db_bench.rs`](../crates/db/examples/db_bench.rs) (directory creation
   only so far), can be run immediately regardless of environment. `location: dfs-mount` currently
   has a script for one real-mount backend only (`dfs-mount-dir-create.ps1`, WinFSP) - check
   `scripts/README.md` for which backends still have none. A missing backend's script is not
   ordinary "new tooling to build eventually": if this environment has the real mount access
   (`/dev/fuse`, WinFSP) that missing backend needs and another environment might not, this is the
   rare chance to close that gap, and worth doing *before* falling back to repeating an
   already-fully-tooled combination elsewhere - build it analogously to the existing script (same
   5-runs/state-between-runs/`Tool`-naming rules), note plainly that it is unvalidated on a first
   real run the way `dfs-mount-dir-create.ps1`'s own header does, and commit it even if the time
   budget does not stretch to actually running it yet. A workload/location combination needing
   genuinely new capability nobody has any tool for yet (an untooled entry in "Further workloads",
   `dfs-cli`, or anything needing file creation rather than directories) is the one case actually
   worth deferring under a tight budget.
4. **Pick what fits the budget, then stop.** One full 5-runs-of-~20-seconds measurement takes
   roughly two minutes hands-off, plus the time to capture Setup fields and write up the protocol -
   budget for a small, complete number of measurements rather than starting more than will fit.
   Finding nothing that fits is a legitimate outcome; report that rather than forcing a rushed or
   partial one.
5. **Run and record it** per `methodology.md`'s recording template, statistical approach (the
   fixed 50%-slower-than-median discard rule, "keep going" state between runs, the `Tool`/`Window`
   fields), and "Machine-readable sidecar" for the matching `.yaml`.
6. **Update `overview.md`** with a row for the new measurement, linking back to its protocol file.
7. **Commit and push** - see the `attributed-commits` skill for how to attribute the commit.

## Not yet measured

The above is how to find what is missing at any point in time - [`overview.md`](overview.md)'s
tables are the current source of truth for what has and has not been measured yet, not a list kept
here.
