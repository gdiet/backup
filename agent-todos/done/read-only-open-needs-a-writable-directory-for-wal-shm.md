# `open_repository_read_only` fails against a pristine repository on a genuinely read-only/unwritable directory

**Why parked**: out-of-scope finding - came up while porting `docker/samba-mount/` (a `dfs mount`
+ Samba dev utility) from the retired `rust-1st-attempt` branch, well outside that task's own
scope (a real `db`/SQLite behavior, not anything Docker- or Samba-specific).
**Size**: medium/large (confirm with the user first) - touches `crates/db/src/lib.rs`'s
`open_repository_read_only`/`crates/db/src/connection.rs`, and the right fix depends on a design
question (see below), not just a one-line change.
**Opened**: 2026-09-23, by Linux/WSL2 session (real `/dev/fuse` and Docker access available here).
**Context**: `crates/db/src/lib.rs::open_repository_read_only`'s own doc comment already states the
goal this finding contradicts: "meant... specifically for [a caller] that still needs to work even
when the filesystem cannot reliably support a full write-mode connection open at all."
`docker/samba-mount/README.md`'s "Build and run" section works around this for that one utility
(dropping the `-v ...:/repo:ro` bind-mount flag it would otherwise use) - this todo is about the
underlying `db` behavior, not that workaround.

## The finding

A genuinely pristine repository (freshly created via `dfs create-repo`, no write-mode connection
ever opened against it since, so no `-shm`/`-wal` files exist alongside `meta/repository.sqlite3`)
fails to open via `open_repository_read_only` when the containing directory is not writable -
reproduced directly via a read-only (`:ro`) Docker bind mount:

```
error: rusqlite_migration error while executing query 'PRAGMA user_version;': unable to open database file
```

Root cause: the database is in `journal_mode = WAL` (set once, permanently, by the write-mode
connection that created it - `crates/db/src/connection.rs`). Opening a WAL-mode SQLite database at
all, even via a connection opened `SQLITE_OPEN_READ_ONLY`, requires creating a `-shm` (shared
memory index) file if one does not already exist - and creating a file needs a writable directory,
regardless of the connection's own read-only flag. Confirmed directly: the identical repository
opens read-only without error once `-shm`/`-wal` files already exist alongside it (left behind by
an earlier write-mode open, even one that exited abnormally) - only a truly pristine repository
(the state immediately after `create-repo`, or after any fully clean write-mode close that removes
them) hits this.

This is not specific to Docker or a `:ro` bind mount - it reproduces identically on any directory
the current process lacks write permission to (a real read-only filesystem, restrictive
permissions, or - per the doc comment quoted above - the exact "filesystem cannot reliably support
a full write-mode connection open" scenario `open_repository_read_only` was specifically built to
still work against, e.g. the WSL<->Windows 9p bridge case in `README.md`'s "Known Limitations").

## Why this needs a design decision, not just a quick fix

The likely fix is opening the read-only connection via a SQLite URI with `immutable=1`
(`file:<path>?mode=ro&immutable=1`) instead of a plain path with `SQLITE_OPEN_READ_ONLY` - this
tells SQLite the file will never change for the lifetime of the connection, letting it skip the
WAL/`-shm` machinery entirely and read the last-checkpointed snapshot directly.

That is only correct if nothing else can be writing to the same repository while this read-only
connection is open. Whether that is actually guaranteed here is not yet established:
`crates/cli/src/mount.rs`'s write lock (REQ-MAINTENANCE-004) is only acquired for a `--read-write`
mount, which suggests a read-only session (`dfs list`, `dfs restore`, a read-only mount) is
*expected* to be usable concurrently with an active `--read-write` session elsewhere - in which
case `immutable=1` would be actively wrong (a concurrent writer's changes would not be a
"never changes" file at all, and reads against it under that flag are documented by SQLite as
producing undefined/stale results). Settling this - whether read-only opens must tolerate a
concurrent writer, and if so, whether `-shm` creation can be avoided some other way (e.g.
pre-creating `-shm` alongside the database file at `create_repo` time, so it always exists by the
time a read-only-only environment ever opens it) - needs its own look before picking a fix, which
is why this is parked rather than fixed inline.

## `immutable=1` verified empirically (2026-09-23), including the concurrent-writer risk

Prompted by the developer asking whether this holds on Windows too, and whether `immutable=1`
genuinely opens a pristine, cleanly-closed repository on read-only media, plus what happens if a
writer shows up anyway - answered here empirically (Linux only; Windows reasoned about, not run,
see below) before picking a fix, per `AGENTS.md`'s "verify uncertain library/runtime behavior
empirically" debugging discipline.

- **`immutable=1` does open a pristine, cleanly-closed repository on genuinely read-only media.**
  Reproduced directly with Python's `sqlite3` module (independent of this project's own `db` crate
  code, to isolate the SQLite behavior itself) against a fresh `dfs create-repo` repository with
  its `meta/` directory `chmod 555`ed: `file:<path>?mode=ro` alone fails the same way the Rust code
  does (`attempt to write a readonly database`); `file:<path>?mode=ro&immutable=1` succeeds and
  reads the schema/data correctly.
- **Platform scope: this is not Linux-specific.** The `-shm`-creation requirement for opening a
  WAL-mode database is part of SQLite's own generic WAL implementation, used identically across its
  Unix and Windows (`win32`) VFS backends - not something either backend layers on top themselves.
  `immutable`'s own documentation (see below) is written platform-agnostically, with no OS-specific
  carve-out. Not run on real Windows/WinFSP in this session (no such access here) - reasoned from
  documented SQLite internals, not independently confirmed on that platform.
- **The concurrent-writer risk is real and documented, not hypothetical.** SQLite's own docs
  (https://www.sqlite.org/uri.html, "immutable"): "If this query parameter... asserts that a
  database file is immutable and that file changes anyhow, then SQLite might return incorrect
  query results and/or SQLITE_CORRUPT errors." That is the actual contract `immutable=1` would ask
  this project to uphold - not merely "you might see a stale snapshot."
  - Empirically, on this SQLite build, actually violating it was much more benign than the docs'
    worst case: a `immutable=1` connection opened before a concurrent write (a real `dfs ingest` in
    a separate process), then queried again after, kept silently returning the pre-write snapshot -
    no exception, no corruption. Repeated with heavier concurrent load (8 rounds of `dfs ingest`
    plus `dfs db-compact` while the same connection stayed open across ~10s) - still just a frozen,
    stale snapshot, no error surfaced. A **fresh** `immutable=1` connection opened after the write
    saw the update correctly.
  - This is not evidence the risk is overstated: the benign outcome only means none of these test
    queries happened to need a disk page SQLite's own page cache had not already cached before the
    write landed. `SQLITE_CORRUPT`/wrong-result is the documented failure mode for the case that
    does need to fetch a page the file no longer matches what `immutable=1` told SQLite to assume -
    not reproduced here, but not ruled out by a handful of manual test runs either.

**Net effect on the design question above**: `immutable=1` is a real, working fix for the
*pristine-repository-on-unwritable-media* case specifically (no other writer possible there, by
definition of "unwritable media"), matching this todo's own opening finding and
`docker/samba-mount/`'s use case. It is not a safe general replacement for
`open_repository_read_only` if a concurrent `--read-write` session is genuinely expected while a
read-only session is open elsewhere - that still needs the "next attempt" investigation below to
settle before `immutable=1` could be adopted unconditionally.

## What the next attempt should do

1. Confirm whether a read-only session is actually expected to run concurrently with an active
   `--read-write` session against the same repository today (check `REQ-MAINTENANCE-004` in
   `requirements/functional/maintenance.md` and any existing test coverage) - this settles whether
   `immutable=1` is viable unconditionally, or only for the specific
   known-unwritable-media case (where a concurrent writer is structurally impossible regardless).
2. If concurrent read-only + read-write is expected in the general case: consider whether
   `create_repo`/an initial write-mode open leaving a `-shm` file behind permanently (rather than
   letting SQLite clean it up on close) is a viable alternative that keeps read-only opens from
   ever needing to create one themselves - or whether `open_repository_read_only` should only use
   `immutable=1` when it can positively detect the directory is not writable (falling back to
   today's plain `mode=ro` otherwise), rather than switching behavior unconditionally.
3. Either way `immutable=1` ends up scoped, add a regression test against a pristine repository on
   a directory the test process cannot write to (verified red against the current code, green after
   the fix, per `AGENTS.md`'s debugging discipline) - and, if feasible, a second test that opens
   `immutable=1` alongside a concurrent writer to document the actual (not just assumed) behavior
   this project ends up relying on.
4. Either way, `docker/samba-mount/README.md`'s current workaround (not bind-mounting `/repo`
   read-only) can be revisited once this is resolved - not blocking on it, since the workaround
   is fully safe as-is (`dfs mount`'s own read-only enforcement, both at the FUSE/kernel level via
   libfuse's `-oro` and at the `Repository` level via `Error::ReadOnlyRepository`, does not depend
   on the bind mount's own read/write mode at all).

## Done

Resolved via option 2 above, in the shape the developer specifically proposed and the empirical
findings above supported: an explicit, per-invocation opt-in (`--assume-read-only-medium`) rather
than an unconditional switch or an auto-detection heuristic - `open_repository_read_only` itself is
completely unchanged and stays the default everywhere, so nothing about today's concurrent
read-only/read-write behavior changes for a caller that does not pass the new flag.

**`crates/db/src/lib.rs`**: added `open_repository_read_only_immutable`, opening via SQLite's
`immutable=1` URI parameter instead of a plain `SQLITE_OPEN_READ_ONLY` flag. `open_repository_read_
only`/`open_repository_read_only_immutable` now share a `finish_read_only_open` tail (migration
check, settings read, `Repository` construction) - the two functions only differ in how the
connection itself is opened. `to_file_uri` builds the required `file:` URI following SQLite's own
documented six-step canonical encoding rules, not a hand-rolled scheme. DESIGN-METADATA-013 in
`docs/design/metadata-storage.md` records the decision, including why an unconditional switch (or
flipping read tools to a write-mode-by-default posture, which the developer and I separately
converged on rejecting - it would reintroduce the unreliable-write-mode-open problem
`open_repository_read_only` exists to avoid, for the common case, to fix a rare one) were both
rejected in favor of this explicit, narrowly-scoped opt-in.

**`crates/cli`**: a new `ReadOnlyMediumArgs` (`--assume-read-only-medium`, `main.rs`, flattened
like the existing `ChunkingArgs`/`RamBudgetArgs`/`BackpressureArgs`) is wired into every command
that opens read-only - `list`/`find`/`stats`/`restore`/`db-backup`, and `mount` without
`--read-write` (meaningless, and documented as such, with `--read-write` - a read-write mount
already holds the write lock, ruling out a concurrent writer regardless). `mount::try_run`/`run`
picked up a `RepoOpenOptions` struct (bundling `cache_size` and the new flag) and `restore::
RestoreOptions` was made `pub(crate)` and constructed directly by `main.rs`, both purely to keep
`try_run`/`run`'s own parameter counts under clippy's `too_many_arguments` threshold once the new
flag pushed them to 8.

**Tests, red/green-verified per `AGENTS.md`'s debugging discipline**: five new tests in
`crates/db/src/lib.rs` (parity with `open_repository_read_only`'s own existing suite, plus the
actual regression test - a pristine repository over a `chmod 555`'d directory: the plain open
fails, the immutable one succeeds and reads correctly), and one CLI-level wiring test in
`crates/cli/src/list.rs` confirming `--assume-read-only-medium` actually reaches the new `db`
function through one representative command (the other five thread it through identically). Also
manually verified end-to-end against the real, compiled `dfs` binary: `dfs list --repo <pristine
repo, meta/ chmod 555>` fails without the flag, succeeds with it.

Full verification suite green (build/fmt/clippy -D warnings/test --workspace/doc).
`docker/samba-mount/README.md` updated too (small enough to just do alongside this, per
`AGENTS.md`): its "Build and run"/problem-5 write-up now documents `--assume-read-only-medium` as
the way to actually use a `:ro` bind mount there, rather than only documenting the workaround of
not using `:ro` at all.
