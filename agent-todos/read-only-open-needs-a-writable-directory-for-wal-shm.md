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

## What the next attempt should do

1. Confirm whether a read-only session is actually expected to run concurrently with an active
   `--read-write` session against the same repository today (check `REQ-MAINTENANCE-004` in
   `requirements/functional/maintenance.md` and any existing test coverage) - this settles whether
   `immutable=1` is viable at all.
2. If concurrent read-only + read-write is expected: consider whether `create_repo`/an initial
   write-mode open leaving a `-shm` file behind permanently (rather than letting SQLite clean it up
   on close) is a viable alternative that keeps read-only opens from ever needing to create one
   themselves.
3. If concurrent access is not actually expected/supported, `immutable=1` is likely the direct fix
   - add a regression test against a pristine repository on a directory the test process cannot
   write to (verified red against the current code, green after the fix, per `AGENTS.md`'s
   debugging discipline).
4. Either way, `docker/samba-mount/README.md`'s current workaround (not bind-mounting `/repo`
   read-only) can be revisited once this is resolved - not blocking on it, since the workaround
   is fully safe as-is (`dfs mount`'s own read-only enforcement, both at the FUSE/kernel level via
   libfuse's `-oro` and at the `Repository` level via `Error::ReadOnlyRepository`, does not depend
   on the bind mount's own read/write mode at all).
