# Two error-path rough edges in open_repository / create-repo

**Why parked**: found during the 2026-09-01 cross-boundary investigation (see
`agent-todos/network-fs-sqlite-reliability-docs.md` for the full context); developer wanted TODOs,
not fixes on the spot.
**Size**: small (two independent, contained fixes; could be one commit or two)
**Opened**: 2026-09-01, native-Windows session on `3327`
**Context**: `crates/db/src/lib.rs` `open_repository` (~line 446) and `init_repository` (~line 420,
builds `meta.tmp/` then `fs::rename` to `meta/`); `crates/db/src/connection.rs`
`configure_write_connection`; `crates/db/src/lib.rs` `Error` `Display` (~line 110-175, note how
`LockUnavailable` / `LockFileInaccessible` / `AlreadyLocked` already carry network-storage-aware,
actionable text - the model to follow); `crates/cli/src/create_repo.rs` (its `remove_dir_all`
calls are `#[cfg(test)]` teardown, NOT a production cleanup path).

## 1. `open_repository` gives a bare SQLite error on a network/bridged FS

When `configure_write_connection` fails because the filesystem cannot support WAL's locking
(observed over 9p bridges: `PRAGMA journal_mode=WAL` hard-fails with SQLITE_BUSY -> "database is
locked", or SQLITE_IOERR -> "disk I/O error"), that propagates as `Error::Sqlite(err)` whose
`Display` is just `write!(f, "{err}")`. The user sees `error: database is locked` /
`error: disk I/O error` with no hint - and (like the developer, like DBeaver in the originating
report) goes hunting for a phantom process holding the DB.

Fix: in `open_repository` (or `configure_write_connection`), when the underlying `rusqlite::Error`
is a BUSY / IOERR / lock-category error, wrap it in a variant with a hint like the lock errors
already have, e.g. "... this can happen on a network-mounted or WSL<->Windows-bridged filesystem
where SQLite's WAL locking is not reliably supported - run `dfs` from the machine where the
repository physically resides. See README.md's Known Limitations." Keep the raw error text in the
message too. (Distinct from `Error::WalUnavailable`, which is only for the silent-fallback case.)

## 2. Failed `create-repo` leaves `data/` + `meta.tmp/` behind

Observed: a `create-repo` that fails during DB init (e.g. the WAL open failing over a 9p bridge)
leaves `data/` and `meta.tmp/repository.sqlite3` in the target directory. `init_repository` builds
in `meta.tmp/` and only `fs::rename`s it to `meta/` on success; on the error path nothing removes
`meta.tmp/` or `data/`. A retry then fails with `error: <path> already exists and is not empty`
(fair on its own, but gives no hint that this is leftover from a failed create).

Fix: on the `init_repository` error path, remove the partial `data/` and `meta.tmp/` it created
(only what this call created - do not blow away a pre-existing non-empty target). Optionally, make
the "already exists and is not empty" message recognise a `meta.tmp/`-without-`meta/` directory as
"a previous `create-repo` here did not finish - remove `data/` and `meta.tmp/` and retry".

## Done

**Completed**: 2026-09-01, by Claude Code on the web session (branch `mount-read-write`), during an
unattended sweep of open `agent-todos`/`developer-todos`.

**Part 1**: Added `Error::ConnectionUnreliable(rusqlite::Error)` (`crates/db/src/lib.rs`) with an
actionable Display message matching the style of `LockUnavailable`/`AlreadyLocked`. Added
`connection::wrap_unreliable_connection_error` (`crates/db/src/connection.rs`), which classifies a
`rusqlite::Error` by its `ErrorCode` - `DatabaseBusy`/`DatabaseLocked`/`SystemIoFailure` map to the
new variant, everything else still falls through to the existing `Error::Sqlite`. Every `PRAGMA`
call in `configure_write_connection` now goes through it. Verified with 5 unit tests constructing
`rusqlite::Error::SqliteFailure` values directly with each relevant `ErrorCode` (a real two-process
lock-contention probe was tried first per `AGENTS.md`'s empirical-verification discipline, but did
not reproduce `SQLITE_BUSY` on this container's local filesystem - consistent with the investigation
that found this specific 9p/v9fs-only) - confirmed red/green by temporarily dropping `DatabaseBusy`
from the match and watching its test fail.

**Part 2**: `init_repository` now wraps its actual creation work
(factored into a new private `init_repository_contents`) and, on any failure, best-effort removes
`data/` and `meta.tmp/` - never `repo_root` itself, which may have pre-existed. Verified with a
regression test that forces a deterministic mid-creation failure (an out-of-range
`cdc_target_size_bits`, rejected by the `repository_settings` table's own `CHECK` constraint - not
validated earlier by `RepositorySettings::new` on purpose) and asserts both directories are gone
afterward while `repo_root` itself stays present and empty - confirmed red/green the same way.

The optional third piece (recognizing a `meta.tmp/`-without-`meta/` leftover in the
`TargetNotEmpty` message as "a previous create-repo did not finish") was deliberately skipped: with
Part 2 in place, future failures no longer leave that debris behind at all, so the only remaining
beneficiary would be debris from a pre-release build already superseded by this fix - not worth the
added message-branching for a case that should no longer arise going forward.
