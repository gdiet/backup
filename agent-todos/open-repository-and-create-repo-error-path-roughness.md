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
