# Genuinely read-only access for read-only commands

**Status**: in progress. `db compact` has been extended with a WAL checkpoint
(step 1 below); the rest is not yet implemented.

**Trigger**: `-v /path:/repo:ro` in the `docker/samba-mount/` setup (see
`docs/plans/implemented/*samba*` history) failed, because SQLite in WAL mode
needs to write `-wal`/`-shm` sidecar files next to the database even for pure
reads - so a repository directory can't currently be mounted truly read-only.

**User requirements** (verbatim, the basis for everything below):
> * If migrations are still pending, read-only commands abort with an error
>   message
> * If WAL/SHM files still exist, read-only commands abort with an error
>   message
> * We document a command (can we reuse an existing one for this, or does it
>   need a new dedicated one?) that does nothing but clean up the database

Side task: check whether the app gives a sensible, actionable error message
when the on-disk schema is "too new" for the installed binary.

## Background: why write access is needed at all today, independent of WAL

`db::open_repository()` (`db/src/lib.rs`) always opens a **read-write**
connection first (`open_connection`), to check for and apply pending
migrations - for *every* command, including pure reads. Only after that do
read commands additionally open a genuinely `SQLITE_OPEN_READ_ONLY`
connection (`Repository::open_read_connection`).

`open_connection` sets `journal_mode = WAL` on every call; on an
already-WAL database this is a no-op *check*, but SQLite still requires a
writable connection to perform even that check.

Separately, and more fundamentally: **even a real `SQLITE_OPEN_READ_ONLY`
connection to a WAL database needs write access to the `-shm` file** -
readers update their "read mark" slots there. This is baseline SQLite/WAL
behavior, not specific to this codebase.

**Empirically confirmed** (Python's `sqlite3`, same engine as `rusqlite`):
`-wal`/`-shm` disappear once (a) `PRAGMA wal_checkpoint(TRUNCATE)` has run and
(b) the last open connection to the database closes. While any connection
stays open, `-wal` is truncated to 0 bytes but not removed; SQLite only
deletes both sidecars itself once the last connection closes.

**"Schema too new" error, tested** (`user_version` forced to 99, then opened
with the real binary):
```
error: failed to open repository at /tmp/future-schema-test: database migration error:
rusqlite_migration error in migrations definition: Attempt to migrate a database with a
migration number that is too high
```
No crash, a clean `Result` error, exit code 1 - but not actionable: it reads
like an internal library message, not "update `backup`".

**`rusqlite_migration` 2.6.0 API** (relevant parts):
- `Migrations::current_version(&conn) -> Result<SchemaVersion>` only needs a
  readable connection (`PRAGMA user_version` internally) - no write access,
  and doesn't itself migrate. Exactly the right tool for the read-only path.
- `SchemaVersion` variants: `NoneSet`, `Inside(NonZeroUsize)` (known range),
  `Outside(NonZeroUsize)` - **"too new"**, carrying the actual version number.
- The "too high" failure from `to_latest()` is structurally matchable as
  `Error::MigrationDefinition(MigrationDefinitionError::DatabaseTooFarAhead)`
  - no string matching needed (the crate's own doc comment warns the
    `Display` text can change between versions; `Error` is `#[non_exhaustive]`).

## CLI command classification (from `cli/src/main.rs`'s `Commands` enum)

- **Read-only candidates** (only read the repository): `Restore` (writes to
  the real target filesystem, not the repo), `Stats`, `List`, `Find`,
  `Check`, `Problems`, `Deleted`, `Db backup` (its `VACUUM INTO` dump already
  goes through `open_read_connection()` - `cli/src/db_maintenance.rs`), and
  `Mount` **without** `--read-write`.
- **Stay write commands** (unchanged, via `open_repository()` /
  `open_write_connection()`): `Store`, `FixProblems`, `Del`, `Undelete`,
  `Db restore`, `Db compact`, `ReclaimSpace`, `CompactStore`,
  `Mount --read-write`.
- `Init`/`MigrateScalaRepo` are special cases (create a new repository, don't
  go through `open_repository()`).

## Design

### 1. `db compact` also checkpoints the WAL (done)

`db compact` (`cli/src/db_maintenance.rs::run_compact`) already does
`PRAGMA incremental_vacuum` under a write connection and is documented as
"safe to run any time" - the natural, existing place for "clean up the
database file" to live, so this reuses it rather than adding a new command
(`db checkpoint`, considered and rejected: same underlying concept - getting
the on-disk file into a clean, minimal state - and a second command would
just be one more thing to remember for no real gain).

Added `PRAGMA wal_checkpoint(TRUNCATE)` right after the existing
`incremental_vacuum`, then drops the connection: folds the WAL back into the
main file, and since this command holds the only open connection at that
point, SQLite removes the `-wal`/`-shm` sidecars entirely on close (not just
truncates `-wal` to 0 bytes, which is as far as `TRUNCATE` gets while a
connection stays open).

### 2. `db::Error` gets actionable variants

Add to `db/src/error.rs`:
- `SchemaTooNew { db_version: usize }` - `Migrations::current_version`
  returned `Outside(v)`. Message: this repository was created/last opened by
  a newer `backup` version than the one currently installed; upgrade
  `backup`.
- `MigrationsPending` - `Inside(v)` with `v` behind the latest known
  migration. Message: pending migrations, run a write command (or `db
  compact`) once to apply them.
- `UncheckpointedWal` - `-wal`/`-shm` sidecars exist next to the database
  file. Message: points at `db compact` to clean them up.

Wire `SchemaTooNew` detection into the *existing* `open_repository()`
(write) path too, not just the new read-only path - today's bad "migration
number too high" message affects write commands exactly the same way, and
fixing it only for reads would be an inconsistent half-measure.

### 3. `db::open_repository_read_only()`

New function alongside `open_repository()` in `db/src/lib.rs`:
1. Check whether `-wal`/`-shm` files exist next to the `.sqlite3` file
   (`Path::exists()`) - if so, `Error::UncheckpointedWal`.
2. Open via `open_connection_read_only()` (already exists).
3. `migrations::migrations().current_version(&conn)`, **not** `to_latest()`:
   - `Outside(v)` -> `Error::SchemaTooNew`
   - `Inside(v)` with `v` < latest -> `Error::MigrationsPending`
   - `Inside(v)` == latest -> proceed, read settings as `open_repository()`
     does today.

### 4. Switch read-only commands over

`cli/src/{restore,stats,list,find,check,problems,deleted,db_maintenance
(backup only),mount}.rs`: use `db::open_repository_read_only()` instead of
`db::open_repository()`. `Mount` only in the non-`--read-write` branch.

## What this touches

- `db/src/lib.rs`: `open_repository_read_only()`.
- `db/src/error.rs`: `SchemaTooNew`, `MigrationsPending`, `UncheckpointedWal`.
- `db/src/migrations.rs`: no schema change, just consuming
  `Migrations::current_version` from `lib.rs`.
- `cli/src/db_maintenance.rs`: `run_compact` (done); `run_backup` switches to
  the read-only opener.
- `cli/src/{restore,stats,list,find,check,problems,deleted,mount}.rs`: switch
  to the read-only opener where applicable.
- `README.md`: document that read commands now require a clean (migrated,
  checkpointed) database, and point at `db compact` as the fix.

## Verification plan

Full `cargo fmt`/`clippy`/`test` (+ `cargo doc` if warnings are treated as
errors there too, per `AGENTS.md`). Then an empirical, not just unit-tested,
check: re-run the `docker/samba-mount/` setup with the repository bind-mounted
`:ro` this time, confirming a read-only command (e.g. `stats`) now actually
works against a truly read-only mount - the original trigger for this whole
plan.

## Open questions / not yet resolved

- Exact wording of the three new error messages.
- Whether `Mount --read-write`'s error path should mention `MigrationsPending`
  differently from a plain write command (it already goes through
  `open_repository()`, so no special-casing should be needed - to confirm
  once implemented).
