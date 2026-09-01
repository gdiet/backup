# Implement the read-only DB connection path DESIGN-METADATA-003 anticipates

**Why parked**: surfaced during the 2026-09-01 cross-boundary investigation (see
`agent-todos/network-fs-sqlite-reliability-docs.md`); it is a medium/large `crates/db`-core change,
and the developer wanted it captured as a TODO to weigh separately.
**Size**: medium/large - confirm scope with the developer before starting. Touches `crates/db`'s
connection model, which every other crate goes through.
**Opened**: 2026-09-01, native-Windows session on `3327`
**Context**: `crates/db/src/connection.rs` - `configure_write_connection`'s own doc comment already
spells out the intent: "A future read-only connection (once reads split off their own, per
DESIGN-METADATA-003) needs none of this except `busy_timeout` - the rest either only matters for
writes, or (for `journal_mode`) is a persistent, whole-database property already set once here."
`docs/design/metadata-storage.md` DESIGN-METADATA-003 ("reads and writes still share one
connection per Repository") and its note (~line 626) that a `SQLITE_OPEN_READ_ONLY` connection
cannot issue `PRAGMA journal_mode = WAL` even as a no-op. `crates/db/src/lib.rs` `Repository` (one
`Mutex<Connection>` today) and `open_repository`.

## Why now / what it buys

Every `dfs` command today - including read-only ones (`unlock`'s guard, `mount` without
`--read-write`) - opens a full write-mode connection: `PRAGMA journal_mode=WAL`,
`auto_vacuum=INCREMENTAL`, `foreign_keys=ON`, `synchronous=NORMAL`, plus running pending
migrations. On a filesystem where a WAL write-open fails (observed: WSL<->Windows 9p bridges ->
`database is locked` / `disk I/O error`), read-only operations fail even though they never needed
to write. A read-only connection path would let read-only `mount` (and any future read-only
tooling) work against such a repo.

## Sketch

- A read-only open: `Connection::open_with_flags(db_path, SQLITE_OPEN_READ_ONLY)`, then set only
  `busy_timeout`. Do **not** touch `journal_mode` / `auto_vacuum` / `foreign_keys` /
  `synchronous`. Do **not** run migrations (a read-only caller must not migrate; decide whether a
  schema-version mismatch on a read-only open is an error or is tolerated - probably error with an
  actionable "open once read-write to migrate" message).
- `Repository` gains a read-only construction path (`db::open_repository_read_only` or a flag),
  and read-side methods use it. Given the current single-`Mutex<Connection>` model, the smallest
  step is a separate `open_repository_read_only` returning a `Repository` whose connection is
  read-only, used by `mount` when `!read_write`; a fuller reads-split-from-writes design is the
  DESIGN-METADATA-003 end state.
- `crates/cli/src/mount.rs`: use the read-only open when `!read_write` (it already only acquires
  the `flock` write lock when `read_write`).
- Interaction with `agent-todos/unlock-should-not-open-the-db.md`: that one should still drop the
  DB open entirely for `unlock` (needs to work even when the DB is unreadable) - a read-only open
  is not enough there.
- Update DESIGN-METADATA-003 (and the `configure_write_connection` doc comment) once the split
  exists.
