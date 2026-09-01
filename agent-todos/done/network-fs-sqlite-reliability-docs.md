# Document SQLite/metadata (un)reliability on 9p-bridged filesystems

**Why parked**: found during a cross-boundary investigation (native-Windows session, 2026-09-01);
the developer wanted it captured as a TODO rather than acted on right then.
**Size**: small-medium (docs only)
**Opened**: 2026-09-01, native-Windows session on `3327`
**Context**: `README.md` "Known Limitations" (bullet "Repository write locking on network-mounted
storage"); `docs/design/metadata-storage.md`'s `journal_mode = WAL` note (~line 586-591, and the
`SQLITE_OPEN_READ_ONLY` note ~line 626); `docs/design/repository-locking.md` (already cross-refs
the README limitation - mirror that). Related TODOs:
`agent-todos/open-repository-and-create-repo-error-path-roughness.md`,
`agent-todos/unlock-should-not-open-the-db.md`,
`agent-todos/read-only-db-connection-path.md`.

## What the investigation found (evidence for the doc text)

Environment: WSL2 `u24`; `/mnt/c` is **v9fs** (9p); `\\wsl.localhost\u24\...` is the reverse 9p
bridge; `\\<host>\c$\...` is SMB loopback to local NTFS. `dfs` (Linux) and cross-built `dfs.exe`.
Root cause: every `dfs` DB connection today runs `configure_write_connection`
(`crates/db/src/connection.rs`) - `PRAGMA journal_mode=WAL` + `-shm` shared-memory coordination -
so even read commands (`unlock`, read-only `mount`) do a full WAL write-open.

| Actor -> target | command | result | message |
|---|---|---|---|
| Windows -> `\\wsl.localhost` (new) | `create-repo` | FAIL, leaves `data/` + `meta.tmp/` | `error: database is locked` |
| WSL -> `/mnt/c` (new) | `create-repo` | OK | - |
| Windows -> `\\wsl.localhost` (existing, clean) | `unlock` (read) | FAIL | `error: database is locked` |
| Windows -> `\\wsl.localhost` (existing) | `mount --read-write` | FAIL at DB open | `error: database is locked` |
| WSL -> `/mnt/c` (existing, has `-wal`/`-shm`) | `unlock` (read) | FAIL | `error: disk I/O error` |
| WSL -> `/mnt/c` (existing, clean single-file DB) | `unlock` (read), x2 | OK | "not locked - nothing to do" |
| WSL -> `/mnt/c` (existing, has `-wal`/`-shm`) | `mount --read-write` | FAIL at DB open | `error: disk I/O error` |
| Windows -> `\\<host>\c$` SMB loopback (new) | `create-repo` | OK | - |
| Windows -> `\\<host>\c$` SMB loopback (existing) | `unlock` (read) | OK | - |
| Windows -> `\\<host>\c$` SMB loopback (existing) | `mount --read-write` | past DB open **and** write-lock, fails only at WinFSP preflight | `error: WinFSP not found (winfsp-x64.dll) - install it from https://github.com/winfsp/winfsp` |
| Windows -> `P:` = real mapped SMB drive, corporate DFS namespace (`\\gtv.grp\dfs\Privat\Home_ER_TCG\...`) | `create-repo` (new), `unlock` (read), `mount --read-write` x2, repeated opens | all OK (write path reaches the same WinFSP-preflight stop); no `-wal`/`-shm` left behind afterwards | - / WinFSP-not-found |

Conclusions:

1. **9p/v9fs specifically**, not "network filesystems" in general. SMB works for single-process
   access - tested both loopback (`\\<host>\c$` to local NTFS) and a **real mapped SMB drive with
   a corporate DFS namespace** (`P:` = `\\gtv.grp\dfs\...`): `create-repo`, read (`unlock`), the
   `flock` write-lock, and repeated opens all succeed, and Windows SQLite over SMB checkpoints and
   removes `-wal`/`-shm` on close (no debris). Concurrent multi-process access over a real network
   link was not tested and stays under the existing README caveat.
2. **Direction + DB state matter.** Windows -> `\\wsl.localhost` fails on everything, even a clean
   single-file DB, even a read (the first `PRAGMA journal_mode=WAL` cannot get its locks).
   WSL -> `/mnt/c` works for a *clean single-file DB* but fails once a `-wal`/`-shm` pair exists
   (`-shm` mmap unsupported over v9fs -> `disk I/O error`); Windows-side access leaves `-wal`/`-shm`
   behind, so a repo that a Windows process has touched then breaks for WSL-side access.
3. **`Error::WalUnavailable` never fires here.** It only catches SQLite's *silent fallback*
   (`PRAGMA` succeeds, returns a non-`"wal"` mode). A live 9p bridge makes the `PRAGMA` *hard-fail*
   (SQLITE_BUSY / SQLITE_IOERR) -> falls through to `Error::Sqlite(err)` -> bare `Display`
   (`database is locked` / `disk I/O error`), with no network-FS hint (unlike the `flock` lock
   errors, which do have one).

The real mapped SMB drive (`P:`, DFS namespace) has now been tested - see the matrix row; it
behaves like local disk, no difference from loopback `c$`.

## The doc changes

1. **README "Known Limitations"**: broaden the "Repository write locking on network-mounted
   storage" bullet to the whole metadata layer, e.g. "SQLite metadata reliability on network /
   bridged filesystems". Cover: it is not only the write lock - the SQLite/WAL layer itself is
   affected, independent of the lock mechanism and of whether multiple processes are involved;
   name the concrete case (a repo on local disk on one side of a WSL<->Windows boundary, accessed
   from the other via `\\wsl.localhost\...` or `/mnt/c/...`, both 9p); note the asymmetry from
   point 2 above; note SMB loopback tested OK for single-process use but concurrent multi-process
   access over any network FS stays under the existing caveat; guidance: run `dfs` from the side
   the repo physically lives on, keep the repo on a real local filesystem.
2. **`docs/design/metadata-storage.md`** `journal_mode = WAL` note: add that `WalUnavailable`
   detection only covers SQLite's *silent fallback*; a filesystem that makes the `PRAGMA` fail
   outright (observed: 9p/v9fs bridges, SQLITE_BUSY / SQLITE_IOERR) bypasses it and surfaces as a
   raw `Error::Sqlite`. Cross-reference the README limitation, the way `repository-locking.md`
   already does.

## Done

**Completed**: 2026-09-01, by Claude Code on the web session (branch `mount-read-write`), during an
unattended sweep of open `agent-todos`/`developer-todos`.

Both doc changes made as specified. One update from the original plan: point 2 now says the hard
`PRAGMA` failure surfaces as the new `Error::ConnectionUnreliable`, not a raw `Error::Sqlite` as
originally written here - `agent-todos/done/open-repository-and-create-repo-error-path-roughness.md`
landed first in this same sweep and gave this exact failure category its own actionable error
variant, so the doc note describes the current, already-fixed behavior rather than the bare-error
problem as originally observed.

README.md's "Known Limitations" got a new, broader top bullet ("SQLite metadata reliability on
network or bridged filesystems") covering the whole connection-open layer, kept separate from the
existing "Repository write locking on network-mounted storage" bullet (now cross-referenced as a
narrower case of the new one) rather than merging the two, since the write-lock bullet's own
`dfs unlock` guidance and marker-file details are specific to that mechanism, not to database
opening in general.

Also fixed, found while editing the same section: `docs/design/metadata-storage.md`'s
DESIGN-METADATA-012 ("SQLite connection pragmas") was still `Status: draft` despite being fully
implemented (`crates/db/src/connection.rs`, exercised by real tests) - updated to
`Status: implemented (crates/db/src/connection.rs)` per `AGENTS.md`'s verification checklist.
