# Genuinely read-only access for read-only commands

**Status**: implemented and verified, including the empirical `:ro`-mount
re-check and `README.md`/`docker/samba-mount/README.md` updates.

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
connection to a WAL database needs to create/write the `-shm` file** -
readers record their "read mark" slot there. This is baseline SQLite/WAL
behavior, not specific to this codebase - see "The `immutable=1` discovery"
below for why it matters far more than it first appears to.

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
- `Migrations::pending_migrations(&conn) -> Result<i32>` - `self.ms.len() -
  user_version`: positive means migrations are pending, negative means the
  database is ahead (the "too new" case, but without the version number -
  `current_version` is what carries that).

## CLI command classification (from `cli/src/main.rs`'s `Commands` enum)

- **Read-only candidates** (only read the repository): `Restore` (writes to
  the real target filesystem, not the repo), `Stats`, `List`, `Find`,
  `Check`, `Problems`, `Deleted`, `Db backup`, and `Mount` **without**
  `--read-write`.
- **Stay write commands** (unchanged, via `open_repository()` /
  `open_write_connection()`): `Store`, `FixProblems`, `Del`, `Undelete`,
  `Db restore`, `Db compact`, `ReclaimSpace`, `CompactStore`,
  `Mount --read-write`.
- `Init`/`MigrateScalaRepo` are special cases (create a new repository, don't
  go through `open_repository()`).

## The `immutable=1` discovery (the actual crux of this plan)

The first implementation of `open_repository_read_only` did exactly what the
user's requirements describe: check whether `-wal`/`-shm` exist next to the
database file, refuse with `UncheckpointedWal` if so, otherwise open a plain
`SQLITE_OPEN_READ_ONLY` connection (`open_connection_read_only`, already
existed for other purposes). This looked right and passed its own unit
tests - but broke the CLI integration tests: **the very first read-only
command against a repository would leave `-shm` (and an empty `-wal`)
behind, and every read-only command run after that would then refuse with
`UncheckpointedWal`**, since the check couldn't tell "harmless leftover from
a previous read" apart from "real pending write."

Root cause, confirmed empirically (see `db::tests::open_repository_read_only_survives_a_prior_plain_read_only_connection`
and the git history of this file for the throwaway `db/examples/wal_repro.rs`
script used to nail this down): opening *any* connection to a WAL-mode
database - even a genuinely `SQLITE_OPEN_READ_ONLY` one - makes SQLite
create the `-shm` file (needed for the reader's own "read mark" slot) and
touch `-wal`. A **reader** can never remove these again on close: only a
connection able to write the *main* database file can ever run a WAL
checkpoint (folding `-wal` back in and deleting both sidecars), and a
read-only connection is by definition not that. So a naive "sidecars exist
→ refuse" check doesn't just fail to enable a `:ro` mount - it makes
*ordinary, already-writable* repositories stop working for a second
consecutive read-only command too.

The fix has two parts:

1. **The `-wal`-presence check became a `-wal`-*size* check.** An empty
   (0-byte) `-wal` genuinely means nothing is pending - that's exactly what
   a prior read-only connection leaves behind. Only a non-empty `-wal` (real
   frames not yet folded into the main file) is disqualifying. `-shm`'s
   presence is never checked at all: its size is fixed regardless of
   content, so it carries no signal either way.
2. **Actual read connections for an `open_repository_read_only`-obtained
   `Repository` use SQLite's `immutable=1` URI parameter**
   (<https://www.sqlite.org/uri.html#uriimmutable>), not plain
   `SQLITE_OPEN_READ_ONLY`. `immutable=1` tells SQLite to trust the file
   won't change underneath it and read straight from the main database
   file, **never touching `-wal`/`-shm` at all** - this is what actually
   makes a genuinely `:ro`-mounted directory work, not the sidecar check by
   itself (which only guards against ignoring real pending writes; without
   `immutable=1`, opening the *first* connection against a clean `:ro`
   mount would still fail trying to create `-shm`). Only correct to rely on
   once the `-wal`-size check above has already ruled out pending writes -
   otherwise "ignoring WAL entirely" would mean silently serving stale data
   instead of the equivalent-to-checkpointed state it depends on.

Building a `file:` URI needs the path percent-encoded (`?`/`#`/`%` and any
non-ASCII/whitespace byte would otherwise be misparsed as URI syntax) and
backslash-normalized on Windows (SQLite's URI parser wants `file:C:/...`,
not backslashes) - `db::sqlite_uri_path` (private) does this with a small
hand-rolled encoder rather than pulling in a URI crate for one call site.

`Repository` gained a private `read_only: bool` field (`false` from
`open_repository`, `true` from `open_repository_read_only`) so
`Repository::open_read_connection` knows which of the two connection styles
to use - the distinction is invisible to every one of this plan's CLI call
sites, which just call `open_repository_read_only(...)` and then
`.open_read_connection()` exactly as before.

## Design

### 1. `db compact` checkpointing the WAL - tried, then reverted

Originally added `PRAGMA wal_checkpoint(TRUNCATE)` to `db compact` (which
already does `PRAGMA incremental_vacuum` under a write connection, and is
documented as "safe to run any time" - the natural, existing place for
"clean up the database file" to live, over a separate new `db checkpoint`
command). The reasoning at the time: folds the WAL back into the main file,
and since `db compact` holds the only open connection at that point, SQLite
removes the `-wal`/`-shm` sidecars entirely on close.

**Reverted after checking empirically**: SQLite already does exactly that -
fold the WAL back in and remove both sidecars - whenever the *last* open
connection to the database closes cleanly, with no explicit checkpoint
pragma needed. `db compact`'s own write connection normally *is* the last
one open, so the explicit pragma turned out to be a no-op in the common
case. The one case it would actually add something is a genuinely
concurrent connection (e.g. an active read-only `mount`) still being open
at the exact moment `db compact` runs - forcing a checkpoint attempt right
then instead of relying on SQLite's own threshold-based passive
auto-checkpoint. Decided not to carry that complexity for a scenario that's
real but has never actually come up - add it back if/when it does.

### 2. `db::Error` gets actionable variants (done)

Added to `db/src/error.rs`:
- `SchemaTooNew { db_version: usize }` - `Migrations::current_version`
  returned `Outside(v)`. Wired into *both* `open_repository()` (the write
  path - today's confusing message affects write commands exactly the same
  way, fixing it only for reads would be an inconsistent half-measure) and
  `open_repository_read_only()`, via a shared `reject_if_schema_too_new`
  helper.
- `MigrationsPending` - `pending_migrations(&conn) > 0`. Only reachable from
  `open_repository_read_only()`: the write path just applies them itself.
- `UncheckpointedWal` - non-empty `-wal` next to the database file (see "The
  `immutable=1` discovery" above for why size, not presence).

### 3. `db::open_repository_read_only()` (done)

Alongside `open_repository()` in `db/src/lib.rs`:
1. `-wal`'s size - if non-empty, `Error::UncheckpointedWal`.
2. Open via the new `open_connection_immutable()` (`file:...?immutable=1`).
3. `reject_if_schema_too_new` (shared with `open_repository()`), then
   `pending_migrations(&conn) > 0` -> `Error::MigrationsPending`.
4. Read settings exactly like `open_repository()`, return a `Repository`
   with `read_only: true`.

### 4. Switch read-only commands over (done)

`cli/src/{restore,stats,list,find,check,problems,deleted,db_maintenance
(backup only)}.rs`: use `db::open_repository_read_only()` instead of
`db::open_repository()`.

`Mount` needed more than a one-line swap: `build_filesystem` used to open a
write connection **unconditionally**, even for a read-only mount ("cheap,
keeps `DedupFs`'s shape identical regardless of `--read-write`" - the old
comment, now removed). Every write-path `MountFilesystem` method already
starts with `if self.read_only { return Err(Errno::EROFS) }` before ever
touching that connection, so it was always structurally unreachable in a
read-only mount, just wastefully (and, per this plan, now
incorrectly) opened anyway. `Inner::write_conn` became
`Mutex<Option<Connection>>` - `None` for a read-only mount, opened via
`open_repository`/`open_repository_read_only` matching `--read-write`.
Every call site unwraps with `.expect(...)`, safe precisely because it's
already behind that same `read_only` guard. The one-time `chunk_extents_sorted`
seeding call moved from `&write_conn` to the always-open plain `&conn` (a
pure read, no reason it ever needed the write connection).

## What this touches

- `db/src/lib.rs`: `open_repository_read_only`, `open_connection_immutable`,
  `sqlite_uri_path`, `reject_if_schema_too_new`, `Repository::read_only`.
- `db/src/error.rs`: `SchemaTooNew`, `MigrationsPending`, `UncheckpointedWal`.
- `cli/src/db_maintenance.rs`: `run_backup` (read-only opener). `run_compact`
  ends up unchanged - see design section 1.
- `cli/src/{restore,stats,list,find,check,problems,deleted}.rs`: read-only
  opener.
- `cli/src/mount.rs`: read-only opener + `write_conn` made `Option`-al for a
  non-`--read-write` mount.
- `README.md`: not yet updated - still to do (see below).

## Verification plan

- `cargo fmt`/`clippy -D warnings`/`doc --no-deps`/`test` across the
  workspace: all green, including the new regression tests for the
  `immutable=1` bug
  (`db::tests::open_repository_read_only_survives_a_prior_plain_read_only_connection`)
  and for a genuinely non-writable `meta/` *directory*, not just the
  database file (`db::tests::open_repository_read_only_works_even_when_the_meta_directory_is_not_writable`,
  `cli::mount::tests::build_filesystem_read_only_works_even_when_the_database_file_is_not_writable`).
  `cargo test --workspace` hung once mid-session on a wedged real FUSE
  mount (a stuck kernel-side request, cleared via
  `/sys/fs/fuse/connections/<id>/abort` without needing a reboot) - several
  repeated full-parallel reruns afterwards all passed cleanly in a few
  seconds, so this was a one-off environmental glitch in the sandbox, not a
  deadlock in this plan's changes and not a general "no concurrent FUSE
  mounts" limitation.
- Re-ran the `docker/samba-mount/` image with the repository bind-mounted
  `:ro` (after a `db compact`): the container's `backup mount` (default,
  read-only) came up successfully, file content read back correctly through
  the mount, and the host-side `meta/` directory stayed completely
  untouched (no `-wal`/`-shm` ever appeared) - the original trigger for
  this whole plan, now confirmed fixed end-to-end, not just unit-tested.
- `README.md` gained a "Read-Only Commands Need A Clean Database" section;
  `docker/samba-mount/README.md` and its `Dockerfile` comment updated to
  say `:ro` now works (plus fixed stale "not merged into `rust`" wording -
  unrelated to this plan, but noticed while editing the same file).

## Resolved while implementing

- WAL-sidecar check is based on `-wal` size, not mere existence - see "The
  `immutable=1` discovery".
- Actual read connections for a read-only-opened `Repository` use
  `immutable=1`, not plain `SQLITE_OPEN_READ_ONLY` - same section.
- `Mount` (non-`--read-write`) needed `Inner::write_conn` to become
  `Option`-al, not just a call-site swap - see design section 4.
- `db compact`'s explicit WAL checkpoint was added, then reverted after
  confirming empirically it was a no-op in the common (single-connection)
  case - see design section 1.

## Addendum: a targeted error for a write connection on a read-only filesystem

Follow-up, spawned by "what happens if you bind-mount `:ro` *and* pass
`--read-write`?" `open_repository()` (the write path) had no equivalent to
`open_repository_read_only()`'s actionable errors - it just hit SQLite's
generic `SQLITE_CANTOPEN` ("unable to open database file"), the same
unhelpful message this whole plan replaced for the read-only path.

### First attempt: an OS-level write probe (reverted)

Tried detecting "is the underlying storage actually read-only" ourselves,
via a real, non-destructive write probe (`OpenOptions::append`, opened and
immediately dropped, never actually written through) checked against
`std::io::ErrorKind::ReadOnlyFilesystem` specifically - confirmed via a
unit test that a plain `chmod`-restricted file does *not* trigger this,
only genuine `EROFS` does, and confirmed on Linux (both a real Docker `:ro`
mount and a much-faster-to-iterate `unshare --mount --user --map-root-user`
read-only bind mount) that the probe never touches the file's
mtime/size/content.

**Reverted after review** (see conversation, not reproduced here): asked to
run this on Windows too, checking Microsoft's own `SetFileTime` docs turned
up an explicit warning - "To prevent file operations using the given
handle from modifying the last write time, call `SetFileTime` immediately
after opening the file handle..." - official confirmation that merely
holding a write-access handle open on Windows/NTFS can touch a file's
last-write time on close, independent of whether anything was actually
written. Also unverified: whether `ErrorKind::ReadOnlyFilesystem` is even
populated correctly by Rust's Windows I/O backend for the equivalent
condition. Neither is something this project's dev setup (no Windows
environment) can test, and the whole point of an active probe was to be
*more* certain than SQLite's own generic error - not worth an unverified
platform-specific side effect risk for that.

### What shipped instead: translate SQLite's own error, don't add a probe

Simpler alternative: don't try to determine the real cause ourselves at
all - just recognize `SQLITE_CANTOPEN` (SQLite's own, single, generic "the
OS-level `open()` call failed" code) wherever `open_connection` hits it,
and rewrite the message to *ask* whether the storage might be read-only
rather than assert it as fact. No extra file operation, so no new
platform-specific risk of any kind - relies on the exact same C SQLite
library, and hence the exact same `SQLITE_CANTOPEN` code, on every
platform this project targets.

**Where `SQLITE_CANTOPEN` actually appears, precisely traced** (source:
`unixOpen`/`sqlite3PagerOpen` in the bundled `sqlite3.c`, cross-checked
against real reproductions): opening the *existing* main database file for
read-write against a genuinely read-only-mounted directory does **not**
itself fail - `unixOpen` has a built-in fallback (`errno != EISDIR &&
isReadWrite` -> retry as `O_RDONLY`) that silently succeeds, since reading
an existing file on a read-only filesystem is always fine. `SQLITE_CANTOPEN`
only appears once some *later* operation needs SQLite to actually set up
its WAL shared-memory index for real (the first genuine read transaction) -
and *which* operation that turns out to be is not reliably the same one
every time: one traced run failed at the `synchronous` pragma, an earlier
one at `journal_mode` - both several calls into `open_connection`'s
sequence, neither the first thing that runs. Because of that
unpredictability, `open_connection`'s pragma sequence was split into an
inner helper (`open_connection_inner`, returning a plain `rusqlite::Result`)
so the classification (`classify_open_error`) is applied exactly once, to
whatever error the *whole* sequence exits with - not repeated at each
individual call site, which would be one call away from silently missing
whichever one actually turns out to fail. `SQLITE_CANTOPEN` is otherwise a
pure OS-level "couldn't even open the file" signal - distinct from
`NotADatabase`/`Corrupt` (which require a successful open first, only
surfacing once SQLite actually reads header/page content - confirmed with
a garbage-bytes file: `Connection::open()` succeeds, only the first real
query fails with `NotADatabase`).

New `Error::CannotOpenForWriting(PathBuf)` variant; message: "the metadata
database at {path} could not be opened for writing - is it on a read-only
filesystem? If you only need to read the repository, a read-only command
(...) doesn't need write access at all." Verified end-to-end via the same
`unshare`-based read-only bind mount, both for an existing repository
(`--read-write` against a `:ro` mount) and for `init_repository` targeting
a read-only parent directory (which turned out to already fail earlier,
at `fs::create_dir_all`, with an already-self-explanatory plain I/O error
- "Read-only file system (os error 30)" - so needed no change).
