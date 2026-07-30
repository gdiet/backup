# Read-write FUSE/WinFSP mount (Phase 2)

**Status**: phase 2a (structural operations) done and verified on both
Linux and Windows - see "Phase 2a - Windows verification notes" below for
the real debugging process that took (two separate WinFSP-specific bugs,
neither of which had a Linux counterpart). Phase 2b (content writes) not
started. Rewritten from its original Linux-only, `fuser`-based draft now
that `docs/plans/cross-platform-mount-crate.md` has shipped: `backup mount`
runs on the platform-independent `mountfs` crate on both Linux (real
libfuse3) and Windows (real WinFSP), read-only or read-write via
`--read-write`. This phase extends `mountfs::MountFilesystem` with write
operations and implements them in `cli`'s `DedupFs` - the platform-specific
plumbing (dispatch trampolines on both backends) was already designed to
make this a mechanical extension, not a redesign (see "What moves where"
in the cross-platform plan).

## Sequencing (revised)

1. **Phase 2a - structural operations, Linux first**: `mkdir`, `create`
   (empty file), `unlink`, `rmdir`, `rename`, `utimens`, `chmod`/`chown`
   (accepted no-ops - see "Not modeling permissions" below). No write-cache
   needed yet - each of these is a single, small, synchronous `db` mutation.
   Extends the trait, Linux dispatch, `DedupFs`, adds a `--read-write` flag
   to `backup mount` (defaults to read-only - this is a real behavior
   change to a backup tool's data, opt-in only). Verify on Linux (this
   environment can build/test that leg directly) before touching Windows.
   **Done.**
2. **Phase 2a - Windows**: wire the same trait methods into
   `mountfs::windows`'s dispatch (the `fuse_operations` slots for all of
   these already exist in `windows/sys.rs`, currently `Unimplemented` -
   same mechanical extension as the Linux side). Needs a real Windows
   verification pass the same way the read-only phase did (this went
   through a real debugging process last time - budget for that, don't
   assume it'll be friction-free just because Linux was). **Done** - see
   "Phase 2a - Windows verification notes" below for what that debugging
   process actually found.
3. **Phase 2b - content writes**: the `write` operation and the write
   cache/persist pipeline (§§1-2, 4 below) - `write`, `truncate` to a
   non-zero/non-current size, and the backpressure mechanism. This is the
   large, stateful part deliberately sequenced after 2a: 2a alone is
   already a real, useful, independently-shippable increment (a mount you
   can `mkdir`/`touch`/`mv`/`rm` on, just not yet write file content
   through), and none of its `db`/trait/dispatch plumbing needs to be
   redone once 2b adds real content.

## Not modeling permissions

`chmod`/`chown` become accepted no-ops (`Ok(())`, nothing persisted) rather
than real per-file permission tracking - consistent with today's read path,
which already fabricates fixed permissions in `getattr` (`cli/src/mount.rs`)
since the schema has no permission columns at all. Adding real permission
storage would be a schema migration and a much bigger scope change; revisit
only if a concrete need shows up (e.g. an editor that refuses to write to a
file it perceives as read-only based on the fabricated mode bits - not
observed yet). The two backends' `getattr` dispatch fabricate different
mode bits for a reason - see "Phase 2a - Windows verification notes" below:
Linux (`mountfs::linux::dispatch_getattr`) still reports `0o444`/`0o555`
(read-only-looking) unconditionally, since real libfuse's kernel VFS
doesn't check them without `-o default_permissions` (not passed); Windows
(`mountfs::windows::dispatch_getattr`) reports `0o666`/`0o777`
unconditionally instead, because WinFSP *does* derive and enforce an NT
security descriptor from these bits before ever calling into this crate's
own write operations - a read-only-looking mode there made every write op
fail at the driver level, regardless of what `DedupFs` would have allowed.
Actual read-only enforcement on both platforms comes from the mount-level
`-oro`/`ReadOnlyVolume` flag (see `mount`'s `read_only` parameter), not
from either backend's fabricated mode bits.

## Phase 2a - Windows verification notes

Two WinFSP-specific bugs surfaced during Windows verification, neither
with a Linux counterpart (Linux's read-write structural test passed on the
first real run) - both are now fixed, but recorded here since they're easy
to reintroduce if `windows/mod.rs`'s `dispatch_getattr`/`open` change
without re-reading this:

- **Mode bits gate write ops at the driver level.** WinFSP performs its
  own NT-style access check derived from `getattr`'s reported `st_mode`
  *before* ever invoking this crate's `mkdir`/`create`/`open`/etc. - unlike
  real libfuse's kernel VFS, which (without `-o default_permissions`, not
  passed here) calls straight through to the filesystem's own operations
  regardless of reported mode. The original read-only-looking `0o555`/
  `0o444` (copied verbatim from the Linux backend when phase 2a's dispatch
  was first wired up) made every structural write op fail with a
  Windows-level "access is denied" - `dispatch_mkdir` was never even
  entered. Fixed by reporting `0o777`/`0o666` unconditionally instead (see
  "Not modeling permissions" above for why this is still consistent with
  the read-only case).
- **`st_uid`/`st_gid` must be populated for the mode bits to grant
  anything.** Fixing the mode bits alone wasn't enough: `getattr` never
  set `st_uid`/`st_gid` (left at `0` by the buffer-zeroing at the top of
  `dispatch_getattr`), so WinFSP built a security descriptor that didn't
  recognize the mounting process as the file's owner, and access was still
  denied even with fully-open mode bits. Fixed by reading the real caller
  identity via `fuse_get_context()->uid/gid` and writing it into
  `st_uid`/`st_gid` - exactly what WinFSP's own `memfs-fuse3.cpp` reference
  implementation does, and the detail that made this easy to miss (nothing
  in the real libfuse/Linux path needs it).
- **`open`'s old blanket `write_intent → EROFS` had to go.** Predating
  phase 2a (from when the whole mount was read-only), `DedupFs::open`
  rejected any write-intent open outright. Phase 2a's `utimens` on an
  *existing* file needs a write-intent open to succeed on Windows
  specifically: `SetFileTime` requires the handle to carry
  `FILE_WRITE_ATTRIBUTES`, unlike POSIX `futimens`, which is permission-
  checked by file ownership rather than by how the fd was opened (why this
  never surfaced verifying Linux alone - `File::open`'s default read-only
  handle was already sufficient there). Removing the check doesn't open a
  content-write hole: `write` still isn't wired into either backend's
  dispatch (phase 2b), so the kernel/WinFSP answers any real write attempt
  with `ENOSYS` on its own regardless of how the file was opened. A
  genuinely read-only mount is unaffected either way - `-oro`/
  `ReadOnlyVolume` rejects a write-intent open before `DedupFs::open` is
  ever called, on both platforms.

## `db` additions needed

Surveyed what already exists (`db/src/tree.rs`, `backup.rs`,
`maintenance.rs`) before adding anything - reuse over reinvention:

- `mkdir` → `insert_directory` (already exists, exactly this).
- `create` (empty file) → `apply_backup_batch` with a single
  `FileBackupRecord { chunks: vec![], .. }` (already handles the
  zero-length-file case - no `contents` row, `content_id` stays `NULL`,
  exactly what the read path already treats as "empty file").
- `unlink`/`rmdir` → `maintenance::soft_delete` (already exists, already
  used by the `del` command) - consistent with this tool's "nothing is
  really gone until `reclaim-space`" philosophy; `rmdir` additionally
  requires the directory to be empty first (`db::list_children` returns
  none), checked in `DedupFs` before calling it - `soft_delete` itself
  would happily recurse through a non-empty directory, which is *not*
  POSIX `rmdir`'s contract.
- **New**: `db::touch_mtime(conn, id, time_millis) -> Result<(), Error>` -
  a plain `UPDATE tree_entries SET time = ?1 WHERE id = ?2 AND deleted_at
  IS NULL`, for `utimens`. Nothing existing covers this standalone (only
  `apply_backup_batch`'s unchanged-content branch does an equivalent
  update, as a side effect of a file backup, not as its own operation).
- **New**: `db::rename_entry(conn, id, new_parent_id, new_name) ->
  Result<(), Error>` - updates `parent_id`/`name` on an existing row.
  **Deliberately no overwrite-existing-target support in the first cut**
  (errors `Errno::EEXIST` if `new_parent_id`/`new_name` is already taken,
  same shape as `insert_directory`'s conflict handling) - full POSIX
  `rename()` semantics (atomically replacing an existing file target,
  directory-target-must-be-empty, cross-device is a different syscall
  entirely) are a meaningfully bigger surface to get right and rarely hit
  in the actual use case (browsing/lightly editing a mounted dedup store,
  not running a build system or package manager against it) - noted as a
  known limitation to revisit if it turns out to matter in practice, not
  silently unhandled: it returns a real error, not a wrong success.

`Errno` (`mountfs/src/lib.rs`) gains `EEXIST = 17`, `ENOTEMPTY = 39`, and
`ENOSPC = 28` (for phase 2b's backpressure/disk-full paths) alongside the
existing `ENOENT`/`EIO`/`EISDIR`/`EROFS`.

## `MountFilesystem` trait additions

```rust
pub trait MountFilesystem: Send + Sync + 'static {
    // ...existing read-only methods unchanged...

    /// Default `EROFS`: a `MountFilesystem` that only ever implements the
    /// read-only methods (this crate's own test fixtures, for instance)
    /// doesn't have to implement any of these just to keep compiling -
    /// only `cli::DedupFs` overrides them for real.
    fn mkdir(&self, path: &str) -> Result<(), Errno> { let _ = path; Err(Errno::EROFS) }
    fn create(&self, path: &str) -> Result<Handle, Errno> { let _ = path; Err(Errno::EROFS) }
    fn unlink(&self, path: &str) -> Result<(), Errno> { let _ = path; Err(Errno::EROFS) }
    fn rmdir(&self, path: &str) -> Result<(), Errno> { let _ = path; Err(Errno::EROFS) }
    fn rename(&self, old_path: &str, new_path: &str) -> Result<(), Errno> {
        let _ = (old_path, new_path);
        Err(Errno::EROFS)
    }
    fn utimens(&self, path: &str, mtime_millis: i64) -> Result<(), Errno> {
        let _ = (path, mtime_millis);
        Err(Errno::EROFS)
    }
    fn chmod(&self, path: &str) -> Result<(), Errno> { let _ = path; Ok(()) }
    fn chown(&self, path: &str) -> Result<(), Errno> { let _ = path; Ok(()) }

    // Phase 2b, not added in 2a:
    // fn write(&self, handle: Handle, offset: u64, data: &[u8]) -> Result<u32, Errno>;
    // fn truncate(&self, path: &str, size: u64) -> Result<(), Errno>;
}
```

No `mode`/`uid`/`gid` parameters anywhere (nothing is stored - see "Not
modeling permissions"). No `mtime_millis` parameter on `mkdir`/`create`:
`DedupFs` reads `SystemTime::now()` itself rather than threading a
platform-sourced timestamp through the dispatch layer - simpler, and
avoids duplicating clock-reading code across the Linux/Windows dispatch
trampolines for something every platform's libc/Win32 layer already gives
`std::time::SystemTime::now()` uniformly.

Path-based throughout (matching the existing read-only methods and the
crate's whole design rationale) - `rename` takes two paths directly rather
than an id/handle, since WinFSP's and libfuse's own `rename` callbacks are
themselves already path-based (`oldpath`, `newpath`), nothing to translate.

## Dispatch layer changes (both backends)

Both `linux/sys.rs` and `windows/sys.rs` already declare `mkdir`/`create`/
`unlink`/`rmdir`/`rename`/`utimens`/`chmod`/`chown` as `Unimplemented`
same-size placeholder fields in their `fuse_operations` structs (see each
file's doc comment - this was deliberate: "no redesign needed to add them
later"). Phase 2a gives each a real signature and a `dispatch_*<T>`
trampoline, mirroring the existing `dispatch_getattr`/`dispatch_open`/etc.
exactly - no new pattern, just more instances of the established one.

## `cli::DedupFs` (Phase 2a)

- `mkdir`/`create`/`unlink`/`rmdir`/`rename`/`utimens` resolve the path via
  `db::resolve_path` (parent) the same way the read path already does,
  then call the new/existing `db` function under the write connection's
  lock.
- **Needs a write connection**: `DedupFs` today only holds
  `conn: Mutex<Connection>` opened via `open_read_connection`. Phase 2a
  adds a second `write_conn: Mutex<Connection>` (via
  `open_write_connection`) held for the mount's whole lifetime - the same
  "one write connection per repository" discipline `store`/`del` already
  follow per-invocation, just held open for as long as the mount runs
  instead of one command's duration. `store`/`del`/`reclaim-space` must
  not be run concurrently against a repository that has a read-write mount
  active - not a new constraint (the existing single-writer discipline
  already implies this), but worth calling out explicitly in the `mount
  --read-write` help text once that flag exists.
- `create` returns a `Handle` - reuses the existing `Handle(tree_id)`
  convention `open` already establishes for the read path.

## `backup mount --read-write`

New flag on `MountArgs` (`cli/src/mount.rs`), default off. Threads through
to `mountfs::mount(fs, &args.mountpoint, !args.read_write)`. Read-only
stays the default: a mount is a much larger blast radius than `store`/
`restore` for a mistake (an editor autosave, a stray `rm -rf` from inside
the mount, a build tool scribbling into it) - opt-in, not opt-out.

## Phase 2b (not started - content writes)

Kept from the original draft, still believed correct, just not
re-litigated in this rewrite:

### 1. Write cache (per open file)

- A process-wide RAM budget: `AtomicU64`, initialized from
  `sysinfo`-detected total system RAM × a configurable fraction,
  overridable via a CLI flag. Reservation via a lock-free CAS retry loop,
  non-blocking - fails fast to the disk-spillover path rather than
  waiting.
- Per-write-chunk (not per-file) decision between the RAM cache and a
  lazily-created sparse temp file - a single open file's pending writes
  can end up split between RAM and disk, with the split able to shift on
  overwrite.
- A sparse "zero-hole" tracker for writes past current EOF / truncate-grow
  - storing only `(position, length)` pairs, never materializing real zero
  bytes until actually read.
- Read-back for a file that's mid-edit merges, in order: the live
  in-progress cache → any older not-yet-persisted generations still queued
  (a file can be written, closed, reopened, and rewritten again before the
  first flush completes) → the already-persisted content (via the same
  chunk-read path the read-only phase's `read` uses).

### 2. Persist pipeline (on last-handle release)

Reuses `store`'s existing many-readers/one-writer architecture: the
accumulated write-cache content (merged with previously-persisted bytes
for any untouched holes) is read once, run through the existing
`BufferingHashingChunker` + chunk dedup lookup, new chunks get written via
`cli::chunk_store`'s existing `SpaceAllocator`/`write_chunk_bytes`, and the
tree is updated via `apply_backup_batch`'s existing soft-delete-old +
insert-new pattern (content mutability decision already settled - see
below). Backpressure is applied per chunk in the `write` callback:
`cacheLoadDelay = bytesInPersistQueue * persistQueueSize / 1_000_000_000`
(ms), uncapped by design (a self-stabilizing negative-feedback throttle
*because* it's applied synchronously before a chunk is accepted - capping
it either breaks that property or replaces a smooth slowdown with a
jarring full stop).

### 3. Prerequisite: store-space reuse after `reclaim-space`

Already implemented (`docs/plans/implemented/03-chunk-extents.md`,
`cli::chunk_store::SpaceAllocator`) - was built ahead of this phase for
exactly this reason.

### 4. Handle/refcount model

One entry per open file (keyed by the `Handle`/tree id `open`/`create`
already return), `{count, current_cache, persisting_queue}` - multiple
concurrent opens of the same file share one entry and a simple refcount;
the write cache only gets handed to the persist pipeline once the count
reaches zero.

### 5. `write`/`truncate` trait methods

```rust
fn write(&self, handle: Handle, offset: u64, data: &[u8]) -> Result<u32, Errno>;
fn truncate(&self, path: &str, size: u64) -> Result<(), Errno>;
```

A single coarse `Mutex` for tree-structure-mutating operations (already
needed by 2a's `mkdir`/`unlink`/`rmdir`/`rename` - not new to 2b), matching
the original design's "Scala's own experience suggests it isn't a real
bottleneck at typical single-mount FUSE call volumes" reasoning.

## Not yet decided

- Exact `sysinfo`-based RAM-budget formula and its CLI override flag name
  (phase 2b).
- Whether `rename`'s no-overwrite limitation (see above) turns out to
  matter in practice once phase 2a is actually used.
- Windows verification budget for phase 2b - phase 2a's Windows pass is
  done (see "Phase 2a - Windows verification notes"), but the same caveat
  the cross-platform plan's "Windows checkpoint" recorded still applies to
  2b: assume it needs real debugging time, not a formality.
