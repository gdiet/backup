# Read-only FUSE mount (`backup mount`) + fix `list <file>` missing mtime

**Status**: implemented - see `backup mount` in the README. Originally planned together with a read-write phase; that phase was split out into its own not-yet-implemented plan, `docs/plans/implemented/06-fuse-mount-readwrite.md`, once this part shipped.

## Context

The Scala tool this Rust rewrite replaces can be FUSE-mounted read-only or read-write. The user wants the same capability here, modeled closely on the Scala design, plus a small unrelated bug fix: `list <path-to-a-file>` doesn't show the file's mtime (only `list <path-to-a-directory>`'s per-child listing does), because both call the same shared `print_file_info` helper, which never prints a timestamp.

Research into the Scala implementation (`server/Server.scala`, `server/Backend.scala`, `server/{Handles,Handle}.scala`, `cache/{MemCache,FileCache,WriteCache,Allocation,CacheBase}.scala`, `server/FreeAreas.scala`, `Main.scala`'s `mount` entry point) surfaced the mechanisms this plan (and its read-write follow-up) are based on.

**Phasing**: **Phase 1 (this doc): read-only mount, fully working.** No write cache, no backpressure, no schema change, no store-space allocator needed - it's almost entirely wiring `fuser` callbacks to already-built `db`/`store` query functions. Read-write (write cache, backpressure, persist pipeline) is designed separately in `docs/plans/implemented/06-fuse-mount-readwrite.md`, not implemented here.

Platform scope: Linux only (via the `fuser` crate, which talks to `/dev/fuse` directly and shells out to `fusermount`/`fusermount3` for the privileged mount syscall - both confirmed present in this environment). Matches every other part of this Rust port, which has no Windows-specific code anywhere. Cross-platform (WinFSP) support is out of scope, not attempted - see the read-write plan's context for why reusing `fuser` there isn't viable either.

---

## 0. Quick fix: `list <file>` missing mtime

`cli/src/format.rs`'s `print_file_info(path_label, name, size)` is shared by `stats <path>` and `list <path>` for the file case, and never prints a timestamp - unlike `list`'s directory-listing path (`print_entry` in `list.rs`), which does. Add a `time_millis: i64` parameter to `print_file_info`, print it on a line (matching the format `list`'s directory case already uses), and update both call sites (`list.rs`, `stats.rs`) and their tests. One small commit, first, independent of everything else in this plan.

---

## 1. Phase 1: read-only FUSE mount

### New pieces

- New dependency `fuser` (the standard, actively maintained Rust FUSE binding - implements the low-level `/dev/fuse` kernel protocol directly, no libfuse linkage needed for the protocol itself).
- New CLI subcommand `backup mount <mountpoint>` (`cli/src/mount.rs`), always read-only in this phase (a `--read-only`-shaped flag isn't needed yet since there's no other mode to select between; a read-write phase would introduce the write path and the flag to choose).
- No `db`/`store` schema or API changes needed at all - every FUSE callback in this phase is answerable with functions that already exist: `db::resolve_path`/`db::get_tree_entry`, `db::list_children`, `db::file_size`, `db::ordered_content_chunks`, `store::LongTermStore::read`.

### Inode mapping

FUSE reserves inode `1` for the mount root by convention; our tree root is `tree_entries.id = 0`. Map `fuse_ino = tree_entries.id + 1` / `tree_entries.id = fuse_ino - 1` - a trivial, symmetric shift, applied at the boundary of every callback (not stored anywhere). The FUSE file handle (`fh`) reuses the inode directly, mirroring Scala's simplification (the file's own DB id *is* the FUSE handle, no separate handle-id allocator) - correct here too, since Phase 1 has no per-handle mutable state to key by something else.

### Callbacks implemented (via `fuser::Filesystem`)

| Callback | Backed by |
|---|---|
| `lookup(parent, name)` | `db::find_tree_entry(conn, parent-1, name)` (via `db::resolve_path`'s single-component form, or `find_tree_entry` directly) |
| `getattr(ino)` | `db::get_tree_entry(conn, ino-1)` + `db::file_size` for files |
| `readdir(ino, offset)` | `db::list_children(conn, ino-1)`, paginated via `offset` |
| `open(ino, flags)` | sets `fh = ino`; rejects any write-intent flag (`O_WRONLY`/`O_RDWR`) with `EROFS` since this phase is read-only |
| `read(ino, fh, offset, size)` | `db::ordered_content_chunks` for the entry's `content_id` + `store::LongTermStore::read` for the requested byte range, reusing the same range-mapping logic `restore`/`check` already have |
| `release` | no-op (nothing to flush in a read-only mount) |
| `statfs` | minimal/approximate values - Scala's own Linux implementation is a no-op here too, per research; not worth over-building |
| everything else (`write`, `mkdir`, `unlink`, `rename`, `create`, `truncate`, `setattr`'s write-affecting fields, etc.) | not implemented; `fuser`'s default trait methods already return `ENOSYS`, and/or explicit `EROFS` where a clearer signal helps (matches Scala's per-operation `EROFS` gate on the write-side callbacks) |

Permissions/uid/gid reported in `getattr`: read-only mode bits (matching Scala's Linux convention), owner reported as whoever's making the request (`req.uid()`/`req.gid()` from `fuser`'s `Request`, same as Scala reads from the FUSE call context) - there's no real multi-user ownership model here, this is cosmetic (FUSE without the `default_permissions` mount option doesn't have the kernel enforce these bits anyway; the mode shown is informational for tools like `ls -l`).

### Mount/unmount lifecycle

Blocking mount call (matches Scala's blocking `fs.mount(..., true, ...)` call). No signal handling added for this phase: unmounting is triggered externally (`fusermount -u <mountpoint>` / `umount`, run from another terminal), exactly like the Scala tool relies on jnr-fuse's own teardown rather than an explicit `sys.addShutdownHook` - no evidence of one there either. Mount-point validation before mounting: must already exist as a directory and be empty (matches Scala's non-Windows check).

### Sequencing (commits)

1. `list <file>` mtime fix (§0).
2. Add `fuser` dependency; `cli/src/mount.rs` skeleton (CLI arg parsing, mount-point validation, inode mapping helper, mount wiring with a `Filesystem` impl that only handles `lookup`/`getattr` for now) + tests.
3. `readdir`.
4. `open`/`read`/`release` (the actual file-content path).
5. `statfs` + permission/uid/gid polish + README section.
6. Manual smoke test: mount a repo with real content, `ls`/`cat`/`stat` through the mountpoint, compare against `list`/`stats`/direct file reads, unmount.

---

## Verification

- `cargo fmt --check && cargo clippy -- -D warnings && cargo test` workspace-wide after every commit.
- Unit tests per callback against a temp repo with seeded rows (same pattern as every other command in this codebase).
- A real mount/unmount integration test, given `/dev/fuse` is accessible in this environment (confirmed: `crw-rw-rw-`, `fusermount`/`fusermount3` present) - mount into a temp directory, perform file ops, unmount, in-process.
- Manual smoke test: build a repo with `store`, mount it, `ls`/`cat`/`stat` through the mountpoint from a shell, compare byte-for-byte and metadata against the source and against `list`/`stats` output, then unmount.
