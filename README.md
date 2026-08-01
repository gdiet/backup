# Rust Components

This workspace contains Rust crates used by the backup application.

## Crates

| Crate   | Description                                                                          |
|---------|--------------------------------------------------------------------------------------|
| `cdc`   | Content-defined chunking library (rolling-fingerprint based)                         |
| `store` | Sequential on-disk byte store, file format compatible with the Scala `LongTermStore` |
| `cli`   | Deduplicating backup application; builds the `backup` binary                         |

## Usage

All commands take `-r <repo>`/`--repo <repo>` for the repository directory
(default `../backup-repository`). Paths given to the commands below are
repository-relative (`/`-separated), not filesystem paths, unless noted
otherwise.

### Show Repository Statistics

```bash
backup stats
```

Shows repository-wide counts (live/deleted files and directories, distinct
chunks and contents), physical storage size, logical size (the total size as
if nothing were deduplicated), the resulting dedup ratio, and how much of
the physical storage is currently unused gaps left by `reclaim-space` that
a later `store` run hasn't reused yet.

To see statistics for a specific file or directory instead:

```bash
backup stats <path>
```

For a directory, this recursively sums files, subdirectories, and total
logical size under it. For a file, it shows the file's size.

### List Files

```bash
backup list <path>
```

Lists a directory's direct children (not recursive), directories first, then
files, each alphabetically. Files are shown with their size and last
modified time (UTC); directories are shown with a `>` marker. If `path` names
a file instead, shows the same file-information block as `stats <path>`.

### Find Files By Name Pattern

```bash
backup find <pattern>
```

Searches the whole repository (not scoped to a subdirectory) for entries
whose full path contains `pattern`, case-insensitively. `*` matches any run
of characters (including none), `?` matches exactly one character - e.g.
`backup find "jre/bin/java"` matches any path containing that sequence of
path segments, and `backup find "*.log"` matches any path ending in `.log`.
Prints one matching path per line. Exit code follows the `grep` convention:
`0` if anything matched, `1` if the search ran fine but found nothing.

### Check Integrity

```bash
backup check [path]
```

Verifies stored chunk data against the metadata database: for every chunk in
scope, reads its bytes back from the data store and checks its length and
content hash still match what's recorded, distinguishing missing/short data,
length mismatches, and hash mismatches. Also verifies (always for the whole
repository, regardless of `path`) that `ref_count` on chunks and contents
matches a live count of what actually references them - this should never
find anything, since it's already kept in sync by database triggers, so it's
a sanity check rather than an expected source of problems.

`path` is optional; omitted, it checks every chunk in the repository. Given a
file, checks just that file's chunks; given a directory, recurses through
everything under it. Exit code reflects the result: `0` if everything
checked out, non-zero if any problem was found - so this can be used in a
script/CI job, unlike the Scala tool's `check`, which always exits `0`
regardless of what it finds.

### Restore Files

```bash
backup restore <source...> <target>
```

Restores one or more repository paths to a real directory on disk. Each
source keeps its own name as a child of `target` (backing up `a/b` into
target `t` produces `t/b`; restoring `t/b` back to a target `out` produces
`out/b`), the same convention `store` uses for how sources land under a
target. A directory source restores recursively; last modified times are
restored from what was recorded at backup time (permissions and ownership
are not - matching what `store` itself doesn't capture either).

By default, an existing file at a restore destination is left untouched and
reported as a warning rather than overwritten; pass `--overwrite` to replace
it. A per-file or per-directory problem (a permission error, an existing
file without `--overwrite`) is logged and only that entry is skipped - the
rest of the restore still runs to completion.

### Delete A File Or A Directory

```bash
backup del <path>
backup del --recursive <path>
```

Soft-deletes a file, or (with `--recursive`, required - refused otherwise,
even for an empty directory) a directory and everything under it, all with
the same timestamp in one atomic step. Soft-deleted entries stay recoverable
and keep referencing their content until a future `reclaim-space` run
actually purges entries older than its retention window - unlike the Scala
tool this replaces, which has no such grace period.

### Database Backup, Restore, and Compaction

```bash
backup db backup
```

Creates a timestamped, fully self-contained snapshot of the metadata
database under `meta/backups/` via SQLite's `VACUUM INTO` - safe to run
against the live database (it never blocks or is blocked by the single
writer or any readers, and never touches the live file), unlike a plain file
copy of a database that might still be open elsewhere. Each run creates a
new file; nothing is ever overwritten.

```bash
backup db restore <file>
```

Restores the metadata database from a backup, **overwriting the live
database**. `<file>` can be a path, or just a filename to look up under
`meta/backups/`. Doesn't require the current database to be openable first
- this is the recovery path for when it isn't.

```bash
backup db compact
```

Reclaims free pages left behind by past deletions (`del`/`reclaim-space`),
shrinking the database file in place, via `PRAGMA incremental_vacuum`. Cheap
compared to a full `VACUUM`: repositories are created with
`auto_vacuum = INCREMENTAL`, which already tracks freed pages as they
happen, so this doesn't need ~2x disk space or a long exclusive lock the way
a full rewrite would.

### Reclaim Space

```bash
backup reclaim-space [--keep-days N] [--no-backup]
```

Hard-deletes entries soft-deleted more than `N` days ago (default `0` -
everything currently soft-deleted), then removes any `contents`/`chunks`
rows that are now unreferenced. This is the one genuinely irreversible
operation in this tool (`del` alone never is - a soft-deleted entry stays
recoverable until this runs), so it backs up the database first by default;
pass `--no-backup` to skip that.

Does not shrink the on-disk chunk store file itself (it has no
delete/truncate operation), but the byte ranges freed by a purged chunk
*are* tracked and reused: the next `store` run's space allocator reuses
those gaps for new chunks' bytes before appending past the end, so
repeated delete/reclaim/store cycles don't make the store grow without
bound. `stats` reports any gaps not yet reused as free space.

### Mount

```bash
backup mount <mountpoint>
```

Mounts the repository's file tree read-only at `<mountpoint>`, so it can be
browsed and read with ordinary tools (`ls`, `cat`, `cp`, etc.) instead of
`list`/`restore`. Works on both Linux (via real system libfuse3) and
Windows (via [WinFSP]) through the platform-abstracted `mountfs` crate -
see `docs/plans/implemented/05-cross-platform-mount-crate.md` for how.

**Linux**: `<mountpoint>` must already exist and be empty. Blocks until
the filesystem is unmounted from another terminal (`fusermount3 -u
<mountpoint>` or `umount <mountpoint>`). Requires `libfuse3-dev`/
`fuse3-devel` at build time and `libfuse3` at runtime (Debian/Ubuntu: `apt
install libfuse3-dev`; Fedora: `dnf install fuse3-devel`).

**Windows**: requires [WinFSP] to be installed (the runtime is enough - no
SDK/import library needed to build this project). `<mountpoint>` must
*not* already exist - WinFSP creates it itself as part of mounting.
Blocks until the process is stopped (Ctrl+C, closing the console, or
killing it - there's currently no separate unmount command, see the plan
doc's "Windows checkpoint" for why).

[WinFSP]: https://github.com/winfsp/winfsp - WinFsp - Windows File System
Proxy, Copyright (C) Bill Zissimopoulos, used here under its FLOSS
exception to the GPLv3 (see the plan doc for the full exception text).

```bash
backup mount --read-write [--write-cache-mb <n>] [--temp <dir>] <mountpoint>
```

`--read-write` additionally allows structural changes
(`mkdir`/`rm`/`mv`/creating files) and real content writes - off by
default, since a mount is a much larger blast radius for a mistake than
`store`/`restore`. Writes are held in a per-file write cache (RAM up to
`--write-cache-mb`, default 128, then spilling to a temp file) and only
committed to the repository once the write handle closes - persisting
runs on a dedicated background thread via a small bounded queue, not on
the closing call itself, so closing a file returns quickly even against a
slow repository disk; only once several persists are already queued up
does a further close start blocking (see
`docs/plans/implemented/bounded-memory-io-pipeline.md`'s "Mount-specific detail" for
the design). At startup,
`--read-write` refuses to run if `--write-cache-mb` looks large enough,
relative to *currently available* RAM, to risk pushing the machine into
swapping - pass `--allow-swap-risk` to start anyway. `--temp` overrides
where the write-cache spillover directory is created (must already exist
and be writable; defaults to the OS temp directory) - for best
throughput, point it at the fastest disk available, ideally not the same
physical drive as the repository. See
`docs/plans/implemented/06-fuse-mount-readwrite.md` for the full design
and verification notes.

### Migrate A Scala Repository

```bash
backup -r <target-repo> migrate-scala-repo --script <export> --old-data <old-data-dir>
```

One-shot migration of an old Scala `dedup` repository into a Rust one.
`<target-repo>` must already be an initialized (`backup init`), empty
repository - this is not a merge tool. `--script` is the H2 SQL script
export produced by the Scala tool's `fsc db-backup` command (`org.h2.tools
.Script`) - either the zipped script as produced directly, or an
already-unzipped `.sql` file; `--old-data` is the old repository's `data/`
directory (read-only, never modified).

Migrates the *entire* old tree, including soft-deleted history (not just
the currently-active files), so restore-from-history capability is
preserved. Content is genuinely re-chunked and re-hashed through the same
CDC pipeline `store` uses, since Scala dedupes whole files while Rust
dedupes at the chunk level - there's no mechanical translation between the
two metadata models. The final summary reports the headline number this
exists for: how much smaller the migrated repository is than a naive
whole-file copy would have been, thanks to chunk-level dedup finding
sharing Scala's whole-file model never could. See
`docs/plans/implemented/scala-rust-store-migration.md` for the full design
(compatibility findings, staging-database approach, decisions made).

A failure partway through leaves the target repository untouched (the
whole migration runs in one transaction, committed only at the very end) -
recovery is simply re-running the command from scratch, not resuming.

## Development

### Prerequisites

A Rust toolchain (`rustc`/`cargo`, e.g. via [rustup](https://rustup.rs)) on
both platforms. Beyond that:

**Linux**:
- `libfuse3-dev` (Debian/Ubuntu) or `fuse3-devel` (Fedora) - build-time
  header for `mount`'s real-libfuse3 backend; `libfuse3`/`fuse3` itself is
  needed at runtime.
- `pkg-config` - used by `mountfs`'s `build.rs` to find the above.

**Windows**:
- Visual Studio Build Tools (the MSVC linker/`cl.exe` - the usual
  prerequisite for any Rust `*-pc-windows-msvc` build, not specific to
  this project).
- [WinFSP](https://github.com/winfsp/winfsp) installed - required at
  *runtime* for `mount` to work; **not** required at build time (`mountfs`
  resolves its DLL exports dynamically, no SDK/import library needed).

**Occasional, not needed for routine build/test** (both platforms):
- The standalone "Debugging Tools for Windows" (`cdb`/`windbg`, via the
  [Windows SDK installer](https://learn.microsoft.com/windows-hardware/drivers/debugger/debugger-download-tools) -
  select only that component) - for diagnosing a native crash inside
  WinFSP itself, not something routine development needs.
- `bindgen-cli` + `libclang` - only for re-running `bindgen` as a one-off
  sanity check against `mountfs/src/windows/sys.rs`'s hand-written struct
  layouts (see that file's doc comment) after touching them or updating
  the vendored WinFSP headers; `mountfs` itself never depends on `bindgen`
  or `libclang` at build time - this project deliberately hand-writes its
  FFI bindings instead of generating them (see
  `docs/plans/implemented/05-cross-platform-mount-crate.md`). No Windows package manager
  (`winget`/`choco`/`scoop`) was available to install `libclang` there
  when this was last needed, so this was done from WSL instead, cross-
  target-parsing the headers for `x86_64-pc-windows-msvc` - works fine
  even though the crate itself only builds this way on native Windows:

  ```bash
  # WSL (Debian/Ubuntu)
  sudo apt-get install -y libclang-dev
  cargo install bindgen-cli

  # Symlink around clang's MSVC/SDK header lookup needing paths without
  # spaces/parens (only needed because we're parsing for a Windows target
  # from Linux - a real Windows+clang setup wouldn't need this) - adjust
  # the SDK/MSVC version numbers to what's actually installed.
  mkdir -p ~/winsdk-links
  ln -sfn "/mnt/c/Program Files (x86)/Windows Kits/10/Include/<version>/ucrt" ~/winsdk-links/ucrt
  ln -sfn "/mnt/c/Program Files (x86)/Windows Kits/10/Include/<version>/um" ~/winsdk-links/um
  ln -sfn "/mnt/c/Program Files (x86)/Windows Kits/10/Include/<version>/shared" ~/winsdk-links/shared
  ln -sfn "/mnt/c/Program Files (x86)/Microsoft Visual Studio/<edition>/BuildTools/VC/Tools/MSVC/<version>/include" ~/winsdk-links/msvc

  # From the repo root (rust/):
  bindgen mountfs/vendor/winfsp/fuse3/fuse.h \
    --no-layout-tests \
    --allowlist-type "fuse_operations|fuse_file_info|fuse_context|fuse_stat|fuse_statvfs|fsp_fuse_env" \
    -o /tmp/winfsp_bindgen.rs \
    -- -target x86_64-pc-windows-msvc \
       -I mountfs/vendor/winfsp/fuse3 -I mountfs/vendor/winfsp/fuse \
       -isystem ~/winsdk-links/msvc -isystem ~/winsdk-links/ucrt \
       -isystem ~/winsdk-links/um -isystem ~/winsdk-links/shared \
       -DFUSE_USE_VERSION=312 -x c
  ```

  Then diff `/tmp/winfsp_bindgen.rs`'s struct definitions against
  `mountfs/src/windows/sys.rs` by hand (field order, types, and - the one
  real bug this already caught once - bitfield packing).

### Build (debug)

```bash
# from rust/
cargo build
```

### Build (release / prod)

```bash
cargo build --release
```

### Run directly via cargo

```bash
# debug build, runs the cli crate's `backup` binary
cargo run -p cli -- <args>

# release build
cargo run --release -p cli -- <args>

# example
cargo run -p cli -- store --create-dirs source1 source2 target
```

### Tests

```bash
cargo test
```

### Linter (Clippy)

```bash
cargo clippy
```

For stricter checks (recommended before committing):

```bash
cargo clippy -- -D warnings
```

### Formatting

```bash
cargo fmt         # apply formatting
cargo fmt --check # check without modifying (e.g. in CI)
```

### All checks at once

```bash
cargo fmt --check && cargo clippy -- -D warnings && cargo test
```

## Performance

A simple single-threaded throughput benchmark runs the chunker on 7 MB of
pseudo-random data (seed 42) for 10 seconds, calling `next()` and `flush()`
per iteration — matching the Go benchmark exactly.

```bash
cargo run --release --example bench
```

Measured on Intel Core i7-1355U, 12 logical cores, 15 GB RAM, Ubuntu 24.04.4 LTS in WSL2 on Windows (single thread, `target_size_bits=20`, ~1 MB average chunk size):

| Implementation | Throughput |
|----------------|------------|
| Rust           | 2.78 GB/s  |
| Go             | 2.61 GB/s  |

Go benchmark for comparison:

```bash
go test -bench=BenchmarkCdc -benchtime=10s -count=1 ./internal/cdc
```

### Hashing: Rust `blake3` vs. Java `MD5`

This project hashes chunks/content with `blake3` (truncated to 20 bytes -
see `cli::store::Blake3Hasher`); the Scala prototype it replaces hashed
whole files with Java's `MessageDigest("MD5")`. A raw single-threaded
throughput comparison - hashing a 128 MiB buffer in a loop for 5 seconds
(1 second discarded first as warm-up) - not a comparison of the
*algorithms'* cryptographic properties (MD5 is broken and isn't used here
for security), only of how fast each hashes bytes on the same hardware:

```bash
cd scrapbook/blake3-vs-md5
cargo run --release                    # Rust blake3
javac Md5Bench.java && java Md5Bench   # Java MD5 - needs a JDK on PATH
```

Measured on Intel Core i5-6200U (2 cores / 4 threads, 2.30 GHz), Rust
1.97.0, Java 21 (Temurin 21.0.6 on Windows / 21.0.7 on WSL2):

| Platform                                          | Rust `blake3` | Java `MD5` | Ratio |
|----------------------------------------------------|---------------|------------|-------|
| Windows 10 IoT Enterprise LTSC 2021                 | 2.74 GB/s     | 0.56 GB/s  | ~4.9x |
| WSL2 (Debian 12 "bookworm", kernel 6.6.87.2)        | 2.60 GB/s     | 0.55 GB/s  | ~4.7x |

`blake3` is roughly 5x faster than Java's `MD5` on this hardware, on both
platforms - consistent with `blake3`'s SIMD-friendly, tree-mode design
(parallelizable internally even within this single-threaded benchmark)
versus MD5's much older, purely serial construction. Benchmark source in
`scrapbook/blake3-vs-md5/` (its own standalone Cargo project, not part of
the `rust/` workspace or `cargo test`/CI - a one-off comparison, kept for
reference; see its own `README.md` for more detail).
