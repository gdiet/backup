# Mount: opt-in `--zero-fill-missing` flag

**Status**: plan complete, ready to implement.

## Context

`mount`'s read path (`cli/src/mount.rs`'s `read_persisted`) already turns
missing/short store data into an explicit `Errno::EIO` for exactly the
affected byte range, never silently zero-filling - confirmed correct/
intentional behavior, and now also addressable in bulk via `backup
problems`/`fix-problems` (`docs/plans/implemented/problem-files.md`).

Raised separately: a normal user (or an app like a JPEG viewer) can't do
anything useful with a partial I/O error, but might still get real value
out of a file that reads as mostly-correct-with-some-zeros - e.g. a JPEG
missing its last few KB may still decode and display up to where the data
stops. Discussed whether to make zero-fill the *default* and require an
explicit flag for strict `EIO`; decided against that (see below) in favor
of the reverse: `EIO` stays the default, zero-fill is opt-in.

## Decision: opt-in, not default

A backup/dedup tool's core value is data fidelity. A default that silently
returns zeros for missing bytes means anything reading through the mount -
a script, a virus scanner, a sync tool, a person - has no signal that what
it just read is incomplete; it looks like a valid, if oddly-zero-padded,
file. Copying it elsewhere then quietly propagates data loss as if it were
real data. Keeping `EIO` as the default preserves "you can always tell
something's wrong here"; the opt-in flag serves exactly the case that
motivated this (a user who already knows a file is affected, e.g. via
`backup problems`, and wants best-effort access anyway) without changing
what happens for everyone else.

## Design

### The zero-filling already exists - it's just discarded

`store::read` (`store/src/lib.rs`) already zero-fills unreadable ranges of
`buf` in place and *additionally* reports `ReadIntegrity::Incomplete` -
that's not new work needed. `read_persisted` currently discards the
already-zero-filled bytes on `Incomplete` and returns `Err(Errno::EIO)`
instead of using them:

```rust
let (bytes, integrity) =
    read_chunk_bytes(&conn, &self.data_store, chunk.chunk_id, chunk_len)
        .map_err(|_| Errno::EIO)?;
if let ReadIntegrity::Incomplete { .. } = integrity {
    return Err(Errno::EIO);
}
```

So the flag's actual effect at the read site is small: when enabled, skip
the early `Err` and let the (already zero-filled) `bytes` flow through to
the rest of the function as normal.

### Flag

New `MountArgs` field, alongside the existing `read_write`/
`write_cache_mb`/etc. flags in `cli/src/mount.rs`:

```rust
/// Serve missing or short store data as zero bytes for exactly the
/// affected range, instead of failing that read with an I/O error. Off by
/// default (see docs/plans/mount-zero-fill-missing.md for why) - a reader
/// has no way to tell zero-filled bytes from real ones once this is on,
/// so only turn it on when you specifically want best-effort access to a
/// file you already know is affected (e.g. via `backup problems`) rather
/// than a hard failure.
#[arg(long)]
zero_fill_missing: bool,
```

Threaded into `Inner`/`DedupFs` the same way `read_only` already is (a
plain `bool` field set once at mount time, read in `read_persisted`).

### Startup notice

Print one line at mount startup when the flag is set (mirroring how
read-write mounts already print their own startup line), so running with
degraded integrity guarantees is never silently invisible even if nobody
reads `--help`:

```
zero-fill-missing enabled: files with missing or short store data will
read as zero-filled instead of failing with an I/O error
```

### Scope

Mount only. `backup check`/`backup problems`/`backup restore` already do
the appropriate thing for their own contexts (report/list/skip-with-
warning) and don't need an equivalent flag - the motivating case
(view-what-you-can in an app that reads through the filesystem) is
specific to mount.

## Verification checklist

- `cargo fmt --check && cargo clippy --workspace --all-targets -- -D
  warnings && cargo test --workspace && cargo doc --no-deps --workspace`.
- New test(s) in `cli/src/mount.rs`: a read overlapping missing store data
  returns `Ok` with zero-filled bytes when `--zero-fill-missing` is set,
  and still returns `Err(Errno::EIO)` when it isn't (regression coverage
  for the existing default).
- Update `README.md`'s `## Mount` section with the new flag.
- Once shipped, move this file under `docs/plans/implemented/`.
