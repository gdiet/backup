# `real_mount_concurrent_handles_converge_toward_the_per_handle_halving_formula` fails: spill entry is a directory, not a file

**Why parked**: out-of-scope finding - came up as a pre-existing test failure while verifying an
unrelated fix (a `pending_files.rs` release-without-matching-open crash) on
`rust-deleted-view-synthetic-roots`. Confirmed via a negative control (`git stash` back to the
unmodified branch HEAD, same failure) that this is not caused by that fix or anything else done in
that session - genuinely pre-existing on this branch already.
**Size**: medium (confirm with the user before starting) - needs actually tracing what changed in
`write_cache.rs`'s spill layout, or in how this test's own spill directory is set up, since the
symptom (a spill directory entry that is itself a directory) does not match either side's expected
shape at first glance.
**Opened**: 2026-09-28, by Linux/WSL2 session (real `/dev/fuse` access available here).
**Context**: `crates/cli/src/dedup_fs.rs`'s
`real_mount_concurrent_handles_converge_toward_the_per_handle_halving_formula` test (around line
1500 at the time of writing); `crates/cli/src/write_cache.rs`'s `SpillFile`.

## The finding

```
thread 'dedup_fs::tests::real_mount_concurrent_handles_converge_toward_the_per_handle_halving_formula' panicked at crates/cli/src/dedup_fs.rs:1509:70:
read spill file: Os { code: 21, kind: IsADirectory, message: "Is a directory" }
```

The test opens two real handles through a real libfuse3 mount against a small shared memory
budget, expects exactly one handle's write cache to spill to disk, and reads the spill file's
content directly to confirm which handle it belongs to (by content, `0xAA` vs `0xBB` fill bytes).
`std::fs::read_dir(spill_dir.path())` still finds exactly one entry (the `assert_eq!` just above
the failure does not fire), but that entry is now a *directory*, not a plain file - `std::fs::read`
on its path fails with `IsADirectory` instead of returning bytes.

This test (and the write_cache spill mechanism itself, DESIGN-MOUNT-019) predates the
`rust-deleted-view-synthetic-roots` branch's own feature work (DESIGN-MOUNT-020..024, the
`[show-deleted]`/`[purge-deleted]` synthetic roots) - not yet established whether that work touched
`write_cache.rs`'s spill layout at all, or whether this is an unrelated regression from something
else on this branch, or even a pre-existing bug from further back that this test just never caught
before due to some other now-changed timing/ordering.

## What the next attempt should do

1. Check `git log -p -- crates/cli/src/write_cache.rs` on this branch for any change to
   `SpillFile`/how a spilled handle's on-disk representation is created, to see whether this is a
   deliberate (but test-breaking) change or an accidental regression.
2. If nothing there explains it, add temporary diagnostics (`ls -la` equivalent via
   `std::fs::read_dir`'s own `file_type()`, or a debug print of the entry's full metadata) to see
   what the unexpected directory actually contains - a stray temp/lock directory the spill
   mechanism itself creates alongside the real spill file it also should be creating, or something
   the real mount/`dfs mount` test harness now leaves behind in the same directory it did not
   before.
3. Fix (or update the test's own assumption) accordingly, verified red/green per `AGENTS.md`'s
   debugging discipline.

## Resolution

Done 2026-09-30, Linux/WSL2 session. The cause was a stale test, not a product bug. Commit
`e2329915` moved every spill file into one dedicated subdirectory (`write_cache::SPILL_SUBDIR`,
`dfs-write-cache`) directly under the spill directory. The test still assumed the spill file sat
directly in the spill directory. The single entry it found was that subdirectory, hence
`IsADirectory`. The test now asserts that exactly one entry exists at the top level and reads the
spill file from inside it. The test passes again and the write cache itself is unchanged.
