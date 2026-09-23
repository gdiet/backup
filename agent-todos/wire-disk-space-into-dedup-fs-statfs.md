# Wire `mountfs::disk_space` into `DedupFs::statfs` for real free-space reporting

**Why parked**: out-of-scope finding - came up while porting `docker/samba-mount/` (a `dfs mount`
+ Samba dev utility) from the retired `rust-1st-attempt` branch and verifying it end-to-end,
genuinely outside that task's own scope (a `dedup_fs.rs`/`mountfs` gap, not Docker/Samba-specific).
**Size**: medium (confirm with the user before starting) - the missing piece
(`mountfs::disk_space`) already exists and is unit-tested, but wiring it into `statfs` and picking
which path to query it against needs its own verification against a real client (ideally Windows
Explorer, which is the concrete case this was found for), not just a unit test.
**Opened**: 2026-09-23, by Linux/WSL2 session (real `/dev/fuse` and Docker access available here).
**Context**: `crates/cli/src/dedup_fs.rs::DedupFs::statfs` (currently returns
`StatfsInfo::default()` for `blocks`/`blocks_free`/`blocks_available`/`files`/`files_free`, only
setting `block_size`/`max_name_length`); `crates/mountfs/src/disk_space.rs` (the already-built,
already-tested single-path `statvfs`/`GetDiskFreeSpaceExW` query this should call).

## The finding

`DedupFs::statfs` reports zero total/free blocks (and zero total/free files) for the mount,
regardless of the repository's actual size or the underlying storage's actual free space -
confirmed directly via a real SMB session against a real mount (`docker/samba-mount/`'s own
end-to-end verification, 2026-09-23): `smbclient` reported "0 blocks available" for the share.

A Windows client that checks free space before permitting a save - Explorer does this - may refuse
to write through a `--read-write` mount as a result of the zero figure, even though the mount
genuinely has room. Not yet confirmed directly against real Explorer (this finding came from a
Linux-only verification session), but the zero value is unambiguous either way and plausible to
cause exactly that.

`mountfs::disk_space(path: &Path) -> io::Result<(u64, u64)>` (total, available bytes) already
exists, is unit-tested, and its own doc comment explains it exists specifically to avoid an
"enumerate every mounted filesystem" self-deadlock a prior implementation of this same idea hit
(`docs/design/`... - see the function's own doc comment for the fully policy) - but nothing in
`cli` currently calls it.

## What the next attempt should do

1. Wire `mountfs::disk_space` into `DedupFs::statfs`, querying it against the repository's own
   `data/` directory (matching `disk_space`'s own doc comment's intended usage) rather than the
   mountpoint itself, to avoid the same self-deadlock its doc comment already warns against.
2. Populate `blocks`/`blocks_free`/`blocks_available` from the real total/available byte counts
   (converted via `StatfsInfo::block_size`); decide whether `files`/`files_free` are worth a real
   figure too or can stay at a defaulted/placeholder value (no reported issue for those two
   specifically, unlike the free-space fields).
3. Verify against a real mount (`real_mount_*` test, and/or `docker/samba-mount/` again) that the
   reported figures are sane, and ideally against real Windows Explorer that this actually resolves
   the "may refuse to write" concern above - the concrete case this was found for.
4. Update `docker/samba-mount/README.md`'s "Known limitations" entry once this ships (remove it, or
   note it is resolved).
