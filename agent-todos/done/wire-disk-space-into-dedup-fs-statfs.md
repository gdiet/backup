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

## Done

`DedupFs` now stores its own `data_dir` (`db::data_dir(repo_root)`, computed once in `new` - the
same path `mount.rs` already passes to `store::ByteStore::new`), and `statfs`
(`crates/cli/src/dedup_fs.rs`) calls `mountfs::disk_space(&self.data_dir)`, converting the
returned (total, available) byte counts to block counts via the existing `block_size = 512`.
`disk_space`'s own error (never expected in practice - `data/` is created by `create-repo` and
always exists once a repository is open) maps to `Errno::EIO`, consistent with this file's
existing style for unexpected I/O failures, rather than silently degrading back to zero.

Added `statfs_reports_the_repository_data_dir_s_real_free_space`, a unit test against the existing
`setup()` fixture (a real temp-directory repository, so `disk_space` genuinely succeeds). Verified
red (temporarily reverted to the old zero-default `statfs` body, confirmed the test fails with
`blocks=0`) and green again, per `AGENTS.md`'s debugging discipline.

Also verified end-to-end against a real mount, beyond the unit test: rebuilt
`docker/samba-mount/`'s image with this fix and re-ran its own verification session (the same
repository, read-only) - `smbclient` now reports real free space for the share
(`1055762868 blocks of size 1024. 766920216 blocks available`), matching `df` on the host almost
exactly (`1055762868` total 1K-blocks, `767019480` available at the time of the separate `df`
check moments later - the small available-space drift is expected, not a discrepancy). Not
verified against real Windows Explorer specifically (no Windows/WinFSP access in this session) -
the concrete client this was originally found for, so the "may refuse to write" concern is
resolved in principle (a real, correct free-space figure is now reported) but not re-confirmed
against that exact client.

Full verification suite green (build/fmt/clippy -D warnings/test --workspace, including
`real_mount_*`/doc). `docker/samba-mount/README.md`'s "Known limitations" section (which existed
only for this one entry) is removed, and its "Verification status" section updated to reflect the
fix instead of the original zero-blocks finding.
