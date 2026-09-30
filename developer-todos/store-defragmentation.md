# On-demand store defragmentation (REQ-STORAGE-005) is not implemented yet

**Noted**: 2026-09-29, while validating the Scala-repository migration tool against real data and
explaining `dfs reclaim`'s actual behavior.
**Size**: medium to large - confirm the desired design with the developer before starting (at
minimum: how relocation is made crash-safe, and how it interacts with REQ-MAINTENANCE-007's
metadata-backup staleness window).
**Context**: REQ-STORAGE-005 in `requirements/functional/storage.md` (Status: agreed, not yet
implemented); DESIGN-STORE-003 in `docs/design/byte-store.md` (only covers REQ-STORAGE-004's
allocate/reclaim logic, explicitly not this); `crates/cli/src/reclaim.rs` (REQ-STORAGE-004, gap
bookkeeping only) and `crates/cli/src/db_compact.rs` (REQ-MAINTENANCE-003, SQLite metadata `VACUUM`
only - despite the name, this never touches `data/`).

The developer's own note: they want to look at this themselves soon.

Confirmed empirically this session (not just by reading the code) that no command currently
relocates existing bytes in `data/` or shrinks it: migrated a real Scala test repository, ran `dfs
stats` before and after `dfs reclaim`, and the reported physical size was byte-for-byte identical
(181,646,574 bytes both times) even though `reclaim` itself reported freeing 65,175 bytes.
`allocation::FreeSpace` (`crates/db/src/allocation.rs`) only ever treats a freed range as available
for a *future* write - REQ-STORAGE-004's gradual reuse - never moves what is already there.

REQ-STORAGE-005 itself describes the missing piece: relocate still-live content into a contiguous
layout and shrink the backing files to match, actually returning freed space to the filesystem/OS.
Its own rationale already flags the one real design risk to resolve deliberately, not by accident:
relocation changes the physical position of content that is still live, which is exactly the kind of
change REQ-MAINTENANCE-007 (in `requirements/functional/maintenance.md`) says can make an
already-taken metadata backup stale/unusable for recovery - worth reading that requirement's own
reasoning before designing this, not just DESIGN-STORE-003's allocator logic.
