# `store`: reference-based backup

**Status**: not started - this is a stub, not a plan.

`.backupignore` used to be covered here too; it now has its own plan at
`docs/plans/backupignore.md`.

## Reference-based backup (Scala: `reference`/`forceReference`)

Scala's `fsc backup <source> <target> reference=<path> [forceReference=true]`
can skip reading/hashing/chunking a source file entirely if a same-named
file exists at the corresponding path under `reference` (typically the
previous backup run's target directory) with the same size and mtime -
in that case it just links the target tree entry to the reference file's
existing content, no I/O on the source file at all.

This is a real, independent optimization *on top of* content-based dedup:
even with perfect chunk-level dedup (which Rust already has and Scala's
whole-file dedup doesn't), an unchanged file still costs a full read +
CDC chunk + blake3 hash of every byte on every `store` run to *discover*
that it dedupes against what's already stored. A reference check based on
cheap metadata (size + mtime, already available from `stat`) can skip
that entirely for the common case (nightly backup of a mostly-unchanged
tree). Worth quantifying against a real large/mostly-static source tree
before deciding whether it's worth the complexity here - Rust's chunk
dedup already caps the wasted *storage*, this would only save re-read/
re-hash *time*.

`reference` resolves against the DedupFS tree with `*`/`?` wildcards,
picking the alphabetically-last match, and (unless `forceReference=true`)
sanity-checks that source and reference "look similar" (a fuzzy
name-overlap heuristic in `BackupTool.validateReference`) before trusting
it - guards against silently backing up against a typo'd/unrelated
reference.

## Rough shape if/when planned

Needs a new CLI flag, a tree lookup against the reference path, and a
"just copy this tree entry's `content_id`" path through
`apply_backup_batch` that doesn't exist yet (today every `FileBackupRecord`
carries real resolved chunks).
