# Restore

### REQ-RESTORE-001: Restore to the real filesystem
Status: agreed
Importance: must

One or more repository paths can be restored to a real directory on disk, without mounting the
repository. Directory structure is preserved beneath each restored path, and each file's recorded
last-modified time is preserved on the restored copy. Restoring several paths in one call places
each one directly under the target directory, named by its own last path component.

Rationale: a backup that cannot be restored is not a backup. Restoring should not need a mount's
overhead and platform-specific setup either - the same reasoning REQ-QUERY-001's listing and
REQ-CLI-003's delete already apply.

### REQ-RESTORE-002: Restore a deleted entry directly
Status: agreed
Importance: should

A soft-deleted entry can be restored straight to disk by its history-entry identity - REQ-TREE-009's
`[deleted]` path-based addressing in [`tree.md`](tree.md), not a separate mechanism of its own -
without first reactivating it in the repository.

Rationale: getting the bytes of something that was deleted should not require changing the
repository's live state first, especially when the goal is a one-off look at old content.

### REQ-RESTORE-003: Fail loudly on corrupted or missing content by default
Status: agreed
Importance: must

If content being restored is missing, incomplete, or (when verification is requested) does not
match its recorded hash, restoring that item fails rather than silently producing incorrect bytes
on disk. Best-effort output (e.g. zero-filling the missing range, or keeping mismatched content
anyway) is available, but only as an explicit, deliberate opt-in.

Rationale: silently writing corrupted-looking-valid data is worse than failing visibly — a
restored file that looks fine but is silently wrong defeats the purpose of restoring at all.

### REQ-RESTORE-004: Non-destructive toward existing files by default
Status: agreed
Importance: must

Restoring never overwrites a file that already exists at the destination unless explicitly told
to.

Rationale: a restore is often run to inspect an old version alongside a current one already on
disk - e.g. restoring yesterday's copy of a file next to today's working copy, to diff them or
recover just a piece of the old content by hand - not necessarily to replace what is already there.
Silently overwriting local changes in that case would be a surprising and potentially costly
default.
