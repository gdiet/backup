# Restore

### REQ-RESTORE-001: Restore to the real filesystem
Status: agreed
Importance: must

One or more repository paths can be restored to a real directory on disk, preserving directory
structure and each file's recorded last-modified time.

Rationale: a backup that cannot be restored is not a backup.

### REQ-RESTORE-002: Restore a deleted entry directly
Status: draft
Importance: should

A soft-deleted entry can be restored straight to disk by its history-entry identity, without first
reactivating it in the repository.

Rationale: getting the bytes of something that was deleted should not require changing the
repository's live state first, especially when the goal is a one-off look at old content.

### REQ-RESTORE-003: Fail loudly on corrupted or missing content by default
Status: draft
Importance: must

If content being restored is missing, incomplete, or (when verification is requested) does not
match its recorded hash, restoring that item fails rather than silently producing incorrect bytes
on disk. Best-effort output (e.g. zero-filling the missing range, or keeping mismatched content
anyway) is available, but only as an explicit, deliberate opt-in.

Rationale: silently writing corrupted-looking-valid data is worse than failing visibly — a
restored file that looks fine but is silently wrong defeats the purpose of restoring at all.

### REQ-RESTORE-004: Non-destructive toward existing files by default
Status: draft
Importance: must

Restoring never overwrites a file that already exists at the destination unless explicitly told
to.

Rationale: a restore is often run to compare against or supplement existing files, not necessarily
replace them — silently overwriting local changes would be a surprising and potentially costly
default.
