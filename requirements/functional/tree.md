# Tree

### REQ-TREE-001: Hierarchical namespace
Status: agreed
Importance: must

Files and directories are organized in a single hierarchical tree per repository, addressed by
path, independent of how their content is physically stored.

Rationale: a familiar filesystem-shaped namespace is what makes the repository browsable,
scriptable, and mountable with ordinary tools.

### REQ-TREE-002: Soft delete
Status: agreed
Importance: must

Deleting a file or directory marks it deleted rather than immediately destroying it; a deleted
entry still counts toward the storage it references and stays recoverable until an explicit,
separate purge (REQ-STORAGE-004 in [`storage.md`](storage.md)) actually removes it.

Rationale: deletion is very often a mistake, or a decision reversed later — a two-step delete
turns an accidental or premature deletion into a recoverable event instead of a permanent loss,
without requiring a restore from an older backup.

### REQ-TREE-003: Deletion history and recovery
Status: agreed
Importance: must

A given tree location has at most one live entry at a time, but any number (0..N) of deleted
entries that previously occupied it - each an independent history entry (REQ-TREE-004), carrying
its own deletion timestamp. A deleted entry's ancestors resolve the same way as any entry's, by
current name and position. So its path reflects whatever has since happened to them - a rename, a
move, or an ancestor also deleted. No separate mechanism records the path as it looked at deletion
time. A deleted entry can be reactivated into the live tree, or have its content restored directly
to disk without first reactivating it.

How deleted entries and their history are displayed and addressed through a specific interface -
including disambiguating same-location repeat deletions in a listing - is covered separately:
REQ-MOUNT-004 in [`mount.md`](mount.md) for the mount, REQ-CLI-007 in
[`cli-commands.md`](cli-commands.md) for a command-line listing. See REQ-TREE-008 for whether a
directory delete cascades to its children.

Rationale: recovering a deleted file should not require restoring the whole repository to an
earlier point in time, and not every recovery should require touching the repository's live state
first — sometimes just getting the bytes back is enough.

### REQ-TREE-004: Independent history per deletion
Status: agreed
Importance: should

If the same path is deleted and a new entry created at that path again (possibly repeatedly), each
deletion is kept as its own distinct, individually recoverable history entry rather than being
overwritten by the next one.

Rationale: without this, deleting-and-recreating the same path would silently lose recoverability
of everything but the most recent deletion at that path.

### REQ-TREE-005: Directory modification time reflects entry changes
Status: agreed
Importance: should

A directory's modification time updates whenever the set of entries directly inside it changes —
an entry created, removed, or renamed — matching ordinary POSIX filesystem behavior. Modifying an
existing entry's own content does not, on its own, count as such a change to its parent directory.
Does not apply to a directory populated by a directed import - REQ-INGEST-005 in
[`ingest.md`](ingest.md) governs that case instead.

Rationale: tools that decide what to re-scan or re-sync based on a directory's modification time
(relied on by `find -newer` and similar tools) would silently miss changes if this did not hold —
mounting the repository as a real filesystem (REQ-MOUNT-001 in
[`mount.md`](mount.md)) should not surprise tools that already assume ordinary POSIX semantics.

### REQ-TREE-006: Atomic visibility of written content
Status: agreed
Importance: must

A write to a file's content becomes visible to a different process only once complete — a
concurrent read from another process sees the file's last complete state (its content before this
write began, or nothing yet if it did not exist before), never a partial, in-progress result.
(Whether a second read handle within the same mount session sees the write's in-progress state, or
also only its last complete state, is left to the mount's own implementation — either is
acceptable.) The same holds across a crash or hard kill interrupting a write: once the system is
back, the affected file's content is either the complete state from before the interrupted write,
or the complete state the write would have produced had it finished — never a partial, torn result.
Content still being written when such an interruption happens is not required to survive it — only
that whatever remains visible afterward is never partial.

Rationale: a backup archive silently containing truncated, corrupted-looking content would
undermine trust in the archive far more than a concurrent reader seeing slightly stale but always
complete content, or losing in-progress work when power was cut. That holds whether the truncated
content is glimpsed mid-write or left behind by an unclean shutdown - either way, it is
indistinguishable from a file that genuinely only ever had that content.

### REQ-TREE-007: Symbolic link support
Status: agreed
Importance: could

A tree entry can be a symbolic link - its content is the path it points to, not chunked file data -
stored and restored alongside ordinary files and directories, distinct from both in kind.

Rationale: symbolic links are a common, if not universal, part of real filesystem trees - a
typical Linux JDK/JRE installation alone commonly contains dozens of them, some pointing outside
the tree being backed up entirely. Supporting them keeps a backed-up tree faithful to what was
actually on disk. The alternative would be silently skipping them, following them to whatever they
currently point at, or otherwise altering them on the way in.

Until implemented, encountering a symbolic link during ingest is treated the same as a per-item
failure (REQ-INGEST-004 in [`ingest.md`](ingest.md)) - skipped with a warning, never silently
dereferenced or duplicated into the backup, and never causing the whole run to fail.

### REQ-TREE-008: Directory delete through the mount requires an empty directory
Status: agreed
Importance: must

Deleting a directory through the mount requires that it have no live (not yet deleted) children -
the mount's delete does not cascade to a directory's contents. See REQ-CLI-003 in
[`cli-commands.md`](cli-commands.md) for a separate, explicit bulk-delete capability outside the
mount.

Rationale: matches ordinary POSIX `rmdir` semantics for the mount specifically, consistent with
REQ-TREE-005's general goal of not surprising tools that already assume them - a mount whose
`rmdir` silently cascaded would violate exactly that expectation. A caller that wants an entire live
subtree gone through the mount already gets that for free via any ordinary recursive tool (`rm -rf`
deletes leaves before their parent directories); REQ-CLI-003 covers the same goal for a caller that
does not want to mount at all.

### REQ-TREE-009: Path-based addressing for a location's soft-deleted entries
Status: agreed
Importance: should

Any directory's soft-deleted children (REQ-TREE-002/003) are reachable by path, through a reserved
segment name, `[deleted]`, directly beneath it - addressable the same way as an ordinary live
child, without a separate lookup step or an opaque identifier standing in for a path. This is the
addressing REQ-RESTORE-002's direct restore and REQ-CLI-004's permanent delete both mean by "a
soft-deleted entry's history-entry identity" - one convention, not two independently designed ones.

If a live entry already occupies that exact name at that location, the live entry always takes
precedence: `[deleted]` continues to name it, as any other live name would, and that location's
soft-deleted children become unreachable through this convention until the live entry is itself
renamed or removed. Nothing about the soft-deleted entries themselves is lost - only this one
specific path-based route to them is unavailable at that location, for as long as the collision
lasts.

Within `[deleted]`, more than one soft-deleted entry can share the same original name (REQ-TREE-004
- delete, recreate, delete again) - each is disambiguated with a deletion-timestamp suffix before
its extension (e.g. `photo [2026-08-22_140414].jpg`; `.env [2026-08-22_140414]` for a dot-file,
which has no splittable extension), falling back to the entry's own id (`photo [42].jpg`) if the
timestamp-suffixed form does not fit a length constraint the calling context imposes, and further
truncating the base name - never the id suffix, the part actually meant to be unique - if even that
does not fit. An entry whose bare name is not shared by any other soft-deleted entry at that same
location is shown as-is, no suffix needed.

Rationale: REQ-MOUNT-004/008 in [`mount.md`](mount.md) needed exactly this same addressing already,
to let a mounted file manager browse and disambiguate a directory's deletion history in place;
REQ-CLI-007 in [`cli-commands.md`](cli-commands.md) needs it too, to let `dfs list`/`dfs restore`
name a specific soft-deleted entry without mounting. Two independently designed addressing schemes
for the same underlying need - one path-shaped inside a mount, one built around an opaque id
outside it - would cost callers a second mental model and this project a second implementation to
keep consistent, for no benefit either design alone does not already provide. A live entry always
winning a name collision, rather than the reserved segment shadowing it, follows directly from
REQ-MOUNT-004's own reasoning for the mount case: a synthetic, addressing-only feature must never
make real, user-created data invisible or unreachable, even in the rare case its owner happened to
pick this exact reserved name themselves.
