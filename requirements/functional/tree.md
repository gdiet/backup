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
separate space-reclamation step (REQ-STORAGE-004 in [`storage.md`](storage.md)) actually purges it.

Rationale: deletion is very often a mistake, or a decision reversed later — a two-step delete
turns an accidental or premature deletion into a recoverable event instead of a permanent loss,
without requiring a restore from an older backup.

### REQ-TREE-003: Deletion history and recovery
Status: agreed
Importance: must

A deleted entry can be listed (with when it was deleted) and either reactivated back into the live
tree, or have its content restored directly to disk without first reactivating it in the
repository. What display path a deleted directory is listed under, especially once an ancestor is
later renamed or moved, and whether deleting a directory requires all its children to already be
deleted, are both open - see "Deleted-directory display path and non-cascading delete" in
[`../open-questions.md`](../open-questions.md).

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

A write to a file's content becomes visible to a different process only once it is complete — a
concurrent read from another process sees the file's last complete state (its content before this
write began, or nothing yet, if the file did not exist before), never a partial, in-progress
result. (Whether a second read handle within the same mount session sees the write's in-progress
state, or also only its last complete state, is left to the mount's own implementation — either is
acceptable.) The same holds across a crash or hard kill interrupting a write: once the system is
back, the affected file's content is either the complete state from before the interrupted write
began, or the complete state the write would have produced had it finished — never a partial, torn
result of the interruption. Content still being written when such an interruption happens is not
required to survive it — only that whatever remains visible afterward, to a different process, is
never partial.

Rationale: a backup archive silently containing truncated, corrupted-looking content — whether
glimpsed mid-write by a concurrent process or left behind by an unclean shutdown, and either way
indistinguishable from a file that genuinely only ever had that content — would undermine trust in
the archive far more than a concurrent reader seeing slightly stale (but always complete) content,
or losing work still in progress when power was cut.

### REQ-TREE-007: Symbolic link support
Status: agreed
Importance: could

A tree entry can be a symbolic link - its content is the path it points to, not chunked file data -
stored and restored alongside ordinary files and directories, distinct from both in kind.

Rationale: symbolic links are a common, if not universal, part of real filesystem trees - a
typical Linux JDK/JRE installation alone commonly contains dozens of them, some pointing to
locations outside the tree being backed up entirely; supporting them keeps a backed-up tree
faithful to what was actually on disk, instead of silently skipping them, following them to
whatever they currently point at, or otherwise altering them on the way in.

Until implemented, encountering a symbolic link during ingest is treated the same as a per-item
failure (REQ-INGEST-004 in [`ingest.md`](ingest.md)) - skipped with a warning, never silently
dereferenced or duplicated into the backup, and never causing the whole run to fail.
