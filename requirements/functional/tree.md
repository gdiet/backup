# Tree

### REQ-TREE-001: Hierarchical namespace
Status: agreed
Importance: must

Files and directories are organized in a single hierarchical tree per repository, addressed by
path, independent of how their content is physically stored.

Rationale: a familiar filesystem-shaped namespace is what makes the repository browsable,
scriptable, and mountable with ordinary tools.

### REQ-TREE-002: Soft delete
Status: draft
Importance: must

Deleting a file or directory marks it deleted rather than immediately destroying it; a deleted
entry still counts toward the storage it references and stays recoverable until an explicit,
separate space-reclamation step actually purges it.

Rationale: deletion is very often a mistake, or a decision reversed later — a two-step delete
turns an accidental or premature deletion into a recoverable event instead of a permanent loss,
without requiring a restore from an older backup.

### REQ-TREE-003: Deletion history and recovery
Status: draft
Importance: must

A deleted entry can be listed (with when it was deleted) and either reactivated back into the live
tree, or have its content restored directly to disk without first reactivating it in the
repository.

Rationale: recovering a deleted file should not require restoring the whole repository to an
earlier point in time, and not every recovery should require touching the repository's live state
first — sometimes just getting the bytes back is enough.

### REQ-TREE-004: Independent history per deletion
Status: draft
Importance: should

If the same path is deleted and a new entry created at that path again (possibly repeatedly), each
deletion is kept as its own distinct, individually recoverable history entry rather than being
overwritten by the next one.

Rationale: without this, deleting-and-recreating the same path would silently lose recoverability
of everything but the most recent deletion at that path.
