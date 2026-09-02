# Directory Modification Time: Touch Mechanism

The mount's ongoing write path (REQ-MOUNT-003 in
[`../../requirements/functional/mount.md`](../../requirements/functional/mount.md), REQ-TREE-005
in [`../../requirements/functional/tree.md`](../../requirements/functional/tree.md)) and a
directed import (REQ-INGEST-005 in
[`../../requirements/functional/ingest.md`](../../requirements/functional/ingest.md)) need
different behavior when a directory's set of entries changes. The mount touches the parent
directory's modification time on every individual structural change, matching live filesystem
semantics a client could observe mid-sequence. An import populating a new subtree instead sets
each directory's modification time once, directly, to the source's own value, once that
directory's own children are all in place - never touching it once per child along the way.

## Decision: application-level touch, not a database trigger
Status: decided

The mount's structural-change touch is implemented (`crates/db/src/tree.rs`'s `touch`, called from
`mkdir`/`settle_file`/`rename`); the import's bulk-population path this also governs does not exist
yet.

The "touch the parent directory's modification time on a structural change" behavior is
implemented as explicit application code in the mount's write-path handlers, not as an
unconditional database trigger firing on every `tree_entries` insert/delete/rename.

A trigger would fire identically for every caller, including a directed import populating
potentially thousands of entries under one directory - there is no clean way for one specific
caller to opt out of an unconditional trigger without a workaround at least as awkward as not
using a trigger at all (e.g. a session-scoped flag its `WHEN` clause would need to check).
Application code naturally distinguishes the two paths instead: the mount's structural-change
handlers call the touch function, the import's bulk-population path does not, setting each
directory's final modification time directly once instead.

This also avoids real, avoidable write amplification specifically for the import case. Without it,
populating a directory with many entries would issue one wasted intermediate touch per entry
before the final explicit override. That would also be the wrong observable behavior in the first
place (REQ-INGEST-005).
