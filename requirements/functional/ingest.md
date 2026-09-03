# Ingest

Ingest covers directed, one-shot import of external filesystem content into the repository —
explicit source paths copied to a target path, as a discrete operation. This is distinct from
writing into the repository through a mounted view (see REQ-MOUNT-003 in
[`mount.md`](mount.md)), where the repository behaves like an ordinary filesystem for ongoing,
ad-hoc reads and writes rather than a bulk copy step.

### REQ-INGEST-001: Store files and directories into the repository
Status: agreed
Importance: must

One or more files/directories from the real filesystem can be copied into the repository, with
their content deduplicated along the way.

Rationale: bringing content in from the real filesystem is what gives every other operation —
restoring, browsing, checking, mounting — something to work with.

### REQ-INGEST-002: Per-directory exclusion rules
Status: agreed
Importance: should

A source directory can declare files and subdirectories to exclude from being stored, via a
`.backupignore` rule file placed in that directory. Each non-empty, non-comment (`#`-prefixed) line
is one rule, matched against entries directly inside that directory.

A rule can span multiple `/`-separated segments (e.g. `log/*.log`). Each segment but the last names
a subdirectory to descend into first, so the remaining segments apply only inside it rather than
everywhere. A segment supports `*` (any run of characters) and `?` (exactly one character) as
wildcards, matched case-sensitively, the same as storage itself (REQ-MOUNT-010); a literal `.` is
not itself a wildcard. The last segment matches an entry by name: without a trailing `/`, a file or
a directory of that name; with a trailing `/`, a directory only. Matching a directory this way
excludes it and everything beneath it outright, without descending into it at all.

A completely empty `.backupignore` file excludes the directory that holds it, and everything
beneath it, in its entirety - without needing a rule inside it for that.

Rationale: not everything under a source tree is worth backing up (caches, temp files, logs), and
that decision is naturally scoped to the directory it applies to rather than a single
repository-wide configuration. Matching a bare name against both files and directories follows the
same convention `.gitignore` already uses, familiar to most callers who would write these rules. A
trailing `/` then narrows an existing match to directories only, rather than being the only way to
match a directory at all. An empty rule file as a whole-directory shorthand avoids writing an
otherwise-redundant single wildcard rule for the common case of excluding an entire subtree
outright.

### REQ-INGEST-003: Accelerated re-ingest via reference
Status: agreed
Importance: should

An earlier backup of largely-the-same source tree can be supplied as a reference; a source file
matching a same-named, same-size, same-modified-time file under the reference is linked to that
existing content without re-reading, chunking, or hashing it.

Rationale: even with perfect deduplication, an unchanged file still costs a full read on every
backup run just to discover that it deduplicates — for the common case of a mostly-unchanged tree
backed up repeatedly (e.g. nightly), skipping that discovery cost entirely is a large, practical
speed difference.

### REQ-INGEST-005: Preserved timestamps
Status: agreed
Importance: must

A stored file's or directory's modification time reflects the source's own modification time at
the moment it was read, not when the import ran.

Rationale: REQ-INGEST-003's unchanged-file detection depends on comparing against real source
modification times; and a directory populated by import would otherwise end up showing only "when
the import happened" (REQ-TREE-005 in [`tree.md`](tree.md)), silently losing the source's own
history the moment it is imported.

### REQ-INGEST-004: Resilience to per-item failures
Status: agreed
Importance: must

A source file or directory that cannot be read (a permission error, something that disappears
mid-run) is logged as a warning and skipped; the rest of the run completes rather than aborting. A
caller (script or tool) can tell the three possible outcomes of a run apart afterward - complete
success, success with some items skipped as warnings, or abort on an error - without having to
parse log output to do so.

The distinction is which side fails. A *source*-read failure - the file being backed up cannot be
read - is exactly the per-item, skip-and-continue case above. A *backend* failure - writing
already-read content into the repository fails (the store write itself, not the source read) - is
not: it triggers the third outcome instead, an immediate, controlled abort of the whole run, not a
per-item skip.

Rationale: a single unreadable file (a locked file, a permissions edge case) should not be able to
prevent backing up everything else that is readable, but a caller automating ingest still needs a
reliable way to notice that some items were silently skipped, not just a human reading the log. A
backend failure gets the opposite treatment because it is not expected to be specific to the one
item being processed when it was discovered - continuing to feed more items into a repository that
just failed to accept a write risks repeating, not containing, the same failure.

### REQ-INGEST-006: Reference validated against sources before use
Status: draft
Importance: should

When a reference (REQ-INGEST-003) is supplied, its own top-level contents are compared against the
ingest sources' own top-level contents before the reference is trusted for accelerated matching; if
too few entries correspond between the two (the exact threshold is not yet decided), the run aborts
instead of silently proceeding. An explicit override lets a caller force the reference through
regardless of this check.

Rationale: REQ-INGEST-003's whole benefit disappears silently if the supplied reference does not
actually correspond to what is being backed up. Every source file then simply falls back to a full
read, one at a time, with no signal that anything is wrong until the run is noticed to have taken
far longer than expected. Catching a plainly mismatched reference immediately turns a silent
performance regression into an actionable error at the point a caller can still do something about
it.

### REQ-INGEST-007: Templated target paths for repeated runs
Status: agreed
Importance: could

A target path passed to ingest can contain a placeholder resolved against the current date/time at
run start (e.g. a `[yyyy-MM-dd]`-style segment), so a scheduled or repeated run writes into a fresh,
self-naming location each time without the caller precomputing and passing a fully-resolved path
itself.

Independently, each path segment can be marked with one of three existence requirements: required
to already exist (the default), created on demand if missing and otherwise reused as-is, or
required to be freshly created, failing if the segment already exists. Marking a segment with
either creatable form also makes every segment below it creatable by default, so a caller need not
mark each level of a multi-segment templated path (e.g. year/month/day) individually.

Rationale: a caller automating repeated ingest runs (a nightly backup script, in particular)
otherwise has to compute the same timestamped-path and existence-checking logic itself before every
call. Folding it into the target-path syntax removes a whole category of scripting boilerplate. The
default, required-to-exist behavior catches a typo'd or unexpectedly absent parent outright, instead
of silently growing a deep, unintended directory tree. Required-to-be-freshly-created catches the
opposite mistake: silently reusing an already-existing target directory when a run was actually
meant to land in a new one, e.g. after an unexpected re-run or a placeholder that resolved
unexpectedly. Letting a marked segment's creatability carry down to the segments below it avoids
having to mark every level of a multi-segment templated path individually.
