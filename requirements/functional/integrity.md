# Integrity

### REQ-INTEGRITY-001: Verify stored content against metadata
Status: agreed
Importance: must

Content can be verified on demand, scoped to the whole repository or a specific path, at either of
two depths: a quick check that stored data exists and has the recorded length, or a thorough check
that also re-reads and re-hashes the content to confirm it still matches. Either depth reports
missing/short data, length mismatches, and hash mismatches as distinct problem categories, with a
result usable at the file level. Cost scaling with respect to deduplication is covered separately
by REQ-PERFORMANCE-001 in
[`non-functional/performance.md`](../non-functional/performance.md).

Rationale: a backup system's core promise is that stored data is still what it claims to be - that
needs a way to actually check it, not just assume it. A thorough, hash-verifying check reads every
byte back from disk, which can take a long time over a large repository. That cost should be chosen
deliberately, not paid every time just to confirm data physically exists. Making thoroughness a
depth on one capability keeps "is my data OK" a single question with an adjustable confidence
level, rather than two separately named tools whose difference needs explaining.

### REQ-INTEGRITY-002: Remediate files affected by missing data
Status: agreed
Importance: should

A file affected by missing or short stored data can be soft-deleted in bulk, optionally leaving a
zero-byte placeholder with the original's last-modified time in its place, so the tree reflects the
loss without requiring manual cleanup entry by entry. This applies only to that one problem
category — content that is present but does not match its recorded hash or length is a different
kind of problem and is not remediated automatically here.

Rationale: once missing data is confirmed unrecoverable, the tree should be able to reflect that
state directly rather than continuing to reference content that is not there. Only missing data is
something this system can safely act on by itself; a mismatch where wrong-but-present data was
found needs investigation, not an automatic response.

### REQ-INTEGRITY-003: Automatic safe-to-purge tracking
Status: agreed
Importance: must

The system always knows, accurately and without requiring manual accounting or a separate repair
step in normal operation, which stored content is no longer referenced by any tree entry and is
therefore safe to purge.

Rationale: this knowledge silently drifting out of sync with what is actually referenced would risk
purging content still in use. It could also mean never being able to reclaim content that is
genuinely unused. Short of either extreme, it would force a purge operation to verify references
itself before trusting anything it deletes, turning what should be a cheap, already-known lookup
into a slow, full-repository scan every time it runs. All three are unacceptable when users are
trusting the system to keep their data safe.
