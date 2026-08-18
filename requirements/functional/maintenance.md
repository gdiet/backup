# Maintenance

### REQ-MAINTENANCE-001: Point-in-time metadata backup
Status: draft
Importance: must

The repository's metadata can be backed up to a self-contained, timestamped snapshot without
blocking, or being blocked by, ongoing use of the repository.

Rationale: metadata is what makes stored bytes meaningful at all — losing it without a way back
would make the actual stored content effectively unusable, even though the bytes are still there.

### REQ-MAINTENANCE-002: Metadata restore
Status: draft
Importance: must

The repository's metadata can be restored from a prior backup, recovering the repository to that
backup's point in time.

Rationale: a backup that cannot be restored provides no actual safety — this is the other half of
REQ-MAINTENANCE-001.

### REQ-MAINTENANCE-003: Metadata compaction
Status: draft
Importance: should

Space freed inside the metadata store by past deletions can be compacted back down without
requiring a long exclusive lock on the repository.

Rationale: metadata churn (files added and removed over years of use) should not leave the metadata
store permanently bloated, and reclaiming that space should not cost meaningful downtime.

### REQ-MAINTENANCE-004: Concurrent-access safety
Status: draft
Importance: must

Operations that physically allocate or relocate stored bytes are mutually exclusive with each
other against the same repository, preventing the data corruption that would result from two such
operations running concurrently. Read-only operations remain unaffected and unblocked throughout.

Rationale: users should be able to safely run a query or a check at any time — including while a
backup is in progress — without needing to reason about internal timing to know it is safe.

### REQ-MAINTENANCE-005: Local usage logging
Status: draft
Importance: could

Every operation run against a repository is recorded locally (what ran, which options were used —
not argument values like paths or ids), purely for later review of how the repository has
actually been used.

Rationale: knowing which features actually see real use, well after the fact, is what makes it
possible to confidently simplify or remove the ones that do not.
