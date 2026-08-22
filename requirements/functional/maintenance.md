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

Only one repository-mutating operation runs against a repository at a time: if such an operation is
already in progress, a second one is refused rather than allowed to proceed. Read-only operations
remain unaffected and unblocked throughout, regardless of whether a mutating operation is currently
running.

Rationale: an operation that physically allocates or relocates stored bytes, or that
wholesale-replaces the metadata store, would corrupt the repository if it ran concurrently with
another such operation. An operation that only writes metadata would not — the database engine's
own cross-process locking already serializes those safely — but requiring the same exclusivity for
every mutating operation, without exception, is simpler for users to reason about than a rule that
depends on which specific kind of write is involved, and does not rely on that safety margin
holding indefinitely as the code around it changes. An occasional unnecessary refusal is a far
smaller cost than the alternative.

### REQ-MAINTENANCE-005: Local usage logging
Status: draft
Importance: could

Every operation run against a repository is recorded locally (what ran, which options were used —
not argument values like paths or ids), purely for later review of how the repository has
actually been used.

Rationale: knowing which features actually see real use, well after the fact, is what makes it
possible to confidently simplify or remove the ones that do not.

### REQ-MAINTENANCE-006: Wait instead of immediate refusal
Status: draft
Importance: should

Instead of being refused immediately, a caller can specify a duration to wait for a conflicting
operation (see REQ-MAINTENANCE-004) to finish before being refused.

Rationale: retrying manually after being refused is a routine annoyance for a caller that already
knows the conflicting operation will finish soon (e.g. a scheduled backup script) — waiting once,
up to a bound the caller controls, avoids that without weakening the safety REQ-MAINTENANCE-004
provides.

### REQ-MAINTENANCE-007: Stale-backup warning after reclamation or compaction
Status: draft
Importance: must

Restoring a metadata backup warns the user if any operation that may have physically relocated or
invalidated stored bytes — space reclamation and compaction in particular — has run against the
repository since that backup was taken, because the backup's tree entries may then resolve to the
wrong physical bytes.

Rationale: without this warning, restoring an outdated-but-not-obviously-outdated backup after such
an operation would silently read back wrong bytes for some entries, with no symptom pointing at the
cause — this directly undermines the trust REQ-MAINTENANCE-001/002 exist to provide in the first
place, not just a convenience gap.
