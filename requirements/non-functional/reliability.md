# Reliability

### REQ-RELIABILITY-001: The repository as a whole survives abrupt termination
Status: agreed
Importance: must

An abrupt termination does not make the repository as a whole unusable. Abrupt termination means
a kill signal, a power loss, or the removal of the storage device while the software is using it.
To the best of the project's knowledge, no sequence of such events destroys the metadata database
or makes the file tree unreachable. A sequence that is later found to do so is a defect, not an
accepted limitation.

Changes that were not yet durable at the time of the termination may be lost. The next start of
the software finds a consistent repository that reflects an earlier point in time.

Rationale: the metadata database is the only way to reach the stored content. Losing it loses the
whole archive, which is a far larger harm than losing the last few changes. Removable storage in
particular is unplugged during use often enough that this case must be planned for.

### REQ-RELIABILITY-002: A stronger guarantee for individual data is bought only at a bounded performance cost
Status: agreed
Importance: should

Beyond REQ-RELIABILITY-001, an abrupt termination may in rare cases damage a small amount of
individual data. High performance takes precedence over closing this gap completely. A design that
closes the gap in addition to high performance is preferred. Such a design may cost about 10% of
the maximum attainable performance. It does not cost 50% or 90%.

Rationale: the guarantee in REQ-RELIABILITY-001 protects the repository as a whole. The remaining
risk concerns a few data items after a rare event. Paying a large share of the throughput on every
run for that remaining risk is a poor trade for a tool whose usability on slow storage is itself a
goal (REQ-PERFORMANCE-004 in [`performance.md`](performance.md)). A small, bounded price is
acceptable because it buys a stronger statement about the archive.
