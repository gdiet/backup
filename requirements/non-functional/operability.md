# Operability

### REQ-OPERABILITY-001: Low resource footprint and easy installation
Status: agreed
Importance: must

The software runs with a small, bounded memory footprint that does not grow with repository size
(e.g. 128 MB RAM), and can be installed and made ready to use with minimal effort — no separately
managed runtime, database server, or complex configuration beyond obtaining the software and
pointing it at a repository directory.

Rationale: operators running this against a personal backup archive, often on modest hardware,
should not need to provision resources or maintain infrastructure disproportionate to the simple
job of storing and retrieving files.

### REQ-OPERABILITY-002: Mirrorable with generic file-sync tools
Status: agreed
Importance: must

A repository can be kept in sync with a secondary copy using ordinary, repository-unaware
file-synchronization tools. Comparing file size and modification time is enough for those tools to
decide what needs copying — they do not need to understand deduplication, chunk boundaries, or the
metadata format. And they do not need to re-transfer stored content that has not actually changed.
Metadata is small relative to the bulk data it describes, so re-transferring it in full on every
sync run stays cheap even though it changes every run.

This guarantee holds only while no process is using the repository - a mirror taken mid-use may be
inconsistent (e.g. capturing the metadata file mid-write, without whatever journal/write-ahead
state belongs with it at that instant); staying mirrorable during active use is a bonus, not a
requirement.

Rationale: operators maintaining an offline or secondary copy of a repository — especially a large
one — need that sync to be fast and to rely on tooling they already trust; building and trusting a
repository-specific sync mechanism would cost more than reusing what already works. This depends
on the storage layout described in REQ-STORAGE-007 in
[`../functional/storage.md`](../functional/storage.md).

### REQ-OPERABILITY-003: Reasonable defaults over required configuration
Status: agreed
Importance: should

Where a parameter has one choice that is right for typical, personal use, a command defaults to
that choice rather than requiring the operator to supply it explicitly on every invocation - an
explicit override stays available wherever a different choice is genuinely needed.

Rationale: a parameter an operator would just copy out of the documentation anyway gains nothing
from being required - it adds friction to the common case without buying a genuinely more informed
choice. Where a default's absence would leave a *permanent, unfixable* choice unexplained (e.g.
REQ-STORAGE-003 in [`../functional/storage.md`](../functional/storage.md)'s chunking
configuration), the fix is making that permanence clearly visible wherever the choice is made, not
withholding the default itself.

### REQ-OPERABILITY-004: Actionable error messages for foreseeable failures
Status: agreed
Importance: should

A foreseeable failure - a missing or unwritable path, a malformed argument, a repository that does
not exist or is already in use, and similar cases a command can reasonably anticipate - is reported
with a clear, specific message that says what went wrong and, where there is one, what to do about
it - never a raw OS error code or an internal panic message standing in as the only explanation.

Rationale: an operator hitting a foreseeable problem should be able to fix it from the error
message alone, without needing to guess, consult external documentation, or read this project's
source.
