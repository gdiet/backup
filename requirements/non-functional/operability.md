# Operability

### REQ-OPERABILITY-001: Low resource footprint and easy installation
Status: agreed
Importance: must

The software runs with a small, bounded memory footprint that does not grow with repository size,
nor with the size of any individual file being processed (e.g. 256 MB RAM — see
REQ-OPERABILITY-006 for the explicit, configurable budget this is built on), and can be installed
and made ready to use with minimal effort — no separately managed runtime, database server, or
complex configuration beyond obtaining the software and pointing it at a repository directory.

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

### REQ-OPERABILITY-005: Local record of which optional features actually get used
Status: moved-to REQ-MAINTENANCE-005

### REQ-OPERABILITY-006: Explicit, configurable memory budget
Status: agreed
Importance: must

The application works within an explicit memory budget for caching and buffering not-yet-durable
content, fixed once at startup (it does not change while the process keeps running) and derived
from an operator-configurable total — a sensible default requiring no configuration for typical
use — minus what the application's own database connection and runtime overhead are expected to
need. A repository whose own configured chunking granularity (REQ-STORAGE-003 in
[`../functional/storage.md`](../functional/storage.md)) cannot possibly fit within this budget is
refused at startup with an actionable error (REQ-OPERABILITY-004), rather than silently exceeding
the stated bound once running. This only applies to a session that actually maintains such a
cache — a read-only session never buffers not-yet-durable content in the first place, so it has no
occasion to be refused over this budget at all.

Rationale: REQ-OPERABILITY-001's bounded-footprint guarantee needs a concrete mechanism to actually
hold, not just an aspiration — an explicit budget an operator can see, size for their own hardware,
and reason about the consequences of (a smaller budget trades some caching benefit for a stronger
footprint guarantee) is what makes "bounded" a checkable property rather than a claimed one.
Refusing to start rather than exceeding the bound keeps the guarantee unconditional: an operator
who configured storage parameters exceeding their own memory budget finds out immediately, at a
moment they can still act on, rather than discovering it as a failure mid-run or a silently-broken
guarantee.

### REQ-OPERABILITY-007: Options meaningless in context are refused, not silently accepted
Status: agreed
Importance: should

When a command is explicitly given an option whose entire purpose does not apply in the mode (or
combination of other arguments) it was actually invoked with — a flag documented as only mattering
in a mode the command was not actually invoked in, for instance — it refuses to run with an
actionable error, rather than silently accepting and ignoring the option. This is about whether the
option applies at all in this context, never about which particular value it was given: an option
set to a value that happens to equal its own default was still explicitly given, and is refused the
same as any other value — an option left at its default by simply not being mentioned is the only
case this does not apply to.

Rationale: an operator who explicitly passes an option almost always expects it to do something;
silently accepting it anyway leaves them believing it took effect when it did not, a wrong belief
nothing else will ever correct. That expectation does not depend on which value they chose - an
option set to its own default was still deliberately given. The right question is therefore "was
this option given at all", not "does its value differ from the default". This is a different
concern from REQ-OPERABILITY-004, which is about explaining a genuine failure clearly
once one occurs — this is about recognizing that a semantically inert combination of inputs should
be treated as a failure in the first place, rather than never surfacing at all.
