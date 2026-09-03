# Ingest Target-Path Template Syntax

## DESIGN-CLI-006: Concrete syntax for REQ-INGEST-007's placeholders and existence markers

Status: implemented (`crates/cli/src/target_path.rs`)

REQ-INGEST-007 in
[`../../requirements/functional/ingest.md`](../../requirements/functional/ingest.md) deliberately
leaves the exact syntax for its two independent features open - a date/time placeholder within a
segment, and a per-segment existence requirement - beyond a non-binding `[yyyy-MM-dd]` example for
the former. This decides the concrete syntax `dfs ingest`'s target path actually parses.

### Date/time placeholders: `[...]`, tokens from REQ-INGEST-007's own example

A segment may contain any number of `[...]` spans. Each span's content is scanned left to right for
the literal tokens `yyyy`, `MM`, `dd`, `HH`, `mm`, `ss` - REQ-INGEST-007's own example vocabulary,
adopted directly rather than inventing a competing one - each substituted with the run's own
captured "now" (REQ-INGEST-007: "resolved against the current date/time at run start"), zero-padded
to a fixed width (`yyyy` to 4 digits, the rest to 2). A character inside the brackets that matches
no token is kept verbatim, so a literal separator can sit between tokens (`[yyyy-MM-dd]`) or a token
can sit directly next to literal text outside the brackets (`backup-[yyyy]`). No token is a prefix
of another, so matching is unambiguous without needing a longest-match rule.

Rejected: pulling in `time`'s own runtime format-description parser (`[year]`, `[month
padding:zero]`, ...) instead of a bespoke token scan. `time` is already a dependency as of this
decision (see "Adopting the `time` crate" below), but its own format-description vocabulary is not
REQ-INGEST-007's `yyyy`/`MM`/`dd` vocabulary - reaching for it would still need this exact
translation layer in front of it, just with an extra indirection (translate to `time`'s syntax,
then have it parse and format that) instead of directly computing each substitution from `time`'s
`OffsetDateTime` accessors (`.year()`, `.month()`, ...). The bespoke scan is the entire translation
layer either approach needs, without the extra indirection.

### Existence markers: a `+`/`!` prefix on the segment itself

A segment prefixed with `+` is REQ-INGEST-007's "created on demand if missing and otherwise reused
as-is". A segment prefixed with `!` is "required to be freshly created, failing if the segment
already exists". Neither prefix - REQ-INGEST-007's default - is "required to already exist". The
prefix is stripped before date/time placeholder resolution runs on the remainder, so `+[yyyy]` and
`![yyyy-MM-dd]` both resolve as expected.

Marking a segment with either creatable form (`+` or `!`) makes every segment *below* it default to
`+` (create-if-missing) rather than must-exist, per REQ-INGEST-007's own cascading rule - tracked
as a single boolean while walking the path left to right, set the first time either prefix is seen
and never cleared. A segment below a creatable ancestor can still opt into `!` (must-be-fresh)
itself; there is no way to force a segment back to plain must-exist below a creatable ancestor, since
REQ-INGEST-007 does not ask for one.

Rejected: a suffix instead of a prefix (`backups+`, `backups!`). Rejected for a `[...]`-templated
segment specifically: a suffix would land after the closing `]`, reading as if it were part of the
resolved date rather than a marker on the segment as a whole (`[yyyy]!` beside `2026!` after
resolution, uncomfortably similar). A prefix stays visually and positionally separate from the
placeholder syntax in every case.

Rejected: reusing `[...]` bracket syntax for the marker itself (e.g. a leading `[+]`/`[!]`
pseudo-token). This would need the placeholder scanner to special-case a marker span from an
ordinary date/time span, and reads as though the marker itself were subject to date/time
resolution, which it is not - a plain, non-bracketed prefix keeps the two concerns visually and
structurally distinct.

### Adopting the `time` crate

This is also where this project first adopts the `time` crate (over `chrono`: leaner, better recent
security/maintenance history, `no_std`-friendly if that ever matters here) as a dependency, per
`developer-todos/adopt-time-crate-for-ingest-007-and-list.md`'s own recommendation - confirmed with
the developer at the point this decision was made, since adding a dependency needs explicit
permission regardless of a standing note's own prior go-ahead. `crates/cli/src/time_format.rs`'s
own `format_time`/`format_deletion_suffix` (previously a hand-rolled, `std`-only implementation of
Howard Hinnant's public-domain `civil_from_days` algorithm) are refactored to use `time::OffsetDateTime`
too at the same time, rather than keeping two separate date-handling approaches in the same crate.
