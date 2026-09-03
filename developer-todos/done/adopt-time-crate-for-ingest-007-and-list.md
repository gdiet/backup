# Adopt the `time` crate when REQ-INGEST-007 is implemented, and reuse it in `dfs list`

**Noted**: 2026-09-03, during the `dfs list`/`dfs restore` implementation session.
**Size**: medium - confirm with the developer before starting. Adding a new dependency needs
explicit permission per `AGENTS.md`'s "Dependencies" section, even though this note itself already
carries a prior go-ahead in spirit ("gerne vormerken").
**Context**: `crates/cli/src/list.rs`'s `format_time`/`civil_from_days` (a small, self-tested,
`std`-only UTC date algorithm - Howard Hinnant's public-domain `civil_from_days`), and
REQ-INGEST-007 in `requirements/functional/ingest.md`.

The developer asked whether a dependency would actually have been better than the hand-rolled,
`std`-only date formatter written for `dfs list`'s timestamp column. For `list` alone - a fixed
UTC `YYYY-MM-DDTHH:MM:SSZ` display, one direction only - the `std`-only approach is a legitimate,
low-risk choice: no new dependency footprint, well-tested against known reference dates (the Unix
epoch, a well-known 2000-01-01 reference, a leap-day case), and the underlying algorithm is a
widely used, public-domain one, not something written from scratch and unverified.

But REQ-INGEST-007 (templated target paths, `Status: agreed`, not yet implemented) will need real
strftime-style formatting: a caller-supplied pattern like `[yyyy-MM-dd]`, resolved against the
current date/time at run start - arbitrary format strings, not a single fixed output shape.
Hand-rolling a pattern parser/formatter for that would be substantially more code and risk than
`list`'s fixed-format helper. When REQ-INGEST-007 is picked up, pull in a real date/time crate
instead - the `time` crate is the recommended pick over `chrono` (leaner, better recent
security/maintenance history, `no_std`-friendly if that ever matters here). At that point, also
refactor `list.rs`'s `format_time`/`civil_from_days` to use it, rather than keeping two separate
date-handling approaches in the same crate.

## Done (2026-09-03)

Confirmed with the developer at the point REQ-INGEST-007 was actually picked up (a fresh
confirmation, per this note's own "Size" field). Added `time` (`std`/`alloc` features only,
`cargo add`) to `crates/cli/Cargo.toml`. `crates/cli/src/time_format.rs`'s `format_time`/
`format_deletion_suffix` now build a `time::OffsetDateTime` and format its own accessors, replacing
`civil_from_days` entirely - `time_format.rs` no longer has its own calendar-arithmetic
implementation. REQ-INGEST-007's own `[yyyy-MM-dd]`-style placeholder resolution
(`crates/cli/src/target_path.rs`) also builds on `time::OffsetDateTime` directly rather than the
crate's runtime format-description parser - see DESIGN-CLI-006 in
`docs/design/ingest-target-template-syntax.md` for why.
