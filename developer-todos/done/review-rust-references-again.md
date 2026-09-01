# rust/ references: review again

**Noted**: 2026-08-21, during the same conversation that was actively working through
`docs/design/metadata-storage.md` point 7 ("Remove references to `rust/`") - a repo-wide cleanup of
`rust/`/`rust/db` citations in `docs/design/`, replacing them with self-contained descriptions of
the chosen design, per "Relationship To Other Implementations" and "This Is A Rewrite, Not A Port"
in `AGENTS.md`.
**Size**: small - a repo-wide grep and read-through, most likely.
**Context**: point 7 in `docs/design/metadata-storage.md`; `AGENTS.md`'s "Relationship To Other
Implementations (Read Once, Reference Nowhere Else)" section.

Developer's own wording: "rust/ Referenzen (nochmal) durchschauen" ("review rust/ references
again"). The "again" suggests the developer wants a second, fresh pass after the one already
happening in the conversation this note came from - not necessarily because that pass was expected
to miss something, but as a deliberate double-check rather than trusting a single pass. Re-run the
repo-wide search this conversation used
(`grep -rlniE "rust/db|rust/AGENTS|\brust/|\bscala/|\bgo/" --include="*.md" --include="*.rs" .`,
excluding `target/`) and confirm every remaining hit is either the canonical `AGENTS.md` section, a
`migration/`-directory file (whose explicit purpose covers `scala/` references, not `rust/`/`go/`),
or otherwise legitimate per that section's rules - not a leftover citation that should have been
rewritten.

## Done

**Completed**: 2026-09-01, by Claude Code on the web session (branch `mount-read-write`), during an
unattended sweep of open `agent-todos`/`developer-todos`.

Re-ran the exact grep. Six hits, all reviewed individually and found legitimate - no leftover
citation needing a rewrite:

- `agent-todos/done/directory-scoped-agents-md-experiment.md`,
  `developer-todos/done/rust2-compatible-with-retired-branches.md`: both `done/` task records
  factually narrating the `rust2/` -> `rust/` rename as part of what actually happened in that
  completed task - historical session record, not forward-looking product documentation, so outside
  the "reference nowhere else" rule's scope.
- `developer-todos/review-rust-references-again.md` (this file): self-referential - its entire
  subject is this cleanup effort.
- `developer-todos/done/document-retiring-recipe-on-retired-branches-itself.md`: a false-positive
  match - "every Rust/Go rewrite-attempt branch" is "Rust-or-Go" with the slash as a separator, not
  a `rust/`/`go/` path reference at all; also a `done/` historical record regardless.
- `docs/agent-setup-plan.md`: explicitly framed as a closed historical audit record ("kept as the
  reference record ..., not as an active plan") about Claude Code session/tooling setup - not
  `docs/design/` or other forward-looking product documentation, so the `rust2/`->`rust/` directory
  naming note there is a navigational aid, not a comparison-to-prior-implementation citation.
- `.claude/rules/design-docs.md`: this is the "reference nowhere else" rule's own text explaining
  the convention (`"not by naming rust/, scala/, or go/ as the source"`) - quoting/describing the
  rule is not a violation of it.

Neither of the two categories the original pass anticipated (a hit inside `AGENTS.md`'s own
canonical section, or a `migration/`-directory file matching `\bscala/`) actually occurs under this
exact grep pattern: `AGENTS.md` phrases its own references without a trailing slash ("the `rust`
branch", "`go`/`go2`/`go3`"), and `migration/`'s Scala references are always the bare word "Scala"
(capitalized, no slash), never literally `scala/` - both already in the non-slash style the
convention prefers, so simply invisible to this particular pattern rather than needing any change.
The earlier cleanup pass holds up; nothing to fix this time.
