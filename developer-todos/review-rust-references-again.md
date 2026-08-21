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
