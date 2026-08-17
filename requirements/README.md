# Requirements Documentation

This directory describes **what** DedupFS is and does — product requirements, not architecture or
implementation. Design/implementation docs have their own place; requirements are
implementation-agnostic.

## Layout

- `glossary.md` — terms used across requirements, defined once
- `goals-non-goals.md` — what this software provides, and what it deliberately does not
- `functional/` — one file per topic area (e.g. `storage-format.md`, `chunking-dedup.md`,
  `cli-commands.md`)
- `non-functional/` — same, for cross-cutting qualities (performance, reliability, compatibility,
  operability)
- `open-questions.md` — unresolved decisions, kept separate from agreed requirements until settled

Migration/feature-parity requirements toward Scala live in `../migration/`, not here — see that
directory's own files.

## Requirement Format

Each requirement inside a `functional/`/`non-functional/` file:

```markdown
### REQ-<AREA>-<NNN>: <short title>
Status: draft | agreed | rejected | superseded-by REQ-...
Priority: must | should | could

<description>

Rationale: <why, and optionally: "an alternative approach of X was considered and rejected
because Y">
```

## ID Scheme

`REQ-<AREA>-<NNN>` — `<AREA>` an uppercase short code for the topic area (e.g. `STORAGE`,
`CHUNKING`, `CLI`), `<NNN>` a zero-padded 3-digit number (`001`, `002`, ...). IDs are permanent
once assigned: a rejected or superseded requirement keeps its ID and gets an updated `Status`
rather than being deleted or renumbered, so any `REQ-...` reference elsewhere stays valid.
