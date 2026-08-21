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
Status: draft | agreed | rejected | superseded-by REQ-... | moved-to REQ-...
Importance: must | should | could

<description>

Rationale: <why, and optionally: "an alternative approach of X was considered and rejected
because Y">
```

## ID Scheme

`REQ-<AREA>-<NNN>` — `<AREA>` an uppercase short code for the topic area (e.g. `STORAGE`,
`CHUNKING`, `CLI`), `<NNN>` a zero-padded 3-digit number (`001`, `002`, ...). IDs are permanent
once assigned: a rejected, superseded, or relocated requirement keeps its ID and gets an updated
`Status` rather than being deleted or renumbered, so any `REQ-...` reference elsewhere stays valid.

`superseded-by` and `moved-to` mark different things - keep them distinct rather than using
`superseded-by` for both: `superseded-by` means the requirement itself changed (a new decision
replaces the old one, with different content); `moved-to` means the content is unchanged, only its
`<AREA>` is - filed at a new ID because it turned out to belong under a different topic area, not
because anything about the requirement was reconsidered.

## Cross-Referencing Another Requirement

When prose in one requirement points at a specific requirement elsewhere, cite the ID as plain text
and link the file it lives in, e.g.:

```markdown
see REQ-MOUNT-003 in [`mount.md`](mount.md)
```

Do not link a heading anchor (`mount.md#req-mount-003-optional-read-write-mount`) — GitHub derives
that anchor from the heading's title text, so retitling the requirement silently breaks the link.
The plain-text ID has no such dependency, which is the same reason IDs are permanent (see "ID
Scheme" above). Do not use `[[wiki-link]]` syntax either — it does not render as a link on GitHub,
where this repository lives.
