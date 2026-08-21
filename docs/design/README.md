# Design Documentation

This directory records non-trivial implementation-design decisions — an algorithm choice,
alternatives weighed, benchmarks or research that informed a decision — at the level of properties
and trade-offs, not implementation mechanics. See "Design Documentation" in
[`../../AGENTS.md`](../../AGENTS.md) for what belongs here versus in code comments.

## Layout

- One file per decision or closely related group of decisions.
- `implemented/` — decisions that have actually shipped in code, moved here once they have.

## ID Scheme

`DESIGN-<AREA>-<NNN>` — `<AREA>` an uppercase short code for the decision family a file (or closely
related group of files) belongs to (e.g. `CDC`, `MOUNT`, `METADATA`), `<NNN>` a zero-padded 3-digit
number (`001`, `002`, ...). IDs are permanent once assigned: a decision later superseded or reversed
keeps its ID and gets a note to that effect, rather than being deleted or renumbered, so any
`DESIGN-...` reference elsewhere (in particular, from code) stays valid.

If a decision turns out to belong under a different `<AREA>` than the one it was first filed under,
it gets a new ID there - the original ID is not reused or deleted, only turned into a one-line
pointer to the new one (e.g. `DESIGN-MOUNT-004 moved to DESIGN-LICENSE-001`), so an existing
`DESIGN-...` reference does not silently point at nothing.

The ID goes at the start of the heading that states the decision, the same place `REQ-...` IDs go
in `requirements/` (see [`../../requirements/README.md`](../../requirements/README.md)):

```markdown
## DESIGN-METADATA-003: Hash computation
```

Only the heading that actually states a decision gets an ID - a supporting subsection (e.g. an
"Alternative considered and rejected" subheading underneath it) is part of explaining that same
decision, not a separate one, and stays unnumbered.

Assign IDs only once a decision is actually settled, not while a proposal is still being weighed -
numbering something still in flux just churns the numbering once it is decided or dropped.

## Cross-Referencing A Design Decision

Same convention as `REQ-...` cross-references: cite the ID as plain text and link the file it lives
in, never a heading anchor (retitling a heading silently breaks that kind of link, which is exactly
what a permanent ID avoids needing).

```markdown
see DESIGN-METADATA-003 in [`metadata-schema-with-contents-table.md`](metadata-schema-with-contents-table.md)
```

## Reference Direction: One Way Only

References only ever point downward - code may cite a design decision or a requirement, a design
document may cite a requirement, a requirement never cites a design document or code. This keeps a
requirement free to be satisfied by a different design later without needing to change the
requirement itself, and a design free to be reimplemented differently without needing to change the
requirement it still satisfies.

Not a mandatory chain: a `DESIGN-...` entry only exists where a non-trivial decision was actually
made (see "Design Documentation" in [`../../AGENTS.md`](../../AGENTS.md)). Code implementing a
requirement with no such decision behind it has no design entry to cite, and cites the `REQ-...`
directly instead.
