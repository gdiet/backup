# Design Documentation

This directory records non-trivial implementation-design decisions — an algorithm choice,
alternatives weighed, benchmarks or research that informed a decision — at the level of properties
and trade-offs, not implementation mechanics. See
[`../../.claude/rules/design-docs.md`](../../.claude/rules/design-docs.md) for what belongs here
versus in code comments.

## Layout

- One file per decision or closely related group of decisions.

## Status

Directly under a decision's heading, a `Status:` line. The values split along one real axis - has
this heading actually been assigned a `DESIGN-...` ID (see "ID Scheme" below) or not:

```markdown
## DESIGN-METADATA-003: Hash computation
Status: decided
```

Without an ID:

- `idea` - still being weighed, no decision recorded here yet.

With an ID:

- `draft` - formalized enough to have an ID and be citable, not yet settled.
- `decided` - settled, not yet shipped in code.
- `implemented` - settled and shipped in code.
- `rejected` - considered and explicitly not going forward, superseded by nothing in particular.
- `superseded-by DESIGN-...` - a later decision replaces this one's content.
- `moved-to DESIGN-...` - unchanged content, filed under a different `<AREA>` than originally (see
  "ID Scheme" below) - not a reconsideration of the decision itself.

`decided`/`implemented` form a linear progression, not two independent facts - shipping in code
implies the decision was already settled - so one field covers both. Nothing beyond the bare value
(or, for `superseded-by`/`moved-to`, the target ID) goes on this line: what was decided, and once
implemented (or, for `decided`, why not yet), goes as the opening of the prose that follows instead
- never a file or path there (see "Reference Direction: One Way Only" below).

Requirements in `requirements/` use a parallel but not identical vocabulary (`draft` / `agreed` /
`rejected` / `superseded-by REQ-...` / `moved-to REQ-...`, plus the same no-ID `idea` stage) - see
[`../../requirements/README.md`](../../requirements/README.md). `agreed` there and `decided` here
name the same "settled" point deliberately with different words: a requirement's `agreed` is
consensus that a described behavior is actually wanted; a design's `decided` is a choice made among
technical alternatives, with no separate party to reach consensus with beyond the choice being
sound. Design additionally distinguishes `decided` from `implemented`, which requirements
deliberately do not track at all - see `implemented`'s own entry above for why design needs that
distinction and requirements do not.

This is the only place implementation state is tracked - a decision never moves file or directory
based on it. A file bundling several closely related decisions (see "Layout" above) cannot
otherwise signal "implemented" as one unit once those decisions ship on different timelines, and a
second, file-location-based signal duplicating the same fact this line already states would only
be one more place for that fact to silently go stale in.

## ID Scheme

`DESIGN-<AREA>-<NNN>` — `<AREA>` an uppercase short code for the decision family a file (or closely
related group of files) belongs to (e.g. `CDC`, `MOUNT`, `METADATA`), `<NNN>` a zero-padded 3-digit
number (`001`, `002`, ...). IDs are permanent once assigned: a decision later dropped or superseded
keeps its ID and gets `Status: rejected` or `Status: superseded-by DESIGN-...` (see "Status" above)
rather than being deleted or renumbered, so any `DESIGN-...` reference elsewhere (in particular,
from code) stays valid.

If a decision turns out to belong under a different `<AREA>` than the one it was first filed under,
it gets a new ID there. The original ID is not reused or deleted; it is only turned into a one-line
pointer to the new one (e.g. `DESIGN-MOUNT-004 moved to DESIGN-LICENSE-001`). This way, an existing
`DESIGN-...` reference does not silently point at nothing.

The ID goes at the start of the heading that states the decision, the same place `REQ-...` IDs go
in `requirements/` (see [`../../requirements/README.md`](../../requirements/README.md)):

```markdown
## DESIGN-METADATA-003: Hash computation
```

Only the heading that actually states a decision gets an ID - a supporting subsection (e.g. an
"Alternative considered and rejected" subheading underneath it) is part of explaining that same
decision, not a separate one, and stays unnumbered.

Assign an ID once a decision is at least a formalized, citable proposal (`Status: draft`) - not
for a passing thought still being weighed with no shape yet (that stays `Status: idea`, unnumbered,
until it is). A `draft` that is later dropped keeps its ID and moves to `Status: rejected` rather
than being deleted or renumbered - permanence (see above) already absorbs the "numbering churn"
concern this might otherwise raise.

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
made (see [`../../.claude/rules/design-docs.md`](../../.claude/rules/design-docs.md)). Code implementing a
requirement with no such decision behind it has no design entry to cite, and cites the `REQ-...`
directly instead.
