# Reduce the length of requirement texts by 20-40% where possible

**Noted**: 2026-08-26, while reviewing `requirements/functional/mount.md` commit by commit on
`feat/mount-and-cli-defaults`.
**Size**: large - confirm with the developer before starting. A deliberate editing pass across
`requirements/`, weighing what to cut per entry, not a mechanical trim.
**Context**: no single commit/design doc - a general observation about the current state of
`requirements/functional/` and `requirements/non-functional/`.

The developer's own wording: reduce the scope/length of requirement texts by 20-40% where
possible.

Go through existing `REQ-...` entries and shorten them where the same intent can be stated in
noticeably fewer words, without losing anything load-bearing - a requirement's actual behavior
guarantee, its rationale, and a genuinely non-obvious rejected alternative worth recording. Likely
candidates for trimming: restating the same point from slightly different angles within one
entry's prose, rationale sentences that elaborate past the point already made, and rejected
alternatives whose reasoning could be stated more tersely without losing the actual argument.

Not a blanket instruction to cut a fixed percentage from every entry - some entries may already be
appropriately tight (nothing to cut without losing content), others may compress well past 40%.
Judge per entry rather than aiming for a uniform reduction, and preserve every entry's actual
requirement/rationale/rejected-alternative content, distinct IDs, and status values exactly -
this is a prose-density pass, not a re-litigation of what was decided.
