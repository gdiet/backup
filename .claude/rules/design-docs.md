---
paths:
  - "docs/design/**/*.md"
---

# Design Documentation

Non-trivial implementation-design decisions (an algorithm choice, alternatives weighed, benchmarks
or research that informed the decision) live in `docs/design/` - one file per decision or closely
related group of decisions, moved into `docs/design/implemented/` once the decision has actually
shipped in code, mirroring how `requirements/` distinguishes `draft` from `agreed`. See
`docs/design/README.md` for the `DESIGN-...` ID scheme a settled decision gets, so code can cite
it directly, and for the one-way `code → design → requirement` reference rule that comes with it.

A design document captures the decision and *why* - including alternatives that were considered
and rejected, per AGENTS.md's "Documentation Philosophy" - at the level of properties and
trade-offs, not implementation mechanics. Once code exists for a decision, the code-adjacent
explanation of exactly how it works belongs in code comments (checked for staleness by `cargo doc`,
see AGENTS.md's "Verification Of Changes"), not duplicated in the design document - a design
document that also tries to be the algorithm's internal reference documentation creates two places
that can silently drift apart.

Write these before code exists whenever the decision is made before implementation starts, not
only retroactively - a decision made in conversation and never written down is effectively lost the
moment the conversation ends.

The "reference nowhere else" rule under AGENTS.md's "Relationship To Other Implementations" applies
here too: weigh a benchmark or a design property on its own merits, not by naming `rust/`, `scala/`,
or `go/` as the source.

When code cites either kind of ID: cite the `DESIGN-...` decision when there is a non-trivial one
behind the code (it explains *why*, not just *what*); cite the `REQ-...` directly when the code is
a straightforward implementation of an unambiguous requirement with no separate decision worth its
own `docs/design/` entry - do not manufacture a design entry just to have something to cite.
