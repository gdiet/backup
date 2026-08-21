---
paths:
  - "requirements/**/*.md"
---

# Requirements Documentation

Product requirements live in `requirements/` (see `requirements/README.md` for the ID scheme,
status values, and directory layout). Read the relevant `requirements/functional/*.md` before
implementing a feature rather than re-deriving intended behavior from scratch.

Requirements are not exempt from AGENTS.md's "This Is A Rewrite, Not A Port" - apply that stance as
directly here as anywhere else. One requirements-specific tell: two entries whose difference
cannot be explained crisply is a signal to reconsider whether they should be one requirement, not
just a prompt to write a better explanation.

When adding or reorganizing requirements:

- Never renumber or reuse a `REQ-...` ID, even for a rejected/superseded requirement - only its
  `Status` changes.
- If a topic area's file grows large (rough guide: past ~30 requirements), split the *file* into a
  directory (e.g. `functional/storage.md` → `functional/storage/format.md` +
  `functional/storage/integrity.md`), keeping the same `<AREA>` prefix across all of them. Find the
  next free number by checking all files sharing that prefix, not just the one you are editing.
- Only introduce a new `<AREA>` prefix when a topic has genuinely grown into its own distinct
  domain, not merely to keep a file short. Existing IDs under the old prefix stay exactly as they
  are; cross-reference from the new area if useful, do not move or rename old entries.

Also apply AGENTS.md's "Documentation Philosophy" here, in particular the self-check before adding
a sentence to a product-facing document: if a sentence's real subject is "whoever is editing this
file" rather than DedupFS itself, it belongs in `AGENTS.md`, not here.
