# rust2 compatible with retired branches?

**Noted**: 2026-08-21, during a chat conversation working through `docs/design/metadata-storage.md`
point 7 (removing `rust/`-citations) and the question of what to call this implementation instead
of the working title "rust2".
**Size**: unclear yet - depends on what the developer actually means, see below.
**Context**: shared origin `git@backup:gdiet/backup.git` holds `main` (Scala), `rust` (a previous
Rust implementation), and `go`/`go2`/`go3` (successive Go implementation stages) as branches
alongside this one (`rust2`) - see "Relationship To Other Implementations" in `AGENTS.md`.

Developer's own wording: "rust2 kompatibel mit retired branches?" ("rust2 compatible with retired
branches?"). Noted verbatim, without further interpretation - ambiguous as given: could mean
several different things, e.g.

- whether this implementation's on-disk repository format needs to stay compatible with (or able to
  read) data produced by one of the retired branches,
- whether some tooling/script here assumes a directory/branch layout that silently breaks if a
  retired branch is absent or laid out differently,
- something about the git history/branch structure itself (e.g. whether `rust2` should be able to
  be pushed to, or coexist safely with, the retired branches on the shared remote).

Clarify with the developer what is actually meant before acting on this.

## Update: found the missing context (2026-08-21)

`main`'s `development-hints.md`, "Retiring Branches" section, documents an established
convention on this shared origin: fake-merge a branch that is not going to `main` anytime soon
into `retired_branches` (`git checkout retired_branches && git merge -s ours origin/branch-to-
retire && git push`, then delete the original remote branch) - keeps the commit reachable "just
in case" without cluttering the branch list. Confirmed `origin/retired_branches` exists; its
current tree is old retired Scala source, no instructions file of its own (the instructions live
in `main`, not on that branch).

This makes a concrete, plausible reading of the original question available: `rust2-intermediate`
(flagged in the session handoff this developer-todo grew out of as "superseded, safe to
ignore/delete once verified, not verified yet") is the obvious local candidate for exactly this
retiring process, rather than being deleted outright or left dangling indefinitely. Still needs
the developer's confirmation that this is actually what was meant, and that `rust2-intermediate`
is indeed safe to retire this way, before acting.
