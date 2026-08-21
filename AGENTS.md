# Agent Guidelines And Best Practices For This Project

## Project Overview

DedupFS is a deduplicating backup application. This Rust implementation is the current, actively developed
  one, and is the official successor to the earlier Scala implementation (see "Successor Status And Migration" below). See also
  "Relationship To Other Implementations".

## Relationship To Other Implementations (Read Once, Reference Nowhere Else)

**Reliable, git-level fact**: this repository's origin (`git@backup:gdiet/backup.git`) also holds - among others -
`main` (the Scala implementation), `rust` (a previous Rust implementation), and
`go`/`go2`/`go3` (successive Go implementation stages) as branches. This means `git push` / branch-deletion commands act on a remote
shared with the other implementations — never push, delete, or force-update a branch other than
`rust2` from this checkout unless explicitly instructed to do so by the developer.

**Not reliable, filesystem-level fact**: on some machines (some of) those other branches also happen to be checked out as sibling directories next to this one
(`rust/`, `scala/`, `go/`), forming a combined workspace. That layout is **not guaranteed to exist** — check for it (e.g. `ls ..`) rather than
assume it, and do not treat its absence as an error or something worth remarking on.

Where that workspace layout does exist, this section exists so an agent has that orientation once
— it must not be repeated or re-litigated anywhere else in this repo: not in code comments, not in
`README.md`, not in `requirements/`, not in `docs/design/`, not in commit messages. Documentation
in this project describes this implementation as it is, forward-looking; it does not narrate how it
differs from or improves on prior implementations.

- `rust/`, where present, is a separate, independently maintained project, not something this
  repository extends or formally supersedes. Do not reference it for rationale ("unlike rust/,
  we..."). If you genuinely need prior-art orientation (e.g. "has anyone solved this kind of
  problem before"), that is a one-off research question to raise with the developer, not something
  to embed in this repo's documentation.
- `go/`, where present, never reached an official status and is even less relevant here than
  `rust/`. Essentially never reference it; if a comparison is ever truly needed (e.g. a performance
  figure), keep it as rare and narrow as the equivalent rule in `rust/AGENTS.md`.

## This Is A Rewrite, Not A Port

Existing design and behavior in `scala/` or `rust/` — architecture, storage layout, code
structure, tooling, requirements, anything — is raw material for understanding what problem this
software solves, not a specification to carry forward by default. When a design question comes
up, actively ask whether the answer a predecessor happened to land on is still the right one here,
rather than assuming it is because it already exists somewhere. A choice that exists in a
predecessor only because of how that predecessor happened to be built is not, by itself, a reason
to keep it.

The one thing this does not apply to is the small core of product requirements this software
actually needs to meet (see "Core" in `requirements/goals-non-goals.md`) — what a user depends on.
Everything about *how* that gets delivered is open to reconsideration.

## Successor Status And Migration

This Rust implementation is the official successor to the Scala implementation (`scala/`). This
carries two concrete obligations, tracked as first-class deliverables rather than incidental notes:

- **Migration path**: `migration/from-scala.md` documents how an existing Scala-DedupFS repository
  is migrated to this implementation. Keep it accurate as storage-format/metadata decisions are
  made — do not let it drift into aspirational/stale territory.
- **Feature comparison**: `migration/feature-comparison.md` tracks, per Scala feature, whether
  this implementation has it (implemented / planned / explicitly not planned, with a one-line
  reason for the latter). This is a release gate: before this implementation is declared a
  release-ready successor, every row must be in a deliberate, explained state — no silently
  missing features.

Both files describe the Scala relationship on their own terms (it is their explicit purpose) — this
is not covered by the "reference nowhere else" rule above, which is specifically about `rust/` and
`go/`.

## Documentation Philosophy

All documentation in this repo — code comments, doc comments, `README.md`, `requirements/`,
`migration/` — is forward-looking: it describes the current, intended state and why it is shaped
that way, not the history of how it got there. Concretely:

- Write "X works like this, because Y" — not "we changed X from Z to this because...".
- It is fine, and often useful, to note a rejected alternative inline to save the next reader from
  re-treading it: "an alternative approach of doing X was considered and rejected because Y."
  That is a forward-looking safeguard, not a change narrative.
- This applies regardless of whether the rejected alternative happens to be what `rust/`, `scala/`,
  or `go/` did — phrase it about the approach itself, not about which prior implementation used it.
- Write in formal, contraction-free prose ("does not", not "doesn't") throughout — this applies to
  every document in this repo, `AGENTS.md` included, not only user-facing or specification-style
  documents.
- **Self-check before adding a sentence to a product-facing document** (`requirements/`,
  `migration/`, `README.md`): ask who the sentence is actually about. If its real subject is
  "whoever is editing this file" rather than DedupFS itself, it does not belong there — recurring
  shapes of this slip: a process/maintenance note ("keep this updated as X changes"), an authoring
  reminder embedded as a comment, a sentence that just restates what its own heading or an
  already-documented layout entry already says, or a reference to `AGENTS.md` for justification.
  All of these belong in `AGENTS.md`, not in the product-facing document. This specific mistake has
  recurred repeatedly while drafting `requirements/` — check for it deliberately, not only when
  it is pointed out.

## Requirements Documentation

Product requirements live in `requirements/`. The ID scheme, status values, directory-splitting
conventions, and requirements-specific documentation-philosophy notes are in
[`.claude/rules/requirements.md`](.claude/rules/requirements.md) - a path-scoped rule that loads
automatically whenever Claude works with a file under `requirements/`, so it is not duplicated
here.

## Design Documentation

Non-trivial implementation-design decisions live in `docs/design/`. The `DESIGN-...` ID scheme,
the `implemented/` split, and the code-citation rules are in
[`.claude/rules/design-docs.md`](.claude/rules/design-docs.md) - a path-scoped rule that loads
automatically whenever Claude works with a file under `docs/design/`, so it is not duplicated
here.

## Interaction With The Developer

Use the same language as the developer for chat interactions, but English as the project language
for code, comments, docs, and commit messages.

The developer is an experienced programmer but still lacks in-depth Rust experience. Call out
subtle mistakes, not just obvious ones, particularly around ownership/lifetime edge cases and
idioms that differ from what a background in other languages would suggest.

In German chat prose, be careful with anglicized verb conjugations of technical jargon. Some are
fully naturalized in German developer speech and read fine (`committen`/`committed`,
`pushen`/`gepusht`, `mergen`/`gemergt`) - but many are not, and forcing an uncommon English term
into German verb conjugation (e.g. `vendorte` for "vendor") reads as invented German, not
established jargon. When in doubt, either paraphrase in German (e.g. "eingebettete/mitgelieferte
Kopie" instead of "vendorte Kopie") or leave the English term uninflected as a technical term ("die
Header im `vendor`-Verzeichnis") rather than conjugating it.

If you see that the developer has staged changes themselves (likely to track further edits
against that baseline via `git status`/a staged diff), do not run `git add` again until they say
otherwise - even for a change that would normally get staged as part of proposing a commit.
Staging on top of their own would erase the very distinction they are using it to see.

## Working Across Environments

This project is, at least occasionally, worked on from more than one environment/machine - an agent
in one environment can hit a wall that is trivial for an agent in another (needs a real Windows
console, WinFSP, network access to a specific host, etc.). In this repository, that mostly comes up
in `mountfs/`, the cross-platform FUSE/WinFSP mount crate - see
[`mountfs/CLAUDE.md`](mountfs/CLAUDE.md) (loads automatically when working under `mountfs/`) for
which skills to load and when a Docker cross-compile check is enough before escalating to a real
Windows/WinFSP environment.

## Agent TODOs (Cross-Environment Handoffs)

`agent-todos/` (see its own `README.md` for the exact file format) is where a task that needs an
environment/capability the current agent does not have gets parked, instead of silently dropped.

When starting work in this repo, check `agent-todos/` for open items:

- **Small item** (a doc/comment fix, a quick local check, anything low-risk and quick): just do it
  yourself, right away, no need to ask first - then move its file to `agent-todos/done/` with a
  short note on what you did.
- **Medium/large item**: read it, but confirm with the user before starting - the file's own
  "Size" field is a starting guess, not a substitute for judgment; if in doubt, ask.
- Don't silently delete an `agent-todos/` file instead of moving it to `done/` - the record of what
  was done (and by which environment) is the point, for whichever agent looks next.
- If you hit a wall yourself that another environment could clear, add a new file there (see the
  README's format) rather than leaving a comment only in chat/session history that will not survive
  past this conversation.

## Developer TODOs

`developer-todos/` (see its own `README.md` for the exact file format) is where the developer parks
something they want looked at or acted on later, without derailing whatever is currently being
worked on.

When starting work in this repo, check `developer-todos/` for open items, the same as
`agent-todos/`:

- **Small item**: just do it yourself, right away, no need to ask first - then move its file to
  `developer-todos/done/` with a short note on what you did.
- **Medium/large item**: read it, but confirm with the developer before starting.
- Don't silently delete a `developer-todos/` file instead of moving it to `done/` - the record of
  what was done is the point, for whichever agent looks next.

If the developer hands over a TODO mid-conversation, write it to `developer-todos/` right then - do
not just note it in chat and move on. A note that only exists in chat history does not survive past
that conversation, which defeats the entire point of this mechanism.

## Verification Of Changes

Scope which checks apply by what actually changed, not by how large the change looks:

- Any `.rs` file, `Cargo.toml`/`Cargo.lock`, or `build.rs` touched: run the full suite below.
- Only non-Rust files touched (docs, requirements, `migration/`): the Rust suite is a no-op; verify
  what is actually at risk instead (e.g. cross-references in changed docs still resolve).
- Mixed changes: run the full suite.
- While iterating mid-task, before actually proposing a commit, `cargo check` is a fine faster
  substitute for `cargo build` to get a quick compile signal. It is not a substitute for the full
  suite below, which still has to run once before proposing a commit.

Full suite:
- `cargo build`
- `cargo fmt` (or `cargo fmt --check` to verify without modifying)
- `cargo clippy -- -D warnings` — treat all warnings as errors; silence a genuine false positive
  explicitly and locally with a comment explaining why, do not leave it unaddressed
- `cargo test`
- `cargo doc --no-deps` and confirm no warnings
- Check whether `requirements/`, `migration/`, `docs/design/`, or `README.md` describe behavior
  this change affects, and update them — stale docs actively mislead the next reader. Move a
  design doc under `docs/design/implemented/` once the decision it describes has actually shipped.
- Same check, but for plain code comments in the files you are touching (not just `///` doc
  comments, which `cargo doc` above already covers): a comment stating *why* code is shaped a
  certain way rarely goes stale on its own, but a comment stating a *current status* ("not
  implemented yet", a specific measured number, "X does not support Y") can silently drift once
  that status changes, with nothing forcing a revisit — `cargo doc`/tests/clippy will not catch
  prose going stale. Update or remove such a comment if the change you are making falsifies it,
  even if it is outside the files you would otherwise touch for the change itself.

Suggest an English semantic commit message following Conventional Commits.

**Only commit when explicitly asked.** Even after proposing a commit message, wait for explicit
permission before running `git commit`.

Before actually running `git commit`, load the `attributed-commits` skill for the `Generated-By`
trailer, the commit's git Author identity, and the pre-commit git-identity check - all of that
lives there now (loads on demand, since it only matters at the moment of committing) rather than
inline here.

## Dependencies

Suggest dependencies, but do not add them without explicit permission. Prefer `cargo add` over
hand-editing `Cargo.toml` so `Cargo.lock` stays in sync.

Check a dependency's license is compatible with this project's MIT/Apache-2.0 licensing before
proposing it. When it is not, that is not automatically disqualifying (see WinFSP, DESIGN-MOUNT-004
in [`docs/design/mount-abstraction.md`](docs/design/mount-abstraction.md)), but the exception needs
its own reasoning recorded in `docs/design/`, not just adopted silently.

## Shell Commands

Never run an unscoped recursive filesystem search (`find /`, `find / -maxdepth N`). Prefer `cargo
metadata`/`cargo tree` for locating crate sources; otherwise scope `find`/`grep` to a known
directory.

## Code Quality

Rust-specific conventions (idioms, the `.unwrap()` policy, doc-comment scope, and the code-comment
self-check) are in [`.claude/rules/rust-code-quality.md`](.claude/rules/rust-code-quality.md) - a
path-scoped rule that loads automatically whenever Claude works with a `.rs` file, so it is not
duplicated here.
