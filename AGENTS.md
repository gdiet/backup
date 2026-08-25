# Agent Guidelines And Best Practices For This Project

## Project Overview

DedupFS is a deduplicating backup application. This Rust implementation is the current, actively developed
  one, and is the official successor to the earlier Scala implementation (see "Successor Status And Migration" below). See also
  "Relationship To Other Implementations".

## Relationship To Other Implementations (Read Once, Reference Nowhere Else)

**Reliable, git-level fact**: this implementation's own history lives on the `rust` branch of this
repository's origin (`git@backup:gdiet/backup.git`), which also holds - among others - `main` (the
Scala implementation) and `go`/`go2`/`go3` (successive Go implementation stages) as branches. This
means `git push` / branch-deletion commands act on a remote shared with the other implementations.

To look at another implementation's code - as an agent or as the developer - use the
`local-reference-worktrees` skill to set up a read-only `.local/` checkout of it. Do not assume
such a checkout already exists; `.local/` is machine-local and git-excluded, set up on demand
rather than ambiently present.

This section exists so an agent has that orientation once - it must not be repeated or
re-litigated anywhere else in this repo: not in code comments, not in `README.md`, not in
`requirements/`, not in `docs/design/`, not in commit messages. Documentation in this project
describes this implementation as it is, forward-looking; it does not narrate how it differs from
or improves on prior implementations.

- The `go`/`go2`/`go3` implementation stages never reached an official status and are essentially
  never relevant here. Essentially never reference them; if a comparison is ever truly needed (e.g.
  a performance figure), keep it as rare and narrow as possible.

## This Is A Rewrite, Not A Port

Existing design and behavior in the Scala implementation — architecture, storage layout, code
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

This Rust implementation is the official successor to the Scala implementation. This
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
is not covered by the "reference nowhere else" rule above, which is specifically about the
`go`/`go2`/`go3` implementation stages.

## Documentation Philosophy

All documentation in this repo — code comments, doc comments, `README.md`, `requirements/`,
`migration/` — is forward-looking: it describes the current, intended state and why it is shaped
that way, not the history of how it got there. Concretely:

- Write "X works like this, because Y" — not "we changed X from Z to this because...".
- It is fine, and often useful, to note a rejected alternative inline to save the next reader from
  re-treading it: "an alternative approach of doing X was considered and rejected because Y."
  That is a forward-looking safeguard, not a change narrative.
- This applies regardless of whether the rejected alternative happens to be what the Scala or Go
  implementations did — phrase it about the approach itself, not about which prior implementation
  used it.
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
in `crates/mountfs/`, the cross-platform FUSE/WinFSP mount crate - see
[`crates/mountfs/CLAUDE.md`](crates/mountfs/CLAUDE.md) (loads automatically when working under
`crates/mountfs/`) for
which skills to load and when a Docker cross-compile check is enough before escalating to a real
Windows/WinFSP environment.

## Agent TODOs (Cross-Environment Handoffs And Out-Of-Scope Findings)

`agent-todos/` (see its own `README.md` for the exact file format) is where a task gets parked
instead of silently dropped - either because it needs an environment/capability the current agent
does not have, or because it came up while working on something else and is genuinely outside that
task's scope.

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
- The same goes for something you notice unprompted while working on a different task: if it is
  small enough to just fix now without derailing what you were asked to do, do that instead of
  parking it - a TODO is for things that genuinely need their own, later attention, not an excuse
  to defer trivial fixes. Otherwise, mention it briefly to the developer in your response *and* add
  a file here - a passing mention in chat does not survive past that conversation, the file does.

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

If a situation is not obviously covered by an existing convention in this file, check `git log`/
commit messages for an established precedent before guessing or silently omitting a convention -
the project may already have settled this.

Suggest an English semantic commit message following Conventional Commits.

**Only commit when explicitly asked.** Even after proposing a commit message, wait for explicit
permission before running `git commit`.

**Exception - ephemeral remote execution environments**: if your system prompt describes this
session as running in a managed, ephemeral remote execution environment (a cloud container that
can be reclaimed after inactivity or when the session ends, discarding anything not pushed), the
above does not apply. There, waiting for permission is not safe the way it is on a developer's own
machine - uncommitted or unpushed work can be lost outright before you are ever asked again. In
that case, first confirm you are on a branch diverged from the relevant implementation's own
branch (never commit directly to `rust`, `main`, or another implementation's branch), then commit
and push on your own judgment, without waiting to be asked, whenever there is work worth not
losing. The "never commit directly" part is about this autonomous case specifically - the
developer explicitly asking you to commit directly to one of those branches overrides it, same as
any other default here.

Before actually running `git commit`, load the `attributed-commits` skill for the `Generated-By`
trailer, the commit's git Author identity, and the pre-commit git-identity check - all of that
lives there now (loads on demand, since it only matters at the moment of committing) rather than
inline here.

## Debugging Discipline

- **Verify a regression test actually catches the bug, not just that it is green.** Before treating
  a new regression test as done, temporarily revert the fix it protects (keep the test), confirm the
  test fails, then restore the fix and confirm it passes again. A test that never ran red could be
  passing vacuously - wrong setup, or an assertion that does not actually exercise the failure path
  - and this red/green cycle is often the only reliable way to tell two distinct, overlapping bugs
  in the same code path apart.
- **Run a negative control against the unmodified baseline before blaming the change just made.** A
  failure surfacing right after an edit is not necessarily caused by that edit - reproduce it
  against the pre-change baseline first, before spending time investigating the new code for a bug
  that may not be there.
- **Verify uncertain library/runtime behavior empirically, not from memory or a reading of the
  spec.** A short, throwaway standalone snippet settles an ambiguous case (a type-conversion
  subtlety, an equality/ordering edge case, an API's exact error behavior) far more reliably than
  reasoning about it - write it, run it, then discard it. This is the same discipline
  `.claude/rules/rust-code-quality.md`'s comment self-check already asks for regarding written
  claims in code comments; it applies just as much mid-investigation, before a claim ever reaches a
  comment.
- **When the same bug needs reproducing across more than one platform or backend - relevant here
  across `crates/mountfs`'s FUSE and WinFSP backends, or across a future read connection versus the
  write connection in `crates/db`'s single-writer model - check for architectural differences
  between them, not just a diff of the affected file.** Code that looks identical on both sides can
  still be driven by a different underlying process or component, changing where a reproduction
  actually needs to happen.
- **When building a branch to reproduce or investigate a bug for the developer to review,
  structure it as a sequence of individually checkoutable, self-explanatory commits** (one scenario
  per commit - e.g. "before fix", "after fix", "experiment reverted") rather than one bundled
  commit, so the developer can jump straight to any one scenario without manually toggling state.

## Dependencies

Suggest dependencies, but do not add them without explicit permission. Prefer `cargo add` over
hand-editing `Cargo.toml` so `Cargo.lock` stays in sync.

Check a dependency's license is compatible with this project's MIT/Apache-2.0 licensing before
proposing it. When it is not, that is not automatically disqualifying (see WinFSP, DESIGN-MOUNT-004
in [`docs/design/mount-abstraction.md`](docs/design/mount-abstraction.md)), but the exception needs
its own reasoning recorded in `docs/design/`, not just adopted silently.

When pinning or overriding a dependency version for a specific, temporary reason (a CVE fix, a bug
not yet released upstream), phrase the accompanying comment with a precise, machine-checkable
removal condition, not just the rationale - e.g. "remove this override once `<dependency>` depends
on `<package>` >= `<version>`" rather than just "pinned because of CVE-XXXX-XXXXX". That lets a
later cleanup, by a human or an agent, verify directly whether the override is still needed instead
of re-researching it from scratch.

## Shell Commands

Never run an unscoped recursive filesystem search (`find /`, `find / -maxdepth N`). Prefer `cargo
metadata`/`cargo tree` for locating crate sources; otherwise scope `find`/`grep` to a known
directory.

## Code Quality

Rust-specific conventions (idioms, the `.unwrap()` policy, doc-comment scope, and the code-comment
self-check) are in [`.claude/rules/rust-code-quality.md`](.claude/rules/rust-code-quality.md) - a
path-scoped rule that loads automatically whenever Claude works with a `.rs` file, so it is not
duplicated here.
