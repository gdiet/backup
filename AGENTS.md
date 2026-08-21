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

Product requirements live in `requirements/` (see `requirements/README.md` for the ID scheme,
status values, and directory layout). Read the relevant `requirements/functional/*.md` before
implementing a feature rather than re-deriving intended behavior from scratch.

Requirements are not exempt from "This Is A Rewrite, Not A Port" above — apply that stance as
directly here as anywhere else. One requirements-specific tell: two entries whose difference
cannot be explained crisply is a signal to reconsider whether they should be one requirement, not
just a prompt to write a better explanation.

When adding or reorganizing requirements:

- Never renumber or reuse a `REQ-...` ID, even for a rejected/superseded requirement — only its
  `Status` changes.
- If a topic area's file grows large (rough guide: past ~30 requirements), split the *file* into a
  directory (e.g. `functional/storage.md` → `functional/storage/format.md` +
  `functional/storage/integrity.md`), keeping the same `<AREA>` prefix across all of them. Find the
  next free number by checking all files sharing that prefix, not just the one you are editing.
- Only introduce a new `<AREA>` prefix when a topic has genuinely grown into its own distinct
  domain, not merely to keep a file short. Existing IDs under the old prefix stay exactly as they
  are; cross-reference from the new area if useful, do not move or rename old entries.

## Design Documentation

Non-trivial implementation-design decisions (an algorithm choice, alternatives weighed, benchmarks
or research that informed the decision) live in `docs/design/` — one file per decision or closely
related group of decisions, moved into `docs/design/implemented/` once the decision has actually
shipped in code, mirroring how `requirements/` distinguishes `draft` from `agreed`. See
`docs/design/README.md` for the `DESIGN-...` ID scheme a settled decision gets, so code can cite
it directly, and for the one-way `code → design → requirement` reference rule that comes with it.

A design document captures the decision and *why* — including alternatives that were considered
and rejected, per "Documentation Philosophy" above — at the level of properties and trade-offs, not
implementation mechanics. Once code exists for a decision, the code-adjacent explanation of exactly
how it works belongs in code comments (checked for staleness by `cargo doc`, see "Verification Of
Changes"), not duplicated in the design document — a design document that also tries to be the
algorithm's internal reference documentation creates two places that can silently drift apart.

Write these before code exists whenever the decision is made before implementation starts, not
only retroactively — a decision made in conversation and never written down is effectively lost the
moment the conversation ends.

The "reference nowhere else" rule under "Relationship To Other Implementations" above applies here
too: weigh a benchmark or a design property on its own merits, not by naming `rust/`, `scala/`, or
`go/` as the source.

When code cites either kind of ID: cite the `DESIGN-...` decision when there is a non-trivial one
behind the code (it explains *why*, not just *what*); cite the `REQ-...` directly when the code is
a straightforward implementation of an unambiguous requirement with no separate decision worth its
own `docs/design/` entry — do not manufacture a design entry just to have something to cite.

Currently running as a live experiment: whether this harness automatically loads a directory-scoped
`AGENTS.md` the way it loads this repository-root one. See `docs/design/AGENTS.md` — temporary,
self-describing, not a real instructions file for that directory (the actual conventions live in
`docs/design/README.md`).

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
console, WinFSP, network access to a specific host, etc.). Cross-environment operational knowledge
lives in skills, not inline here, since it is only relevant on the (comparatively rare) occasions
this actually comes up - see the `wsl-windows-sync` and `julius-winfsp-ssh` skills. Load the
relevant one before doing that kind of work rather than re-deriving it from scratch.

Before reaching for one of those, though: `scripts/build-windows-docker.sh` cross-compiles the
Windows backend from right here via Docker (see `docs/design/mount-abstraction.md`'s "Verifying
the Windows backend from Linux") - a compile/link check, not a substitute for real WinFSP
behavior, but worth running first for anything touching `mountfs/src/windows/` before escalating
to a real Windows/WinFSP environment.

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

### Attributing Agent-Authored Commits

This repository is on GitHub (a shared origin — see "Relationship To Other Implementations"
above), and the developer wants agent-authored work distinguishable from their own. Whenever you are the sole author or a co-author of a commit, add a
trailer alongside the standard `Co-Authored-By:` trailer (keep that one too — GitHub parses it
specifically to show co-author avatars):

```
Generated-By: <agent-name> (harness: <harness>; model: <model>; role: author|co-author)
```

- `role: co-author` — a human genuinely reviewed *this specific change* before it was committed:
  read the diff/content, asked questions about it, requested revisions, or otherwise engaged with
  what was actually produced. `role: author` — the agent produced the change from a short or
  high-level instruction and no such review happened before commit. Authorizing a commit ("ja",
  "go ahead") is not the same as reviewing its content — the common plan-summary → "ja" → implement
  → verify → commit cycle is `role: author` by default, even though a human is present and
  explicitly permitted the commit (unaffected by the commit-permission rule above — permission is
  still always required either way). Only use `co-author` when review of the actual content
  demonstrably happened.
- **Determine this from context you already have — do not ask.** Decide `author` vs. `co-author`
  from what already happened in the conversation; if nothing indicates real review, default
  straight to `author`, silently, with no confirmation question. Only surface a question here if
  the developer's own words already asked for review/confirmation on this change.
- If you do need to show a change for review, do not paste a raw unified diff into chat — present a
  prose summary grouped by file/concern, or point the developer at a proper diff tool.
- `harness` is the interface this session is running through (terminal CLI, Desktop app, web app,
  an IDE extension, etc.) — not reliably inferable, so ask for it rather than guessing.

### Distinct Git Author Identity For Agent-Authored Commits

When `role: author` applies, also make the commit's actual git Author identity reflect that, not
just the trailer: set `GIT_AUTHOR_NAME`/`GIT_AUTHOR_EMAIL` as env vars scoped to that one `git
commit` invocation only, e.g.:

```
GIT_AUTHOR_NAME="Claude Sonnet 5" GIT_AUTHOR_EMAIL="noreply@anthropic.com" git commit -m "..."
```

Use the same name/email already used in the `Co-Authored-By` trailer. Never do this by editing
global (`~/.gitconfig`) or even this repo's local git config — it must only ever apply to the
single `git commit` invocation it is set for. Leave `GIT_COMMITTER_NAME`/`GIT_COMMITTER_EMAIL`
unset so the ambient (human) identity is used for Committer — GitHub then displays "X authored, Y
committed" when the two differ.

When `role: co-author` applies, leave the git Author identity alone (ambient/human) — only the
trailer changes.

**Ask early, not at commit time.** As soon as it looks like a commit will eventually be wanted in
this session, ask once, before you are mid-commit:

> "Über welche Oberfläche läuft diese Session?" ("Which interface is this session running
> through?") — options along the lines of: Terminal-CLI · Desktop-App · Web-App (claude.ai) ·
> VSCode-Extension · JetBrains-Extension · (something else, free text)

List the IDE options separately, not bundled as "VSCode/JetBrains". The harness is tied to how
*this session* was launched, not to the machine/environment it runs in — do not cache the answer
anywhere durable; just hold onto it for the rest of the current session once asked.

### Verify Git Identity At Session Start (Privacy)

Cheap and worth doing every session, before any commit: check the effective git identity (`git
config user.name` / `git config user.email`) that commits in this session would actually use.

The developer's human commits on this project should use a privacy-preserving identity like `gdiet
<gdiet@users.noreply.github.com>` unless explicitly requested otherwise — regardless of what a
given machine/environment happens to have configured globally.

If the effective git identity does not match: fix it scoped to this repo only if it is obvious how
(`git config --local user.name "gdiet"` / `git config --local user.email
"gdiet@users.noreply.github.com"`), never touching global config. Only escalate to asking the
developer if something is genuinely ambiguous (e.g. local config already holds a different,
seemingly intentional override).

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

- Follow Rust idioms and conventions; prefer simple, idiomatic code.
- Keep functions focused and testable; write self-documenting code with clear variable names.
- Avoid complex or non-obvious logic where avoidable; where it is genuinely unavoidable, add an
  explaining comment.
- Doc comments (`///`) describe an item's public contract — what it is, how to use it, its
  invariants. Keep pure implementation rationale (why this internal representation was chosen over
  an alternative) out of `///` and in a regular `//` comment next to the code instead, so `cargo
  doc` output for public API stays focused on what callers need. This matters most for `pub` items
  in library crates; private items in a binary crate's own code have no external consumers, so the
  distinction is less load-bearing there.
- In production code (not tests), never use a bare `.unwrap()`. Use `.expect("...")` instead, with
  a message that states *why* the failure cannot happen here (a poisoned mutex, a value just
  established a few lines above, a hardcoded literal that cannot fail to parse, etc.). If the
  failure genuinely *can* happen at runtime (I/O, external input, anything filesystem- or
  network-dependent), return a `Result`/`Errno` instead of panicking — this matters especially in a
  FUSE/WinFSP mount callback, where a panic can take down the whole mount session, not just the one
  request. Bare `.unwrap()` remains fine in `#[cfg(test)]` code.
- **Self-check before adding a sentence to a code comment that goes beyond what the type
  signature already shows**: is this already visible from the signature or the trait it
  implements (a borrow, a return type, a delegated method)? Is it describing a hypothetical
  caller or use case that does not actually exist yet in this codebase, rather than an actual
  constraint? Is the technical claim itself verified against the real, current implementation —
  not written from memory, general algorithm knowledge, or plausibility? A recurring pattern:
  speculative "here is how you might use this" prose, or a claim about a caller's needs that
  turns out wrong on inspection, both cost more to write and later un-write than just describing
  what the code does and its actual, checked constraints.
