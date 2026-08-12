# Agent Guidelines And Best Practices For This Project

## Project Overview

The goal of this project is a deduplicating backup application. The directory
this AGENTS.md file is in contains the Rust implementation (see
[README.md](README.md) for user-facing usage, or
[docs/development.md](docs/development.md) for the crate layout and build
setup).

## Interaction With The Developer

Use the same language as the developer for chat interactions with the developer, but
use English as project language (see below 'Code Quality').

The developer is an experience programmer, but this is his first real rust project.
Expect him to make both obvious and subtle mistakes, and point them out to him,
especially the latter.

## Shell Commands

Avoid unscoped, recursive filesystem searches such as `find /` or `find / -maxdepth N`
— these traverse the entire filesystem and can take a very long time. Scope `find` (and
similar tools) to a specific, known directory instead (e.g. the workspace, or
`~/.cargo/registry/src/...` for crate sources). Prefer more targeted alternatives when
available, e.g. `cargo metadata` or `cargo tree` to locate crate sources/dependencies
instead of searching the filesystem blindly.

## Working Across Windows And WSL

The primary checkout is on the Windows filesystem (this repository's location under
`C:\...`). Linux-specific verification (real FUSE mount tests, `cargo build`/`clippy`/
`test` as they'd actually run on Linux) needs WSL - but run it against the native WSL
clone at `~/git/backup` (Linux `ext4`), not against the Windows checkout reached via
`/mnt/c/...`. `cargo` through the `/mnt/c` DrvFs/9p bridge is noticeably slower and can
surface filesystem quirks (permissions, locking, `/dev/fuse` access) that don't reflect
real Linux behavior - the whole point of testing on WSL in the first place.

Keep the two in sync through the shared remote, never by copying files directly:

- Make and commit all changes in the Windows checkout, same as always.
- After pushing from the Windows checkout, sync `~/git/backup` with:
  `git fetch origin <branch> && git merge --ff-only origin/<branch>`
  (fast-forward only - if that fails, something unexpected changed `~/git/backup`
  itself; investigate rather than force it).
- Verify `git status --short` is clean in `~/git/backup` right after syncing, before
  trusting any build/test output from it.
- Never make or commit changes directly in `~/git/backup` - it exists only to verify
  Linux behavior against what's already pushed, not as a second place to develop. If a
  Linux-only fix is needed, make it in the Windows checkout, push, then sync as above.

When invoking `wsl` from this (Git Bash/MSYS) shell with a WSL-only path (anything
under `/home/...`, not `/mnt/c/...`) anywhere in the command - e.g.
`wsl bash -lc '/home/georg/.../java -version'` - MSYS's automatic path conversion can
silently mangle it into a bogus Windows path (`C:/Program Files/Git/home/...`), failing
with a confusing `No such file or directory`. `/mnt/c/...` paths are unaffected (MSYS
already special-cases them), so this only bites when a command touches something that
only exists inside WSL, like a JRE unpacked under the Linux home directory. Fix: prefix
the call with `MSYS_NO_PATHCONV=1`, e.g. `MSYS_NO_PATHCONV=1 wsl bash -lc '...'`.

## Agent TODOs (Cross-Environment Handoffs)

This project is routinely worked on from more than one environment (see "Working Across Windows
And WSL" above, and in practice sometimes also a separate Windows machine reached over SSH) - an
agent in one environment regularly hits a wall that's trivial for an agent in another (needs a
real Windows console, WinFSP, Docker, network access to a specific host, etc.). `agent-todos/`
(see its own `README.md` for the exact file format) is where those get parked instead of silently
dropped.

When starting work in this repo, check `agent-todos/` for open items:

- **Small item** (a doc/comment fix, a quick local check, anything low-risk and quick): just do it
  yourself, right away, no need to ask first - then move its file to `agent-todos/done/` with a
  short note on what you did.
- **Medium/large item**: read it, but confirm with the user before starting - the file's own
  "Size" field is a starting guess, not a substitute for judgment; if in doubt, ask.
- Don't silently delete a `agent-todos/` file instead of moving it to `done/` - the record of what
  was done (and by which environment) is the point, for whichever agent looks next.
- If you hit a wall yourself that another environment could clear, add a new file there (see the
  README's format) rather than leaving a comment only in chat/session history that won't survive
  past this conversation.

## Verification Of Changes

When making changes to the codebase, always verify your changes before finishing and
suggesting commits. Scope which checks apply by what actually changed, not by how large
the change looks:

- Any `.rs` file, `Cargo.toml`/`Cargo.lock`, or `build.rs` touched — run the full suite
  below, regardless of how small the diff looks. Diff size doesn't predict blast radius
  in Rust: a one-line signature change can break call sites elsewhere, a `Cargo.lock`
  bump can silently change behavior, a single added `.unwrap()` violates this file's own
  conventions. This workspace is small enough that the full suite is cheap (under a
  minute with a warm build cache, ~10s measured on one dev machine) - there's no real
  time saved by carving out an exception for "micro" changes, so don't; stay strict.
- Only non-Rust files touched (docs, shell scripts, `Dockerfile`s, etc.) — the Rust suite
  below is a no-op and can be skipped; instead verify what's actually at risk (e.g. run a
  changed shell script, `docker build` a changed `Dockerfile`, confirm doc
  cross-references still resolve).
- Mixed changes — run the full suite.
- While iterating mid-task, before actually proposing a commit, `cargo check` is a fine
  faster substitute for `cargo build` to get a quick compile signal. It is not a
  substitute for the full suite below, which still has to run once before proposing a
  commit.

Full suite:
- Verify that the code compiles: `cargo build`
- Run `cargo fmt` (or `cargo fmt --check` to verify without modifying)
- Run `cargo clippy -- -D warnings` — always treat Clippy warnings as errors, not just
  as informational; do not leave warnings in place assuming they're pre-existing or
  harmless, fix them (or, if a lint truly is a false positive, silence it explicitly
  and locally with a comment explaining why)
- Run `cargo test`
- Run `cargo doc --no-deps` and check it produces no warnings - doc comments drift out
  of sync with renamed/removed items just like any other reference to them (broken
  intra-doc links, links to now-private items), and this is the cheapest way to catch
  that before it's the next reader's problem
- Check whether `docs/` (plan stubs, `docs/plans/implemented/bounded-memory-io-pipeline.md`, etc.) or
  `README.md` describe behavior this change affects, and update them - stale docs are
  worse than no docs, since they actively mislead the next read. Move a plan doc under
  `docs/plans/implemented/` once the work it describes has actually shipped.
- Same check, but for plain code comments in the files you're touching (not just `///`
  doc comments, which `cargo doc` above already covers): a comment stating *why* code
  is shaped a certain way rarely goes stale on its own, but a comment stating a
  *current status* ("not implemented yet", a specific measured number, "X doesn't
  support Y") can silently drift once that status changes, with nothing forcing a
  revisit - `cargo doc`/tests/clippy won't catch prose going stale. Update or remove
  such a comment if the change you're making falsifies it, even if it's outside the
  files you'd otherwise touch for the change itself.

Suggest an English semantic commit message following the Conventional Commits format.

Only commit changes when explicitly asked to do so by the user. Even if you've made
changes and suggested a commit message, wait for explicit permission before running
`git commit`.

## Dependencies

Suggest required or helpful dependencies to the user, but do not add them to the
project without explicit permission. When adding new dependencies, prefer `cargo add`
over manually editing `Cargo.toml`, so `Cargo.lock` stays in sync and the resolved
version is correct:

- `cargo add <crate>`
- `cargo add <crate> --path <path>` for local workspace crates

## Code Quality

- Use English for code, comments, and commit messages
- Follow Rust idioms and conventions; prefer simple, idiomatic code
- Keep functions focused and testable
- Write self-documenting code with clear variable names
- If possible, avoid complex logic and not obvious code. If unavoidable, add
  explaining comments
- Doc comments (`///`) describe an item's public contract - what it is, how to use
  it, its invariants. Keep pure implementation rationale (why this internal
  representation was chosen over an alternative, e.g. "a delegating enum instead of
  `Box<dyn Trait>`, since there are only ever two concrete types") out of `///` and in
  a regular `//` comment next to the code instead, so `cargo doc` output for public
  API stays focused on what callers need. This mainly matters for `pub` items in the
  library crates (`cdc`, `db`, `mountfs`, `store`, `spillcache`); private items in
  `cli`'s binary code have no
  external consumers, so the distinction is less load-bearing there and existing
  rationale-heavy `///` comments on private items don't need to be split up.
- In production code (not tests), never use a bare `.unwrap()`. Use `.expect("...")`
  instead, with a message that states *why* the failure can't happen here (a poisoned
  mutex, a value just established a few lines above, a hardcoded literal that can't fail
  to parse, etc.) — this is what an audit of the whole codebase found already in
  consistent use, so keep it that way. If the failure genuinely *can* happen at runtime
  (I/O, external input, anything filesystem- or network-dependent), return a `Result`/
  `Errno` instead of panicking - this matters especially in `mount.rs`, where a panic in
  one FUSE/WinFSP callback can take down the whole mount session, not just the one
  request. Bare `.unwrap()` remains fine in `#[cfg(test)]` code.
- Do not reference the Go implementation (`go/`) in Rust code or comments (e.g.
  "matches the Go implementation's ...") — the Go implementation is not being
  developed further and such references add no value going forward. The only
  exception is performance comparisons, e.g. "comparable to a similar
  implementation in Go, which achieves ..."
