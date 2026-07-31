# Agent Guidelines And Best Practices For This Project

## Project Overview

The goal of this project is a deduplicating backup application. The directory
this AGENTS.md file is in contains the Rust implementation (see [README.md](README.md)
for the crate layout).

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

## Verification Of Changes

When making changes to the codebase, always verify your changes before finishing and
suggesting commits:

- Verify that the code compiles: `cargo build`
- Run `cargo fmt` (or `cargo fmt --check` to verify without modifying)
- Run `cargo clippy -- -D warnings` — always treat Clippy warnings as errors, not just
  as informational; do not leave warnings in place assuming they're pre-existing or
  harmless, fix them (or, if a lint truly is a false positive, silence it explicitly
  and locally with a comment explaining why)
- Run `cargo test`
- Check whether `docs/` (plan stubs, `docs/plans/bounded-memory-io-pipeline.md`, etc.) or
  `README.md` describe behavior this change affects, and update them - stale docs are
  worse than no docs, since they actively mislead the next read. Move a plan doc under
  `docs/plans/implemented/` once the work it describes has actually shipped.

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
