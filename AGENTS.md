# Agent Guidelines And Best Practices For This Project

## Project Overview

The goal of this project is a deduplicating backup application. The directory
this AGENTS.md file is in contains the Rust implementation (see [README.md](README.md)
for the crate layout).

## Interaction With The Developer

Use the same language as the developer for chat interactions with the developer, but
use English as project language (see below 'Code Quality').

## Shell Commands

Avoid unscoped, recursive filesystem searches such as `find /` or `find / -maxdepth N`
— these traverse the entire filesystem and can take a very long time. Scope `find` (and
similar tools) to a specific, known directory instead (e.g. the workspace, or
`~/.cargo/registry/src/...` for crate sources). Prefer more targeted alternatives when
available, e.g. `cargo metadata` or `cargo tree` to locate crate sources/dependencies
instead of searching the filesystem blindly.

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
- Do not reference the Go implementation (`go/`) in Rust code or comments (e.g.
  "matches the Go implementation's ...") — the Go implementation is not being
  developed further and such references add no value going forward. The only
  exception is performance comparisons, e.g. "comparable to a similar
  implementation in Go, which achieves ..."
