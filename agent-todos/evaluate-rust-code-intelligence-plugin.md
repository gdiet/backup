# Evaluate a Rust code-intelligence plugin

**Needs**: nothing environment-blocking to *evaluate* - this file exists mainly to carry the one
concrete finding forward, since installing anything touches the developer's machine and is better
done with a quick heads-up than silently.
**Size**: small
**Opened**: 2026-08-21, by a Claude Desktop/WSL2 session. Carried over from
`docs/agent-setup-plan.md` item 8 when that document was closed out and condensed.
**Context**: [large-codebases.md](https://code.claude.com/docs/en/large-codebases.md#reduce-file-reads-with-code-intelligence) -
a code intelligence plugin connects Claude Code to a language server (LSP) so it can jump to
definitions and find references directly instead of grep/file-read chains, worth it for a
Rust-heavy repository this size. Requires the language's language server binary installed locally.

For Rust, that is `rust-analyzer`. Checked on this machine (2026-08-21): a `rust-analyzer` shim
exists at `~/.cargo/bin/rust-analyzer` (placed there by `rustup`), but running it fails with
`error: Unknown binary 'rust-analyzer' in official toolchain 'stable-x86_64-unknown-linux-gnu'` -
`rustup component list` confirms `rust-analyzer-x86_64-unknown-linux-gnu` is not marked
`(installed)`. The actual binary is not present yet; `rustup component add rust-analyzer` would
install it.

To finish this: confirm with the developer before installing anything (a rustup component plus a
Claude Code plugin, both touching machine-level tooling, not just this repository), then run
`rustup component add rust-analyzer` and `/plugin install rust-lsp@claude-plugins-official` (or
the current equivalent plugin name - check `/plugin marketplace` first, naming may have changed).
Confirm it actually improves symbol lookups on a real task in this repo before treating this as
done, rather than just confirming the install succeeded.

**Open question to resolve when this is picked up**: this project is worked on from more than one
environment/machine (WSL2 here, native Windows, possibly others later), and the availability check
above (`rust-analyzer` present or not, which plugin marketplace is reachable) is inherently
per-machine - the answer found on this machine does not tell you the answer on another one. Given
that, should checking/installing this be a repeatable [skill](https://code.claude.com/docs/en/skills)
instead of a one-off agent-todo, so the same check-and-install procedure is available on demand
wherever it comes up again, rather than re-deriving it from scratch (or re-writing a fresh
agent-todo) each time? Decide this when actually acting on the item, not before.
