---
paths:
  - "**/*.rs"
---

# Code Quality

- Follow Rust idioms and conventions; prefer simple, idiomatic code.
- Keep functions focused and testable; write self-documenting code with clear variable names.
- Avoid complex or non-obvious logic where avoidable; where it is genuinely unavoidable, add an
  explaining comment.
- Doc comments (`///`) describe an item's public contract - what it is, how to use it, its
  invariants. Keep pure implementation rationale (why this internal representation was chosen over
  an alternative) out of `///` and in a regular `//` comment next to the code instead, so `cargo
  doc` output for public API stays focused on what callers need. This matters most for `pub` items
  in library crates; private items in a binary crate's own code have no external consumers, so the
  distinction is less load-bearing there.
- In production code (not tests), never use a bare `.unwrap()`. Use `.expect("...")` instead, with
  a message that states *why* the failure cannot happen here (a poisoned mutex, a value just
  established a few lines above, a hardcoded literal that cannot fail to parse, etc.). If the
  failure genuinely *can* happen at runtime (I/O, external input, anything filesystem- or
  network-dependent), return a `Result`/`Errno` instead of panicking - this matters especially in a
  FUSE/WinFSP mount callback, where a panic can take down the whole mount session, not just the one
  request. Bare `.unwrap()` remains fine in `#[cfg(test)]` code.
- **Self-check before adding a sentence to a code comment that goes beyond what the type
  signature already shows**: is this already visible from the signature or the trait it
  implements (a borrow, a return type, a delegated method)? Is it describing a hypothetical
  caller or use case that does not actually exist yet in this codebase, rather than an actual
  constraint? Is the technical claim itself verified against the real, current implementation -
  not written from memory, general algorithm knowledge, or plausibility? A recurring pattern:
  speculative "here is how you might use this" prose, or a claim about a caller's needs that
  turns out wrong on inspection, both cost more to write and later un-write than just describing
  what the code does and its actual, checked constraints.
