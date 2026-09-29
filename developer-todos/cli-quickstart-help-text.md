# Look at Scala's README/QUICKSTART for `dfs`'s own help text

**Noted**: 2026-09-29, while comparing the Scala predecessor's CLI documentation against this
project's during migration-tool research.
**Size**: medium - confirm scope with the developer before starting (which commands get expanded
help, how much).
**Context**: Scala predecessor's `README.html`/`QUICKSTART.html` (in a `dedupfs-*-windows` release
directory, or the `main` branch's own doc sources); this project's current top-level help text is
just `about = "DedupFS: a deduplicating backup filesystem"` in `crates/cli/src/main.rs`.

The developer's own request: look at the Scala predecessor's `README`/`QUICKSTART` documents and
check whether `dfs`'s own `--help` output should grow something analogous - a new user reading only
`dfs --help`/`dfs <command> --help` currently gets clap's per-flag descriptions, not the kind of
"here is how to actually get started" walkthrough those two documents provide (initializing a
repository, mounting it, restoring from it, when to reclaim space).

Explicitly **not** a request to duplicate `README.html`/`QUICKSTART.html` verbatim into help text,
or vice versa - find what is genuinely missing from this project's own user-facing documentation
(a top-level `README.md`? a getting-started section in one of the `requirements/` files? richer
clap `long_about`s?) and add that, rather than copying the Scala docs' own wording. Per `AGENTS.md`'s
documentation philosophy, whatever gets written should describe this implementation as it is, not
reference the Scala predecessor as the reason it exists.
