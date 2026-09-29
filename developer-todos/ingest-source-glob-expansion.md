# `dfs ingest` has no built-in glob expansion for source paths

**Noted**: 2026-09-29, while comparing `dfs ingest` against the Scala predecessor's `fsc backup`
command during migration-tool research.
**Size**: small to medium - confirm the desired behavior with the developer before starting
(whether to add glob expansion at all, and if so, matching which glob syntax).
**Context**: `crates/cli/src/main.rs`'s `Ingest` command (`paths: Vec<String>`, REQ-INGEST-007's
`target_path` template syntax covers only the *target* side); the Scala predecessor's `fsc backup`
resolves `*`/`?` wildcards in *source* arguments itself (`BackupTool.resolveSource` in the Scala
source, `main` branch).

The developer's own observation: unlike Scala's `fsc backup /notes/* /backup/...`, `dfs ingest`
does not resolve any wildcard/glob pattern in its own source arguments - a caller who wants to
ingest `/notes/*` needs their own shell to expand that before `dfs ingest` ever sees the paths (and
a shell that does not do this the way the operator expects, or none available in the calling
context, cannot get that behavior at all).

Worth deciding deliberately, not by omission: does `dfs ingest` want its own glob support (matching
Scala's `*`/`?` semantics, or something else), or is relying on the calling shell's own expansion
an acceptable, permanent design choice (e.g. because ingest is expected to run from a real shell in
practice, unlike, say, a scheduled task with no shell expansion at all)? If glob support is wanted,
decide the syntax question on its own merits rather than assuming Scala's `*`/`?` choice is right
for this rewrite - REQ-INGEST-007's `+`/`!` target-path markers already established Rust's own
symbol conventions for this command family, and reusing `?`/`*` for a *second*, source-side meaning
should be weighed against that, not adopted by default.
