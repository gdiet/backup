# CLI Version String

`--version` (REQ-CLI-002 in
[`../../requirements/functional/cli-commands.md`](../../requirements/functional/cli-commands.md))
prints a version token identifying exactly which build is running, followed — on a build that
carries the Windows mount backend — by the WinFSP attribution REQ-MOUNT-006 in
[`../../requirements/functional/mount.md`](../../requirements/functional/mount.md) requires.

## DESIGN-CLI-001: Version token — package version, date, short commit hash

The version token is three space-separated fields:

```
0.0.1 2026-08-22 a1b2c3d4
```

- **Package version** (`0.0.1`): `CARGO_PKG_VERSION` of the binary crate, from a single
  `[workspace.package].version` shared across the workspace.
- **Date** (`2026-08-22`): the date of whichever commit is currently checked out, not the
  wall-clock time the build happened to run. Two builds from the same commit therefore always
  print the same token, and the date answers "how old is this code", not "when did someone type
  `cargo build`".
- **Short commit hash** (`a1b2c3d4`): the checked-out commit's hash, abbreviated to a fixed 8 hex
  characters. A working tree with uncommitted changes at build time appends `-modified` to this
  field (`a1b2c3d4-modified`) — chosen over git's own `-dirty` for more neutral wording.

Date and short hash are obtained by a small `build.rs` shelling out to `git` and emitting
`cargo:rustc-env=...`, read back via `env!(...)` — this runs only on the machine building the
crate, baking the values in as compile-time constants. After that, the compiled binary itself
never shells out to `git` or needs it installed. No build-info crate (e.g. `vergen`) is added for
this — package version needs no such step, `CARGO_PKG_VERSION` is already provided by Cargo
itself. If `git`/`.git` is unavailable at build time (e.g. building from a source archive with no
history), the short hash falls back to a fixed placeholder (`unknown`); the date instead falls
back to the wall-clock build date — still more useful than a placeholder, and reproducibility is
already unavailable in this case regardless of what the date field does.

### Alternative considered and rejected: wall-clock build timestamp instead of commit date

Rejected as the primary source (it is still the fallback when no commit date is available, see
above): makes the token non-reproducible (rebuilding identical source produces a different version
string) and less meaningful (tells you when a machine happened to compile it, not how stale the
code itself is) — see "Commit date" above.

### Alternative considered and rejected: a build-info crate (`vergen` or similar)

Rejected: three git values, read once at build time, do not justify an added dependency (see
AGENTS.md's "Dependencies") when a short `build.rs` using `std::process::Command` covers exactly
what is needed, with full control over the fallback behavior above.

## DESIGN-CLI-002: WinFSP attribution, included only on builds that carry it

When the binary is built for Windows (and therefore includes `mountfs`'s Windows backend, capable
of dynamically loading WinFSP at runtime — DESIGN-MOUNT-003 in
[`mount-abstraction.md`](mount-abstraction.md)), `--version` output additionally prints a one-line
lead-in, followed by the exact short notice WinFSP's FLOSS exception requires, already quoted
verbatim in [`../../NOTICE.md`](../../NOTICE.md):

```
Uses WinFSP for Windows mount support:
WinFsp - Windows File System Proxy, Copyright (C) Bill Zissimopoulos
https://github.com/winfsp/winfsp
```

The lead-in is not part of what the FLOSS exception requires — only the two notice lines below it
are — but without it, a user reading `--version` output has no reason to expect an unrelated
project's name and copyright notice to show up there.

Gated on `#[cfg(windows)]`, not on whether a mount actually ran this session: the FLOSS exception's
condition is about the software carrying the notice, not about a particular invocation having used
WinFSP. A non-Windows build never compiles in the code path that could link WinFSP at all, so the
notice would be noise there.

Only this two-line notice is printed, not the full FLOSS exception text quoted in `NOTICE.md` —
the exception's own condition 2 requires exactly "the copyright notice ... and a link to the WinFsp
repository", which this satisfies verbatim; the surrounding legal reasoning belongs in
`NOTICE.md`/`README.md` for whoever wants it, not repeated on every `--version` invocation.

### Alternative considered and rejected: full FLOSS exception text in `--version` output

Rejected: far longer than a version command's output is expected to be, and condition 2 does not
ask for it — only the short copyright-notice-and-link does (see `NOTICE.md`).
