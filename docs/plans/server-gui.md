# Mount server GUI

**Status**: not started - this is a stub, not a plan. Rust's `backup
mount` is CLI-only; no GUI dependency exists anywhere in the workspace
today.

## What it is (Scala reference)

`gui=true` (or the `gui-dedupfs.bat`/`gui-readonly.bat` launcher scripts,
which set it and additionally run the JVM detached via `javaw` instead of
`java` so closing the console doesn't matter) starts a small desktop GUI
(`server.ServerGui` in the `scala` checkout) alongside the mount, showing
at minimum mount status and - per the README - a checkbox for
`docs/plans/mount-copy-when-moving.md`'s toggle. It's the documented,
recommended way end users are expected to run the Scala tool
interactively on both Windows and Linux (`dedupfs.bat`/`readonly.bat`
without `gui=` are the batch/non-interactive alternative).

## Why this is a bigger decision than the other stubs here

Every other doc in this directory is "port an existing, self-contained
Scala feature." A GUI is a different kind of decision for this project:
it needs a UI toolkit dependency (nothing GUI-related is in the Rust
workspace's dependency tree at all right now), a design for what it
should actually show (Scala's version is minimal - status + one
checkbox - but that's a low bar, not necessarily the right target), and a
judgment call on whether a CLI-first tool run from a terminal (today's
Rust `backup mount`'s whole model) actually wants one, versus staying
deliberately CLI-only and letting users who want a tray icon/status
window wrap it themselves. Recorded here because it's a real Scala
feature end users may expect, not because a Rust port is obviously the
right call - worth an explicit decision, not a default "port it," before
this gets its own real plan.

## If it's ever wanted

No rough shape written here on purpose - the toolkit choice (native
Win32/Cocoa-style vs. a cross-platform Rust GUI crate) and the "how
minimal" question would need to be decided first, and both are genuine
open questions, not implementation details.
