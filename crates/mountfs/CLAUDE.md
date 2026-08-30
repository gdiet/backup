## Working Across Environments (mountfs-Specific)

This crate is the one place in the repository where "which machine/OS is this session actually
running on" matters operationally - it is the cross-platform FUSE/WinFSP mount abstraction, and
verifying a change here sometimes needs a real Windows console, a real WinFSP install, or network
access to a specific host, none of which an arbitrary agent session necessarily has. See
`docs/design/mount-abstraction.md` for why the crate is structured to make this a runtime, not
build-time, concern.

Cross-environment operational knowledge (how to reach the right machine/shell, known gotchas) lives
in skills, not inline here, since it is only relevant on the occasions this actually comes up - see
the `wsl-windows-sync` and `julius-winfsp-ssh` skills. Load the relevant one before doing that kind
of work rather than re-deriving it from scratch.

Before reaching for one of those, though: `scripts/build-windows-docker.sh` cross-compiles the
Windows backend from right here via Docker (see `docs/design/mount-abstraction.md`'s "Verifying
the Windows backend from Linux") - a compile/link check, not a substitute for real WinFSP
behavior, but worth running first for anything touching `crates/mountfs/src/windows/` before escalating
to a real Windows/WinFSP environment.

`cargo test` by default includes tests that need a real libfuse3/WinFSP mount (`/dev/fuse` access
on Linux, WinFSP installed on Windows) - named with a `real_mount_` prefix so they can be told
apart from everything else. In a session running in an environment known not to have that access
(a sandboxed/containerized session with no `/dev/fuse`, most agent sessions outside a real
Windows/WinFSP environment) their failure is an environment limitation, not a regression - run
`cargo test --workspace -- --skip real_mount` instead of treating a `real_mount_*` failure as
something to fix, and say so explicitly rather than silently ignoring red tests. See
`docs/development.md`'s "Tests" section for the same note aimed at a human developer.
