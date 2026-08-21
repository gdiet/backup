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
behavior, but worth running first for anything touching `mountfs/src/windows/` before escalating
to a real Windows/WinFSP environment.
