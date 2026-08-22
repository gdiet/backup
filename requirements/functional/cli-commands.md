# CLI Commands

### REQ-CLI-001: Usage help without prior knowledge
Status: agreed
Importance: must

The command-line tool provides a way to request usage information — at the top level and for any
individual command — sufficient to discover available commands and their arguments without
consulting external documentation.

Rationale: a backup/repository tool is operated both interactively and from scripts; both need to
be able to discover its interface from the tool itself, not only from the README.

### REQ-CLI-002: Build identity on request
Status: agreed
Importance: must

The command-line tool provides a way to report its own build identity: its version, and, when the
running build links WinFSP, the attribution WinFSP's FLOSS exception requires — see REQ-MOUNT-006
in [`mount.md`](mount.md), which this requirement is the concrete implementation of.

Rationale: a build-identity command is the natural place a user actually looks, which is exactly
why REQ-MOUNT-006 names it as one of the required surfaces.
