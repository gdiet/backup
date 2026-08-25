# Performance baselines for tree and file-content operations across environments

**Noted**: 2026-08-25, mid-conversation during the tree-namespace-case-sensitivity design work
(REQ-MOUNT-010, DESIGN-MOUNT-005) - that discussion touched on whether a Rust-side scan-and-compare
fallback for Windows case-insensitive lookups stays fast enough for large directories, which
surfaced the broader, currently unanswered question below.
**Size**: large - confirm with the developer before starting. A real measurement effort across
multiple environments and configurations, not a quick benchmark.
**Context**: `requirements/functional/mount.md` (REQ-MOUNT-010); `docs/design/tree-namespace-case-sensitivity.md`
(DESIGN-MOUNT-005's "revisit if" trigger, which this would help answer with real numbers instead of
a guess).

Establish real performance baselines for:

- Tree operations (`mkdir`/`create`/`rename`/lookup/`readdir` in `crates/db`) across different
  environments and, separately, different power/energy settings (e.g. a laptop on battery-saver
  versus plugged in/performance mode) - relevant wherever a decision currently rests on an assumed
  directory size or scan cost rather than a measured one (see DESIGN-MOUNT-005's Rust-side
  case-insensitive scan fallback for a concrete example).
- File-content operations, equivalently, across different storage media, filesystems, and
  environments (an internal SSD versus external/USB storage versus a network share; ext4 versus
  NTFS versus others) - once REQ-STORAGE-007's byte store actually exists to measure.

The developer's own framing: "Performance Baselines für Tree-Operationen auf verschiedenen
Umgebungen / mit verschiedenen Energie-Settings; genauso auch für file content Operationen auf
verschiedenen Datenträgern / Dateisystemen und Umgebungen."
