# Print a short warning at start when a potentially dangerous option is given

**Noted**: 2026-10-05, while planning REQ-MOUNT-005 (`dfs mount --best-effort`).
**Size**: medium - a cross-cutting behavior, so it needs a requirement first, then a design entry
if the mechanism is non-trivial. Confirm with the developer before starting.
**Context**: `developer-todos/manual-dangerous-options.md`; REQ-OPERABILITY-007 and
REQ-OPERABILITY-004 in `requirements/non-functional/operability.md` are the nearest existing
requirements.

The developer's own idea: when an option that is potentially dangerous is explicitly given, the
command prints one line to the console at start, along the lines of "option XY is potentially
dangerous", with a pointer to where it is explained. The point is that an explicit opt-in is a
deliberate decision, and the warning confirms that the consequence was understood.

Open points to settle:

- The set of options that count as dangerous (see `developer-todos/manual-dangerous-options.md`).
- One shared helper, so that the wording and the output stream (stderr) are the same everywhere.
- Only for an explicitly given option, never for a default.
- The pointer cannot say "read the manual" before the manual exists and ships with the download
  (the planned DESIGN-CLI-008, see `docs/manual.md` once created).
- Whether scripts need a way to silence it. Probably not, if it stays a single stderr line.

`--best-effort` is the first user, implemented with REQ-MOUNT-005.
