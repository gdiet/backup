# Mount `[deleted]` listing: disambiguation suffix before the extension

**Status**: implemented (2026-08-14). Surfaced in conversation while discussing
`docs/plans/implemented/undelete-cli-replace-flag.md` - not a bug report, a UX improvement to an
already-shipped feature (`docs/plans/implemented/mount-deleted-folder.md`).

## What changed

The mount's `[deleted]` folder disambiguates two deleted entries that share a bare name by
appending `[<id>]`. That used to go at the very end of the whole name (`photo.jpg [42]`) - which
means the string no longer ends in a real extension, so a file manager's icon/type/"open with"
association and sort-by-type both break for exactly the entries that most need disambiguating.

Now the suffix goes before the extension instead: `photo [42].jpg`. One exception: a name starting
with `.` (a Unix "dot file", e.g. `.gitignore`) has no splittable extension in the usual sense - the
whole name is treated as opaque and the suffix goes at the end, same as a name with no dot at all
(`README [42]`).

Deliberately a simple last-`.` split, not an attempt to recognize multi-part extensions like
`.tar.gz` as one unit (`archive.tar.gz` → `archive.tar [42].gz`, not `archive [42].tar.gz`) -
matches Windows Explorer's own "(2)" duplicate-naming convention (this project already
targets/mirrors Explorer elsewhere for the mount), and avoids needing a maintained list of known
compound extensions that doesn't otherwise belong in a generic dedup backup tool.

## Scope

Only `cli/src/mount_deleted.rs`'s `display_name`/`parse_component` (the mount's synthetic
directory listing) - checked, not assumed: `backup deleted` (the CLI listing command,
`cli/src/deleted.rs`) shows `[id]` as its own leading column (`[42] - path`), never embedded in the
name itself, so it was never affected by this format either way.

## Implementation

- `display_name`: splits at the rightmost `.` (skipped entirely for a name starting with `.`),
  inserts `" [<id>]"` between the base and the extension.
- `parse_component` (the reverse, used when a client renames/drags something out of `[deleted]` to
  recover it): had to change from "suffix must be at the very end" to "find the rightmost
  well-formed `" [<digits>]"` marker anywhere in the component, splice it out, join what's left on
  either side" - a required, symmetric change, not optional polish, since recovery for any
  disambiguated entry would otherwise break outright. Return type changed from `(&str, Option<i64>)`
  to `(Cow<'_, str>, Option<i64>)`, since the reconstructed name is no longer necessarily a
  contiguous substring of the original component.

## Verification

`cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test
--workspace`, `cargo doc --no-deps --workspace` all clean. Tests updated/added in
`mount_deleted.rs`: `display_name` covers a normal extension, a `.tar.gz`-style compound extension
(confirming the deliberate last-dot-only behavior), no extension at all, and two dot-file cases
(including one with a further dot in it, `.env.local`, confirming the whole name stays opaque);
`parse_component` round-trips all of those back, plus the pre-existing false-positive-avoidance
case (`weird[name]`, no valid marker, left alone). README and the original
`mount-deleted-folder.md` design doc updated to match (the latter via a pointer to this doc, not a
silent rewrite of its own historical record).
