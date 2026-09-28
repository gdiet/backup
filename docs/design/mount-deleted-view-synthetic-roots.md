# Deleted-View Synthetic Root Folders

How REQ-TREE-009's `[deleted]` addressing (in
[`../../requirements/functional/tree.md`](../../requirements/functional/tree.md)) and REQ-MOUNT-004/007/008/012/013
(in [`../../requirements/functional/mount.md`](../../requirements/functional/mount.md)) are actually
reached and recovered through the mount and the CLI, and why this design replaced an earlier one
that exposed the same view inline within every directory.

## DESIGN-MOUNT-020: Synthetic `[show-deleted]`/`[purge-deleted]` root folders replace inline exposure
Status: implemented (crates/cli/src/dedup_fs.rs, crates/cli/src/deleted.rs)

The mount's ordinary, live tree carries no synthetic content at all. Two dedicated root entries
mirror it instead: `[show-deleted]` (always present, browsing and recovery) and `[purge-deleted]`
(present only on a `--read-write --purge` mount, additionally allows permanently removing an entry
from the view). REQ-TREE-009's `[deleted]`/`[all]`/`[all]/[by-time]` addressing appears at the
corresponding location within either mirror, for every directory - never inline within the live tree
itself. `dfs list`/`dfs restore`/`dfs del --purge` reach the same mirror the same way (DESIGN-MOUNT-024
below).

### Rejected: `[deleted]` inline within every live directory, gated by a mount-time `--show-deleted`/`--purge` flag pair

The design this replaced. Two experimental branches worked through it in full before it was
abandoned:

**No-op delete on the view itself, without `--purge`.** Idea: without `--purge`, a delete-type
operation against `[deleted]`/`[time]` themselves would return a no-op `Ok` instead of `EACCES`, in
the hope that a recursive delete tool (Explorer, `rm -rf`) would still reach its actual target
directory without genuinely destroying anything. Live-tested against a real WinFSP mount and found
completely ineffective: Windows/`.NET`'s `Remove-Item -Recurse` (and, by extension, any comparable
Explorer mechanism) verifies client-side, repeatedly, whether a directory is genuinely empty before
ever attempting the underlying `rmdir` at all - a lying return value is never even exercised. A
`--debug-log` capture showed zero `rmdir` calls despite several seconds of the tool "trying". This
also directly contradicted what became REQ-MOUNT-007's own "Rejected" clause (reporting success
without actually purging) before that clause was even written down as a requirement.

**Genuine removal once the view is actually empty, under `--purge`.** Built on the base opt-in
instead: `unlink`/`rmdir` on `[deleted]`/`[time]` themselves would succeed for real once
`list_deleted_children` was genuinely empty. This worked once a second, necessary fix was found -
`[time]` had been listed unconditionally even when empty, which kept `[deleted]` looking non-empty to
any caller relying on its own `readdir` (exactly what an ordinary recursive-delete tool does before
attempting to remove a directory) even after everything under it had genuinely been purged.

Verified live: removing `[deleted]`/`[time]` themselves, once genuinely empty, worked correctly after
both fixes. But a deeper, structural problem remained, and is the actual reason this whole design was
abandoned: removing the last live child of a directory is itself an ordinary soft delete
(REQ-TREE-002), which immediately regenerated fresh `[deleted]` content *in the very directory a
recursive purge had just finished emptying* - content a single top-down delete pass, having already
enumerated that directory's children once, never revisits. As long as `[deleted]` shared a namespace
with the live tree it described, removing that tree's last live content and viewing its deletion
history were unavoidably the same operation on the same location - fully removing a directory with
any deletion history in one pass was structurally impossible under `--purge` alone.

### Addressing through an already-dead ancestor needed no new capability

A live/dead-boundary crossing can itself be nested arbitrarily deep once soft-deleted directories are
involved - reaching `[show-deleted]/a/[deleted]/b` when `a` itself has since been deleted, say. This
does not need identity-based addressing distinct from an ordinary path walk: `list_deleted_children`
already accepts a `parent_id` regardless of whether that entry is itself live or already
soft-deleted (`crates/db/src/tree.rs`), and `deleted::continue_from_deleted_entry`
(`crates/cli/src/deleted.rs`) already resolves arbitrarily far into an already-dead subtree once a
`[deleted]` segment has been reached. What is new is only that this resolution is reached through a
root-level prefix (`[show-deleted]`/`[purge-deleted]`) instead of inline within the live tree - a
routing change, not a new traversal capability.

### Accepted trade-off: a completely naive recursive walk from `/` still reaches `[show-deleted]`

`[show-deleted]` being an always-present, ordinarily-named directory at the mount root means a
completely unscoped recursive tool walking the whole mount from `/` (not one that specifically asks
for `[show-deleted]`) still descends into it - the same class of surprise the earlier, flag-gated
design existed to avoid entirely, just now through one discoverable entry point instead of scattered
through every directory. Accepted as-is: one clearly-named entry a tool could filter out by name, far
smaller and more contained exposure than every directory silently carrying its own `[deleted]` child.

## DESIGN-MOUNT-021: `[deleted]` shows only the most recent version per name; full history moves to `[all]`/`[all]/[by-time]`
Status: implemented (crates/cli/src/deleted.rs)

REQ-TREE-009's `[deleted]` shows at most one entry per distinct original name (the most recently
deleted one), under its own unmodified name. `[deleted]/[all]` carries what `[deleted]` used to mean
- REQ-TREE-004's full, disambiguated history - and `[deleted]/[all]/[by-time]` presents that same
data chronologically. This applies recursively at every level, including within an already-deleted
directory's own children, not only at a still-live directory's direct `[deleted]` view.

### Motivation

An ordinary recovery gesture (drag, cut-and-paste, or copy) that reuses the display name it is
handed would otherwise bake a disambiguation timestamp into the recovered entry's name - for the
single most common recovery case, "undo the last mistake". Confirmed live against a real WinFSP
mount, across three different tools (Explorer cut-and-paste, Explorer drag-and-drop, Total
Commander): a plain move always sends the full destination path with the source's own unmodified
basename, never just a target directory - POSIX `rename(2)` and Windows' rename APIs both require a
full destination path, constructed by the calling tool before the mount ever sees the call. Because
`[deleted]`'s own display name is already the true name, this holds for a copy exactly as it does for
a move - no operation-specific handling needed (contrast DESIGN-MOUNT-022 below, which does need to
special-case `rename()` for the *older*-version case this section does not cover).

### The recursion-scope reversal

Recovering a directory that itself carries deleted content - ten photos in a deleted folder, say -
needs the same "clean name, no digging required" property for its own children, not only for the
outermost entry. An earlier draft of this design limited "most recent version, plain name" to a
still-live directory's own outer `[deleted]` view, leaving a nested, already-dead directory's own
children at full, always-disambiguated history. Reversed once the ten-photo scenario was worked
through concretely: without recursion, there would be no view showing the photos' original names at
all, and no way to recover the whole directory in one step with clean results (DESIGN-MOUNT-023
below). The simplification this reversal costs - one more level of "which of these names is
current" bookkeeping at every depth, not only the outermost one - is worth it for the same reason the
outer-level version was worth it in the first place.

## DESIGN-MOUNT-022: `rename()` never substitutes a name; `--restore-original-names` opts into that convenience explicitly
Status: implemented (crates/cli/src/dedup_fs.rs, crates/db/src/tree.rs's `deleted_name_by_id`)

A `rename()`/move call through the mount always uses exactly the destination name the caller
specified - never a different one, regardless of source. REQ-MOUNT-012's `--restore-original-names`
opt-in is the only way to get automatic original-name recovery for an entry reached through
`[all]`/`[all]/[by-time]` (an *older* version than DESIGN-MOUNT-021's "most recent" already covers
for free).

### Rejected: substituting the true name automatically whenever the caller's given name matches the source's own display name

An earlier version of this idea ("if the caller kept the name unchanged, it must be a plain move -
recover under the true name; if they typed something different, honor it as a deliberate rename")
looked promising once DESIGN-MOUNT-021's empirical finding confirmed a plain move's destination name
always equals the source's own display name - the check would reduce to a direct string comparison
between the two paths' own final segments, nothing display-format-dependent to go stale.

Dropped anyway, for a reason independent of how reliably that comparison could be implemented:
`rename()`'s own protocol carries no "here is what actually happened" return value, only success or
failure. A result that does not match what a caller's own destination path literally asked for -
even when the substitution is objectively an improvement - is exactly the false-success problem
REQ-MOUNT-007 already rejects for `unlink`/`rmdir` (reporting purge success without actually purging),
just for `rename`. A script that moves an entry to a specific name and then addresses that exact name
afterward would find nothing there. Making this an explicit, mount-operator-level opt-in
(`--restore-original-names`) keeps the convenience available without ever surprising a caller under
an unmodified default mount - the same reasoning that already put `--purge` behind its own,
separate opt-in rather than folding it into `--read-write`.

## DESIGN-MOUNT-023: Cascading directory recovery targets exactly what browsing would have shown
Status: implemented (crates/db/src/tree.rs's `recover_latest_children`)

REQ-MOUNT-013: recovering a soft-deleted directory recursively recovers exactly what
DESIGN-MOUNT-021's "most recent per name" view would have shown for it and every one of its own
soft-deleted descendants - not literally every soft-deleted row ever recorded beneath it.

### Guiding principle

Technically, recovering a directory is always a `rename()`/move of that one directory entry. From
the user's perspective, it is an ordinary drag-and-drop move. At the *destination*, the result should
behave exactly like an ordinary move: what browsing showed already existing at the source now exists
at the destination too. At the *source*, that expectation does not hold unconditionally - because
the "most recent per name" slot (DESIGN-MOUNT-021) is dynamically recomputed, moving away the current
occupant of a name can reveal an older, independent entry of the same name that was previously
hidden behind it. Not a violation of DESIGN-MOUNT-022's honesty guarantee: that guarantee is about
this one `rename()` call's own result, not about a separate, unrelated entry later reappearing at the
same path - the same way a "most recently modified" or "most recent version" view naturally
re-surfaces its new top item once the previous one is removed, with no special-casing required or
expected.

Concretely: an older, superseded entry for a name that already has a more recent soft-deleted sibling
stays soft-deleted, untouched by the recovery, still reachable afterward through the now-again-live
directory's own `[deleted]`, `[all]`, and `[all]/[by-time]` views - exactly as before the directory
itself was deleted.

### No new per-descendant collision handling needed

The directory being recovered lands either at an entirely new location (nothing existing there to
collide with) or its own top-level collision is refused outright by REQ-MOUNT-009's existing
directory-collision rule (a directory colliding on either side is always refused) before any
cascading happens at all. A previously-drafted version of this design assumed cascaded children would
need their own, new collision-handling logic, mirroring `no_replace`/REQ-MOUNT-009 recursively for
potentially many simultaneous collisions - dropped once it became clear that case cannot actually
arise: `photos` recovered into a brand-new location has nothing to collide with; `photos` recovered
onto an existing live entry of the same name is already refused at the top level regardless of what
is inside it, precisely because REQ-MOUNT-009 never allows a directory-vs-anything collision to
proceed far enough to reach cascading logic in the first place.

## DESIGN-MOUNT-024: `dfs list`/`dfs restore`/`dfs del --purge` share the mount's `[show-deleted]` addressing
Status: implemented (crates/cli/src/list.rs, crates/cli/src/restore.rs, crates/cli/src/del.rs)

REQ-CLI-007: `dfs list` reaches REQ-TREE-009's addressing only through `[show-deleted]`, the same
root entry the mount exposes - not through its own, separate `--show-deleted` flag. `dfs restore`
and `dfs del --purge`'s own soft-deleted target addressing move from the earlier, flag-adjacent inline
`[deleted]` convention to the same `[show-deleted]`-prefixed form.

### What does and does not carry over per command

Only `dfs list` actually had a `--show-deleted` flag to replace - confirmed by reading
`crates/cli/src/main.rs` rather than assumed, after an earlier, unverified claim in the design
discussion behind this decision turned out to be wrong. `dfs find` has never had any deleted-content
option at all (it only ever searches live entries) and stays that way; `dfs restore` never had a flag
either, since `deleted::resolve`'s own inline `[deleted]` addressing already worked without one - only
its addressing *convention* moves to the new prefix, nothing to remove.

`[purge-deleted]` has no CLI counterpart. None of these commands mutate through path choice alone -
`dfs del --purge`'s own `--purge` flag already decides whether a soft-deleted target it reaches
through `[show-deleted]` gets permanently removed, the same way it already did before this addressing
existed. Exposing `[purge-deleted]` to `dfs list`/`dfs find`/`dfs restore` specifically would have
added a second, read-only-only path to the exact same content `[show-deleted]` already reaches, for
no behavioral difference - dropped for that reason, not on request.
