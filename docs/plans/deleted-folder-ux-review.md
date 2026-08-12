# UX review: the mounted `[deleted]` folder

**Status**: review guide, not a code-change plan. For the user to walk through hands-on and record
findings/commentary against - the goal is a judgment on **whether the experience is good**, not
just whether the feature works as implemented (that part is already covered by
`docs/plans/implemented/mount-deleted-folder.md`'s verification checklist and existing automated
tests).

## Why this is worth a dedicated pass

`mount-deleted-folder.md` made several UX-shaped decisions during design/implementation, each a
judgment call rather than something derivable purely from correctness - flagged individually below
as specific things to form an opinion on, not just background. Nobody has done an end-to-end,
"pretend I'm an ordinary user who just deleted something by accident" walkthrough since the
original implementation's own Windows Explorer drag-and-drop spike (which was scoped narrowly, to
confirm `rename()` is really what Explorer calls - not a UX review).

## Setup

1. Build the `backup` binary (`cargo build --release -p cli`).
2. Init a disposable test repository and mount it read-write:
   ```bash
   backup -r /tmp/deleted-ux-test init
   backup -r /tmp/deleted-ux-test mount --read-write /path/to/mountpoint
   ```
   (Windows: `backup mount --read-write J:` or a path - see README's "Mount" section.)
3. Use a real file manager (Explorer on Windows, Nautilus/Files/Dolphin/etc. on Linux, or a plain
   terminal `cp`/`mv`/`ls`/`rm` if a GUI isn't available) against the mountpoint for everything
   below - the point is the experience through normal tools, not the CLI (`backup deleted`/
   `backup undelete` already exist for the CLI path and aren't what's under review here).

## Walkthrough scenarios

For each, note not just "did it work" but **how it felt** - was the result what you expected before
you tried it, did you have to think about it, would you trust this with something you actually
cared about recovering.

1. **Basic delete-and-recover**: create a file, delete it (through the file manager, not `backup
   del`), navigate into the containing folder, find `[deleted]`, open it, confirm the file is
   there, drag/move it back out to its original location. Did `[deleted]` show up where you'd
   look for it? Was the recovery gesture (drag/move out) discoverable, or did you have to already
   know the trick?

2. **Deleted directory, recursive recovery**: delete a directory containing several files and
   subdirectories, find it under `[deleted]`, browse *into* it (nested browsing - the deleted
   directory's own contents should be reachable, not just the top-level deleted entry), then
   recover the whole directory by moving it out. Confirm everything inside came back, not just the
   top-level folder. Note: recovery is always all-or-nothing for a directory (no way to recover
   just one file from inside a deleted directory without recovering the whole thing) - does that
   match what you'd expect, or is that a limitation worth surfacing to users somewhere (README,
   an in-app hint)?

3. **Same-named repeat deletions**: create `photo.jpg`, delete it, create a new `photo.jpg` with
   different content, delete that too. Look at `[deleted]` - both should be listed, disambiguated
   by an id suffix (e.g. `photo.jpg [42]`). Is that disambiguation format clear enough on sight, or
   would you have guessed wrong about which one to recover without extra care? Is there a better
   way to tell them apart (e.g. showing the deletion timestamp) that would have helped here?

4. **Per-directory scoping**: delete files in two different directories, confirm each directory's
   own `[deleted]` only shows *that* directory's deletions, not a repository-wide trash bin. Does
   "each folder has its own trash" match your mental model of how this should work, or would you
   have expected one central trash can (like a real recycle bin) instead?

5. **`[deleted]` visibility**: check a directory with no deletion history at all - `[deleted]`
   should not appear there (only shown once that directory actually has something deleted in it,
   per the "Refined during implementation" note in `mount-deleted-folder.md`). Does its
   appearing/disappearing based on history feel natural, or does the inconsistent presence (some
   folders have it, some don't) feel confusing on its own?

6. **The `rmdir` limitation**: delete a file inside some directory `foo/`, then try to delete `foo`
   itself (now empty of *active* files, but with deletion history). This should currently **fail**
   - `foo` can't be removed through the mount until its deletion history is gone (recovered or
   purged by `reclaim-space`). Was the failure's error message (whatever your file manager shows
   for it) clear about *why*, or just a generic "can't delete this folder" that would leave a real
   user confused/stuck? This is a known, documented, accepted limitation - the question here isn't
   whether to change the behavior, but whether the *error experience* around it needs work (a
   clearer message, a README callout, something else).

7. **Real `[deleted]` name conflict** (edge case, easy to miss): create a real folder literally
   named `[deleted]` somewhere, delete something else in that same parent directory, and confirm
   the real folder is what you see (the synthetic trash view silently doesn't appear for that one
   directory - "the real entry always wins," per the design doc). Would a user in this situation
   have any way to notice their deletions aren't recoverable through the mount there, or would they
   just quietly lose the trash-can safety net without any signal? Worth an opinion on whether this
   silent fallback is acceptable or whether some indication is needed.

8. **Read-only mount**: mount the same repository read-only (`backup mount` without
   `--read-write`), confirm `[deleted]` is still browsable and files inside are still readable, then
   try to recover something (drag it out) and confirm it fails. Is the failure mode reasonable for
   a read-only mount (matches every other write attempt failing the same way), or does "I can see
   my deleted file but can't get it back" feel like a trap worth a clearer signal (e.g. greying out,
   a tooltip) if your file manager supports that kind of thing at all?

9. **Cross-platform, if feasible**: if you have access to both a Linux/FUSE and a Windows/WinFSP
   environment, repeat at least the basic recover flow (#1) on both and note any difference in
   feel - drag-and-drop mechanics, icons/affordances a file manager might show differently, error
   dialog wording.

## What to record

For each scenario: what you tried, what happened, and - the actually important part - your honest
reaction as a user (confusing / fine / delightful / would-file-a-bug). Also flag anything you
noticed that isn't covered by the scenarios above but stood out during the walkthrough.

## After the walkthrough

Bring findings back as a punch list, roughly bucketed:
- **Working as intended, feels good** - no action needed, worth noting so it doesn't get
  re-litigated later.
- **Working as intended, but the experience could be better** - candidates for a follow-up plan
  (e.g. a clearer `rmdir`-blocked error message, a visibility indicator for the name-conflict
  case, a different disambiguation format).
- **Genuinely surprising or feels wrong** - worth a deeper look at whether the original design
  decision should be revisited, not just polished.
