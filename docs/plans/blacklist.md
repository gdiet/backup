# Content exclusion ("blacklist"), redesigned

**Status**: not implemented. A first implementation (`backup blacklist add`/
`backup blacklist process`) was built, reviewed, and merged, then removed
again once a design discussion revealed it didn't actually fit this
project's chunk-level dedup model well - see "Why the first
implementation was removed" below. This doc now describes a different,
not-yet-built design instead.

## What it is (Scala reference)

A separate utility (`fsc blacklist` / `blacklist.bat`, `dedup.blacklist` in
`Main.scala`, `dedup.db.blacklist` in the `scala` checkout) for permanently
excluding specific content from the dedup store while keeping a record of
it in the tree - e.g. installer caches, `Thumbs.db`, virus-scanner
quarantine files, or anything else a user has decided is not worth backing
up but doesn't want silently reappearing on the next run.

Two things happen in Scala, either or both:
1. **Add external files to the blacklist**: files placed in a
   `blacklistDir` are hashed and copied into the tree under a
   `dfsBlacklist` directory (at the repository root) in a subdirectory
   named by timestamp, optionally deleting the originals (`deleteFiles`,
   default **true** - checked directly in `Main.scala`).
2. **Process the internal blacklist**: for every file already under
   `dfsBlacklist`, its storage allocation is removed (`db.
   removeStorageAllocation`) - reading the file afterwards yields all-zero
   bytes of the original length, the tree entry itself stays *visible*.
   Optionally (`deleteCopies`, default false) every *other* tree entry that
   shares the same content is also marked deleted.

## Why this doesn't map directly onto the Rust dedup model

Scala dedupes whole files (one `dataId`/hash per file's full contents);
"remove this content's storage but keep the tree entry showing zeros" is a
natural operation there. The Rust rewrite dedupes at the CDC chunk level -
a blacklisted file's chunks may be shared with other, non-blacklisted files
entirely unrelated to whatever's being blacklisted (a common file header, a
zero-run, etc.), so "zero out this file's storage" doesn't have a clean
chunk-level equivalent: a chunk still referenced by something else can't be
zeroed without corrupting whatever else references it.

## Why the first implementation was removed

The first implementation sidestepped the "zero out storage" problem by
using plain `db::soft_delete` instead (see git history / the removed
`cli/src/blacklist.rs` for the full design). That avoided the chunk-zeroing
corruption risk, but a chat walkthrough of the "blacklist a file that was
never backed up before" case (a virus found lying around externally, never
previously stored) surfaced two real problems with it:

1. **It wrote the content to the store before removing it.** `blacklist
   add` reused `store`'s own hash-and-store pipeline unconditionally - for
   content not already present in the repository, this meant genuinely new
   chunks got written to `data/` first, and only the *tree entry*
   referencing them was soft-deleted by a later `blacklist process` call.
   The physical bytes stayed on disk (recoverable within the soft-delete/
   `reclaim-space` window) even though the entire point of blacklisting
   this content was to keep it out.
2. **It didn't actually prevent the content from reappearing later.**
   Soft-delete only touches `tree_entries.deleted_at` - the underlying
   `chunks`/`contents` rows (and their physical bytes) are untouched until
   a later `reclaim-space` run purges them. Until that happens, an
   unrelated, completely ordinary `store` run over some *other* location
   containing the same content would hit the existing chunks via normal
   dedup and happily create a new, active, non-blacklisted tree entry for
   it - silently undoing the blacklist. This directly contradicts the
   feature's own stated purpose ("doesn't want silently reappearing on a
   later run").

Trying to fix problem 1 by blocking the *write* of specific chunks up
front reintroduces the problem described in the section above: you can't
safely refuse to store a chunk without first knowing nothing else needs
it, and you can't know that without the same kind of ref-count analysis
`reclaim-space` already does after the fact. Both problems point at the
same missing piece: a way to recognize known-bad content *before* deciding
whether to touch tree entries at all, decoupled from ever writing
anything.

## Redesign: register a signature, then sweep for exact matches

Proposed shape (still rough - naming in particular is a placeholder, "the
process formerly known as blacklisting" needs a name that doesn't imply
"add a file to the tree" anymore, since it no longer does that):

1. **Register** (optional step - can be skipped to just re-run step 2
   against previously registered signatures): given one or more files/
   directories, compute each file's ordered chunk sequence (lengths and
   hashes, in order) and store its overall signature - in practice, this
   reduces to exactly what this project's `contents.hash` already is (a
   hash computed over the length+hash of every chunk in order - see
   `db::backup::resolve_content`/`content_hasher`), so registration is
   really just "hash this file the same way `store` would, but discard the
   bytes instead of writing them" - the cheap, non-buffering `cdc::
   HashingChunker` (currently unused in production, kept for exactly this
   kind of case - see its own doc comment) is enough; nothing needs
   `SpillingHashingChunker`'s RAM/disk-spilling buffer, because nothing
   gets written. Store the signature (content hash, byte length, source
   path, timestamp) in a new, dedicated table - not reusing `contents`,
   since a registered signature may not correspond to anything actually
   stored in the repository at all.
2. **Sweep** (always runs, whether step 1 registered anything new this
   time or not): for every registered signature, look up whether any
   `contents` row has a matching hash - if so, find every currently-active
   tree entry referencing that `content_id` (via `db::entries_for_content`,
   already built and tested, currently unused pending this) and
   `db::soft_delete` each one.

This sidesteps both problems above:
- Registration never writes anything to `data/` - there's no "temporarily
  present, then removed" window, because nothing beyond a small signature
  row is ever created for content that wasn't already in the repository.
- Sweeping isn't a one-shot side effect of adding a file - it's its own,
  independent, re-runnable operation. If blacklisted content reappears
  later (backed up again through an ordinary `store`/`mount` write, unaware
  of the blacklist - nothing in this design makes `store`'s own write path
  blacklist-aware, see "What this doesn't solve" below), running the sweep
  again finds and removes it again.
- No corruption risk from partially-shared chunks: matching happens at the
  whole-content (ordered chunk sequence) level, the same granularity this
  project already dedupes at - never at the level of an individual chunk
  in isolation, so a chunk shared between blacklisted and legitimate
  content is never at risk of being "un-stored" out from under the
  legitimate content still using it.

**What this doesn't solve** (worth saying plainly, since it came up
directly in the conversation that led to this redesign): the "a virus was
just found and must never touch disk, not even briefly" use case still
isn't handled as cleanly as it could be. Between two sweep runs, if the
same content gets backed up through an unrelated, blacklist-unaware
`store`/`mount` write, it *is* present in the repository again until the
next sweep removes it - registering a signature doesn't retroactively
protect a write that already happened, nor prevent a future one in real
time. That would need `store`/`mount`'s own write paths to consult the
signature table before accepting new content - a genuinely bigger,
cross-cutting change (touches every write path, not just this feature),
deliberately out of scope for this rough plan. If that turns out to matter
in practice, it's a natural follow-up once this simpler version exists,
not a reason to hold off on the simpler version.

## Interaction with the Scala repository migration tool

Checked directly (see `cli/src/migrate_scala_repo.rs`): a Scala-blacklisted
`dataId` (its `DataEntries` rows already zeroed - `start == stop == 0`) has
no recoverable bytes in the export at all, regardless of what this
project's own content-exclusion feature ends up looking like. The old
Scala blacklist operation already discarded the original bytes
permanently, before the export was even taken - there is nothing to
migrate, and nothing a redesigned Rust-side feature could retroactively
recover either. The migration tool already skips these entries with a
warning; the warning message was updated to say so explicitly (permanent,
pre-existing data loss from the old repository, not a migration
limitation) rather than leave it ambiguous ("blacklisted or corrupt?").

No other interaction: the migration tool doesn't need to know anything
about this project's own (removed, and not-yet-rebuilt) content-exclusion
feature - it only ever deals with the *old* Scala repository's already-
committed state.

## Open questions for when this gets planned in detail

- Naming, for the operation and its subcommand(s)/flags.
- Exact schema for the signature table (columns beyond hash/length/path/
  timestamp - e.g. does it need its own "processed at" / "still active"
  bookkeeping, or is a plain append-only log enough since sweeping is
  idempotent either way?).
- Whether "register" and "sweep" should be one subcommand (register-if-
  given-paths, always sweep after) or two separate ones (matches the
  removed implementation's `add`/`process` split, but that split existed
  for different reasons there).
- Whether a `--delete-copies`-style opt-out is still meaningful - the
  removed implementation's `--delete-copies` distinguished "the canonical
  copy just added" from "other copies elsewhere"; this redesign has no
  canonical copy at all, so sweeping already removes *every* matching
  active entry unconditionally. Worth deciding whether that's always
  wanted or should stay optional.
