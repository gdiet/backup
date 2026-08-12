# Optional chunk-hash verification during `restore` (and other read commands)

**Status**: design decided, ready to implement (see "Decisions" below) - not yet built.

**Trigger**: today, verifying that stored chunk bytes still match their recorded content hash only
happens via a separate `backup check` run. `restore` already reads every chunk it writes out but
only checks [`ReadIntegrity`](../../store/src/lib.rs) (bytes physically present/complete-length),
never the content hash - so a restore can silently write out bit-rotten data as long as the store
file itself isn't missing or short. Requested: an opt-in flag to verify hashes while restoring
(and consider whether other read commands should get it too), plus a decision on what happens when
a mismatch is found.

## Decisions

- **Corruption handling**: Variant B (skip the file, delete the partial output, warn, continue with
  the rest, but the overall command now exits `FAILURE` if any hash mismatch was found) as the
  *default* - **plus** a new opt-in flag to keep the output anyway instead of deleting it ("maybe
  something is still salvageable"), rather than only offering that as a future follow-up.
- **Scope widened to both problem kinds**: apply the same treatment to the *existing* missing/short
  data case too, not just the new hash-mismatch case - both now cause `FAILURE`, and both get their
  *own* separate opt-in "keep the output anyway" flag (two distinct flags, not one shared one - a
  user recovering from bit rot and a user recovering from a partially-lost store file are making two
  different informed trade-offs, not necessarily both at once). This is a **behavior change from
  today**: currently a restore with missing/short store data still exits `SUCCESS` (just prints
  "N warning(s)") - that changes to `FAILURE` as part of this work. Worth calling out prominently in
  the changelog/README, since a script relying on today's exit-0-despite-warnings behavior would be
  affected.
- **Naming precedent already exists in this codebase**: `mount --zero-fill-missing` (README.md:645-
  660) is exactly this same trade-off for the mount read path - off by default, and once on, "there's
  no way to tell zero-filled bytes from real ones." `read_chunk_bytes` (`chunk_store.rs:99-126`)
  already zero-fills the buffer for whatever portion of a chunk is missing/short before returning
  it alongside `ReadIntegrity::Incomplete` - so "keep the output anyway" for the missing-data case
  is *not* new plumbing, just no longer deleting a buffer that's already correctly assembled
  (zero-filled where necessary). Reuse the same flag name/semantics for `restore` for consistency:
  `restore --zero-fill-missing` for the missing/short-data case. The hash-mismatch case needs its
  own, differently-named flag (the buffer there isn't zero-filled, it's simply wrong) - e.g.
  `--keep-on-hash-mismatch` (exact name still open, not load-bearing).

## What already exists (this is a wiring task, not new crypto/hashing work)

`check_chunk` in [`cli/src/check.rs:144-194`](../../cli/src/check.rs) already does exactly the
verification this needs, scoped per-chunk:

```rust
let mut hash = [0u8; 20];
blake3::Hasher::new().update(&buf).finalize_xof().fill(&mut hash);
if hash.as_slice() != chunk.hash.as_slice() { /* BAD chunk ... */ }
```

`check [path]` already scopes to a file or a whole directory subtree via `scoped_chunks`
(`check.rs:110-140`), reusable as-is. So the actual gap isn't "how do we verify a hash" - it's
"`restore` reads bytes and only checks `ReadIntegrity`, never the hash, and doesn't offer the
option to."

## Why this needs a real decision, not just "add a flag"

Verifying costs something: it means hashing every restored byte a second time (once during
chunking/dedup at `store` time, once again here), which is real CPU on a large restore - the
existing `check` command's own doc note ("an unscoped run against a large repository can take a
while") makes that cost explicit already. That's exactly why it should be opt-in, matching the
request, not the default.

## Which commands should get this option?

- **`restore` - yes, primary target.** It already reads full chunk bytes for every file it writes;
  adding a hash check is a marginal CPU cost on data already in hand, not a new I/O pass.
- **`list` - no.** `list` ([`cli/src/list.rs`](../../cli/src/list.rs)) is metadata-only today -
  names, sizes, timestamps from `tree_entries`/`contents` - and never reads a single chunk byte.
  Adding hash verification would turn a near-instant metadata query into a full data read, a much
  bigger behavior change than "add a flag" suggests, and `check <path>` already exists as the
  dedicated tool for "verify this file/subtree's data without restoring it anywhere." Recommend
  against adding it here - if the goal is "verify before/instead of listing," that's `check`.
- **`find` - no**, same reasoning as `list` (name-pattern search over metadata, no data read).
- **`mount` (read path) - worth a separate look, not in this plan's scope.** Every file read
  through the mount already goes through `read_chunk_bytes`/`ReadIntegrity` the same way `restore`
  does, so the same option could apply, but mount reads are latency-sensitive and typically partial
  (arbitrary offset/length via FUSE `read`, not "restore this whole file") - verifying a chunk's
  *entire* content hash on every partial read would mean re-reading/re-hashing far more than what
  was actually requested. If wanted later, treat as its own follow-up rather than folding into this
  plan.
- **`check`/`problems` already do this** - no change needed there.

Recommendation: scope this plan to **`restore` only** for a first pass.

## What happens when a problem is found (decided)

`restore_file_at` ([`cli/src/restore.rs:339-363`](../../cli/src/restore.rs)) already has an
established pattern for missing/short store data: print a `warning:` line, increment a `warnings`
counter, delete the partially-written target file, skip to the next file, keep going. Today the
whole restore still exits `ExitCode::SUCCESS` regardless (`restore.rs:146-151`) - only the printed
"N warning(s)" distinguishes a partial restore from a clean one.

Going forward, both missing/short data *and* hash mismatches (once `--verify` is on) follow the
same shape, each with its own counter, its own opt-in "keep it anyway" flag, and each now causing
the overall exit code to be `FAILURE`:

| | Default behavior | Opt-in override |
|---|---|---|
| Missing/short store data (unconditional, today's existing check) | skip file, delete partial output, warn, continue, **exit `FAILURE`** (changed from today's `SUCCESS`) | `--zero-fill-missing`: keep the output - `read_chunk_bytes` already zero-fills the missing portion (`chunk_store.rs:106`), so this is "stop deleting an already-correct buffer," not new zero-filling logic |
| Hash mismatch (only checked when `--verify` is on) | skip file, delete partial output, warn, continue, exit `FAILURE` | `--keep-on-hash-mismatch` (name not final): keep the output even though its content hash doesn't match - "maybe something is still salvageable" |

Both override flags are independent - a user can ask for one, the other, both, or neither. Neither
changes the warning/exit-code behavior, only whether the file is left on disk afterward.

This is a deliberate widening of scope from the original request (verify hashes, decide what to do
on a hash mismatch) to also fix the *existing* missing-data case's silent `SUCCESS` exit at the same
time, since leaving that inconsistent with the new hash-mismatch handling would be an odd asymmetry
to ship. **This is a real behavior change**: a script/cron job relying on today's "restore always
exits 0, warnings are only in the text output" behavior will start seeing `FAILURE` for a restore
that has missing store data, even without ever passing `--verify`. Needs a call-out in the
changelog/README, not just the new-flag documentation.

## Suggested shape for a first implementation pass

1. Add a `--verify`/`--verify-hashes` flag to `restore`'s `RestoreArgs`, plus the two independent
   override flags (`--zero-fill-missing`, `--keep-on-hash-mismatch`) from the table above.
2. In `restore_file_at`'s per-chunk loop (`restore.rs:339-363`):
   - When `--verify` is set, hash the buffer the same way `check_chunk` does and compare against
     `chunk.hash` right after the existing `ReadIntegrity::Complete` match arm, before writing it
     out.
   - Replace the unconditional `fs::remove_file` calls (`restore.rs:349-350` and the hash-mismatch
     equivalent) with a check against the relevant override flag.
   - Track two separate counters (`incomplete`/`corrupted`, or similar - distinct from the existing
     `warnings`) so the final message and exit code can report/react to each kind distinctly, even
     though both now map to `FAILURE`.
3. Update `restore.rs:146-151`'s final summary/exit-code logic: `FAILURE` if either counter is
   nonzero, not just on a hard error as today.
4. Extract the actual hash-compare logic (currently inlined in `check_chunk`) into a small shared
   helper in `chunk_store.rs` (next to `read_chunk_bytes`) so `check.rs` and `restore.rs` call the
   same code instead of duplicating the blake3 XOF/truncation details.
5. README: document the new flags, and prominently call out the exit-code behavior change for the
   pre-existing missing-data case.
