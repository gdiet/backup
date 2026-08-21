# Metadata Schema Comparison: `contents` Table Or Not

Decision record for point 4 of [`metadata-storage.md`](metadata-storage.md), comparing
[`metadata-schema-with-contents-table.md`](metadata-schema-with-contents-table.md) (chosen) against
[`metadata-schema-without-contents-table.md`](metadata-schema-without-contents-table.md) (rejected,
kept for reference). The sections below lay out the trade-offs as weighed at the time; see
"Decision" at the end for the outcome and reasoning.

## At a glance

| | With `contents` (chosen) | Without `contents` (rejected) |
|---|---|---|
| Tables (files + content) | 5 | 4 |
| Ref-count-maintaining triggers | 2 | 1 |
| Sentinel rows needing guard protection | 1 (root entry only) | 1 (root entry only) |
| Whole-file content deduplicated in metadata | yes | no (chunk-level dedup only) |
| "Find every file sharing this content" | direct index lookup | narrow-by-first-chunk, then verify |
| New schema elements needed beyond point 4's other fixes | none | `tree_entries.length`, `content_chunks` FK retarget |

## Structural complexity and correctness risk

Removing `contents` removes a class of problem the schema review originally surfaced around
in-place `content_id` mutation - though that specific gap is now closed architecturally for both
proposals regardless: a `tree_entries` row for a file is never inserted before its content is
settled (see "In-progress files are not written to the database" in each schema document), so
`content_id`/`length` is never mutated in place in either design, and neither needs an `AFTER
UPDATE` trigger to guard against it.

What removing `contents` still avoids: the two `tree_entries` ref-count triggers, which have
nothing to maintain once there is no shared, mutable content identity to keep synchronized in the
first place - with `content_chunks` scoped 1:1 to the tree entry that owns it (created and
cascade-deleted together, never re-pointed), that identity does not exist at all in the
without-`contents` proposal. Fewer tables and fewer triggers are a real, ongoing complexity cost
either way, independent of which proposal is chosen later. (Sentinel-row count is no longer a
differentiator between the two proposals: neither keeps a distinct, pre-seeded empty-content row -
see "In-progress files are not written to the database" in
`metadata-schema-with-contents-table.md`.)

## Metadata overhead for duplicate files

With `contents`, M byte-identical files (each with N chunks) share one `contents` row and N
`content_chunks` rows in total. Without it, each of the M files gets its own N `content_chunks`
rows - M×N total. Chunk-level deduplication is unaffected either way: the actual stored bytes are
identical in cost under both proposals, since `chunks`/`chunk_extents` do not change. This is
purely a metadata-row overhead question, not a storage-space one, and how much it matters in
practice depends on how much exact whole-file duplication real repositories actually contain
(templates copied across projects, duplicate photos, etc.).

### Empirical measurement

Measured against a real, long-used personal backup archive (a Scala DedupFS metadata export):
roughly 7.2 million tree entries in total, of which about 6.6 million reference real file content
and about 800,000 of those reference *distinct* content - a duplication factor of roughly 8.2
(each distinct content referenced by about 8.2 tree entries on average). Scala's own on-disk
structure does not use content-defined chunking (its multiple rows per content are physical
storage fragments from gap-filling allocation, not deduplicated chunk boundaries - closer in kind
to this project's `chunk_extents` than to `chunks`/`content_chunks`), so its own historical
row counts do not transfer directly. What does transfer, unaffected by that difference: each
distinct content's actual total length, and how many tree entries reference it.

Using those two real quantities, and estimating chunk count per content as
`round(length / 2^20)` (chunk_size_bits = 20) with a minimum of 1: the with-`contents` proposal
needs roughly 2.9 million `content_chunks` rows (one set per distinct content); the
without-`contents` proposal needs roughly 24-25 million (one set per tree entry) - a ratio close
to the measured duplication factor, as expected. Converting to bytes with the same rough per-row
estimates used elsewhere in this document (~30-35 bytes per `content_chunks` row including its
index entry, ~80 bytes per `contents` row including its unique index): on the order of 150-200 MB
of total overhead with `contents` (nearly all of it the `content_chunks` rows, a small remainder
the `contents` table itself) versus on the order of 750-800 MB without it - a difference in the
range of 550-650 MB for this specific archive, favoring the with-`contents` proposal.

This is one real data point, not a universal constant - the actual duplication factor depends
entirely on how a given repository is used (how often the same content is stored at more than one
location or point in time). It is large enough here, though, that "purely a metadata-row overhead
question" undersells it: for a workload like this one, the difference is a substantial fraction of
the metadata store's total size, not a rounding error.

## Finding duplicate content

With `contents`, "which other files share this exact content" is a single
`WHERE content_id = ?` lookup - directly useful for `blacklist process --delete-copies`, finding
every other occurrence of a blacklisted file's content so all copies can be removed together, not
just the one explicitly blacklisted.

Without `contents`, the same question is answered by narrowing on an existing index
(`content_chunks_chunk_id_idx`, filtered to each candidate's first chunk) and then verifying full
sequence equality for the resulting candidates - see
[`metadata-schema-without-contents-table.md`](metadata-schema-without-contents-table.md#finding-duplicate-content)
for the exact approach. This is not O(1) the way the direct lookup is, but should be close to it
in the typical case (a content-addressed chunk id is high-cardinality, so the candidate set is
usually small), with a real but bounded cost specifically when many files happen to share a first
chunk without being fully identical (e.g. a common document header).

Blacklisting-with-delete-copies is not yet a confirmed requirement - it is still an open question
in [`../../requirements/open-questions.md`](../../requirements/open-questions.md) - so this
trade-off is not urgent today, but is relevant if that requirement is later adopted.

## Diagnosing a broken chunk

`problems`/`check`'s "which files does this broken chunk affect" flow benefits from the opposite
direction. With `contents`, the lookup is two hops (chunk → distinct `content_id`s → tree entries
referencing each), which incidentally shares the "how many of this content's chunks are broken"
computation across every tree entry referencing that content. Without `contents`, the same lookup
is one hop (chunk → `content_chunks.entry_id` directly, no intermediate table) - simpler for the
common case, but repeats that computation once per affected tree entry rather than once per
distinct content when duplicates exist and are affected together.

## Consistency risk of a stored length

Both proposals store a file's total length directly (`contents.length` in one, `tree_entries.length`
in the other) rather than deriving it from summed chunk lengths on every read, for the same reason
in both cases: `getattr`/`stat`/`list`/`find`-style lookups need it often and cheaply. Both carry
the same consequence: nothing at the database level stops that stored value from drifting away
from the actual sum of its referenced chunks' lengths, since SQLite `CHECK` constraints cannot
aggregate across tables. Not a difference between the two proposals - relevant to both, and in
both cases best closed by having the repository's integrity check
(REQ-INTEGRITY-001 in [`../../requirements/functional/integrity.md`](../../requirements/functional/integrity.md))
verify it explicitly, not by a schema-level constraint.

## What is unaffected by this choice

`chunks` and `chunk_extents` - and everything decided about them in point 4 of
`metadata-storage.md` (the hash-width choice, the two new `CHECK` constraints) - are identical in
both proposals. Chunk-level deduplication, the primary mechanism behind REQ-STORAGE-001/002, is
unaffected either way.

## Decision

Status: decided - keep a `contents` table
([`metadata-schema-with-contents-table.md`](metadata-schema-with-contents-table.md)).

Reasons for the decision:

- The empirical measurement above is not a constructed worst case - it is the measured behavior of
  a real, long-used personal backup archive, which is the kind of workload this project is built
  for. A duplication factor around 8 turning into several hundred MB of avoidable metadata
  overhead is a real cost, not a theoretical one.
- The correctness concerns that originally counted against `contents` (the ref-count gap on an
  in-place `content_id` update, the two unprotected sentinel rows) are now closed - the first
  architecturally (no row is ever inserted before its content is settled, so there is no in-place
  update to guard against in the first place), the second by removing the empty-content sentinel
  entirely rather than protecting it, plus a guard trigger for the one sentinel row that remains
  (the tree's root) - documented in the chosen schema - they no longer weigh against it the way
  they did before those fixes existed.
- `contents` preserves a direct, cheap "find every other file with this exact content" lookup,
  useful if blacklisting-with-delete-copies is later adopted (still an open question in
  [`../../requirements/open-questions.md`](../../requirements/open-questions.md)), at no ongoing
  cost.
- The remaining complexity difference against the rejected alternative (two more triggers, one
  more table) is modest and fully documented, unlike the metadata-size difference, which scales
  with how much duplication a given repository actually contains.

No third structural alternative surfaced during this review that avoids the trade-off entirely
(see [`metadata-storage.md`](metadata-storage.md) point 4 and the schema review that preceded
these two schemas).
