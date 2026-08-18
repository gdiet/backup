# `SpillingHashingChunker` could wrap `HashingChunker` instead of duplicating its slicing loop

**Needs**: no special environment/capability - a deliberate look and a decision, parked here so
it is not lost rather than because anything is blocked.
**Size**: small
**Opened**: 2026-08-18, by Desktop App session (surfaced while working through `cdc`'s
`HashingChunker` in a sibling project's design discussion)
**Context**: `cli/src/spilling_chunker.rs`'s own doc comment already explains the duplication
("mirrors `cdc::BufferingHashingChunker::next`'s slicing logic (duplicated here rather than
wrapped ...), since `HashingChunker` hashes and discards internally without exposing the bytes").

`cdc::HashingChunker` doesn't expose the wrapped `Chunker`'s `bytes_into_chunk()`, even though
`Chunker` itself already has that method as part of its public trait. `SpillingHashingChunker::
next` re-implements `HashingChunker::next`'s exact slicing arithmetic (`end_in_data = length -
bytes_into_chunk`) rather than wrapping it, apparently only because that one piece of state isn't
reachable from outside `HashingChunker`.

Adding `pub fn bytes_into_chunk(&self) -> u64` to `HashingChunker` (delegating to the wrapped
`Chunker`) would let `SpillingHashingChunker` hold a `HashingChunker<H, C>` instead of separate
`chunker`/`hasher` fields, use it for the hashing half, and drive its own `WriteCache` buffer
management using `bytes_into_chunk()` plus the returned `length` - removing the duplicated
slicing loop without losing any capability.

Not urgent (both versions are correct today; this is a de-duplication opportunity, not a bug) -
worth a deliberate look next time `spilling_chunker.rs` or `cdc`'s `HashingChunker` are touched
anyway.
