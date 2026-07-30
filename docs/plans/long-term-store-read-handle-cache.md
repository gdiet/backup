# `LongTermStore`: cache open handles for reads

**Status**: finding recorded, not planned in detail, not implemented.

## Finding

`store::LongTermStore` opens the underlying OS file fresh on every single
`read`/`write` call (and again for each additional physical 100 MB file a
call happens to span) - see its own doc comment/implementation. The
question "does that cost anything measurable?" came up while discussing
`store`/`mount`'s I/O paths and was answered with a real benchmark rather
than assumed: `store/examples/bench.rs` (not run as part of `cargo test`/
CI - `cargo run --release -p store --example bench`) measures
`LongTermStore` as shipped against a same-access-pattern baseline that
opens one `File` once and keeps it open for the whole run, at the block
sizes the rest of the codebase actually uses (64 KiB -
`cli::store::READ_BUFFER_SIZE`; 256 KiB - `cli::chunk_store::
DRAIN_PIECE_SIZE`/`cli::mount::PERSIST_CHUNK_SIZE`).

Results (release build, 5 s per measurement, single run each - not
repeated/averaged, so treat as indicative, not precise):

|                  | Windows `store` | Windows open handle | Linux `store` | Linux open handle |
|------------------|-----------------|----------------------|----------------|---------------------|
| Write 64 KiB     | 0.20 GB/s       | 0.23 GB/s             | 0.49 GB/s      | 0.48 GB/s            |
| Write 256 KiB    | 0.13 GB/s       | 0.15 GB/s             | 0.33 GB/s      | 0.09 GB/s (noise)    |
| Read 64 KiB      | 0.88 GB/s       | 3.06 GB/s             | 3.58 GB/s      | 5.65 GB/s            |
| Read 256 KiB     | 1.59 GB/s       | 3.95 GB/s             | 5.74 GB/s      | 7.08 GB/s            |

**Writes**: re-opening costs little to nothing (within noise) on either
platform - something else (the actual disk write) dominates regardless of
open/close behavior.

**Reads**: re-opening costs a real, consistent, measurable amount - **2.5x
-3.5x slower on Windows**, **1.2x-1.6x slower on Linux** - at both block
sizes, on both platforms. Reads are also the more common path in practice
(`restore`, `check`, `mount` read/read-write, and every dedup-hit lookup
during a `store` run that skips writing but still reads to verify/serve
content elsewhere) - this is worth acting on at some point, unlike the
write side.

## Rough shape if/when planned

Only a **read** handle cache is justified by the data above - no need to
also cache write handles.

- Bounded, not unbounded: a large repository can have many more physical
  100 MB LTS files than are worth holding open at once (file-descriptor
  limits, and diminishing returns past whatever working set a typical
  access pattern actually revisits). An LRU eviction policy over a fixed
  cap (exact number not yet chosen - would want to check typical FD
  limits and revisit patterns first) rather than caching everything ever
  opened.
- Where it lives: most naturally inside `LongTermStore` itself (`store`
  crate), so every caller (`chunk_store::read_chunk_bytes`, used by
  `store.rs`, `mount.rs`, `check.rs`, `restore.rs`) benefits automatically
  without each needing its own cache. Needs interior mutability behind
  `&self` (`LongTermStore::read` currently takes `&self`, not `&mut
  self`) - either a `Mutex`-protected bounded map, or a `thread_local!`
  cache per calling thread (mirrors `cli::store`'s own `READ_CONNECTION`
  thread-local pattern for DB connections, avoiding cross-thread lock
  contention on a hot read path at the cost of one cache per thread
  instead of one shared cache). Worth comparing both before picking one -
  not obviously a slam dunk either way.
- Correctness: LTS files are only ever appended to or overwritten in
  place, never deleted/truncated/renamed during normal operation
  (`reclaim-space` doesn't shrink the physical files - see its own doc
  comment) - so a cached read handle shouldn't ever need active
  invalidation. Should double-check this assumption holds for every code
  path that touches `data/` directly before relying on it, though - not
  verified as part of this write-up.
- Windows-specific consideration: holding many file handles open longer
  than "for the duration of one call" may interact differently with
  other processes (antivirus scanning, backup software, concurrent
  `db backup`/external tools touching the repository) than the current
  momentary-open-close pattern does - worth a deliberate look before
  shipping, given the Windows numbers above are exactly the ones this
  would most help.
