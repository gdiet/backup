# `LongTermStore`: cache open handles for reads

**Status**: implemented (`store::with_read_handle`/`READ_HANDLES` in
`store/src/lib.rs`). Kept under `docs/plans/implemented/` as a record of
the finding, the design questions it raised, and how they were resolved.

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

## Implementation

Only a **read** handle cache was built - the data above doesn't justify
also caching write handles.

- **Bounded**: `READ_HANDLE_CACHE_CAPACITY = 8` - a small, per-thread LRU
  (linear-scan eviction, cheaper than hashing at this size), not chosen
  from any specific file-descriptor-limit measurement, just a modest cap
  against a repository with many physical LTS files.
- **Where it lives**: inside `LongTermStore` itself (`store` crate,
  `with_read_handle`/the `READ_HANDLES` thread-local), so every caller
  (`chunk_store::read_chunk_bytes`, used by `store.rs`, `mount.rs`,
  `check.rs`, `restore.rs`) benefits automatically without each needing
  its own cache.
- **Concurrency model chosen**: `thread_local!`, not a shared
  `Mutex`-protected map - mirrors `cli::store`'s own `READ_CONNECTION`
  thread-local pattern for DB connections, for the identical reason:
  avoids introducing a cross-thread lock/contention point on what was
  (and remains) a fully lock-free read path. `LongTermStore::read`'s doc
  comment now states this explicitly under "Thread safety". The tradeoff
  accepted: a cache per calling thread, not one shared cache - fine here,
  since callers are already per-thread workers (`store`'s Rayon pool,
  `mount`'s FUSE/WinFSP dispatch pool), not many short-lived threads that
  would each pay a cold cache.
- **Correctness**: confirmed (not just assumed) that nothing in the
  codebase deletes/truncates/renames a physical LTS file outside of
  `LongTermStore::write` extending it - grepped the whole workspace for
  `remove_file`/`remove_dir` touching `data_dir` before relying on this;
  none found. A cached read handle therefore never needs active
  invalidation - it just always `seek`s to an absolute position before
  reading, same as before, so reusing it across calls changes nothing
  observable.
- **Windows-specific concern, checked**: does keeping a read handle open
  longer than one call interfere with deleting the file/directory it
  belongs to (NTFS share-mode semantics)? Verified directly with a
  dedicated test (`a_cached_read_handle_does_not_prevent_deleting_its_
  directory`) rather than trusting `TempDir`'s own error-swallowing
  cleanup to never visibly fail - it doesn't block deletion; Rust's
  `std::fs::File::open` already requests share-mode flags permissive
  enough on Windows. Interaction with *other processes* (antivirus,
  backup tools) holding their own locks on the same files was not tested
  (would need a real second process) - not a currently known issue, just
  not exhaustively ruled out.

## Measured result after implementing

Re-ran `store/examples/bench.rs` after landing the cache (same
methodology as above - single 5 s run each, indicative not precise):

|                  | Windows `store` (before → after) | Linux `store` (before → after) |
|------------------|-----------------------------------|----------------------------------|
| Read 64 KiB      | 0.88 → 1.78 GB/s                  | 3.58 → 4.25 GB/s                 |
| Read 256 KiB     | 1.59 → 2.11 GB/s                  | 5.74 → 5.98 GB/s                 |

Roughly doubles Windows read throughput at these block sizes, closing
most (Linux: nearly all) of the gap to the open-handle baseline measured
earlier. Write throughput is unaffected, as expected (writes were never
changed).
