# Crash Recovery: Store Sync, Durability Watermark, Automatic Backups, Recovery Check

Open design questions for REQ-RELIABILITY-001 and REQ-RELIABILITY-002 in
[`../../requirements/non-functional/reliability.md`](../../requirements/non-functional/reliability.md),
together with REQ-MAINTENANCE-001/002 (metadata backup and restore) in
[`../../requirements/functional/maintenance.md`](../../requirements/functional/maintenance.md).

The byte store writes chunk bytes through the operating system's cache. A power loss or the removal
of the storage device can therefore lose bytes that the metadata already refers to. The four
parts below work together. They let the software know which data might be affected after such an
event, let the user check it, and always leave a clean metadata backup to fall back on. None of
them is decided yet.

New directory entries are already made durable immediately, see DESIGN-STORE-007 in
[`byte-store.md`](byte-store.md). The parts below are about file contents.

## Not yet decided: store sync of file contents

Status: idea

`ByteStore::write` never flushes file contents. The store keeps a short list of data files with
unflushed writes, and gains a `sync()` call.

- **Contract of `sync()`:** every `write` call that returned before `sync()` was called is durable
  when `sync()` returns.
- **Bounded list:** the list holds at most a few files, for example 3. When a write would add a
  further file, the store flushes the least recently written one first. Writes mostly go to the
  newest data file, plus the next one at a file boundary and an older one when a gap is reused. A
  file that drops out of the list is one the writer has left, so its flush costs once.
- **The list alone guarantees nothing.** The newest data file stays at the head of the list while
  writes continue, so it is never flushed by eviction. The guarantee comes from `sync()`. A byte
  threshold ("flush when X MB are unflushed") is a possible addition that would bound the loss
  without any `sync()` call. Not weighed yet.
- **Threads:** the store is shared between threads, so the list sits behind a mutex. A file is
  added to the list only after its write returned. The list is taken under the lock, and the
  flushes run after the lock is released, because a flush can take tens of milliseconds on a hard
  disk. A write that finishes during a flush adds its file again. A flush acts on the whole file,
  not only on the handle that wrote, so reopening by path is enough.
- **Errors:** a failed flush is reported, never retried. On Linux, a failed `fsync` can drop the
  dirty pages, so a retry may report success for data that is gone.
- **Cost:** a flush per 100 MB data file is negligible. The cost depends on how often callers call
  `sync()`. REQ-RELIABILITY-002 allows about 10%. To be measured on the USB hard disk and the USB
  stick before deciding on an interval.

## Not yet decided: durability watermark

Status: idea

The repository metadata stores a **durability watermark**: the highest chunk id whose bytes are
known to be durable. It is raised only after `sync()` has returned, at the end of a write session
and at regular points during it (interval not decided).

- **"Possibly damaged" is a single query.** If `MAX(chunks.id)` is above the watermark, the last
  session ended before its bytes were known to be durable. The query is one lookup on the primary
  key.
- **Nothing has to be durable before the first write.** If the commit of a new chunk row is lost,
  the chunk does not exist. If the row survives but its bytes do not, its id is above the
  watermark. If the commit that raises the watermark is lost, the watermark stays lower, which only
  makes the check do more work. All three outcomes are safe.
- **Chunk ids must only grow.** `chunks.id` is an `INTEGER PRIMARY KEY` without `AUTOINCREMENT`
  today. SQLite may then reuse the ids of deleted rows at the top of the table, and a new chunk
  could get an id below the watermark. `AUTOINCREMENT`, or a separate counter, prevents that.
- **Migration:** chunks registered by the Scala migration point at bytes that already exist. The
  migration sets the watermark when it finishes, so these chunks do not count as unchecked.

### Alternative considered and rejected: a separate "dirty" flag

A flag set when a write session opens the repository and cleared after the final `sync()` would
tell the same thing as the watermark comparison. It is redundant, and it is weaker. The "dirty"
value would have to be durable before the first write, otherwise a lost commit could leave the
repository looking clean. It also only says that something might be wrong, not which chunks to
check. The leftover lock file of a crashed session (DESIGN-MAINTENANCE-002 in
[`repository-locking.md`](repository-locking.md)) is a similar signal, but it is gone once
`dfs unlock` removes it. The watermark survives that, and it is part of every metadata backup.

### Alternative considered and rejected: a timestamp per chunk

A modification time per chunk would let a check tool look at "the last few seconds". The
watermark answers the same question exactly, without guessing a time window and without the index
a "newest chunks" query would need. On storage size alone, seconds since repository creation would
not save anything either. SQLite stores integers in 1 to 8 bytes by size. Both Unix seconds and
seconds since creation need 4 bytes until 2038. Milliseconds need 6 bytes, which is 2 bytes more per
chunk, about 6 to 20 MB for 3 to 10 million chunks.

## Not yet decided: automatic metadata backup

Status: idea

This answers "Automatic metadata backup before a mutating session" in
[`../../requirements/open-questions.md`](../../requirements/open-questions.md) for two cases.

- **At the start of every read-write mount**, before the session writes anything. If the previous
  session ended with the watermark at the top, the backup is clean.
- **Right after a generation change**, that is, after a reclaim, a purge, or a future compaction
  has freed or moved stored bytes (see the generation counter idea in
  [`stale-backup-detection.md`](stale-backup-detection.md)). The backup belongs to the new
  generation, so restoring it is safe. A backup taken before the change is the dangerous kind,
  which is why the stale-backup warning of REQ-MAINTENANCE-007 is still needed for older backups.
- **Not before `ingest` by default.** An ingest of a few files must stay fast. An option can switch
  it on.
- **Retention is needed**, or the backups fill the disk. One candidate: keep the last N backups plus
  the first backup of each generation. Not decided.
- **Cost:** a metadata database of 2 to 3 GB, as expected for the developer's large repository,
  might take 20 to 30 s to copy on the USB hard disk. This is an estimate, not a measurement.
- A backup records whether it was clean, that is, whether the watermark was at the top when it was
  taken.

## Not yet decided: recovery check

Status: idea

A command checks a repository whose newest chunks lie above the watermark. It reads the bytes of
every such chunk again and compares their BLAKE3 hash with the stored hash.

- **Default:** report the affected files, as a list of repository paths.
- **Option:** delete the affected files, and remove the damaged chunks so that deduplication cannot
  reuse them. This matters: a chunk row whose bytes are wrong still matches its hash in the
  metadata, so a later ingest of the same content would link to it and spread the damage.
- When the check finds nothing, or after the option has removed the damage, the command raises the
  watermark to the top.
- Whether a read-write mount or an ingest refuses to start, or only warns, while chunks lie above
  the watermark is not decided.

## Open questions across the parts

- How often to call `sync()` and raise the watermark during a long session.
- Where automatic backups are stored, and the retention numbers.
- How the store's `sync()` and the watermark commit interact with SQLite's own durability settings.
  The order "sync the store, then commit the watermark" is the only hard rule found so far.
