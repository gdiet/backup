# Prototype a group commit after N operations for `dfs ingest` and measure it

**Why parked**: Planned follow-up of the commit-batching experiment, not yet begun. It is parked as a
handoff for whoever picks it up next. The experiment gave an upper bound only (one transaction
around the whole ingest). The next step is a realistic group size, and that needs design decisions
(see "Open design questions").
**Size**: medium/large. **Do not start without talking to the developer first.**
**Opened**: 2026-10-25, by the Linux/WSL2 session on `3327`
**Context**: [`performance/notes/2026-10-07-ingest-commit-batching-on-throttled-device.md`](../performance/notes/2026-10-07-ingest-commit-batching-on-throttled-device.md)
(results and method), [`ingest-on-slow-removable-media.md`](ingest-on-slow-removable-media.md)
(the candidate this belongs to), `REQ-RELIABILITY-001` and `REQ-RELIABILITY-002` in
`requirements/non-functional/reliability.md`, `REQ-PERFORMANCE-007` in
`requirements/non-functional/performance.md`, `Repository::migration_begin_batch` and
`run_in_transaction` in `crates/db/src/lib.rs`

## Starting point

One transaction around a whole ingest of 400 files of 100 B was 30 times faster on an emulated
device without write cache, 6 times faster with write cache and 2 times faster on a local SSD.
The written volume dropped by about 48 times with write cache. The cause is that every commit
rewrites the same few pages of the write-ahead log.

The experiment patch is stored as
[`performance/scripts/slow-disk/experiment-outer-transaction.patch`](../performance/scripts/slow-disk/experiment-outer-transaction.patch).
It applies to `crates/cli/src/ingest.rs` with `git apply`. With the environment variable
`DFS_EXPERIMENT_OUTER_TX` set, the whole run is one transaction. It is throwaway code. It has no
rollback on failure and no ordering guarantee between chunk rows and chunk bytes.

## How the slow device was simulated

The scripts are in `performance/scripts/slow-disk/` and are described in
`performance/scripts/README.md`. They need `sudo`.

1. `sudo setup.sh --write-ms 10 --sync` builds a sparse image file, a loop device, a `dm-delay`
   layer with 10 ms per write request, and ext4. `--sync` mounts without write cache and resembles
   a Windows device set to quick removal. Without `--sync`, the device has a write cache.
2. `measure.sh <files> <bytes> <runs> [outer]` ingests a generated tree into fresh repositories on
   the device. It prints the time and the write requests and MiB that reached the device. The mode
   `outer` needs the patch above. `MAXSEC` limits one ingest, 90 s by default.
3. `sudo teardown.sh` removes the device again.

Facts about this setup:

- The WSL2 kernel on `3327` provides loop devices, device-mapper and the `dm-delay` module. There
  is no `strace`. `/proc/<pid>/io` counts write calls without extra rights.
- The delay applies to every request, not to every commit. In sync mode, one ext4 operation issues
  several requests in sequence. The model is much harsher than the USB2 stick, so only counts and
  ratios carry over, not absolute times.
- In sync mode, the commit-per-operation variant takes 434 s for 400 files of 100 B. Do not repeat
  it. Use the one-transaction variant or a group commit there.
- The bandwidth and IOPS limit (`--wbps`, `--iops`, `enter-throttle.sh`) was never run.

## The planned step

Prototype a group commit: commit and begin a new transaction as soon as the open batch has run N
operations. The place to start is `run_in_transaction`, which already counts `batch_ops` while a
batch is open. For the prototype, N can come from an environment variable. Then measure:

- N = 1, 10, 100, 1,000 and unlimited, with and without device write cache, with the 400-file
  workload of the note. Cached runs take seconds. In sync mode, expect about 55 s for N = 10 and
  about 20 s for N = 100 (extrapolated, not measured).
- A 10 MB workload, to see whether the gain disappears when chunk writes dominate.
- The hypothesis that the 2,032 requests left in sync mode are the chunk appends to the data file.
- Repeat the best variants on `julius` with the real USB hard disk. The emulation does not
  reproduce the stalls of a flash stick.

The measurement script should keep a fixed amount of work and take N from a short probe run, so
that the baseline lasts about 20 s. A fixed time window does not suit ingest, because an aborted
run loses its open transaction.

## Open design questions

- **Order of rows and bytes.** A commit must not make a chunk row durable before its bytes are
  written. Today the row is committed first and the bytes follow. A group commit therefore needs
  either a count of chunks between row insert and byte write that must reach zero, or a reversed
  order.
- **Flushing the data files.** Before the commit, the dirty data files need an `fsync`.
  `ByteStore::write` opens and closes the segment file on every call, so the store would keep a
  set of dirty segment numbers and reopen each of them for `sync_all`. A new segment file also
  needs an `fsync` of its directory on Linux. The cost on a slow device must be measured.
- **When to commit.** After N operations, after a byte or time bound, at the end of the run and in
  an interrupt handler. A time bound needs a timer, otherwise the last group waits for the next
  operation.
- **Visibility.** Readers on other connections or in other processes see only committed rows. This
  is irrelevant while one connection serves reads and writes. It matters once reads move to their
  own connections.
- **Rollback.** A failed group rolls back as a whole. The name cache inside `Locked` must not keep
  entries that the rollback removed.
- **Scope.** Ingest and the migration tool are restartable and can use a group commit. Operations
  that delete or move bytes after the database state is fixed cannot. The read-write mount needs
  its own decision.
