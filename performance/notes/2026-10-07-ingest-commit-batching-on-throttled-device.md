# Does fewer database commits speed up `dfs ingest` on slow media? - informal experiment

**Not a `performance/measurements/` protocol.** These are short, exploratory runs on an emulated
device. They answer "does the mechanism work, and roughly how large is the effect". They do not
answer "how fast is ingest on real hardware". Every number is from machine `3327` under WSL2
(kernel 6.18.40.1-microsoft-standard-WSL2), with a release build of commit `9ab3822c` plus the
experiment patch below.

## Question

`agent-todos/ingest-on-slow-removable-media.md` lists fewer database commits as a candidate. A
small file causes at least three commits. Each commit appends several 4 KiB pages to the
write-ahead log. How much does one transaction around the whole ingest save, as an upper bound for
any group commit?

## Method

### The emulated device

`../scripts/slow-disk/setup.sh` stacks a loop device, a `dm-delay` target with 10 ms added to every
write request, and ext4. Two variants were used:

- **Sync:** mounted with `-o sync`. Every `write()` waits for the device. This resembles a Windows
  device set to quick removal.
- **Cached:** mounted normally. Writes go to the page cache. Only `fsync` and write-back wait for
  the device.

The 10 ms apply to every request, not to every commit. A single ext4 operation in sync mode issues
several requests in sequence. The model is therefore much harsher than the USB2 stick of
`2026-10-06-julius-ingest-on-usb2-stick.md`. The absolute times do not compare with the stick. The
request and byte counts do not depend on the delay.

### The workload

400 files of 100 B with unique random content, spread over 20 subdirectories. The source tree is on
the local disk. The repository is fresh for every run and sits on the emulated device. `measure.sh`
reads the write requests and sectors from `/sys/block/dm-N/stat` before and after each run.

### The experiment patch

A temporary patch to `crates/cli/src/ingest.rs`, not part of the repository. It reuses the batch
functions of the migration tool:

```rust
// In try_run, before the loop over the sources:
let experiment_outer_tx = std::env::var_os("DFS_EXPERIMENT_OUTER_TX").is_some();
if experiment_outer_tx {
    repo.migration_begin_batch().map_err(|err| format!("error: {err}"))?;
}
// ... and after that loop, before the result message is built:
if experiment_outer_tx {
    repo.migration_commit_batch().map_err(|err| format!("error: {err}"))?;
}
```

With the environment variable set, the whole run is one transaction, and the per-operation
transactions turn into savepoints. Without it, the binary behaves as before.

## Results

### Device without write cache (sync), one run each

| Variant | Time | Write requests | Written |
|---|---|---|---|
| Commit per operation | 434 s | 41,008 | 126.6 MiB |
| One transaction | 14.5 s | 2,032 | 6.7 MiB |

The default variant issues about 100 write requests per file and needs about 1.1 s per file. The 434 s run
is too long to repeat, so this row is a single run.

### Device with write cache, four runs each

| Variant | Time | Write requests | Written |
|---|---|---|---|
| Commit per operation | 1.33, 1.38, 1.41, 2.59 s | 247-259 | 28.9 MiB |
| One transaction | 0.21, 0.22, 0.22, 0.24 s | 58 | 0.6 MiB |

### Local disk (SSD), three runs each

| Variant | Time |
|---|---|
| Commit per operation | 0.125, 0.129, 0.143 s |
| One transaction | 0.059, 0.067, 0.069 s |

### Correctness checks

- `dfs stats` is identical for both variants: 400 chunks, data end at 40,000 bytes.
- A second ingest of the same tree into the one-transaction repository adds no data. The data end
  stays at 40,000 bytes.

## Findings

1. **The gain exists in every device mode.** It is about 30 times without write cache, 6 times with
   write cache and 2 times on a fast disk. It grows with the latency of the device.
2. **The WAL volume explains the effect.** The default variant writes 28.9 MiB with the cache and
   126.6 MiB without it, for 40 KB of payload. The estimate of 400 files times 3 commits times about
   6 pages times 4 KiB gives about 28 MiB. The same pages are written again for every commit. One
   transaction writes each page once. The ratio of written bytes is about 48 times with the cache.
3. **With a cache, the saving is smaller in seconds.** The 10 ms then apply only to `fsync` and
   write-back. How a real device with a cache behaves is not covered by this model.
4. **The remaining requests are probably the chunk appends.** In sync mode, 2,032 requests are about
   400 times 5. This matches one `write()` per chunk to the data file in sync mode. This is a
   hypothesis and was not checked.

## Limits

- One transaction around the whole run is the upper bound. A group commit after N operations lies
  between the two variants. How quickly the gain drops with a smaller N has not been measured.
- The experiment ignores the order of chunk rows and chunk bytes. A real group commit must not
  commit a row before its bytes are written.
- Ingest only. Only 100 B files were measured. 10 MB files are dominated by the chunk writes.
- The sync row is a single run.
- The emulated device does not reproduce the stalls of the USB2 stick with mixed large and small
  writes.

## Possible next steps

- Prototype a group commit after N operations and measure N = 1, 10, 100, 1,000 in both modes.
- Measure a 10 MB workload.
- Repeat the one-transaction variant on `julius` with the real USB hard disk.
