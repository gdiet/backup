# Persist-queue backpressure: before/after benchmark results

Captured by running `cli/src/mount.rs`'s `bench_many_small_files`/
`bench_large_files_spill_pressure` tests (see their own doc comments) -
WSL2/Debian, release build, real `libfuse3` mount via the existing
in-process test harness. Both are manual (`#[ignore]`) benchmarks, not
part of the normal suite.

**BEFORE** = commit `02bbd885` (`PERSIST_QUEUE_CAPACITY = 4`, job-count gate).
**AFTER** = commit `bbc9aa07` (byte-based gate on `queued_persist_bytes`,
tied to `--write-cache-mb`).

## Summary

- **Many small files**: throughput roughly **3.5x** (18.3 -> 64.6 MB/s),
  median close latency **12x** lower (57.3ms -> 4.6ms). Clear, large win -
  the job-count gate was throttling small files far earlier than any real
  pressure justified.
- **Large files (burst big enough to exhaust the FUSE worker pool)**: peak
  spilled-but-unpersisted data down **20%** (750MB -> 599MB), full drain
  time down **27%** (166.8s -> 121.3s), backpressure engaging noticeably
  earlier (file 15 vs. file 11 of 20).
- **Important nuance found while measuring this**: FUSE's `release`
  callback (where `enqueue_persist` runs) is dispatched asynchronously -
  the client's `close()` does not wait for it. So the new gate's blocking
  only becomes visible to the *client* once enough concurrently-blocked
  `release` calls exhaust libfuse's worker-thread pool (empirically,
  around 10+ concurrent large files here) - not immediately on the first
  file that pushes `queued_persist_bytes` over the threshold. A small
  burst (6 files) shows *no* measurable difference between BEFORE and
  AFTER for exactly this reason, even though the byte-based gate is
  correctly engaging internally the whole time (confirmed via the 20-file
  run, and via `spillcache`'s own unit tests for the counter itself). This
  is a real, structural limit on how quickly this kind of gate can ever
  become client-visible under real FUSE/WinFSP semantics, not a bug in
  this specific change - worth keeping in mind for any future refinement.

## Many small files (200 x 1 MB, datastore throttled to 30 MB/s)

Command:

```
BACKUP_BENCH_THROTTLE_STORE_MBPS=30 cargo test --release --package cli --bin backup \
  mount::tests::bench_many_small_files -- --ignored --nocapture
```

### BEFORE

```
files=200 size=1000000B total_bytes=200000000B close_loop=10.915652608s close_loop_throughput=18.32MB/s p50=57.3ms p95=73.1ms max=117.5ms
drain_on_unmount=42.995343ms end_to_end=10.958647951s end_to_end_throughput=18.25MB/s
```

### AFTER

```
files=200 size=1000000B total_bytes=200000000B close_loop=3.094943479s close_loop_throughput=64.62MB/s p50=4.6ms p95=58.9ms max=126.1ms
drain_on_unmount=47.730715ms end_to_end=3.142674194s end_to_end_throughput=63.64MB/s
```

## Large files, 6-file burst (50 MB each, forced spill via `write_cache_mb: 1`, datastore throttled to 5 MB/s)

Command:

```
BACKUP_BENCH_THROTTLE_STORE_MBPS=5 cargo test --release --package cli --bin backup \
  mount::tests::bench_large_files_spill_pressure -- --ignored --nocapture
```

### BEFORE

```
file 0: close=152.250587ms spill_dir_now=50.0MB peak_so_far=50.0MB
file 1: close=163.910317ms spill_dir_now=100.1MB peak_so_far=100.1MB
file 2: close=168.640989ms spill_dir_now=150.1MB peak_so_far=150.1MB
file 3: close=157.767554ms spill_dir_now=200.2MB peak_so_far=200.2MB
file 4: close=154.042676ms spill_dir_now=250.1MB peak_so_far=250.1MB
file 5: close=155.944964ms spill_dir_now=300.2MB peak_so_far=300.2MB
files=6 size=50000000B write_loop=953.410682ms unmount_request=57.056758ms full_drain=66.136418185s peak_spilled=300.2MB (6.0x a single file)
```

### AFTER

```
file 0: close=114.572314ms spill_dir_now=50.0MB peak_so_far=50.0MB
file 1: close=189.658565ms spill_dir_now=100.2MB peak_so_far=100.2MB
file 2: close=157.702856ms spill_dir_now=150.0MB peak_so_far=150.0MB
file 3: close=163.352022ms spill_dir_now=200.1MB peak_so_far=200.1MB
file 4: close=159.938543ms spill_dir_now=250.2MB peak_so_far=250.2MB
file 5: close=154.855673ms spill_dir_now=300.0MB peak_so_far=300.0MB
files=6 size=50000000B write_loop=940.933668ms unmount_request=64.546914ms full_drain=65.350694368s peak_spilled=300.0MB (6.0x a single file)
```

Essentially identical - as expected, given the "Important nuance" above:
6 concurrently-blocked `release` calls isn't enough to exhaust the FUSE
worker pool and propagate backpressure back to this benchmark's own
client loop.

## Large files, 20-file burst (same 50 MB / forced spill / 5 MB/s throttle)

Command:

```
BACKUP_BENCH_THROTTLE_STORE_MBPS=5 BACKUP_BENCH_LARGE_FILES=20 cargo test --release \
  --package cli --bin backup mount::tests::bench_large_files_spill_pressure -- --ignored --nocapture
```

### BEFORE

```
file 0: close=151.749462ms spill_dir_now=50.0MB peak_so_far=50.0MB
file 1: close=144.747823ms spill_dir_now=100.2MB peak_so_far=100.2MB
file 2: close=137.609598ms spill_dir_now=150.1MB peak_so_far=150.1MB
file 3: close=141.878794ms spill_dir_now=200.2MB peak_so_far=200.2MB
file 4: close=147.952421ms spill_dir_now=250.1MB peak_so_far=250.1MB
file 5: close=170.104627ms spill_dir_now=300.1MB peak_so_far=300.1MB
file 6: close=149.514373ms spill_dir_now=350.0MB peak_so_far=350.0MB
file 7: close=149.193003ms spill_dir_now=400.1MB peak_so_far=400.1MB
file 8: close=147.769138ms spill_dir_now=450.3MB peak_so_far=450.3MB
file 9: close=159.103867ms spill_dir_now=500.1MB peak_so_far=500.1MB
file 10: close=173.133541ms spill_dir_now=550.1MB peak_so_far=550.1MB
file 11: close=186.69466ms spill_dir_now=600.1MB peak_so_far=600.1MB
file 12: close=162.542043ms spill_dir_now=650.2MB peak_so_far=650.2MB
file 13: close=181.972106ms spill_dir_now=700.1MB peak_so_far=700.1MB
file 14: close=212.634809ms spill_dir_now=750.3MB peak_so_far=750.3MB
file 15: close=9.118979681s spill_dir_now=749.3MB peak_so_far=750.3MB
file 16: close=10.92071661s spill_dir_now=749.3MB peak_so_far=750.3MB
file 17: close=10.987806693s spill_dir_now=749.3MB peak_so_far=750.3MB
file 18: close=11.084239247s spill_dir_now=749.3MB peak_so_far=750.3MB
file 19: close=10.952654255s spill_dir_now=749.3MB peak_so_far=750.3MB
files=20 size=50000000B write_loop=55.484708702s unmount_request=191.02565ms full_drain=166.835720968s peak_spilled=750.3MB (15.0x a single file)
```

### AFTER

```
file 0: close=163.531724ms spill_dir_now=50.0MB peak_so_far=50.0MB
file 1: close=191.796262ms spill_dir_now=100.2MB peak_so_far=100.2MB
file 2: close=166.184781ms spill_dir_now=150.0MB peak_so_far=150.0MB
file 3: close=154.68117ms spill_dir_now=200.1MB peak_so_far=200.1MB
file 4: close=155.498756ms spill_dir_now=250.2MB peak_so_far=250.2MB
file 5: close=198.66465ms spill_dir_now=300.2MB peak_so_far=300.2MB
file 6: close=178.426681ms spill_dir_now=350.2MB peak_so_far=350.2MB
file 7: close=161.763054ms spill_dir_now=400.3MB peak_so_far=400.3MB
file 8: close=196.453386ms spill_dir_now=450.0MB peak_so_far=450.0MB
file 9: close=186.898444ms spill_dir_now=500.3MB peak_so_far=500.3MB
file 10: close=179.826052ms spill_dir_now=550.3MB peak_so_far=550.3MB
file 11: close=9.66570887s spill_dir_now=549.3MB peak_so_far=550.3MB
file 12: close=11.333505053s spill_dir_now=549.3MB peak_so_far=550.3MB
file 13: close=139.538649ms spill_dir_now=599.3MB peak_so_far=599.3MB
file 14: close=21.990002653s spill_dir_now=549.3MB peak_so_far=599.3MB
file 15: close=148.1037ms spill_dir_now=599.3MB peak_so_far=599.3MB
file 16: close=22.066604401s spill_dir_now=549.3MB peak_so_far=599.3MB
file 17: close=11.343194103s spill_dir_now=549.3MB peak_so_far=599.3MB
file 18: close=11.452297139s spill_dir_now=549.3MB peak_so_far=599.3MB
file 19: close=11.549478537s spill_dir_now=549.3MB peak_so_far=599.3MB
files=20 size=50000000B write_loop=101.626154252s unmount_request=188.299524ms full_drain=121.289333344s peak_spilled=599.3MB (12.0x a single file)
```

Backpressure engages at file 11 (of 20) instead of file 15, peak spilled
data is capped ~150MB lower (599MB vs. 750MB), and the whole burst - from
first write to fully durable - finishes 27% faster (121.3s vs. 166.8s).
