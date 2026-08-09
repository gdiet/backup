# Persist-queue backpressure: before/after benchmark results

Captured by running `cli/src/mount.rs`'s `bench_many_small_files`/
`bench_large_files_spill_pressure` tests (see their own doc comments) -
WSL2/Debian, release build, real `libfuse3` mount via the existing
in-process test harness. Both are manual (`#[ignore]`) benchmarks, not
part of the normal suite.

## BEFORE (commit `02bbd885`, `PERSIST_QUEUE_CAPACITY = 4`, job-count gate)

### `bench_many_small_files` (200 x 1 MB, datastore throttled to 30 MB/s)

Command:

```
BACKUP_BENCH_THROTTLE_STORE_MBPS=30 cargo test --release --package cli --bin backup \
  mount::tests::bench_many_small_files -- --ignored --nocapture
```

Result:

```
files=200 size=1000000B total_bytes=200000000B close_loop=10.915652608s close_loop_throughput=18.32MB/s p50=57.3ms p95=73.1ms max=117.5ms
drain_on_unmount=42.995343ms end_to_end=10.958647951s end_to_end_throughput=18.25MB/s
```

### `bench_large_files_spill_pressure` (6 x 50 MB, forced spill, datastore throttled to 5 MB/s)

Command:

```
BACKUP_BENCH_THROTTLE_STORE_MBPS=5 cargo test --release --package cli --bin backup \
  mount::tests::bench_large_files_spill_pressure -- --ignored --nocapture
```

Result:

```
file 0: close=152.250587ms spill_dir_now=50.0MB peak_so_far=50.0MB
file 1: close=163.910317ms spill_dir_now=100.1MB peak_so_far=100.1MB
file 2: close=168.640989ms spill_dir_now=150.1MB peak_so_far=150.1MB
file 3: close=157.767554ms spill_dir_now=200.2MB peak_so_far=200.2MB
file 4: close=154.042676ms spill_dir_now=250.1MB peak_so_far=250.1MB
file 5: close=155.944964ms spill_dir_now=300.2MB peak_so_far=300.2MB
files=6 size=50000000B write_loop=953.410682ms unmount_request=57.056758ms full_drain=66.136418185s peak_spilled=300.2MB (6.0x a single file)
```
