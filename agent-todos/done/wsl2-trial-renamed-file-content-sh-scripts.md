# WSL2 trial run of the renamed/added file-content `.sh` scripts

**Why parked**: needs a WSL2/Linux shell with a real `/dev/urandom` and Linux NTFS-free
filesystem (`~/dedupfs-perf/...` on the WSL2 ext4 disk). The session that made the change ran
natively on Windows `3327` (PowerShell only) and could only `bash -n` syntax-check the scripts.
**Size**: small (just run the scaled-down trials, same as the other `.sh` scripts were validated)
**Opened**: 2026-08-28, by native-Windows `3327` session
**Context**: commit `b2fd3fd9` on `rust-performance-test-idea`; `performance/scripts/README.md`
"validated ... the same way" paragraph; `performance/methodology.md` "File-content workloads".

`performance/scripts/file10kb-*.sh` -> `file30kb-*.sh` and `file1mb-*.sh` -> `file10mb-*.sh`
were renamed, with `size=` and header comments updated; the create/read loop bodies are unchanged
from the versions already validated on `3327`'s WSL2. `file10mb-create.sh` writes 10 MB files
(new largest size).

Do:

1. Scaled-down trial run of `file30kb-create.sh`, `file30kb-read.sh`, `file10mb-create.sh`,
   `file10mb-read.sh` on `3327`'s WSL2 (smaller Scale, shorter window - not a full
   5-runs-of-20-seconds measurement), the same way the other `.sh` scripts were checked. Confirm
   per-file size is correct and files are distinct.
2. While there, measure `/dev/urandom` throughput on that WSL2 (`head -c 10485760 /dev/urandom`
   in a bare timed loop). `methodology.md`'s "File-content workloads" note assumes the per-file
   `/dev/urandom` read is negligible at 30 KB but not at 10 MB; the actual figure decides whether
   the 10 MB `.sh` rung needs the "record generator-only throughput alongside" treatment or is
   fine as-is. If `/dev/urandom` there is slow enough that even 30 KB is affected, that is worth
   raising - it would push toward unifying `.ps1` and `.sh` on a shared fast generator (a small
   Rust `perf-gen` helper, currently only noted as an option).
3. Update the `README.md` paragraph to drop the "wants one more WSL2 trial run" caveat once done.

## Done

**Completed**: 2026-08-28, by WSL2/Linux session on `3327`.

Scaled-down trial runs (2-second window, 2 runs instead of 5x20s) of all four renamed scripts -
`file30kb-create.sh`, `file30kb-read.sh`, `file10mb-create.sh`, `file10mb-read.sh` - found no bugs.
Per-file size confirmed exact (`stat -c %s`: 0 files of 30,720/10,485,760 bytes were off-size out
of 1,314/64 files created) and content confirmed pairwise distinct (`md5sum` over a 50-file sample
for 30 KB, all 64 files for 10 MB - all hashes unique). Trial data deleted afterwards, along with
unrelated orphaned state from an earlier, pre-rename trial of the since-deleted `file10kb-*.sh`/
`file1mb-*.sh` scripts.

`/dev/urandom` throughput on this WSL2 (5x `head -c 10485760 /dev/urandom > /dev/null`, bare timed
loop): 222-335 MB/s, i.e. roughly 31-47 ms to generate 10 MB. Against the observed
`file10mb-create.sh` trial throughput (11-20 ops/s, i.e. ~50-90 ms/file), that generation cost is
a substantial fraction (roughly 40-70%) of the total per-file time - confirming
`methodology.md`'s assumption that at 10 MB the `/dev/urandom` read is *not* negligible on this
machine, so the "record generator-only throughput alongside" treatment for that rung is correctly
scoped, not something to drop. At 30 KB, the same generator (~0.1 ms for 30 KB at this throughput)
is a small fraction of the observed ~2.6-3.7 ms/file - confirming the 30 KB rung needs no such
treatment. `/dev/urandom` is not slow enough here to push the 30 KB rung into the same camp, so
this trial does not add urgency to `developer-todos/perf-gen-shared-content-generator.md` beyond
what was already noted there - `perf-gen` was not built as part of this task, per scope.
