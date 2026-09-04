# DedupFS repository on a WebDAV network drive - informal exploration

**Not a `performance/measurements/` protocol.** This is a single, informal exploratory session,
not `methodology.md`'s 5-runs/~20s statistical approach - each number below is a single ~10-15 s
run, meant to answer "does this basically work, and roughly how does it compare" quickly, not to
be a rigorous, repeatable baseline. Treat every figure as a rough order of magnitude, not a
precise measurement. If any of this turns into a real question worth tracking over time, it
belongs in `../measurements/` properly, following `../methodology.md`.

## Question

Can a DedupFS repository live on a WebDAV-backed network drive (here: `H:`, a HiDrive WebDAV mount
on `julius`, mapped via `\\diet-rich.webdav.hidrive.ionos...`), the way it clearly cannot on a
WSL2 `/mnt/c/...` DrvFs bridge? And if so, how does performance compare - through the mount, and
through the `dfs` CLI tools - against doing the same operations directly on the network drive
without DedupFS at all?

## Setup

- Machine: julius, native Windows, `H:` mapped as a WebDAV drive (see `../machines.md` for the
  rest of this machine's profile).
- DedupFS build: `write-cache` branch, same commit this session added `--spill-dir` on (see
  "New CLI option" below) - `crates/cli` release build.
- Repository created directly on `H:` via `dfs create-repo`; mounted read-write via `dfs mount
  --spill-dir <path-on-local-SSD>`, pointing the write cache's spillover (DESIGN-MOUNT-010) at
  `C:\dedupfs-perf\spillover` rather than leaving it at the OS temp directory's default.
- A same-shape "native control" directory tree on the same `H:` drive, next to the repository, for
  the without-DedupFS comparison.
- `H:` had roughly 78-82 GB free throughout (usage fluctuated a little even at idle, plausibly a
  WebDAV usage-reporting quirk rather than anything this session did) - every test tree was deleted
  again afterward, and free space was checked to have returned to its starting range at the end.
- Content: single random 100 B files for "small", single random 5 MB files for "large" - matching
  this project's existing small/large size convention, generated in-process (`System.Random` +
  `WriteAllBytes`) rather than via `perf-gen`, since these runs are single, short, one-off timings
  rather than the repeated-runs case `../methodology.md`'s own content-generation-cost discussion
  is about.
- A `dfs` CLI quirk specific to *this session's own tooling*, not DedupFS: invoking `dfs.exe` from
  a Git-Bash shell with a leading-`/` repository path argument (e.g. `/testdir`) gets silently
  rewritten by MSYS's own path conversion into a Windows path fragment, producing a confusing "no
  such repository path" error. Fixed by setting `MSYS_NO_PATHCONV=1` for those invocations - noted
  here since it looks like a DedupFS bug at first glance and is not one.

## New CLI option added along the way: `dfs mount --spill-dir`

Before this exploration could isolate DedupFS's own write path from the OS temp directory's
storage location, it needed the write cache's spillover to be explicitly steerable rather than
implicit - see DESIGN-MOUNT-018 in
[`../../docs/design/mount-write-path.md`](../../docs/design/mount-write-path.md) for the design,
`crates/cli/src/main.rs`/`mount.rs`/`dedup_fs.rs` for the implementation. All mount-path numbers
below used `--spill-dir` pointed at local SSD.

## Question 1: Does a repository on a WebDAV drive actually work?

Yes, with no WSL2-path-style breakage observed. `create-repo` (including SQLite's mandatory WAL
mode initializing successfully - a real risk on network filesystems, since WAL coordination
normally leans on `mmap`-backed shared-memory `-shm` files), mounting read-write, writing and
reading through the mount, directory listing, and the `dfs list`/`ingest`/`restore`/`unlock` CLI
tools all worked correctly throughout roughly an hour of exercising them - no corruption, no
unexplained failures, nothing resembling the DrvFs bridge problems WSL2 paths have.

Two things worth flagging, not as failures but as real behavior an operator should expect:

- **Detecting an already-held write lock over WebDAV is itself slow.** Attempting to open the
  repository while another process (the mount) still held the write lock took about 22 s just to
  report the "already locked" error - compare this project's other native-SSD lock-related timings,
  which are near-instant. Clearing a stale lock afterward via `dfs unlock` was fast (well under 1 s)
  despite being on the same drive - so this specific slowness is about detecting a *live* lock
  conflict, not about `H:` I/O being uniformly slow for every small metadata operation.
- **`Stop-Process -Force` against the mount process consistently took several extra seconds beyond
  the call returning before the process actually disappeared** (once even 14 s, with an idle
  session that had nothing left to settle - see "A closer look" below for why that last detail
  rules out the first, plausible-looking explanation this write-up originally gave). Not
  investigated further - flagged as a real characteristic of tearing down this mount on this
  machine, not explained by this session's data.

**Not tested**: genuine concurrent multi-process access to the same repository over WebDAV (every
test here used one process at a time) - the scenario most likely to actually stress a non-POSIX
network filesystem's locking semantics, and the one this exploration says the least about.

## Question 2: Performance, mount and CLI vs. the raw network drive

| Operation | Mount | Native `H:` (no DedupFS) | `dfs` CLI |
|---|---|---|---|
| Directory create | 285.9 ops/s | 4.0 ops/s | *(no direct CLI primitive - see below)* |
| Directory listing (50 entries, fair) | 108.3 ops/s | 3.5 ops/s | 2.0 ops/s (`dfs list`, one process per call) |
| Small file (100 B) write, client-visible | 101.7 ops/s | 2.0 ops/s | 3.6 ops/s (`dfs ingest`, 30 files/call) |
| Small file (100 B) read (21-file working set, fair) | 356.3 ops/s | 180.5 ops/s | 19.2 ops/s (`dfs restore`, 30 files/call) |
| Large file (5 MB) write, client-visible | 28.4 MB/s (6.0 ops/s) | 1.1 MB/s (0.22 ops/s) | did not finish in 60 s+ for 10 MB (< 0.17 MB/s) |
| Large file (5 MB) write, **real settle-to-`H:` rate** (see below) | ~0.17-0.21 MB/s | *(same as client-visible - no buffering layer exists)* | *(`dfs ingest`'s number above already is this, unbuffered)* |
| Large file (5 MB) read (4-file working set, fair) | 0.3 MB/s (0.07 ops/s) | 0.3 MB/s (0.05 ops/s) | not measured (ingest above never completed, so nothing was in the repo yet to restore) |

Every "fair" comparison above uses a matched working-set size across mount and native - an early,
unmarked small-file-read attempt compared the mount's 1017-file set against native's 21-file set
and showed the mount *losing* badly (10.1 ops/s); redone with matched 21-file sets on both sides,
the mount actually wins (356.3 vs 180.5 ops/s). That first number was a working-set-size artifact
(the read-handle cache, capacity 8, thrashing hard across 1017 distinct files - see
DESIGN-STORE-004 in `../../docs/design/byte-store.md`), not a real characteristic, and is not in
the table above.

### A closer look: client-visible write speed is not the same question as real throughput to `H:`

The first draft of this write-up reported the mount's 28.4 MB/s large-file write number
uncritically, as if it meant DedupFS writes large files to `H:` 26x faster than doing it directly.
That is wrong, and worth walking through explicitly since the mistake is easy to make again
elsewhere: `[System.IO.File]::WriteAllBytes`'s call against the mount returning quickly only means
DESIGN-MOUNT-010's write cache accepted the bytes into memory (and, past its 256 MiB session
budget, `--spill-dir`'s local SSD) - not that the content has actually reached `H:` yet. The actual
network write happens later, off the client's critical path, in DESIGN-MOUNT-006's background
settle job. A single 91-file, ~456 MB burst like the original write test fits easily within one
session's write-cache capacity without ever triggering DESIGN-MOUNT-006's backpressure, so the
client never sees the real cost at all during the timed loop - it is simply deferred, not
eliminated, and the original number measured "how fast can the write cache accept bytes," a
question about local memory/SSD speed, not about `H:`.

**Measured directly, to check this**: wrote a fresh 3-file, 15 MB batch through the mount (client-
visible: 0.27 s, i.e. the same "instant" pattern as before), then polled the repository's actual
`data/` directory size on `H:` every 1-2 s until it stopped growing. The real, durable settle took
roughly 75-90 s total (imprecise - the polling spanned two separate tool invocations with an
untimed gap between them) to land the full 15 MB, i.e. **roughly 0.17-0.21 MB/s** - not just far
below the 28.4 MB/s client-visible figure, but **noticeably slower than even the raw native `H:`
write rate of 1.1 MB/s measured earlier**. The likely reason (not confirmed further): content-
defined chunking splits a 5 MB file into several ~1 MB chunks, and settling writes each chunk (plus
its own SQLite commit) as a separate synchronous operation against the network-backed store, rather
than one continuous bulk transfer the way a native single-file write is - many smaller round trips
costing more in aggregate than one larger one, on a connection whose overhead scales more with
operation count than with bytes for small-to-medium chunks. This is exactly the same shape of cost
`dfs ingest` pays directly and synchronously (see below) - the mount does not avoid this cost for
large files, it only defers when the *client* has to wait for it.

A 50-file, 100 B small-file batch was spot-checked the same way and showed no comparable lag - the
store's on-disk size stopped changing within about 2 s, matching (roughly - see the file itself for
an unresolved byte-count discrepancy not chased down further) the client-visible write's own
timing. Small files apparently settle about as fast as they are accepted; large files do not. This
was a single spot-check, not repeated - treat the small-file settle-speed claim as considerably
less certain than the large-file one, which was measured directly and carefully.

**Practical takeaway**: the mount's write-cache genuinely helps for *bursts* - an operator who
writes a large file and moves on does not have to wait for `H:`'s slow real throughput
synchronously, which is a real, valuable property for an interactive session. It does **not** mean
DedupFS moves large-file bytes to a WebDAV-backed repository any faster than the raw connection
allows - sustained large-file write throughput is bounded by the real settle rate (~0.17-0.21
MB/s here), not the client-visible accept rate, and a large enough sustained burst would eventually
hit DESIGN-MOUNT-006's backpressure and slow down to something close to that real rate directly,
not stay at 28.4 MB/s. This session did not push far enough to actually observe backpressure engage
(the burst was well within the memory+local-SSD budget) - that would be the next thing to check
with a much larger, sustained write.

**CLI writes (`dfs ingest`) pay the real, unbuffered cost directly, with nothing to defer it** -
`ingest_file` (`crates/cli/src/ingest.rs`) calls `settle::settle` synchronously per file, the same
engine the mount's background settle job uses, but with no write-cache in front of it at all (there
is no mount session, so nothing to buffer into) and no parallelism across files either (unlike the
mount's settle pool, which runs `available_parallelism()` workers). So `dfs ingest`'s large-file
number is not a separate, CLI-specific weakness - it is a direct, honest measurement of
approximately the same real cost the mount's settle job pays in the background, just without a
buffer hiding it and without the mount's worker-pool parallelism helping it along. This tracks:
`dfs ingest` had not finished writing even one 5 MB file's content 60 seconds into a two-file
(10 MB) batch, consistent with a real rate in the same ~0.17-0.2 MB/s neighborhood as the settle
measurement above. Small-file CLI writes (3.6 ops/s) are also behind the mount's client-visible
101.7 ops/s, for the same reason at a smaller, less dramatic scale - though per the settle spot-
check above, the mount's small-file number may be closer to its own real throughput than the
large-file one is.

**Reads tell a more mixed story, and depend on scale.** Small-file reads through the mount are
about 2x *faster* than native `H:` once the working set is fair - the read-handle cache (capacity
8) clearly helps even though it is smaller than the 21-file working set tested. Large-file reads,
however, land at essential parity between mount and native (~0.3 MB/s both ways) - the mount has
no read-side equivalent to the write cache's local buffering, so a cold read of previously-written
large content goes straight to the network-backed store either way, and both paths bottleneck on
the same underlying WebDAV read bandwidth. This is not a DedupFS weakness relative to the raw
drive - it is parity with the raw drive's own ceiling for that specific access pattern - but it is
also not a win the way every write case and small-file reads are.

**The `dfs` CLI tools pay their own per-invocation overhead**, most visible for directory listing
(2.0 ops/s via `dfs list`, each call opening the repository fresh, versus the mount's 108.3 ops/s
against an already-open session) - expected, the same process-spawn-dominates-small-work pattern
already documented elsewhere in this project (see
`../../agent-todos/done/verify-read-handle-cache-releases-on-real-winfsp-under-load.md`'s account
of the same effect showing up with PowerShell background jobs), not specific to a network drive.

## Bottom line

For this specific WebDAV drive, a DedupFS repository works correctly, and the mount's write cache
makes *small*, *bursty* writes (directories, small files, and large-file bursts that fit the
session's buffer) feel dramatically faster than doing the same thing directly on `H:` - genuinely
useful for an interactive session. That said, this is a smaller, more qualified win than this
write-up's own first draft claimed: the large-file write number the mount reports client-side
(28.4 MB/s) reflects local memory/SSD buffering speed, not real throughput to `H:`, which is
closer to ~0.17-0.21 MB/s once actually measured - slower than even the raw drive's own native
write rate (1.1 MB/s), not faster. A backup workload dominated by many small files still looks like
a clearly good fit for this drive; one that regularly writes large files in volumes beyond what the
write cache can absorb in memory/local-SSD would eventually feel this real, slower rate directly
once backpressure engages, not the fast client-visible number measured here. The other soft spot,
unchanged from the original draft, is reading back large content cold - mount and native land at
the same ~0.3 MB/s ceiling either way, since neither path buffers reads the way writes are
buffered.
