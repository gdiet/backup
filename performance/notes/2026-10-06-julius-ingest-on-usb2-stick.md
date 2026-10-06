# Why `dfs ingest` is slow on the USB2 stick - informal investigation

**Not a `performance/measurements/` protocol.** These are short, exploratory runs (two or three
repetitions each) on a non-isolated machine. They answer "where does the time go", not "how fast is
it". Treat the figures as rough. Every number is from `julius`, native Windows, Power Saver overlay,
on AC power, with the release build of commit `d38d156c`. Where an experiment needed a code change
(worker count, SQLite `synchronous`), the change was temporary and is not part of the repository.

## Question

The 2026-10-06 ingest protocols (`../measurements/2026-10-06-julius-ingest-*-usb.md`) put the
repository on the USB2 stick and the source on the SSD. There, ingest is never faster than native
creation, and at 10 MB it reaches only about a third of the native throughput. Where does the time
go?

## Method

The repository's `meta/` and `data/` directories were placed independently on the SSD or on the
stick, using an NTFS junction. Raw write patterns were reproduced with a small C# program, without
any DedupFS code. The stick's device counters (`Win32_PerfFormattedData_PerfDisk_PhysicalDisk`)
were sampled during some runs.

## Findings

### 1. Writing the content is limited by the size of each write

Ingest writes every chunk with one write call. A 10 MB source file at the 20-bit target size yields
chunks of about 1.2 MB on average. The same pattern without DedupFS gives the same throughput. The
stick is faster for larger writes:

| Raw write pattern, 146 MB in total | Throughput |
|---|---|
| 15 files of 10 MB, one `WriteAllBytes` each (the native baseline) | 9.3-9.4 MB/s |
| One file, 15 appends of 10 MB | 9.7-10.0 MB/s |
| One file, 125 appends of 1.2 MB (the byte store's pattern) | 5.4-5.8 MB/s |
| The same, with the file reopened for every chunk | 5.6-5.7 MB/s |
| The same, with the file preallocated with `SetLength` | 5.8-6.0 MB/s |
| The same, written by 2 threads | 5.5 MB/s |
| The same, written by 4 threads | 4.2-4.5 MB/s |
| One file, 1,500 appends of 100 KB | 3.8-4.0 MB/s |
| 125 separate files of 1.2 MB | 7.6-8.5 MB/s |

Ingest with the data on the stick and the metadata on the SSD reaches 5.1-5.9 MB/s (25-31 s for
150 MB). That equals the raw pattern at the same write size. DedupFS adds no measurable overhead to
the data path here.

The device counters show the cause: the stick receives writes of about the size the application
writes (94 KB for 100 KB writes, 850 KB for 1.2 MB, 3.3 MB for 10 MB). Larger writes are faster.
Reopening the file, preallocating it and the number of threads do not help.

### 2. The number of worker threads does not matter

Temporarily forcing 1, 2 or 4 ingest workers (150 MB, data on the stick, metadata on the SSD)
gave 25-31 s in every case. With the metadata on the stick as well, the runs ranged from 36 to
49 s with no order by worker count. No setting for the worker count would help on this device.

### 3. SQLite `synchronous` does not matter

With data and metadata both on the stick, `synchronous=OFF` took 51-52 s, `NORMAL` 48-62 s and
`FULL` 41-43 s. Flushing is therefore not the cost.

### 4. Metadata on the stick costs a lot for small files

400 files of 100 B, ingested with the default settings:

| Metadata | Data | Time |
|---|---|---|
| SSD | SSD | 0.50-0.60 s |
| stick | SSD | 8.7-12.1 s |
| SSD | stick | 2.4-3.0 s |
| stick | stick | 15.1-27.0 s |

Ingest runs at least three separate database transactions per file (the chunk, the content and the
file). Each commit appends to the write-ahead log, which is a small write. On this stick, that costs
roughly 7-10 ms. With the metadata on the stick, those commits dominate. Putting the data on the
stick costs far less, because a 100 B file is one small append.

For 10 MB files, metadata on the stick alone costs little (0.2-0.6 s more for 150 MB), because the
four workers hide the commit latency behind the data work. The combination is the problem: with
data and metadata both on the stick, the 150 MB run takes 41-62 s instead of 25-31 s. The counters
in such a run show seconds in which the stick takes only a few 4 KB writes while a queue of 3-5
requests waits. The stick stalls when small writes are interleaved with large ones. The likely
reason is the flash controller. This was not verified.

## Conclusion

On this stick, ingest runs close to what the device allows for writes of that size. Two device
properties explain the gaps against the native baseline:

- The stick writes 10 MB blocks about 1.6 times faster than 1.2 MB blocks. Native creation writes
  whole files in one call, and ingest writes chunk by chunk.
- The stick handles small, scattered writes badly, in particular next to large ones. SQLite
  produces many of them.

Both effects are weaker or absent on a normal SSD, which explains why ingest beats native creation
there. A fast external hard disk, which is the device that matters in practice, has not been
measured. Its behavior may differ from the stick's in both points.

## Possible improvements

Not decided. See `../../agent-todos/ingest-on-slow-removable-media.md`.

- Keep `meta/` on a fast local disk. This works today with a junction, but `dfs` has no option for
  it.
- Commit less often during ingest, for example once per file or per batch of files.
- Collect several chunks and write them in larger blocks.
