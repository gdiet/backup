# Measure whether larger writes help on the external USB hard disk

**Why parked**: Needs `julius` with the developer's external USB hard disk attached, the device
that matters for slow-media ingest. The Linux/WSL2 session on `3327` has neither the disk nor
Windows.
**Size**: small. One short measurement and a note. Ask the developer only for the target folder
on the disk and for the disk's details listed below.
**Opened**: 2026-10-25, by the Linux/WSL2 session on `3327`
**Context**: [`performance/notes/2026-10-06-julius-ingest-on-usb2-stick.md`](../performance/notes/2026-10-06-julius-ingest-on-usb2-stick.md)
(the same question on the USB2 stick), candidate 3 in
[`ingest-on-slow-removable-media.md`](ingest-on-slow-removable-media.md), `ByteStore::write` in
`crates/store/src/lib.rs`, `complete_chunk` in `crates/cli/src/settle.rs`

## Question

`ByteStore::write` writes every chunk with one write call. A 10 MB file yields chunks of about
1.2 MB, and a small file is one chunk of its own size. On the USB2 stick, 10 MB writes ran at
9.7-10.0 MB/s and 1.2 MB writes at 5.4-5.8 MB/s. Does the external USB hard disk also write
larger blocks clearly faster? The answer decides whether a write buffer that merges chunk writes
is worth designing. A hard disk handles large sequential writes well, so the effect may be small.

## The measurement

Write a script `performance/scripts/write-pattern.ps1` (`location: native`, no DedupFS code). The
C# program of the USB2 note was not kept, so this script replaces it and makes the measurement
repeatable.

- Parameters: `-Target <folder>`, `-TotalMB` (default 1024), `-Runs` (default 3), `-Sizes`
  (default 16 KB, 100 KB, 1,200 KB, 10 MB).
- One run per size: create one fresh file in `-Target`, append `-TotalMB` in blocks of the given
  size, call `Flush($true)` (that is, `FlushFileBuffers`) inside the timed span, delete the file
  afterwards. This is the byte store's pattern: one file, sequential appends.
- Open the `FileStream` with a buffer size of 1, so that .NET does not merge the writes itself.
- Fill one random buffer of the largest size once, outside the timed span, and write slices of it.
- Run the sizes round-robin (all sizes once, then all sizes again), not each size three times in a
  row, so that a drift of the device does not favor one size.
- Print one line per run: size, number of writes, seconds, MB/s.
- Optionally sample `\PhysicalDisk(*)\Avg. Disk Bytes/Write` during the runs, as the USB2 note
  did. It shows whether the device receives writes of the size the application writes.

Keep the whole measurement within a few minutes. First run a single probe with 10 MB blocks and
choose `-TotalMB` so that one run takes about 10 s. Keep `-TotalMB` well above the disk's own
cache size, at least 512 MB.

Record in the note what the measurement depends on. Ask the developer for what cannot be queried:

- disk model, and whether it is a spinning disk or an SSD in a USB case (`Get-PhysicalDisk`)
- USB connection (USB 2 or USB 3) and file system (`Get-Volume`)
- the removal policy in Device Manager: "Quick removal" or "Better performance"
- the git commit of the script

## Result

Write `performance/notes/<date>-julius-write-size-on-usb-hard-disk.md` in the shape of the USB2
note: question, method, a results table per size, findings, limits. Then add one sentence with the
outcome and a link to the note under candidate 3 of `ingest-on-slow-removable-media.md`.

For the reader of the result: if 10 MB writes are clearly faster than 1,200 KB writes (say, by a
third or more), a write buffer is worth designing. If the difference stays within the run-to-run
spread, it is not, for this device. The decision is the developer's.
