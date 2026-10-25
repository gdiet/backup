# Does the external USB hard disk write larger blocks faster? - informal measurement

**Not a `performance/measurements/` protocol.** This is one short measurement (three rounds) on a
non-isolated machine. It answers one question, whether larger writes are faster on this device. It
does not establish a baseline.

## Question

`ByteStore::write` writes every chunk with one write call. A 10 MB file yields chunks of about
1.2 MB, and a small file is one chunk of its own size. On the USB2 stick, 10 MB writes ran clearly
faster than 1.2 MB writes (see `2026-10-06-julius-ingest-on-usb2-stick.md`). If the external USB
hard disk behaved the same way, a write buffer that merges chunk writes into larger blocks would be
worth designing (candidate 3 in `../../agent-todos/ingest-on-slow-removable-media.md`).

## Setup

- Machine: `julius`, native Windows (Windows 10 IoT Enterprise LTSC, build 19044).
- Power profile: Balanced base scheme, Power Saver overlay (`OVERLAY_SCHEME_MIN`). On AC power
  (`Win32_Battery` `BatteryStatus` 2).
- Device: WD Elements 25A3 (USB vendor and product id `1058:25A3`), 5.59 TB (`Get-Disk`).
  A spinning hard disk, as stated by the developer and reported by `Get-PhysicalDisk`.
- Connection: on the USB 3.0 root hub of the Intel xHCI controller, driver `USBSTOR` (the USB
  bulk-only protocol, not UAS). The probe below reached 97.6 MB/s, more than USB 2 can carry, so
  the disk runs at USB 3 speed. This is inferred from the throughput.
- File system: NTFS, 4 KB clusters, GPT. 2,082 GB of 5,589 GB free. Target folder `G:\TEMP`.
- Removal policy in Device Manager: "Quick removal", as stated by the developer.
- Tool: `../scripts/write-pattern.ps1` at commit `8bd8e4e4`, with its defaults: 1,024 MB per run,
  three rounds, blocks of 16,384, 100,000, 1,200,000 and 10,000,000 bytes. A second measurement
  with two rounds used the version committed together with this note, which also reports the
  queue length and the idle share of the disk.

## Method

Each run creates one fresh file, appends 1,024 MB in blocks of one size, and calls
`Flush($true)` inside the timed span. The `FileStream` has a buffer size of 1, so .NET does not
merge the writes. The sizes run round-robin. The raw disk counters before and after each run give
the average size of the writes the disk actually received.

A probe with 10 MB blocks took 10.5 s for 1,024 MB, so the default amount gave runs of about 10 s.
The whole measurement took 115 s.

## Results

| Block | MB/s (runs 1, 2, 3) | Bytes per write at the disk |
|---|---|---|
| 16,384 B | 130.9, 133.3, 131.0 | 685,000-707,000 |
| 100,000 B | 119.3, 126.2, 126.6 | 95,800-96,400 |
| 1,200,000 B | 102.9, 99.1, 98.7 | 806,000-868,000 |
| 10,000,000 B | 94.5, 95.4, 94.4 | 3,479,000-3,937,000 |

The second measurement (85 s) added two counters. "Queue" is the average number of outstanding
requests at the disk during the run. "Idle" is the share of the run in which the disk had no
request at all.

| Block | MB/s (runs 1, 2) | Queue | Idle |
|---|---|---|---|
| 16,384 B | 68.6, 132.0 | 4.68, 1.44 | 4%, 7% |
| 100,000 B | 124.5, 123.4 | 1.20, 1.29 | 15%, 11% |
| 1,200,000 B | 97.7, 91.4 | 1.23, 1.35 | 11%, 10% |
| 10,000,000 B | 93.7, 94.4 | 0.96, 0.99 | 12%, 10% |

The first 16 KB run of the second measurement is an outlier (68.6 MB/s with a queue of 4.7). It
was the first run of that measurement and did not repeat. Its cause is unknown.

## Findings

1. **Larger writes are not faster on this disk.** 10 MB blocks ran at 94-95 MB/s, 1.2 MB blocks
   at 99-103 MB/s. The 10 MB blocks are about 5% slower, and the spread between runs is smaller
   than that difference. The stick showed the opposite: 9.4-9.8 against 5.9-6.0 MB/s.
2. **The smallest blocks are the fastest.** Windows merged the 16 KB writes into writes of about
   0.7 MB, as it did on the stick, and they reached 131-133 MB/s. The 100 KB writes reached the disk
   unmerged at about 96 KB each and still ran at 119-127 MB/s. The disk handles many small
   sequential writes well.
3. **Why larger writes are slower remains open.** The guess was that large write calls leave the
   disk waiting between transfers, while small writes keep it busy. The counters support this only
   in part. With 10 MB blocks, the queue stayed just below 1, so no second request was ever
   waiting. With smaller blocks it was 1.2-1.4. But the idle share was about the same for 100 KB,
   1.2 MB and 10 MB blocks (10-15%), so idle gaps do not explain the difference.

## Conclusion

For this disk, merging chunk writes into larger blocks would not make ingest faster. The criterion
of the measurement was a gain of a third or more from 1.2 MB to 10 MB blocks. The measured change
is a loss of about 5%. A write buffer is therefore not worth designing for this device. The
decision is the developer's.

## Limits

- Only sequential appends to one file by one thread. Ingest also writes the SQLite metadata. On the
  stick, the interleaving of small metadata writes with large data writes was the bigger cost
  (see the USB2 note). That effect was not measured here.
- One session with three rounds, on a machine that was not isolated.
- Where on the platters the files landed is unknown. Throughput of a hard disk depends on the
  track position, but the round-robin order gives every size the same conditions in each round.
- The size of the disk's own cache is unknown. 1,024 MB per run is assumed to exceed it.
- The removal policy "Quick removal" may influence how Windows merges and passes on writes. The
  other policy, "Better performance", was not tried.
