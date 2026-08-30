# Machines

Alias-to-hardware mapping for the machine names used in `measurements/` (see
[`methodology.md`](methodology.md)'s "Machine" field) - so a measurement naming just "julius" or
"3327" stays interpretable later without having to ask the developer each time.

## julius

- Kind: a physical machine - a Toshiba Satellite C70-C-1DV laptop (confirmed via
  `Win32_ComputerSystem`'s Manufacturer/Model, not a VM platform name). See the `julius-winfsp-ssh`
  skill for how to reach it over SSH from elsewhere.
- CPU: Intel Core i5-6200U @ 2.30GHz, 2 cores / 4 threads; `MaxClockSpeed` reports 2401 MHz, which
  is this CPU's base clock, not its turbo boost ceiling - worth keeping in mind when interpreting
  single-thread results against this figure alone, same caveat as the `3327` entry below.
- RAM: 8 GB (reported as 7.86 GB physical via `Win32_ComputerSystem.TotalPhysicalMemory`, the usual
  shortfall from hardware-reserved memory).
- Local storage: WDC WDS100T2B0A-00SM50, SATA SSD, 931.5 GB (~1 TB nominal) - confirmed as SSD via
  `Get-PhysicalDisk` (`Win32_DiskDrive`'s own `MediaType` field is not reliable for this, it labels
  both SSDs and HDDs "Fixed hard disk media"). A second, removable SD-card reader is also present
  but is not this machine's local storage for measurement purposes.
- OS: Windows 10 IoT Enterprise LTSC, version 10.0.19044 (build 19044).
- WSL2: version 2.7.12.0, distro Debian 12 (bookworm), kernel 6.18.33.2-microsoft-standard-WSL2.
  WSL2 on this machine sees ~3.8 GiB of RAM (`free -h`) against the host's ~7.86 GB - the default
  WSL2 cap of half the host's physical RAM, not a hardware limit, same as the `3327` entry below -
  meaning a measurement running under WSL2 here is memory-constrained relative to native Windows
  even though the CPU/storage are shared with the host, and doubly memory-constrained compared to
  `3327`'s WSL2 (~15 GiB there) given this host's much smaller total RAM to begin with.
- External IO device: an external USB stick, drive letter `I:` when attached, NTFS, labeled
  "USB Stick", ~4 GB total capacity (`Get-Volume`: 4,023,349,248 B). Measured write throughput
  ~8.7-10.5 MB/s (a single 100 MB probe, and the effective rate across a 5-run 10 MB-file-creation
  measurement) - USB2-class speed, matching the developer's own description of it as "the slow
  USB2 stick". Its small capacity means a 10 MB-file-creation-style measurement needs a free-space
  check first (a 20 s window here writes ~200 MB, not the several GB it would on faster storage).

## 3327

- Kind: the developer's other laptop.
- CPU: Intel Core i7-1355U (13th Gen), 10 cores / 12 threads; `MaxClockSpeed` reports 1700 MHz,
  which for this CPU is the base clock, not its turbo boost ceiling (up to 5.0 GHz per Intel's
  spec) - worth keeping in mind when interpreting single-thread results against this figure alone.
- RAM: 32 GB (reported as 31.68 GB physical, the usual shortfall from hardware-reserved memory).
- Local storage: SK Hynix HFS001TEJ9X162N, NVMe SSD, 953.9 GB (~1 TB nominal).
- OS: Windows 11 Enterprise, version 10.0.26200 (build 26200).
- WSL2: version 2.6.3.0, distro Ubuntu 24.04.4 LTS (Noble Numbat), kernel
  6.6.87.2-microsoft-standard-WSL2. WSL2 on this machine sees only ~15 GiB of RAM (`free -h`)
  against the host's 32 GB - the default WSL2 cap of half the host's physical RAM, not a hardware
  limit - so a measurement running under WSL2 here is memory-constrained relative to native
  Windows even though the CPU/storage are shared with the host.

## Adding a machine

Add an entry here the first time a measurement protocol references a new machine alias - fill in
whatever is actually known at the time, and leave a field `TBD` rather than guessing at it. Update
a `TBD` later if it turns out to matter for interpreting some result, rather than trying to get
every field right up front.

## Coverage goal

Over time, aim to run every workload/location combination on both `julius` and `3327`, not just
whichever machine happened to be convenient when a given measurement was first taken - the two
differ enough (roughly 4x the RAM, more CPU cores, a newer/faster CPU generation, NVMe vs. SATA
storage) that a gap on one machine's side is a real, not merely cosmetic, hole in the picture.
`overview.md`'s per-operation tables make a coverage gap visible: a row missing for a machine that
has rows for other operations is a candidate for a follow-up measurement, not necessarily a sign
that operation cannot be measured there. See `README.md`'s "Running a self-directed measurement
session" for how to turn a coverage gap like this into an actual measurement without further
guidance.
