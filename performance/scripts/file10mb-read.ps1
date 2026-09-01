# Native 10 MB file read-back, pseudo-random - see ../methodology.md's "Statistical approach" and
# "Workload catalog". Reads back files file10mb-create.ps1 created, picking a pseudo-randomly
# chosen existing file within whichever range it has produced so far (run file10mb-create.ps1
# first - this script only reads, it never grows the tree itself, so nothing here needs the "state
# between runs" bookkeeping the create scripts do).
# Tool (record this in the measurement protocol): PowerShell `[System.IO.File]::ReadAllBytes`.
#
# Validated on `3327` native Windows (PowerShell 5.1) via scaled-down trial runs - a shorter
# window, not a full 5-runs-of-20-seconds measurement. One bug found and fixed on the first run:
# the read result was discarded with `| Out-Null`, which enumerates the byte[] element by element
# and so measured pipeline overhead rather than read cost (see the discard site below). This bug
# scaled with file size - at 10 MB the pre-fix trial completed only a fraction of a read per second.
#
# `$idx` used to be `($ops % $total) + 1`, restarting from file 1 on every one of the 5 runs
# (`$ops` itself resets each run) - see the identical `file10mb-read.sh`'s own comment for the full
# effect this has at this file size. Fixed to pick pseudo-randomly instead, matching
# dir-lookup.ps1's own approach for the equivalent problem.

$ErrorActionPreference = "Stop"
# Force invariant number formatting so ops/s output is always `1234.5`, not locale-dependent
[System.Threading.Thread]::CurrentThread.CurrentCulture = [Globalization.CultureInfo]::InvariantCulture

$root = "C:\dedupfs-perf\files10mb"
$counterFile = "$root\..\counter-files10mb.txt"
if (-not (Test-Path $counterFile)) {
    throw "no files to read yet - run file10mb-create.ps1 first"
}
$total = [int](Get-Content $counterFile)

for ($run = 1; $run -le 5; $run++) {
    $start = Get-Date
    $end = $start.AddSeconds(20)
    $ops = 0
    while ((Get-Date) -lt $end) {
        $idx = Get-Random -Minimum 1 -Maximum ($total + 1)
        # Discard the byte[] via assignment, not `| Out-Null`: piping an array into the
        # pipeline enumerates it element by element, so `| Out-Null` here would measure
        # per-byte PowerShell pipeline overhead (scaling with file size) instead of read
        # cost. `$null = ` discards the whole object at once.
        $null = [System.IO.File]::ReadAllBytes("$root\sub$($idx % 20)\f$idx")
        $ops++
    }
    $elapsed = ((Get-Date) - $start).TotalSeconds
    "{0}: {1} reads, {2:N2}s, {3:N1} ops/s" -f $run, $ops, $elapsed, ($ops / $elapsed)
}
