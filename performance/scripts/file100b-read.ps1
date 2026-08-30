# Native 100 B file read-back, sequential - see ../methodology.md's "Statistical approach" and
# "Workload catalog". Reads back files file100b-create.ps1 created, cycling through whichever
# range it has produced so far (run file100b-create.ps1 first - this script only reads, it never
# grows the tree itself, so nothing here needs the "state between runs" bookkeeping the create
# scripts do).
# Tool (record this in the measurement protocol): PowerShell `[System.IO.File]::ReadAllBytes`.
#
# Validated on `3327` native Windows (PowerShell 5.1) via scaled-down trial runs - a shorter
# window, not a full 5-runs-of-20-seconds measurement. One bug found and fixed on the first run:
# the read result was discarded with `| Out-Null`, which enumerates the byte[] element by element
# and so measured pipeline overhead rather than read cost (see the discard site below).

$ErrorActionPreference = "Stop"
# Force invariant number formatting so ops/s output is always `1234.5`, not locale-dependent
[System.Threading.Thread]::CurrentThread.CurrentCulture = [Globalization.CultureInfo]::InvariantCulture

$root = "C:\dedupfs-perf\files100b"
$counterFile = "$root\..\counter-files100b.txt"
if (-not (Test-Path $counterFile)) {
    throw "no files to read yet - run file100b-create.ps1 first"
}
$total = [int](Get-Content $counterFile)

for ($run = 1; $run -le 5; $run++) {
    $start = Get-Date
    $end = $start.AddSeconds(20)
    $ops = 0
    while ((Get-Date) -lt $end) {
        $idx = ($ops % $total) + 1
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
