# Native directory listing, sequential - see ../methodology.md's "Statistical approach" and
# "Workload catalog". Builds one directory with a fixed, large number of entries once (large
# enough that per-entry cost dominates over fixed per-call overhead), then repeatedly lists it in
# full - nothing grows between runs or invocations; only the setup step is idempotent-once.
#
# Before starting a *new* measurement, delete $dir first so the entry count is rebuilt from
# scratch (not required otherwise - the directory stays fixed size across measurements).
# Tool (record this in the measurement protocol): PowerShell `Get-ChildItem`.
#
# Scale here is "entries in the listed directory" (see the recording template), not "listings
# performed" - $ops below counts listing calls, each of which enumerates all $entryCount entries.
#
# Validated on `3327` native Windows (PowerShell 5.1) via scaled-down trial runs - a smaller entry
# count and a shorter window, not a full 5-runs-of-20-seconds measurement; no bugs found.

$ErrorActionPreference = "Stop"
# Force invariant number formatting so ops/s output is always `1234.5`, not locale-dependent
[System.Threading.Thread]::CurrentThread.CurrentCulture = [Globalization.CultureInfo]::InvariantCulture

$dir = "C:\dedupfs-perf\listing\entries"
$entryCount = 50000

New-Item -ItemType Directory -Force -Path $dir | Out-Null
$existing = (Get-ChildItem -Path $dir -File | Measure-Object).Count
if ($existing -lt $entryCount) {
    for ($i = $existing + 1; $i -le $entryCount; $i++) {
        New-Item -ItemType File -Path "$dir\f$i" | Out-Null
    }
}

for ($run = 1; $run -le 5; $run++) {
    $start = Get-Date
    $end = $start.AddSeconds(20)
    $ops = 0
    while ((Get-Date) -lt $end) {
        Get-ChildItem -Path $dir | Out-Null
        $ops++
    }
    $elapsed = ((Get-Date) - $start).TotalSeconds
    "{0}: {1} listings, {2:N2}s, {3:N1} ops/s" -f $run, $ops, $elapsed, ($ops / $elapsed)
}
