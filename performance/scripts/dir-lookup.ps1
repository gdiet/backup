# Native directory lookup, sequential - see ../methodology.md's "Statistical approach" and
# "Workload catalog". Builds a fixed-size tree once (large enough that lookup cost is not
# dominated by the whole tree fitting in some cache), then repeatedly looks up a
# pseudo-randomly chosen existing entry within it - unlike the create-workload scripts, nothing
# here grows between runs or invocations; only the setup step is idempotent-once.
#
# Before starting a *new* measurement, delete $root first so the tree is rebuilt from scratch (not
# required otherwise - the tree stays fixed size across measurements).
# Tool (record this in the measurement protocol): PowerShell `Test-Path`.
#
# Validated on `3327` native Windows (PowerShell 5.1) via scaled-down trial runs - a smaller tree
# and a shorter window, not a full 5-runs-of-20-seconds measurement; no bugs found.

$ErrorActionPreference = "Stop"
# Force invariant number formatting so ops/s output is always `1234.5`, not locale-dependent
[System.Threading.Thread]::CurrentThread.CurrentCulture = [Globalization.CultureInfo]::InvariantCulture

$root = "C:\dedupfs-perf\lookup"
$treeSize = 100000

New-Item -ItemType Directory -Force -Path $root | Out-Null
$existing = (Get-ChildItem -Path $root -Directory | Measure-Object).Count
if ($existing -lt $treeSize) {
    for ($i = $existing + 1; $i -le $treeSize; $i++) {
        New-Item -ItemType Directory -Path "$root\d$i" | Out-Null
    }
}

for ($run = 1; $run -le 5; $run++) {
    $start = Get-Date
    $end = $start.AddSeconds(20)
    $ops = 0
    while ((Get-Date) -lt $end) {
        $idx = Get-Random -Minimum 1 -Maximum ($treeSize + 1)
        Test-Path "$root\d$idx" | Out-Null
        $ops++
    }
    $elapsed = ((Get-Date) - $start).TotalSeconds
    "{0}: {1} lookups, {2:N2}s, {3:N1} ops/s" -f $run, $ops, $elapsed, ($ops / $elapsed)
}
