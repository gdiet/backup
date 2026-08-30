# Native directory creation, sequential - see ../methodology.md's "Statistical approach" and
# "Workload catalog". 5 runs, each ~20 s; the counter (and therefore the directory tree) keeps
# growing across runs and across repeated invocations of this script, per the "state between runs"
# rule - do not delete $root between runs of the same measurement.
#
# Before starting a *new* measurement, delete $root first so Scale has a clean starting point.
# Tool (record this in the measurement protocol): PowerShell `New-Item -ItemType Directory`.

$ErrorActionPreference = "Stop"
# Force invariant number formatting so ops/s output is always `1234.5`, not locale-dependent
[System.Threading.Thread]::CurrentThread.CurrentCulture = [Globalization.CultureInfo]::InvariantCulture

$root = "C:\dedupfs-perf\dirs"
New-Item -ItemType Directory -Force -Path $root | Out-Null
$counterFile = "$root\..\counter-dirs.txt"
$counter = if (Test-Path $counterFile) { [int](Get-Content $counterFile) } else { 0 }

for ($run = 1; $run -le 5; $run++) {
    $start = Get-Date
    $end = $start.AddSeconds(20)
    $ops = 0
    while ((Get-Date) -lt $end) {
        $counter++
        New-Item -ItemType Directory -Path "$root\d$counter" | Out-Null
        $ops++
    }
    $elapsed = ((Get-Date) - $start).TotalSeconds
    "{0}: {1} dirs, {2:N2}s, {3:N1} ops/s" -f $run, $ops, $elapsed, ($ops / $elapsed)
}
Set-Content -Path $counterFile -Value $counter
