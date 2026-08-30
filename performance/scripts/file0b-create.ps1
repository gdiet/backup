# Native zero-byte file creation, spread across several directories, sequential - see
# ../methodology.md's "Statistical approach" and "Workload catalog". 5 runs, each ~20 s; the
# counter keeps growing across runs and across repeated invocations of this script, per the "state
# between runs" rule - do not delete $root between runs of the same measurement.
#
# Before starting a *new* measurement, delete $root first so Scale has a clean starting point.
# Tool (record this in the measurement protocol): PowerShell `New-Item -ItemType File`.

$ErrorActionPreference = "Stop"
# Force invariant number formatting so ops/s output is always `1234.5`, not locale-dependent
[System.Threading.Thread]::CurrentThread.CurrentCulture = [Globalization.CultureInfo]::InvariantCulture

$root = "C:\dedupfs-perf\files0b"
New-Item -ItemType Directory -Force -Path $root | Out-Null
0..19 | ForEach-Object { New-Item -ItemType Directory -Force -Path "$root\sub$_" | Out-Null }
$counterFile = "$root\..\counter-files0b.txt"
$counter = if (Test-Path $counterFile) { [int](Get-Content $counterFile) } else { 0 }

for ($run = 1; $run -le 5; $run++) {
    $start = Get-Date
    $end = $start.AddSeconds(20)
    $ops = 0
    while ((Get-Date) -lt $end) {
        $counter++
        New-Item -ItemType File -Path "$root\sub$($counter % 20)\f$counter" | Out-Null
        $ops++
    }
    $elapsed = ((Get-Date) - $start).TotalSeconds
    "{0}: {1} files, {2:N2}s, {3:N1} ops/s" -f $run, $ops, $elapsed, ($ops / $elapsed)
}
Set-Content -Path $counterFile -Value $counter
