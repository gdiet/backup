# location: dfs-mount, 10 MB file read-back, pseudo-random - see ../methodology.md's "Statistical
# approach" and the Location catalog's `dfs-mount` entry. Reads back files
# `dfs-mount-file10mb-create.ps1` created, through a read-only mount of the same repository (run
# that script first - this script only reads, it never grows the tree itself).
# Tool (record this in the measurement protocol): PowerShell `[System.IO.File]::ReadAllBytes`
# against a `dfs mount` (no `--read-write`) mountpoint.

$ErrorActionPreference = "Stop"
[System.Threading.Thread]::CurrentThread.CurrentCulture = [Globalization.CultureInfo]::InvariantCulture

$base = "C:\dedupfs-perf\dfs-mount-files10mb"
$repoRoot = Join-Path $base "repo"
$mountPath = Join-Path $base "mnt-read"
$counterFile = Join-Path $base "counter.txt"

if (-not (Test-Path $counterFile)) {
    throw "no files to read yet - run dfs-mount-file10mb-create.ps1 first"
}
$total = [int](Get-Content $counterFile)

cargo build --release -p cli
if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }
$dfsExe = Resolve-Path (Join-Path $PSScriptRoot "..\..\target\release\dfs.exe")

if (Test-Path $mountPath) {
    Remove-Item -Recurse -Force $mountPath
}

$mountProc = Start-Process -FilePath $dfsExe `
    -ArgumentList @("mount", "--repository", $repoRoot, $mountPath) `
    -PassThru -WindowStyle Hidden

try {
    # Unlike dfs-mount-dir-create.ps1/dfs-mount-file10mb-create.ps1's empty-tree case, the
    # repository already has content by the time this script runs (the create script ran first) -
    # so "wait for a non-empty listing" works fine as the readiness signal here, and a write-based
    # probe would not work anyway (this mount is read-only).
    $deadline = (Get-Date).AddSeconds(15)
    while ($true) {
        $ready = $false
        try {
            $ready = (Get-ChildItem -Path $mountPath -ErrorAction Stop | Measure-Object).Count -gt 0
        } catch {}
        if ($ready) { break }
        if ((Get-Date) -gt $deadline) {
            throw "mount did not become ready within 15s (requires WinFSP to be installed)"
        }
        Start-Sleep -Milliseconds 200
    }

    for ($run = 1; $run -le 5; $run++) {
        $start = Get-Date
        $end = $start.AddSeconds(20)
        $ops = 0
        while ((Get-Date) -lt $end) {
            $idx = Get-Random -Minimum 1 -Maximum ($total + 1)
            $target = Join-Path $mountPath "sub$($idx % 20)\f$idx"
            # Discard via assignment, not `| Out-Null` - see file10mb-read.ps1's own comment for
            # why the pipeline form would measure per-byte overhead instead of read cost.
            $null = [System.IO.File]::ReadAllBytes($target)
            $ops++
        }
        $elapsed = ((Get-Date) - $start).TotalSeconds
        "{0}: {1} reads, {2:N2}s, {3:N1} ops/s" -f $run, $ops, $elapsed, ($ops / $elapsed)
    }
}
finally {
    Stop-Process -Id $mountProc.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
}
