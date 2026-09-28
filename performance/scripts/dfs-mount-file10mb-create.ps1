# location: dfs-mount, 10 MB file creation, sequential - see ../methodology.md's "Statistical
# approach", "Workload catalog", "File-content workloads", and the Location catalog's `dfs-mount`
# entry. Builds a release `dfs.exe`, creates a repository (if not already present), mounts it
# read-write, and times `[System.IO.File]::WriteAllBytes` against the *mounted* path - the same
# once-filled-template-plus-poke content scheme `file10mb-create.ps1` uses natively, so the two
# numbers are directly comparable (content generation itself stays out of the timed loop either
# way).
#
# 5 runs, each ~20 s; the counter (and the tree under the mount) keeps growing across runs and
# across repeated invocations of this script, per the "state between runs" rule - matches
# `dfs-mount-dir-create.ps1`'s own convention, not `file10mb-create.ps1`'s plain-native one.
#
# Before starting a *new* measurement, delete $base first (removes the repo, so the next run
# starts from an empty tree) - this script always removes a stale $mountPath itself on every run,
# see `dfs-mount-dir-create.ps1`'s own comment for why that one is not optional.
#
# Tool (record this in the measurement protocol): PowerShell `[System.IO.File]::WriteAllBytes`
# against a `dfs mount --read-write` mountpoint.
#
# `dfs-mount-file10mb-read.ps1` reads the files this script creates.

$ErrorActionPreference = "Stop"
[System.Threading.Thread]::CurrentThread.CurrentCulture = [Globalization.CultureInfo]::InvariantCulture

$size = 10485760
$base = "C:\dedupfs-perf\dfs-mount-files10mb"
$repoRoot = Join-Path $base "repo"
$mountPath = Join-Path $base "mnt"
$counterFile = Join-Path $base "counter.txt"

New-Item -ItemType Directory -Force -Path $base | Out-Null

cargo build --release -p cli
if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }
$dfsExe = Resolve-Path (Join-Path $PSScriptRoot "..\..\target\release\dfs.exe")

if (-not (Test-Path $repoRoot)) {
    & $dfsExe create-repo $repoRoot
    if ($LASTEXITCODE -ne 0) { throw "dfs create-repo failed" }
}

# Same reasoning as dfs-mount-dir-create.ps1: never trust a leftover $mountPath from an unclean
# previous exit, always start from no $mountPath at all.
if (Test-Path $mountPath) {
    Remove-Item -Recurse -Force $mountPath
}

$mountProc = Start-Process -FilePath $dfsExe `
    -ArgumentList @("mount", "--repository", $repoRoot, $mountPath, "--read-write") `
    -PassThru -WindowStyle Hidden

try {
    # Same probe-until-it-succeeds readiness signal as dfs-mount-dir-create.ps1 (a fresh
    # repository's mounted tree can legitimately be empty, so "wait for non-empty listing" does
    # not work here).
    #
    # `New-Item -ItemType Directory` silently creates missing *parent* directories too, even
    # without `-Force` (confirmed empirically) - so the probe must not run against a path under
    # $mountPath until $mountPath itself is confirmed to exist, or it would silently create
    # $mountPath as a plain native NTFS directory and "succeed" against that instead of the real
    # mount (found the hard way in dfs-mount-dir-create.ps1 - see that script's own comment).
    $deadline = (Get-Date).AddSeconds(15)
    while (-not (Test-Path $mountPath)) {
        if ($mountProc.HasExited) {
            throw "dfs mount process exited early (exit code $($mountProc.ExitCode)) before the mountpoint appeared - requires WinFSP to be installed"
        }
        if ((Get-Date) -gt $deadline) {
            throw "mount point did not appear within 15s (requires WinFSP to be installed)"
        }
        Start-Sleep -Milliseconds 200
    }
    $probePath = Join-Path $mountPath "_ready_probe"
    while ($true) {
        try {
            New-Item -ItemType Directory -Path $probePath -ErrorAction Stop | Out-Null
            Remove-Item -Path $probePath -ErrorAction Stop
            break
        } catch {
            if ($mountProc.HasExited) {
                throw "dfs mount process exited early (exit code $($mountProc.ExitCode)) - requires WinFSP to be installed - $_"
            }
            if ((Get-Date) -gt $deadline) {
                throw "mount did not become ready within 15s (requires WinFSP to be installed) - $_"
            }
            Start-Sleep -Milliseconds 200
        }
    }

    0..19 | ForEach-Object {
        $sub = Join-Path $mountPath "sub$_"
        if (-not (Test-Path $sub)) { New-Item -ItemType Directory -Path $sub | Out-Null }
    }

    $counter = if (Test-Path $counterFile) { [int](Get-Content $counterFile) } else { 0 }

    # One-time random template, filled outside the timed loop; matches file10mb-create.ps1 exactly.
    $crng = [System.Security.Cryptography.RNGCryptoServiceProvider]::new()
    $template = New-Object byte[] $size
    $crng.GetBytes($template)

    $pokeOffsets = New-Object System.Collections.Generic.List[int]
    for ($o = 0; $o + 8 -le $size; $o += 65536) { $pokeOffsets.Add($o) }
    if ($size -ge 8 -and ($pokeOffsets.Count -eq 0 -or $pokeOffsets[$pokeOffsets.Count - 1] -ne ($size - 8))) {
        $pokeOffsets.Add($size - 8)
    }
    $pokeBlock = New-Object byte[] ($pokeOffsets.Count * 8)

    for ($run = 1; $run -le 5; $run++) {
        $start = Get-Date
        $end = $start.AddSeconds(20)
        $ops = 0
        while ((Get-Date) -lt $end) {
            $counter++
            $crng.GetBytes($pokeBlock)
            for ($k = 0; $k -lt $pokeOffsets.Count; $k++) {
                [System.Array]::Copy($pokeBlock, $k * 8, $template, $pokeOffsets[$k], 8)
            }
            $target = Join-Path $mountPath "sub$($counter % 20)\f$counter"
            [System.IO.File]::WriteAllBytes($target, $template)
            $ops++
        }
        $elapsed = ((Get-Date) - $start).TotalSeconds
        "{0}: {1} files, {2:N2}s, {3:N1} ops/s" -f $run, $ops, $elapsed, ($ops / $elapsed)
    }
    Set-Content -Path $counterFile -Value $counter
}
finally {
    Stop-Process -Id $mountProc.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
}
