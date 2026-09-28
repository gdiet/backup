# location: dfs-mount, directory creation, sequential - see ../methodology.md's "Statistical
# approach" and "Workload catalog", and ../methodology.md's Location catalog entry for
# `dfs-mount`. Builds a release `dfs.exe`, creates a repository, mounts it read-write, and times
# `New-Item -ItemType Directory` against the *mounted* path instead of a native one. Directory
# creation only, to match `db_bench.rs`'s own scope one layer further down the stack -
# `dfs-mount-file10mb-{create,read}.ps1` cover file content through the mount separately.
#
# 5 runs, each ~20 s; the counter (and the tree under the mount) keeps growing across runs and
# across repeated invocations of this script, per the "state between runs" rule.
#
# Before starting a *new* measurement, delete $base first (removes the repo, so the next run
# starts from an empty tree) - this script always removes a stale $mountPath itself on every run,
# see the comment at that step for why that one is not optional.
#
# Tool (record this in the measurement protocol): PowerShell `New-Item -ItemType Directory`
# against a `dfs mount --read-write` mountpoint.
#
# Unmounts by killing the mount process directly (Stop-Process -Force) rather than a graceful
# Ctrl+C shutdown - the same simplifying choice `windows_mount.rs`'s own read-only-op-set test
# makes; acceptable for a benchmark's ephemeral, throwaway repository, not for a real operator
# session (see requirements/functional/mount.md - Ctrl+C is the documented, clean way to stop a
# real `dfs mount`).
#
# First run against a real WinFSP install (2026-09-29, `julius`) found and fixed a real bug in
# this script's own readiness probe (see the comment at that step) - the reported throughput and
# resulting repository were silently meaningless until then. Confirmed correct after the fix: the
# repository's own `dfs stats` afterward reports a matching directory count.

$ErrorActionPreference = "Stop"

$base = "C:\dedupfs-perf\dfs-mount-dirs"
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

# A leftover $mountPath from a previous run that crashed or was killed uncleanly would be a plain,
# real, writable directory - not a live mount. Without removing it first, the readiness probe
# below could "succeed" against that stale directory instead of the actual mount, silently
# benchmarking native NTFS instead of DedupFS. Always start from no $mountPath at all; WinFSP
# creates it itself once the mount is live (same as `windows_mount.rs`'s test, which never
# pre-creates its mount path either).
if (Test-Path $mountPath) {
    Remove-Item -Recurse -Force $mountPath
}

$mountProc = Start-Process -FilePath $dfsExe `
    -ArgumentList @("mount", "--repository", $repoRoot, $mountPath, "--read-write") `
    -PassThru -WindowStyle Hidden

try {
    # The mounted tree can legitimately be empty at this point (a fresh repository has no
    # entries), so "wait for a non-empty directory listing" - the readiness signal
    # `windows_mount.rs`'s tests use, relying on their fixed test filesystem always having
    # content - does not work here. Instead, retry the actual operation being benchmarked
    # (mkdir) until it succeeds, then remove that probe entry so it does not pollute the Scale
    # count below.
    #
    # Bug found on this script's first real run (2026-09-29): `New-Item -ItemType Directory`
    # silently creates missing *parent* directories too, even without `-Force` - confirmed
    # empirically, not assumed. If the probe below ran while $mountPath itself did not exist yet
    # (WinFSP not attached there yet), it would silently create $mountPath as an ordinary native
    # NTFS directory, the probe would "succeed" against that fake directory, and every
    # `New-Item`/`WriteAllBytes` call for the rest of the run would then go to plain NTFS instead
    # of the mount - no error anywhere, but the reported throughput and the resulting repository
    # (still empty) would both be meaningless. Fixed by waiting for $mountPath to actually exist
    # first (`Test-Path` only ever checks, never creates), before ever calling `New-Item` against
    # a path under it.
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

    $counter = if (Test-Path $counterFile) { [int](Get-Content $counterFile) } else { 0 }

    for ($run = 1; $run -le 5; $run++) {
        $start = Get-Date
        $end = $start.AddSeconds(20)
        $ops = 0
        while ((Get-Date) -lt $end) {
            $counter++
            New-Item -ItemType Directory -Path (Join-Path $mountPath "d$counter") | Out-Null
            $ops++
        }
        $elapsed = ((Get-Date) - $start).TotalSeconds
        "{0}: {1} dirs, {2:N2}s, {3:N1} ops/s" -f $run, $ops, $elapsed, ($ops / $elapsed)
    }
    Set-Content -Path $counterFile -Value $counter
}
finally {
    Stop-Process -Id $mountProc.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
}
