# location: dfs-cli, `dfs ingest` of pre-existing directories or files - see ../methodology.md's
# "Statistical approach", "Workload catalog", "File-content workloads", and the Location catalog's
# `dfs-cli` entry.
#
#   ingest.ps1 -Workload dir|file100b|file10mb -Count <n> -SourceRoot <dir> -RepoBase <dir> [-Runs 5]
#
# The source tree is generated once under $SourceRoot (not timed) and reused by every run and by
# repeated invocations with the same Workload and Count. Directories are flat (d1..dN), files are
# spread round-robin over 20 subdirectories, as in the matching native create scripts. File content
# is unique per file (same once-filled-template-plus-poke scheme as file10mb-create.ps1).
#
# Each run ingests the whole source tree into a FRESH repository below $RepoBase (created before
# the clock starts, deleted after it stops). Ingest is a one-shot batch job, and a second ingest
# into the same repository would deduplicate against the first instead of repeating its work, so
# the "state accumulates between runs" rule of ../methodology.md does not apply here. $SourceRoot
# and $RepoBase may be on different devices: that is the point of the two parameters.
#
# Tool (record this in the measurement protocol): `dfs ingest --repository <repo> <source> /`.
#
# Before starting a measurement with a different Workload or Count, delete $SourceRoot first.

param(
    [Parameter(Mandatory = $true)][ValidateSet("dir", "file100b", "file10mb")][string]$Workload,
    [Parameter(Mandatory = $true)][int]$Count,
    [Parameter(Mandatory = $true)][string]$SourceRoot,
    [Parameter(Mandatory = $true)][string]$RepoBase,
    [int]$Runs = 5
)

$ErrorActionPreference = "Stop"
# Force invariant number formatting so the output is always `1234.5`, not locale-dependent
[System.Threading.Thread]::CurrentThread.CurrentCulture = [Globalization.CultureInfo]::InvariantCulture

$size = switch ($Workload) { "dir" { 0 } "file100b" { 100 } "file10mb" { 10485760 } }
$source = Join-Path $SourceRoot "$Workload-$Count"

# cargo reports its progress on stderr, which Stop would turn into a terminating error whenever the
# caller redirects this script's error stream.
$ErrorActionPreference = "Continue"
cargo build --release -p cli
if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }
$ErrorActionPreference = "Stop"
$dfsExe = Resolve-Path (Join-Path $PSScriptRoot "..\..\target\release\dfs.exe")

if (-not (Test-Path $source)) {
    New-Item -ItemType Directory -Force -Path $source | Out-Null
    if ($Workload -eq "dir") {
        for ($i = 1; $i -le $Count; $i++) {
            New-Item -ItemType Directory -Path (Join-Path $source "d$i") | Out-Null
        }
    }
    else {
        0..19 | ForEach-Object { New-Item -ItemType Directory -Force -Path (Join-Path $source "sub$_") | Out-Null }
        $crng = [System.Security.Cryptography.RNGCryptoServiceProvider]::new()
        $template = New-Object byte[] $size
        $crng.GetBytes($template)
        # Poke offsets: every 64 KiB plus the final 8 bytes - see file100b-create.ps1 for why the
        # spacing must stay well below the chunker's minimum chunk size.
        $pokeOffsets = New-Object System.Collections.Generic.List[int]
        for ($o = 0; $o + 8 -le $size; $o += 65536) { $pokeOffsets.Add($o) }
        if ($pokeOffsets.Count -eq 0 -or $pokeOffsets[$pokeOffsets.Count - 1] -ne ($size - 8)) {
            $pokeOffsets.Add($size - 8)
        }
        $pokeBlock = New-Object byte[] ($pokeOffsets.Count * 8)
        for ($i = 1; $i -le $Count; $i++) {
            $crng.GetBytes($pokeBlock)
            for ($k = 0; $k -lt $pokeOffsets.Count; $k++) {
                [System.Array]::Copy($pokeBlock, $k * 8, $template, $pokeOffsets[$k], 8)
            }
            [System.IO.File]::WriteAllBytes((Join-Path $source "sub$($i % 20)\f$i"), $template)
        }
    }
}

New-Item -ItemType Directory -Force -Path $RepoBase | Out-Null
for ($run = 1; $run -le $Runs; $run++) {
    $repo = Join-Path $RepoBase "repo-$run"
    if (Test-Path $repo) { Remove-Item -Recurse -Force $repo }
    $null = & $dfsExe create-repo $repo
    if ($LASTEXITCODE -ne 0) { throw "dfs create-repo failed" }

    $start = Get-Date
    $null = & $dfsExe ingest --repository $repo $source "/"
    if ($LASTEXITCODE -ne 0) { throw "dfs ingest failed" }
    $elapsed = ((Get-Date) - $start).TotalSeconds

    $line = "{0}: {1} items, {2:N2}s, {3:N1} ops/s" -f $run, $Count, $elapsed, ($Count / $elapsed)
    if ($size -gt 0) {
        $line += ", {0:N1} MB/s" -f (([int64]$Count * $size / 1MB) / $elapsed)
    }
    $line
    Remove-Item -Recurse -Force $repo
}
