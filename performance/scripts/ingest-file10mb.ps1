# location: dfs-cli, `dfs ingest` of pre-existing 10 MB files - see ../methodology.md's "Statistical
# approach", "Workload catalog", "File-content workloads", and the Location catalog's `dfs-cli`
# entry. Generates $fileCount already-existing 10 MB files natively first (same template-and-poke
# content scheme as file10mb-create.ps1, unique content per file, generation itself not timed),
# then times a single `dfs ingest` call importing that whole batch into a fresh repository.
#
# Ingest is a one-shot batch job, not a request-response loop like the other scripts here - this
# measures one timed run over a fixed Scale rather than the usual 5-runs-of-~20-seconds window
# (see ../methodology.md's "Statistical approach" note on using a longer/different window "in
# consultation with the developer" when a fixed window would not yield a robust number - a single
# ingest call has no natural way to subdivide into 5 independent runs without either re-ingesting
# the same content five times, which dedupes trivially after the first, or growing Scale five-fold,
# which is a different measurement).
#
# Tool (record this in the measurement protocol): `dfs ingest`.
#
# Before starting a *new* measurement, delete $base first.

param(
    [string]$Base = (Join-Path $env:TEMP "dedupfs-perf\ingest-files10mb")
)

$ErrorActionPreference = "Stop"
[System.Threading.Thread]::CurrentThread.CurrentCulture = [Globalization.CultureInfo]::InvariantCulture

$fileCount = 30
$size = 10485760
$base = $Base
$sourceRoot = Join-Path $base "source"
$repoRoot = Join-Path $base "repo"

if (Test-Path $repoRoot) {
    throw "$repoRoot already exists - delete $base first to start a fresh measurement"
}

# cargo reports its progress on stderr, which Stop would turn into a terminating error whenever the
# caller redirects this script's error stream.
$ErrorActionPreference = "Continue"
cargo build --release -p cli
if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }
$ErrorActionPreference = "Stop"
$dfsExe = Resolve-Path (Join-Path $PSScriptRoot "..\..\target\release\dfs.exe")

New-Item -ItemType Directory -Force -Path $sourceRoot | Out-Null

# Same once-filled-template-plus-poke content scheme as file10mb-create.ps1 - unique content per
# file, generator kept out of the timed section below (this script only times the ingest call
# itself, not source-file generation).
$crng = [System.Security.Cryptography.RNGCryptoServiceProvider]::new()
$template = New-Object byte[] $size
$crng.GetBytes($template)
$pokeOffsets = New-Object System.Collections.Generic.List[int]
for ($o = 0; $o + 8 -le $size; $o += 65536) { $pokeOffsets.Add($o) }
if ($size -ge 8 -and ($pokeOffsets.Count -eq 0 -or $pokeOffsets[$pokeOffsets.Count - 1] -ne ($size - 8))) {
    $pokeOffsets.Add($size - 8)
}
$pokeBlock = New-Object byte[] ($pokeOffsets.Count * 8)

for ($i = 1; $i -le $fileCount; $i++) {
    $crng.GetBytes($pokeBlock)
    for ($k = 0; $k -lt $pokeOffsets.Count; $k++) {
        [System.Array]::Copy($pokeBlock, $k * 8, $template, $pokeOffsets[$k], 8)
    }
    [System.IO.File]::WriteAllBytes((Join-Path $sourceRoot "f$i.bin"), $template)
}

& $dfsExe create-repo $repoRoot
if ($LASTEXITCODE -ne 0) { throw "dfs create-repo failed" }

$start = Get-Date
& $dfsExe ingest --repository $repoRoot $sourceRoot "/"
if ($LASTEXITCODE -ne 0) { throw "dfs ingest failed" }
$elapsed = ((Get-Date) - $start).TotalSeconds
$totalBytes = [int64]$fileCount * $size
$mbPerSecond = ($totalBytes / 1MB) / $elapsed
"{0} files, {1} bytes, {2:N2}s, {3:N1} files/s, {4:N1} MB/s" -f $fileCount, $totalBytes, $elapsed, ($fileCount / $elapsed), $mbPerSecond
