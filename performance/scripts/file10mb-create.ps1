# Native 10 MB file creation, spread across several directories, sequential - see
# ../methodology.md's "Statistical approach", "Workload catalog" and "File-content workloads".
# 5 runs, each ~20 s; the counter keeps growing across runs and across repeated invocations of
# this script, per the "state between runs" rule - do not delete $root between runs of the same
# measurement. At this size the accumulated files reach several GB over the five runs - check free
# space first (see ../methodology.md's "File-content workloads" note).
#
# Before starting a *new* measurement, delete $root first so Scale has a clean starting point.
# Content: one random template buffer, filled once before the timed loop; each file is that
# template with 8 fresh random bytes poked in every 64 KiB and again in the final 8 bytes, so no
# chunk-sized window is byte-identical between two files. The generator never runs inside the
# timed loop. On native NTFS the non-deduplicating property is irrelevant, but every file-content
# script shares one content discipline so native and future chunked-path numbers stay comparable:
# at the 20-bit CDC default a 10 MB file is roughly 8-10 chunks, so this is the multi-chunk point
# of the size ladder. file10mb-read.ps1 reads the files this script creates.
# Tool (record this in the measurement protocol): PowerShell `[System.IO.File]::WriteAllBytes`.
#
# Validated on `3327` native Windows (PowerShell 5.1) via scaled-down trial runs - a smaller Scale
# and a shorter window, not a full 5-runs-of-20-seconds measurement; no bugs found.

$ErrorActionPreference = "Stop"
# Force invariant number formatting so ops/s output is always `1234.5`, not locale-dependent
[System.Threading.Thread]::CurrentThread.CurrentCulture = [Globalization.CultureInfo]::InvariantCulture

$size = 10485760
$root = "C:\dedupfs-perf\files10mb"
New-Item -ItemType Directory -Force -Path $root | Out-Null
0..19 | ForEach-Object { New-Item -ItemType Directory -Force -Path "$root\sub$_" | Out-Null }
$counterFile = "$root\..\counter-files10mb.txt"
$counter = if (Test-Path $counterFile) { [int](Get-Content $counterFile) } else { 0 }

# One-time random template, filled outside the timed loop; the generator never runs inside it.
$crng = [System.Security.Cryptography.RNGCryptoServiceProvider]::new()
$template = New-Object byte[] $size
$crng.GetBytes($template)

# Poke offsets: every 64 KiB, plus the final 8 bytes. The 64 KiB spacing must stay well below the
# CDC minimum chunk size (2^(target_size_bits - 1) = 512 KiB at the 20-bit default) so that every
# chunk - including the smaller-than-minimum final chunk flushed at EOF - contains at least one
# poke and therefore differs between files.
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
        [System.IO.File]::WriteAllBytes("$root\sub$($counter % 20)\f$counter", $template)
        $ops++
    }
    $elapsed = ((Get-Date) - $start).TotalSeconds
    "{0}: {1} files, {2:N2}s, {3:N1} ops/s" -f $run, $ops, $elapsed, ($ops / $elapsed)
}
Set-Content -Path $counterFile -Value $counter
