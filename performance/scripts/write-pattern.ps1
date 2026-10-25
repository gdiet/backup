# location: native, no DedupFS code - how the write size affects the throughput of one device. See
# ../methodology.md and agent-todos/done/write-size-on-usb-hard-disk.md for the question behind it.
#
#   write-pattern.ps1 -Target <folder> [-TotalMB 1024] [-Runs 3] [-Sizes 16384,100000,1200000,10000000]
#                     [-TotalMBBySize @{16384 = 32}]
#
# Each run creates one fresh file in -Target, appends -TotalMB in blocks of one size, calls
# Flush($true) (FlushFileBuffers) inside the timed span, and deletes the file afterwards. This is
# the byte store's pattern: one file, sequential appends, one write call per chunk.
#
# - The FileStream has a buffer size of 1, so .NET does not merge the writes itself.
# - One random buffer of the largest size is filled once, outside the timed span; every write is a
#   slice of it.
# - The sizes run round-robin (all sizes once, then all sizes again), so that a drift of the device
#   does not favor one size.
# - -TotalMBBySize overrides -TotalMB for single sizes, for example to keep very small blocks on a
#   slow device within a reasonable time.
# - For every run, the script also reads the raw disk counters of the target's physical disk before
#   and after it. "B/write" is the average size of the writes the device actually received during
#   the run, "queue" the average number of outstanding requests, and "idle" the share of the run in
#   which the disk had no request at all. All three include any other activity on that disk at the
#   same time.
#
# Tool (record this in the measurement protocol): .NET FileStream.Write, via this script.

param(
    [Parameter(Mandatory = $true)][string]$Target,
    [int]$TotalMB = 1024,
    [int]$Runs = 3,
    [int[]]$Sizes = @(16384, 100000, 1200000, 10000000),
    [hashtable]$TotalMBBySize = @{}
)

$ErrorActionPreference = "Stop"
[System.Threading.Thread]::CurrentThread.CurrentCulture = [Globalization.CultureInfo]::InvariantCulture

Add-Type -TypeDefinition @"
using System;
using System.IO;
public static class WritePattern {
    // Appends totalBytes to a fresh file in blocks of blockSize, flushes to the device, and
    // returns the elapsed seconds. The last block is shortened so that exactly totalBytes are written.
    public static double Run(string path, byte[] buffer, int blockSize, long totalBytes) {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using (var f = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1)) {
            long written = 0;
            while (written < totalBytes) {
                int n = (int)Math.Min(blockSize, totalBytes - written);
                f.Write(buffer, 0, n);
                written += n;
            }
            f.Flush(true);
        }
        return sw.Elapsed.TotalSeconds;
    }
}
"@

New-Item -ItemType Directory -Force -Path $Target | Out-Null
$targetFull = (Resolve-Path $Target).Path
$driveLetter = $targetFull.Substring(0, 2).ToUpperInvariant()
$diskInstance = (Get-CimInstance Win32_PerfRawData_PerfDisk_PhysicalDisk |
        Where-Object { $_.Name -match [regex]::Escape($driveLetter) } | Select-Object -First 1).Name

function Get-DiskCounters {
    if (-not $diskInstance) { return $null }
    $d = Get-CimInstance Win32_PerfRawData_PerfDisk_PhysicalDisk | Where-Object { $_.Name -eq $diskInstance }
    return @{
        Writes = [double]$d.DiskWritesPersec
        Bytes = [double]$d.DiskWriteBytesPersec
        QueueTime = [double]$d.AvgDiskQueueLength
        Time100ns = [double]$d.Timestamp_Sys100NS
        Idle = [double]$d.PercentIdleTime
        IdleBase = [double]$d.PercentIdleTime_Base
    }
}

# Average device write size, average queue length and idle share between two counter snapshots.
# AvgDiskQueueLength accumulates queue length x time in 100 ns units, so its delta divided by the
# elapsed 100 ns gives the average number of outstanding requests.
function Format-DiskDelta($before, $after) {
    if (-not $before -or ($after.Writes - $before.Writes) -le 0) { return @("n/a", "n/a", "n/a") }
    $elapsed = $after.Time100ns - $before.Time100ns
    return @(
        ("{0:N0}" -f (($after.Bytes - $before.Bytes) / ($after.Writes - $before.Writes))),
        ("{0:N2}" -f (($after.QueueTime - $before.QueueTime) / $elapsed)),
        ("{0:N0} %" -f (100 * ($after.Idle - $before.Idle) / ($after.IdleBase - $before.IdleBase)))
    )
}

$maxSize = ($Sizes | Measure-Object -Maximum).Maximum
$buffer = New-Object byte[] $maxSize
[System.Security.Cryptography.RNGCryptoServiceProvider]::new().GetBytes($buffer)

"target: $targetFull (disk counters: $(if ($diskInstance) { $diskInstance } else { 'not found' }))"
"{0,-4} {1,10} {2,8} {3,9} {4,9} {5,10} {6,6} {7,6}" -f "run", "block", "writes", "seconds", "MB/s", "B/write", "queue", "idle"
for ($run = 1; $run -le $Runs; $run++) {
    foreach ($size in $Sizes) {
        $mb = if ($TotalMBBySize.ContainsKey($size)) { [int]$TotalMBBySize[$size] } else { $TotalMB }
        $totalBytes = [int64]$mb * 1MB
        $file = Join-Path $targetFull "write-pattern-$size.bin"
        if (Test-Path $file) { Remove-Item $file }
        $before = Get-DiskCounters
        $seconds = [WritePattern]::Run($file, $buffer, $size, $totalBytes)
        $after = Get-DiskCounters
        $device = Format-DiskDelta $before $after
        $writes = [math]::Ceiling($totalBytes / $size)
        "{0,-4} {1,10:N0} {2,8:N0} {3,9:N2} {4,9:N1} {5,10} {6,6} {7,6}" -f $run, $size, $writes, $seconds, ($totalBytes / 1MB / $seconds), $device[0], $device[1], $device[2]
        Remove-Item $file
    }
}
