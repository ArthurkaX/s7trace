param(
    [string]$Snap7Root = (Join-Path (Resolve-Path ".").Path "snap7"),
    [string]$Db = (Join-Path $env:TEMP "s7trace_snap7_ui_demo.db"),
    [string]$Bin = (Join-Path (Resolve-Path ".").Path "target\debug\s7trace.exe"),
    [string]$MultiBatchMax = "0",
    [switch]$KeepDb,
    [switch]$NoLaunch
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $Bin)) {
    Write-Host "s7trace.exe not found, building target\debug\s7trace.exe"
    cargo build --bin s7trace
    if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }
}

& (Join-Path $PSScriptRoot "start-snap7-sim.ps1") -Snap7Root $Snap7Root

if (-not $KeepDb) {
    Remove-Item -LiteralPath $Db, "$Db-wal", "$Db-shm" -Force -ErrorAction SilentlyContinue
}

$nowUs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 1000
$tagRows = New-Object System.Collections.Generic.List[string]
$metricSetTagRows = New-Object System.Collections.Generic.List[string]
$tagId = 1

function Add-DemoTag {
    param(
        [int]$DbNum,
        [int]$Offset,
        [int]$ByteSize,
        [string]$DataType,
        [string]$Role = "state",
        [Nullable[int]]$BitOffset = $null
    )

    $prefix = "DB$DbNum"
    if ($DataType -eq "bool") {
        $name = "$prefix.Bit$Offset.$BitOffset"
        $address = "$prefix.DBX$Offset.$BitOffset"
    } elseif ($ByteSize -eq 4) {
        $name = "$prefix.DWord$Offset"
        $address = "$prefix.DBD$Offset"
    } else {
        $name = "$prefix.Word$Offset"
        $address = "$prefix.DBW$Offset"
    }

    $bitSql = if ($null -eq $BitOffset) { "NULL" } else { "$BitOffset" }
    $script:tagRows.Add("    ($script:tagId, 1, '$name', '$address', $DbNum, $Offset, $bitSql, $ByteSize, '$DataType', '$Role', 1, 'periodic', 'periodic', 1, 0, 1, 0, 'pending', $script:nowUs, $script:nowUs)")
    $script:metricSetTagRows.Add("    (1, $script:tagId, 1, $script:nowUs)")
    $script:tagId++
}

# The packaged Snap7 plain-c server exposes DB1=512, DB2=128, DB3=1024.
# Sparse 2-byte tags with gaps > 8 bytes force multi-read items instead of one block read.
Add-DemoTag -DbNum 1 -Offset 0 -ByteSize 2 -DataType "i16"
Add-DemoTag -DbNum 1 -Offset 2 -ByteSize 2 -DataType "i16"
Add-DemoTag -DbNum 1 -Offset 4 -ByteSize 4 -DataType "i32" -Role "counter"
Add-DemoTag -DbNum 1 -Offset 10 -ByteSize 1 -DataType "bool" -Role "diagnostic" -BitOffset 0
Add-DemoTag -DbNum 1 -Offset 12 -ByteSize 2 -DataType "i16"

foreach ($offset in 32, 56, 80, 104, 128, 152, 176, 200, 224, 248, 272, 296, 320, 344, 368, 392, 416, 440, 464, 488) {
    Add-DemoTag -DbNum 1 -Offset $offset -ByteSize 2 -DataType "i16"
}

foreach ($offset in 0, 12, 24, 36, 48, 60, 72, 84, 96, 108, 120) {
    Add-DemoTag -DbNum 2 -Offset $offset -ByteSize 2 -DataType "i16"
}

foreach ($offset in 0, 12, 24, 36, 48, 60, 72, 84, 96, 108, 120, 132, 144, 156, 168, 180, 192, 204, 216, 228, 240, 252, 264, 276, 288, 300, 312, 324, 336, 348, 360, 372, 384, 396, 408, 420, 432, 444, 456, 468, 480, 492, 504, 516, 528, 540, 552, 564, 576, 588, 600, 612, 624, 636, 648, 660, 672, 684, 696, 708, 720, 732, 744, 756, 768, 780, 792, 804, 816, 828, 840, 852, 864, 876, 888, 900, 912, 924, 936, 948, 960, 972, 984, 996, 1008, 1020) {
    Add-DemoTag -DbNum 3 -Offset $offset -ByteSize 2 -DataType "i16"
}

$tagValues = $tagRows -join ",`n"
$metricSetTagValues = $metricSetTagRows -join ",`n"
$tagCount = $tagRows.Count

$sql = @"
INSERT OR REPLACE INTO plc
    (id, name, host, rack, slot, enabled, connectivity)
VALUES
    (1, 'snap7-sim', '127.0.0.1:102', 0, 2, 1, 'unknown');

INSERT OR REPLACE INTO poll_group
    (id, name, period_ms, enabled)
VALUES
    (1, 'demo_150ms', 150, 1);

INSERT OR REPLACE INTO metric_set
    (id, plc_id, name, enabled, description, created_ts, updated_ts)
VALUES
    (1, 1, 'snap7_pdu_split', 1, 'Local Snap7 DB1/DB2/DB3 sparse PDU split demo, $tagCount tags', $nowUs, $nowUs);

INSERT OR REPLACE INTO tag
    (id, plc_id, name, address, db_num, byte_offset, bit_offset, byte_size,
     data_type, value_role, poll_group_id, store_mode, storage_hook,
     enabled, bad, desired_version, applied_version, apply_state,
     created_ts, updated_ts)
VALUES
$tagValues;

INSERT OR REPLACE INTO metric_set_tag
    (metric_set_id, tag_id, plc_id, created_ts)
VALUES
$metricSetTagValues;

INSERT OR REPLACE INTO dataset
    (id, plc_id, name, state, started_ts, created_ts, updated_ts, note)
VALUES
    (1, 1, 'snap7_ui_demo', 'active', $nowUs, $nowUs, $nowUs, 'Started by scripts/start-ui-snap7-demo.ps1');

INSERT OR REPLACE INTO dataset_metric_set
    (dataset_id, metric_set_id, plc_id, created_ts)
VALUES
    (1, 1, 1, $nowUs);
"@

Write-Host "Preparing demo database: $Db"
Write-Host "  Metric set: snap7_pdu_split"
Write-Host "  Tags: $tagCount across DB1, DB2, DB3"
& $Bin --db $Db --local-db admin-sql $sql
if ($LASTEXITCODE -ne 0) { throw "failed to prepare demo database" }

if ($NoLaunch) {
    Write-Host "Demo database ready; not launching s7trace server because -NoLaunch was set"
    exit 0
}

Write-Host "Starting s7trace server UI against Snap7 simulation"
Write-Host "  DB: $Db"
Write-Host "  PLC: 127.0.0.1:102"
Write-Host "  S7TRACE_MULTI_BATCH_MAX: $MultiBatchMax"
$env:S7TRACE_MULTI_BATCH_MAX = $MultiBatchMax
$proc = Start-Process -FilePath $Bin -ArgumentList @("--db", $Db, "server") -PassThru
Write-Host "s7trace server started pid=$($proc.Id)"
