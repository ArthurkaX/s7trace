# Poller-fairness check against the Snap7 simulator (Block 4 / per-PLC actor).
#
# Seeds ONE PLC with TWO poll groups sharing it:
#   - fast  = 50 ms,   4 small tags in DB1
#   - slow  = 1500 ms, 60 sparse tags in DB3 (a long multi-read sweep)
#
# With the old shared-`Mutex<dyn PlcClient>` design the fast group blocked
# behind the slow group's whole sweep, so its effective period ballooned and it
# barely cycled. With the per-PLC actor the fast group preempts (lower period =
# higher priority) and its `actual_ms` stays near its 50 ms target.
#
# PASS criteria (over a ~12 s window):
#   1. fast group ran many more cycles than slow (>= 8x — period ratio is 30x)
#   2. fast group median actual_ms is well under the slow sweep time
#   3. both groups made progress (no starvation either way)

param(
    [string]$Snap7Root = (Join-Path (Resolve-Path ".").Path "snap7"),
    [string]$Db = (Join-Path $env:TEMP "s7trace_snap7_fairness.db"),
    [string]$Bin = (Join-Path (Resolve-Path ".").Path "target\debug\s7trace.exe"),
    [int]$RunSeconds = 12
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $Bin)) {
    Write-Host "Building s7trace.exe"
    cargo build --bin s7trace
    if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }
}

& (Join-Path $PSScriptRoot "start-snap7-sim.ps1") -Snap7Root $Snap7Root

Remove-Item -LiteralPath $Db, "$Db-wal", "$Db-shm" -Force -ErrorAction SilentlyContinue

$nowUs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 1000
$tagRows = New-Object System.Collections.Generic.List[string]
$mstRows = New-Object System.Collections.Generic.List[string]
$tagId = 1

function Add-Tag {
    param([int]$DbNum, [int]$Offset, [int]$Group)
    $name = "DB$DbNum.W$Offset"
    $addr = "DB$DbNum.DBW$Offset"
    $script:tagRows.Add("($script:tagId, 1, '$name', '$addr', $DbNum, $Offset, NULL, 2, 'i16', 'state', $Group, 'periodic', 'periodic', 1, 0, 1, 0, 'pending', $script:nowUs, $script:nowUs)")
    $script:mstRows.Add("(1, $script:tagId, 1, $script:nowUs)")
    $script:tagId++
}

# Fast group (id=1): a handful of DB1 words.
foreach ($o in 0, 2, 4, 6) { Add-Tag -DbNum 1 -Offset $o -Group 1 }
# Slow group (id=2): 60 sparse DB3 words (gaps > 8 force multi-read items).
foreach ($o in 0..59) { Add-Tag -DbNum 3 -Offset ($o * 16) -Group 2 }

$tagValues = $tagRows -join ",`n"
$mstValues = $mstRows -join ",`n"
$tagCount = $tagRows.Count

$sql = @"
INSERT OR REPLACE INTO plc (id, name, host, rack, slot, enabled, connectivity)
VALUES (1, 'snap7-sim', '127.0.0.1:102', 0, 2, 1, 'unknown');

INSERT OR REPLACE INTO poll_group (id, name, period_ms, enabled) VALUES
    (1, 'fast_50ms', 50, 1),
    (2, 'slow_1500ms', 1500, 1);

INSERT OR REPLACE INTO metric_set (id, plc_id, name, enabled, description, created_ts, updated_ts)
VALUES (1, 1, 'fairness', 1, 'Block 4 fairness check, $tagCount tags', $nowUs, $nowUs);

INSERT OR REPLACE INTO tag
    (id, plc_id, name, address, db_num, byte_offset, bit_offset, byte_size,
     data_type, value_role, poll_group_id, store_mode, storage_hook,
     enabled, bad, desired_version, applied_version, apply_state,
     created_ts, updated_ts)
VALUES
$tagValues;

INSERT OR REPLACE INTO metric_set_tag (metric_set_id, tag_id, plc_id, created_ts) VALUES
$mstValues;

INSERT OR REPLACE INTO dataset (id, plc_id, name, state, started_ts, created_ts, updated_ts, note)
VALUES (1, 1, 'fairness', 'active', $nowUs, $nowUs, $nowUs, 'test-snap7-fairness.ps1');

INSERT OR REPLACE INTO dataset_metric_set (dataset_id, metric_set_id, plc_id, created_ts)
VALUES (1, 1, 1, $nowUs);
"@

Write-Host "Seeding $Db ($tagCount tags: 4 fast DB1 + 60 slow DB3)"
& $Bin --db $Db --local-db admin-sql $sql
if ($LASTEXITCODE -ne 0) { throw "failed to seed database" }

Write-Host "Starting server for $RunSeconds s..."
$proc = Start-Process -FilePath $Bin -ArgumentList @("--db", $Db, "server") -PassThru
try {
    Start-Sleep -Seconds $RunSeconds
}
finally {
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
}
Start-Sleep -Milliseconds 800

$query = @"
SELECT poll_group_id,
       COUNT(*) AS cycles,
       SUM(ok) AS ok_cycles,
       CAST(AVG(actual_ms) AS INT) AS avg_ms,
       CAST(MAX(actual_ms) AS INT) AS max_ms
FROM poll_cycle
GROUP BY poll_group_id
ORDER BY poll_group_id;
"@
Write-Host "`n=== poll_cycle by group ==="
& $Bin --db $Db --local-db sql --format table $query

# Pull raw counts for the assertions. The default `sql` output is plain text
# (a header line then the value), and the binary's tracing logs interleave on
# the same stream, so just grab the lone all-digits line.
function Get-Scalar {
    param([string]$Sql)
    $out = & $Bin --db $Db --local-db sql $Sql
    $num = ($out | Where-Object { $_ -match '^\d+$' } | Select-Object -Last 1)
    return [int]$num
}

$fast = Get-Scalar "SELECT COUNT(*) FROM poll_cycle WHERE poll_group_id=1 AND ok=1"
$slow = Get-Scalar "SELECT COUNT(*) FROM poll_cycle WHERE poll_group_id=2 AND ok=1"
$fastMs = Get-Scalar "SELECT CAST(COALESCE(AVG(actual_ms),0) AS INT) FROM poll_cycle WHERE poll_group_id=1 AND ok=1"

Write-Host "`n=== fairness verdict ==="
Write-Host "fast(50ms) ok-cycles  = $fast"
Write-Host "slow(1500ms) ok-cycles = $slow"
Write-Host "fast avg actual_ms     = $fastMs"

$pass = $true
if ($slow -lt 1) { Write-Host "FAIL: slow group made no progress (starved the other way)"; $pass = $false }
if ($fast -lt 1) { Write-Host "FAIL: fast group made no progress"; $pass = $false }
if ($fast -lt ($slow * 8)) { Write-Host "FAIL: fast group not running freely (expected >= 8x slow; period ratio is 30x)"; $pass = $false }
if ($fastMs -gt 300) { Write-Host "FAIL: fast group actual_ms=$fastMs ms inflated (blocked behind slow sweep?)"; $pass = $false }

if ($pass) { Write-Host "`nPASS: fast group is not starved by the slow group." } else { throw "fairness check FAILED" }
