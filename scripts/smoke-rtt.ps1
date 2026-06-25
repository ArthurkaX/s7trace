# Smoke-test for s7trace against a real Siemens S7-300 PLC.
#
# Run from the s7trace project root on the Windows host.
# Requires: s7trace.exe already built (cargo build).
#   .\scripts\smoke-rtt.ps1
#
# What it does:
#   1. Creates a fresh DB at $Env:TEMP\s7trace_smoke.db
#   2. Inserts a PLC row for 192.168.0.50:102 (rack 0 / slot 2)
#   3. Adds 5 representative tags from rtt_diagnostic.yaml:
#        - cfg.TRS1.Conv_Speed_mm_s  (DB5999.DBW22  i16)
#        - DB5997.Section_Run         (DB5997.DBX10.1 bool)
#        - DB5997.Entries_Used        (DB5997.DBW0   i16)
#        - TRS5.Statistical.Missing   (DB5999.DBW3486 i16)
#        - RTT_ISD_ID_1.LIC          (DB5999.DBD2196 i32)
#      Each `tag add` does a sync PLC read; expects bad=0
#   4. Shows the tag table with validation results
#   5. Starts the server, polls for 5 seconds, prints summary
#   6. Stops the server

$ErrorActionPreference = "Stop"

$DB = Join-Path $Env:TEMP "s7trace_smoke.db"
$Bin = $Env:S7TRACE_BIN
if (-not $Bin) {
    $Bin = Join-Path $PSScriptRoot "..\target\debug\s7trace.exe"
    $Bin = [System.IO.Path]::GetFullPath($Bin)
}
if (-not (Test-Path $Bin)) {
    Write-Error "s7trace binary not found at $Bin. Build first: cargo build"
    exit 2
}

$PLC_HOST = "192.168.0.50:102"
$PLC_NAME = "RTT1"
$RACK = 0
$SLOT = 2

# Cleanup
Remove-Item -Path "$DB","$DB-wal","$DB-shm" -ErrorAction SilentlyContinue

function Run($Args) {
    Write-Host ">> $Bin $Args" -ForegroundColor DarkGray
    & $Bin --db $DB --local-db @Args
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "command exited with $LASTEXITCODE"
    }
}

Write-Host "==> Step 1. Create PLC and poll groups" -ForegroundColor Cyan
Run @("admin-sql", "INSERT INTO plc (name, host, rack, slot, enabled) VALUES ('$PLC_NAME','$PLC_HOST',$RACK,$SLOT,1);")
Run @("admin-sql", "INSERT INTO poll_group (name, period_ms, enabled) VALUES ('fast',100,1),('medium',250,1),('slow',1000,1);")

Write-Host "==> Step 2. Add 5 tags (each does sync PLC read; expect bad=0)" -ForegroundColor Cyan
Run @("tag","add","--plc",$PLC_NAME,"--name","cfg.TRS1.Conv_Speed_mm_s",
     "--address","DB5999.DBW22","--type","i16","--poll-group","fast","--storage-hook","on_change")
Run @("tag","add","--plc",$PLC_NAME,"--name","DB5997.Section_Run",
     "--address","DB5997.DBX10.1","--type","bool","--poll-group","fast","--storage-hook","on_change")
Run @("tag","add","--plc",$PLC_NAME,"--name","DB5997.Entries_Used",
     "--address","DB5997.DBW0","--type","i16","--poll-group","fast","--storage-hook","on_change")
Run @("tag","add","--plc",$PLC_NAME,"--name","TRS5.Statistical.Missing",
     "--address","DB5999.DBW3486","--type","i16","--poll-group","fast","--storage-hook","on_change")
Run @("tag","add","--plc",$PLC_NAME,"--name","RTT_ISD_ID_1.LIC",
     "--address","DB5999.DBD2196","--type","i32","--poll-group","fast","--storage-hook","on_change")

Write-Host "==> Step 3. Tag list with validation" -ForegroundColor Cyan
Run @("sql","SELECT id, name, address, data_type, bad, COALESCE(bad_class,'') AS bc, last_value_i, last_value_f FROM tag ORDER BY id;")

Write-Host "==> Step 4. Start server, collect for 5 seconds" -ForegroundColor Cyan
$Proc = Start-Process -FilePath $Bin -ArgumentList @("--db",$DB,"server","--http-bind","127.0.0.1:18080") -PassThru
try {
    Start-Sleep -Seconds 5

    Write-Host "  poll_cycle summary:" -ForegroundColor Yellow
    Run @("sql","SELECT plc_id, COUNT(*) AS cycles, MIN(ts) AS first_ts, MAX(ts) AS last_ts, SUM(ok) AS ok_cycles FROM poll_cycle GROUP BY plc_id;")

    Write-Host "  sample summary (per tag):" -ForegroundColor Yellow
    Run @("sql","SELECT t.name, COUNT(s.id) AS samples, MIN(s.ts) AS first_ts, MAX(s.ts) AS last_ts FROM tag t LEFT JOIN sample s ON s.tag_id = t.id GROUP BY t.id ORDER BY t.id;")

    Write-Host "  read_span summary:" -ForegroundColor Yellow
    Run @("sql","SELECT plc_id, COUNT(*) AS spans, SUM(ok) AS ok_spans FROM read_span GROUP BY plc_id;")

    Write-Host "  PLC state:" -ForegroundColor Yellow
    Run @("sql","SELECT name, connectivity, last_ok_ts, COALESCE(last_error,'') AS err FROM plc;")
}
finally {
    Write-Host "==> Step 5. Stop server" -ForegroundColor Cyan
    Stop-Process -Id $Proc.Id -Force -ErrorAction SilentlyContinue
    Remove-Item -Path "$DB","$DB-wal","$DB-shm" -ErrorAction SilentlyContinue
}

Write-Host "Done." -ForegroundColor Green
