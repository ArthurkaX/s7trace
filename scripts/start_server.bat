@echo off
setlocal
set DB=s7trace.db
set BIN=%~dp0s7trace.exe
set LOG=server.log
set BIND=0.0.0.0:8080

echo [s7trace] Starting server...
echo   DB:   %DB%
echo   Bind: %BIND%
echo   Log:  %LOG%

rem === Multi-read batch size (S7-300/400 multivar tuning; see docs/spike-multiread-chunking.md) ===
rem   3 = up to 3 items per multi-read request (default). 1 = single reads (no multivar). 0 = no item cap (PDU budget only).
set BATCH_MAX=3
set /p BATCH_MAX=[s7trace] Multi-read batch max [S7TRACE_MULTI_BATCH_MAX] (Enter=%BATCH_MAX%):
set S7TRACE_MULTI_BATCH_MAX=%BATCH_MAX%
echo   Batch: %S7TRACE_MULTI_BATCH_MAX%

rem === First run: initialize DB if plc table is empty ===
set DB_STATE=
for /f "skip=1 tokens=1" %%A in ('"%BIN%" --db "%DB%" --local-db sql "SELECT CASE WHEN COUNT(*) = 0 THEN 'empty' ELSE 'ready' END AS state FROM plc" 2^>nul') do if not defined DB_STATE set DB_STATE=%%A
if /i "%DB_STATE%"=="empty" (
    echo [s7trace] First run - initialising database...
    "%BIN%" --db "%DB%" --local-db plc-add --name example-plc --host 192.168.0.50:102 --rack 0 --slot 2
    if %errorlevel% neq 0 exit /b %errorlevel%
    "%BIN%" --db "%DB%" --local-db poll-group-add --name fast --period-ms 40
    if %errorlevel% neq 0 exit /b %errorlevel%
    "%BIN%" --db "%DB%" --local-db tag-import --plc example-plc --file "%~dp0rtt_diagnostic.yaml" --set rtt_diag --poll-group fast
    if %errorlevel% neq 0 exit /b %errorlevel%
) else (
    echo [s7trace] Database already initialised.
)

rem === Validate configuration ===
"%BIN%" --db "%DB%" server --dry-run
if %errorlevel% neq 0 exit /b %errorlevel%

rem === Start server ===
echo [s7trace] Starting server daemon...
echo [s7trace] Logging to %LOG%
title s7trace server - Example-PLC
"%BIN%" --db "%DB%" server --http-bind %BIND% > "%LOG%" 2>&1
