@echo off
setlocal
set DB=test_pipeline.db
set BIN=target\release\s7trace.exe

del %DB% 2>nul

echo === 1. Init PLC2 ===
%BIN% --db %DB% --local-db plc-add --name PLC2 --host 192.168.0.51 --rack 0 --slot 2
if %errorlevel% neq 0 exit /b %errorlevel%
%BIN% --db %DB% --local-db poll-group-add --name fast --period-ms 40
if %errorlevel% neq 0 exit /b %errorlevel%
%BIN% --db %DB% --local-db set-create --plc PLC2 --name test
if %errorlevel% neq 0 exit /b %errorlevel%
%BIN% --db %DB% --local-db set-enable test
if %errorlevel% neq 0 exit /b %errorlevel%
echo OK

echo.
echo === 2. Dry-run ===
%BIN% --db %DB% server --dry-run

echo.
echo === 3. Tag add (enqueue) ===
%BIN% --db %DB% --local-db tag-add --plc PLC2 --name s_TRS5_FB_TRK_Track_Samo.o_ST_PEC_Miss --address DB1291.DBX3400.0 --type bool --poll-group fast --store-mode on_change --storage-hook on_change --value-role raw --set test --async

echo.
echo === 4. Enqueue PLC read ===
%BIN% --db %DB% --local-db read --plc PLC2 --address DB1291.DBX3400.0 --type bool

echo.
echo === 5. Start server (background) ===
start /MIN /B %BIN% --db %DB% server
rem Wait for server to initialize
C:\Windows\System32\timeout.exe /t 3 /nobreak >nul
C:\Windows\System32\tasklist.exe /fi "imagename eq s7trace.exe" 2>nul | C:\Windows\System32\find.exe "s7trace" >nul && echo Server running || echo Server NOT running

echo.
echo === 6. Command list (server processed) ===
%BIN% --db %DB% --local-db command-list --state done

echo.
echo === 7. Status ===
%BIN% --db %DB% --local-db status --format json

echo.
echo === 8. Collection start ===
%BIN% --db %DB% --local-db collection-start --plc PLC2 --dataset test_run_001 --set test

echo.
echo === 9. Status after collection start ===
%BIN% --db %DB% --local-db status --format json

echo.
echo === 10. Collection stop ===
%BIN% --db %DB% --local-db collection-stop --plc PLC2

echo.
echo === DONE ===
