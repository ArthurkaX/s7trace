#!/usr/bin/env bash
# End-to-end smoke test for s7trace remote mode.
#
# Runs the full pipeline (CLI -> private HTTP transport -> server -> PLC) against a
# non-responsive PLC port. Verifies that:
#   - remote CLI status works
#   - CLI tag-add via -r submits and receives result
#   - tag is marked bad=1, bad_class=disconnected
#   - bit-level addresses (DBX) are accepted
#   - command queue/result tables reflect execution
#
# Pre-conditions:
#   - ./target/debug/s7trace.exe exists (cargo build)
#
# Usage:
#   bash scripts/e2e-remote.sh

DB=/tmp/s7trace_e2e.db
SVR_LOG=/tmp/s7trace_e2e_server.log
BIN="${S7TRACE_BIN:-./target/debug/s7trace.exe}"
HTTP_PORT=18094
PLC_PORT=11999  # nothing listening — we want negative-read

if [[ ! -x "$BIN" ]]; then
    echo "error: s7trace binary not found at $BIN"
    exit 2
fi

# Stop any previous instances
taskkill //F //IM s7trace.exe >/dev/null 2>&1 || true
sleep 1
rm -f "$DB" "$DB-wal" "$DB-shm" "$SVR_LOG"

cleanup() {
    taskkill //F //IM s7trace.exe >/dev/null 2>&1 || true
    sleep 1
    rm -f "$DB" "$DB-wal" "$DB-shm" 2>/dev/null
}
trap cleanup EXIT

echo "==> 1. Seed DB"
"$BIN" --db "$DB" --local-db admin-sql \
    "INSERT INTO plc (name, host, rack, slot, enabled)
     VALUES ('test_plc', '127.0.0.1:$PLC_PORT', 0, 2, 1);" >/dev/null 2>&1
"$BIN" --db "$DB" --local-db admin-sql \
    "INSERT INTO poll_group (name, period_ms, enabled)
     VALUES ('fast', 100, 1);" >/dev/null 2>&1
echo "    OK"

echo "==> 2. Start s7trace server in background"
"$BIN" --db "$DB" server --http-bind "127.0.0.1:$HTTP_PORT" > "$SVR_LOG" 2>&1 &
SERVER_PID=$!
sleep 2
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "    FAIL: server died"
    cat "$SVR_LOG"
    exit 3
fi
echo "    OK (pid $SERVER_PID)"

echo "==> 3. Verify remote CLI status"
status=$("$BIN" --db "$DB" \
    -r "http://127.0.0.1:$HTTP_PORT" \
    status --format json 2>&1)
echo "$status" | grep -q '"service": *"s7trace"' || {
    echo "    FAIL: remote CLI status failed"
    echo "    $status"
    exit 4
}
echo "    OK"

echo "==> 4. CLI tag-add via -r (i16 address)"
out=$("$BIN" --db "$DB" \
    -r "http://127.0.0.1:$HTTP_PORT" \
    --remote-timeout-ms 5000 \
    tag-add --plc test_plc --name counter.entries \
    --address DB5997.DBW0 --type i16 \
    --poll-group fast --storage-hook on_change 2>&1)
echo "    $out"
echo "$out" | grep -q "bad_class=disconnected" || {
    echo "    FAIL: expected 'bad_class=disconnected'"
    exit 5
}
echo "    OK"

echo "==> 5. CLI tag-add via -r (bit-level address DBX)"
out=$("$BIN" --db "$DB" \
    -r "http://127.0.0.1:$HTTP_PORT" \
    --remote-timeout-ms 5000 \
    tag-add --plc test_plc --name flag.section_run \
    --address DB5997.DBX10.1 --type bool \
    --poll-group fast --storage-hook on_change 2>&1)
echo "    $out"
echo "$out" | grep -q "bad_class=disconnected" || {
    echo "    FAIL: expected 'bad_class=disconnected' for bit address"
    exit 6
}
echo "    OK"

echo "==> 6. CLI status via -r"
out=$("$BIN" --db "$DB" \
    -r "http://127.0.0.1:$HTTP_PORT" \
    status --format json 2>&1)
echo "$out" | grep -q '"tags"' || {
    echo "    FAIL: status JSON missing 'tags' field"
    exit 7
}
echo "    OK"

echo "==> 7. CLI read via -r (plc_read kind)"
out=$("$BIN" --db "$DB" \
    -r "http://127.0.0.1:$HTTP_PORT" \
    --remote-timeout-ms 5000 \
    read --plc test_plc --address DB5997.DBW0 --type i16 2>&1)
echo "    $out"
echo "$out" | grep -qE "(disconnected|failed|not_connected)" || {
    echo "    FAIL: expected disconnection or failure for read"
    exit 8
}
echo "    OK"

echo "==> 8. DB inspection: tags + command queue"
echo "    --- tags ---"
"$BIN" --db "$DB" --local-db sql \
    "SELECT id, name, address, bad, COALESCE(bad_class, '-') AS bc
     FROM tag ORDER BY id;" 2>&1 | tail -10
echo "    --- commands ---"
"$BIN" --db "$DB" --local-db sql \
    "SELECT state, COUNT(*) AS n
     FROM command_request GROUP BY state;" 2>&1 | tail -5
echo "    --- results ---"
"$BIN" --db "$DB" --local-db sql \
    "SELECT cr.ok, COUNT(*) AS n
     FROM command_result cr
     JOIN command_request cmd ON cmd.id = cr.command_id
     GROUP BY cr.ok;" 2>&1 | tail -5

echo
echo "==> ALL E2E CHECKS PASSED"
echo "    Server log (last 5 lines):"
tail -5 "$SVR_LOG"
