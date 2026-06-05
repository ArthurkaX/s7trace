#!/usr/bin/env bash
# Smoke-test for s7trace against a real Siemens S7-300 PLC.
#
# This script:
#   1. Creates a fresh SQLite DB.
#   2. Adds a PLC pointing at 192.168.0.50:102 (rack 0 / slot 2).
#   3. Adds 5 representative tags from rtt_diagnostic.yaml:
#        - cfg.TRS1.Conv_Speed_mm_s  (DB5999.DBW22  i16)
#        - DB5997.Section_Run         (DB5997.DBX10.1 bool)
#        - DB5997.Entries_Used        (DB5997.DBW0   i16)
#        - TRS5.Statistical.Missing   (DB5999.DBW3486 i16)
#        - RTT_ISD_ID_1.LIC          (DB5999.DBD2196 i32)
#      Each `tag add` performs a synchronous PLC read and prints
#      `bad=0` if the address is valid, `bad=1` otherwise.
#   4. Runs one `read` per tag (synchronous, command-queue style) and
#      prints the result.
#   5. Starts the server in the background, waits a few seconds,
#      prints poll_cycle / sample / read_span counts via SQL.
#   6. Stops the server.
#
# Usage (on the Windows host that has the s7trace.exe binary built):
#   bash scripts/smoke-rtt.sh
#
# The script is designed to be SAFE: it never deletes any data, it only
# writes to a single DB file that is removed at the end.

set -euo pipefail

DB=/tmp/s7trace_smoke.db
BIN="${S7TRACE_BIN:-./target/debug/s7trace.exe}"
PLC_HOST=192.168.0.50:102
PLC_NAME=RTT1
RACK=0
SLOT=2

# If the binary lives in a different relative path, allow override
if [[ ! -x "$BIN" ]]; then
    BIN=./target/release/s7trace.exe
fi
if [[ ! -x "$BIN" ]]; then
    echo "error: cannot find s7trace binary; set S7TRACE_BIN env var"
    exit 2
fi

cleanup() {
    rm -f "$DB" "$DB-wal" "$DB-shm"
}
trap cleanup EXIT

echo "==> Step 0. Cleanup"
rm -f "$DB" "$DB-wal" "$DB-shm"

echo "==> Step 1. Create PLC row"
"$BIN" --db "$DB" admin-sql \
  "INSERT INTO plc (name, host, rack, slot, enabled) VALUES
   ('$PLC_NAME', '$PLC_HOST', $RACK, $SLOT, 1);"
"$BIN" --db "$DB" admin-sql \
  "INSERT INTO poll_group (name, period_ms, enabled) VALUES
   ('fast', 100, 1),
   ('medium', 250, 1),
   ('slow', 1000, 1);"

echo "==> Step 2. Add 5 tags (each does a sync PLC read; expect bad=0)"
# group fast (100ms)
"$BIN" --db "$DB" tag-add --plc $PLC_NAME --name cfg.TRS1.Conv_Speed_mm_s \
    --address DB5999.DBW22 --type i16 --poll-group fast --storage-hook on_change || true
"$BIN" --db "$DB" tag-add --plc $PLC_NAME --name DB5997.Section_Run \
    --address DB5997.DBX10.1 --type bool --poll-group fast --storage-hook on_change || true
"$BIN" --db "$DB" tag-add --plc $PLC_NAME --name DB5997.Entries_Used \
    --address DB5997.DBW0 --type i16 --poll-group fast --storage-hook on_change || true
"$BIN" --db "$DB" tag-add --plc $PLC_NAME --name TRS5.Statistical.Missing \
    --address DB5999.DBW3486 --type i16 --poll-group fast --storage-hook on_change || true
"$BIN" --db "$DB" tag-add --plc $PLC_NAME --name RTT_ISD_ID_1.LIC \
    --address DB5999.DBD2196 --type i32 --poll-group fast --storage-hook on_change || true

echo "==> Step 3. Show tag list with bad/status"
"$BIN" --db "$DB" sql \
  "SELECT id, name, address, data_type, bad, COALESCE(bad_class,'') AS bc, last_value_i, last_value_f FROM tag ORDER BY id;"

echo "==> Step 4. One-shot `read` (requires server, but we'll skip if not running)"
echo "    (start the server in another terminal as:"
echo "       $BIN --db $DB server --http-bind 127.0.0.1:18080"
echo "     and re-run this step manually)"

echo "==> Step 5. Start server, collect for 5s, then stop"
"$BIN" --db "$DB" server --http-bind 127.0.0.1:18080 &
SERVER_PID=$!
sleep 5

echo "  poll_cycle summary:"
"$BIN" --db "$DB" sql \
  "SELECT plc_id, COUNT(*) AS cycles, MIN(ts) AS first_ts, MAX(ts) AS last_ts, SUM(ok) AS ok_cycles
   FROM poll_cycle GROUP BY plc_id;"

echo "  sample summary (per tag):"
"$BIN" --db "$DB" sql \
  "SELECT t.name, COUNT(s.id) AS samples, MIN(s.ts) AS first_ts, MAX(s.ts) AS last_ts
   FROM tag t LEFT JOIN sample s ON s.tag_id = t.id GROUP BY t.id ORDER BY t.id;"

echo "  read_span summary:"
"$BIN" --db "$DB" sql \
  "SELECT plc_id, COUNT(*) AS spans, SUM(ok) AS ok_spans FROM read_span GROUP BY plc_id;"

echo "  PLC state:"
"$BIN" --db "$DB" sql \
  "SELECT name, connectivity, last_ok_ts, COALESCE(last_error,'') AS err FROM plc;"

echo "==> Step 6. Stop server"
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true
echo "done."
