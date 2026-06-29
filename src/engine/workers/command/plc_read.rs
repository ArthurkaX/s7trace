//! PlcRead command handler.

use rusqlite::Connection;
use std::sync::Arc;

use crate::engine::server::PlcRegistry;
use crate::engine::snapshot::Snapshot;
use crate::types::*;

pub(super) fn execute_plc_read(
    conn: &Connection,
    plcs: &PlcRegistry,
    snapshot: &Arc<Snapshot>,
    cmd_id: i64,
    req: &serde_json::Value,
) -> Result<(bool, String), anyhow::Error> {
    let action = req["action"].as_str().unwrap_or("read");
    let plc_name = req["plc"].as_str().unwrap_or("");

    // Get plc_id from name
    let plc_id: i64 = conn
        .query_row(
            "SELECT id FROM plc WHERE name = ?1",
            rusqlite::params![plc_name],
            |row| row.get(0),
        )
        .map_err(|_| anyhow::anyhow!("PLC '{plc_name}' not found"))?;

    // Get client from registry
    let client_arc = {
        let reg = plcs.lock();
        reg.get(&plc_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("PLC {plc_id} client not registered"))?
    };
    let mut client = client_arc.lock();

    match action {
        "read_raw" => {
            let db: u16 = req["db"]
                .as_i64()
                .ok_or_else(|| anyhow::anyhow!("missing db"))? as u16;
            let start: usize = req["start"]
                .as_i64()
                .ok_or_else(|| anyhow::anyhow!("missing start"))?
                as usize;
            let len: usize = req["len"]
                .as_i64()
                .ok_or_else(|| anyhow::anyhow!("missing len"))?
                as usize;
            let format_str = req["format"].as_str().unwrap_or("hex");

            let data = client.read_db(db, start, len)?;
            let ts = now_us();

            // Format the output
            let json_result = match format_str {
                "hex" | "raw" => {
                    let hex_str: String = data.iter().map(|b| format!("{b:02x}")).collect();
                    serde_json::json!({
                        "command_id": cmd_id,
                        "ok": true,
                        "plc": plc_name,
                        "db": db,
                        "start": start,
                        "len": len,
                        "ts_us": ts,
                        "bytes_hex": hex_str,
                        "format": format_str,
                    })
                }
                "dec" => {
                    let dec_vals: Vec<String> = data.iter().map(|b| format!("{b}")).collect();
                    serde_json::json!({
                        "command_id": cmd_id,
                        "ok": true,
                        "plc": plc_name,
                        "db": db,
                        "start": start,
                        "len": len,
                        "ts_us": ts,
                        "bytes_raw": data,
                        "bytes_dec": dec_vals,
                        "format": format_str,
                    })
                }
                _ => {
                    serde_json::json!({
                        "command_id": cmd_id,
                        "ok": true,
                        "plc": plc_name,
                        "db": db,
                        "start": start,
                        "len": len,
                        "ts_us": ts,
                        "bytes_raw": data,
                        "format": "json",
                    })
                }
            };

            // Save raw probe if requested
            if req
                .get("save_probe")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                let ds_id: Option<i64> = conn
                    .query_row(
                        "SELECT id FROM dataset WHERE plc_id = ?1 AND state = 'active' LIMIT 1",
                        rusqlite::params![plc_id],
                        |row| row.get(0),
                    )
                    .ok();
                let now = ts;
                conn.execute(
                    "INSERT INTO raw_probe (dataset_id, plc_id, db_num, byte_offset, byte_len, ts, bytes)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    rusqlite::params![
                        ds_id, plc_id, db, start, len as i64, now, data,
                    ],
                )?;
            }

            Ok((true, json_result.to_string()))
        }
        _ => {
            // Default typed one-off read (action = "read" or unspecified)
            let address = req["address"].as_str().unwrap_or("");
            let data_type = req["data_type"].as_str().unwrap_or("");

            let dt: DataType = data_type
                .parse()
                .map_err(|e: String| anyhow::anyhow!("invalid data type: {e}"))?;
            let addr =
                S7Address::parse(address).map_err(|e| anyhow::anyhow!("invalid address: {e}"))?;

            let data = client.read_db(addr.db_num, addr.byte_offset, dt.byte_size())?;

            // Decode
            let (vi, vf) = crate::codec::decode_value(&data, dt, None);

            // Update snapshot (negative tag_id = ephemeral one-off read)
            snapshot.update(-cmd_id, plc_id, "", address, dt, vi, vf, now_us());

            let json_result = serde_json::json!({
                "command_id": cmd_id,
                "ok": true,
                "value_i": vi,
                "value_f": vf,
                "ts_us": now_us(),
            });

            Ok((true, json_result.to_string()))
        }
    }
}
