//! Command-queue worker: claims, dispatches, and settles pending commands.

use log::{debug, error, info, warn};
use parking_lot::Mutex;
use rusqlite::Connection;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;

use crate::db;
use crate::engine::poller::OverrunMode;
use crate::engine::server::{PduCache, PlcRegistry, PollerRegistry, SampleBatch};
use crate::engine::snapshot::Snapshot;
use crate::types::*;

/// Execute a PLC read command.
fn execute_command(
    conn: &Connection,
    plcs: &PlcRegistry,
    pollers: &PollerRegistry,
    pdu_cache: &PduCache,
    snapshot: &Arc<Snapshot>,
    sample_tx: &Sender<SampleBatch>,
    cmd_id: i64,
    kind: CommandKind,
    request_json: &str,
    overrun_mode: OverrunMode,
) -> Result<(bool, String), anyhow::Error> {
    use CommandKind::*;

    let req: serde_json::Value = serde_json::from_str(request_json)?;

    // Unified core: actions migrated to `ops::execute` are handled here,
    // bypassing the legacy match below. The CLI sends a serialized `Op`.
    let migrated_action = req["action"].as_str().unwrap_or("");
    if crate::engine::ops::MIGRATED_ACTIONS.contains(&migrated_action) {
        let op: crate::engine::ops::Op = serde_json::from_str(request_json)?;
        let ctx = crate::engine::ops::ExecCtx::server(
            conn,
            plcs,
            pollers,
            pdu_cache,
            snapshot,
            sample_tx,
            overrun_mode,
        );
        let out = crate::engine::ops::execute(&ctx, op)?;
        return Ok((out.ok, out.message));
    }

    match kind {
        Admin => {
            let action = req["action"].as_str().unwrap_or("");
            match action {
                "delete_dataset" => {
                    let dataset = req["dataset"].as_str().unwrap_or("");
                    // Refuse if dataset is still active (retention.md D009)
                    let state: Option<String> = conn
                        .query_row(
                            "SELECT state FROM dataset WHERE name = ?1",
                            rusqlite::params![dataset],
                            |r| r.get(0),
                        )
                        .ok();
                    if let Some(s) = state {
                        if s == "active" {
                            return Ok((
                                false,
                                format!("dataset '{dataset}' is active; stop it first"),
                            ));
                        }
                    } else {
                        return Ok((false, format!("dataset '{dataset}' not found")));
                    }
                    // ON DELETE CASCADE handles dependent rows.
                    let n = conn.execute(
                        "DELETE FROM dataset WHERE name = ?1",
                        rusqlite::params![dataset],
                    )?;
                    Ok((true, format!("dataset '{dataset}' deleted rows={n}")))
                }
                "history_clean" => {
                    let plc_id = req["plc_id"].as_i64();
                    let cutoff = req["cutoff_ts"].as_i64();
                    let batch_size = req["batch_size"].as_u64().unwrap_or(10_000) as i64;

                    if plc_id.is_none() || cutoff.is_none() {
                        return Ok((false, "missing plc_id or cutoff_ts".into()));
                    }
                    let plc_id = plc_id.unwrap();
                    let cutoff = cutoff.unwrap();

                    // Refuse if any active dataset for this PLC (retention.md)
                    let active: i64 = conn
                        .query_row(
                            "SELECT COUNT(*) FROM dataset WHERE plc_id = ?1 AND state = 'active'",
                            rusqlite::params![plc_id],
                            |r| r.get(0),
                        )
                        .unwrap_or(0);
                    if active > 0 {
                        return Ok((
                            false,
                            format!("PLC has {active} active dataset(s); stop collection first"),
                        ));
                    }

                    // Refuse if any poller is still running for this PLC
                    {
                        let reg = pollers.lock();
                        let has_poller = reg.keys().any(|(pid, _)| *pid == plc_id);
                        if has_poller {
                            return Ok((
                                false,
                                format!("PLC {plc_id} has running pollers; stop collection first"),
                            ));
                        }
                    }

                    let mut total_deleted: i64 = 0;
                    // 1. sample
                    loop {
                        let n = conn.execute(
                            "DELETE FROM sample
                             WHERE rowid IN (
                               SELECT rowid FROM sample
                               WHERE ts < ?1 AND dataset_id IN (
                                 SELECT id FROM dataset WHERE plc_id = ?2
                               )
                               LIMIT ?3
                             )",
                            rusqlite::params![cutoff, plc_id, batch_size],
                        )?;
                        if n == 0 {
                            break;
                        }
                        total_deleted += n as i64;
                    }
                    // 2. sample_rollup
                    loop {
                        let n = conn.execute(
                            "DELETE FROM sample_rollup
                             WHERE rowid IN (
                               SELECT rowid FROM sample_rollup
                               WHERE window_end_ts < ?1 AND plc_id = ?2
                               LIMIT ?3
                             )",
                            rusqlite::params![cutoff, plc_id, batch_size],
                        )?;
                        if n == 0 {
                            break;
                        }
                        total_deleted += n as i64;
                    }
                    // 3. event_marker
                    loop {
                        let n = conn.execute(
                            "DELETE FROM event_marker
                             WHERE rowid IN (
                               SELECT rowid FROM event_marker
                               WHERE ts < ?1 AND plc_id = ?2
                               LIMIT ?3
                             )",
                            rusqlite::params![cutoff, plc_id, batch_size],
                        )?;
                        if n == 0 {
                            break;
                        }
                        total_deleted += n as i64;
                    }
                    // 4. plc_time_sample
                    loop {
                        let n = conn.execute(
                            "DELETE FROM plc_time_sample
                             WHERE rowid IN (
                               SELECT rowid FROM plc_time_sample
                               WHERE host_before_ts < ?1 AND plc_id = ?2
                               LIMIT ?3
                             )",
                            rusqlite::params![cutoff, plc_id, batch_size],
                        )?;
                        if n == 0 {
                            break;
                        }
                        total_deleted += n as i64;
                    }
                    // 5. raw_probe
                    loop {
                        let n = conn.execute(
                            "DELETE FROM raw_probe
                             WHERE rowid IN (
                               SELECT rowid FROM raw_probe
                               WHERE ts < ?1 AND plc_id = ?2
                               LIMIT ?3
                             )",
                            rusqlite::params![cutoff, plc_id, batch_size],
                        )?;
                        if n == 0 {
                            break;
                        }
                        total_deleted += n as i64;
                    }
                    // 6. orphaned poll_cycle (no remaining samples/events/time referencing them)
                    loop {
                        let n = conn.execute(
                            "DELETE FROM poll_cycle
                             WHERE rowid IN (
                               SELECT rowid FROM poll_cycle
                               WHERE finished_ts < ?1 AND plc_id = ?2
                                 AND NOT EXISTS (SELECT 1 FROM sample s WHERE s.cycle_id = poll_cycle.id)
                                 AND NOT EXISTS (SELECT 1 FROM event_marker em WHERE em.cycle_id = poll_cycle.id)
                                 AND NOT EXISTS (SELECT 1 FROM plc_time_sample pts WHERE pts.cycle_id = poll_cycle.id)
                               LIMIT ?3
                             )",
                            rusqlite::params![cutoff, plc_id, batch_size],
                        )?;
                        if n == 0 {
                            break;
                        }
                        total_deleted += n as i64;
                    }
                    // 7. inactive read_span no longer referenced by sample
                    loop {
                        let n = conn.execute(
                            "DELETE FROM read_span
                             WHERE rowid IN (
                               SELECT rowid FROM read_span
                               WHERE plc_id = ?1
                                 AND NOT EXISTS (SELECT 1 FROM sample s WHERE s.span_id = read_span.id)
                               LIMIT ?2
                             )",
                            rusqlite::params![plc_id, batch_size],
                        )?;
                        if n == 0 {
                            break;
                        }
                        total_deleted += n as i64;
                    }

                    Ok((
                        true,
                        format!(
                            "history_clean plc={plc_id} cutoff={cutoff} deleted={total_deleted}"
                        ),
                    ))
                }
                other => Ok((false, format!("unknown admin action '{other}'"))),
            }
        }
        PlcRead => {
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
                        .ok_or_else(|| anyhow::anyhow!("missing db"))?
                        as u16;
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
                            let dec_vals: Vec<String> =
                                data.iter().map(|b| format!("{b}")).collect();
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
                        let ds_id: Option<i64> = conn.query_row(
                            "SELECT id FROM dataset WHERE plc_id = ?1 AND state = 'active' LIMIT 1",
                            rusqlite::params![plc_id], |row| row.get(0),
                        ).ok();
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
                    let addr = S7Address::parse(address)
                        .map_err(|e| anyhow::anyhow!("invalid address: {e}"))?;

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
        Control => {
            let action = req["action"].as_str().unwrap_or("");
            match action {
                "tag_add" => {
                    let plc_id = req["plc_id"]
                        .as_i64()
                        .ok_or_else(|| anyhow::anyhow!("missing plc_id"))?;
                    let tag_id = req["tag_id"]
                        .as_i64()
                        .ok_or_else(|| anyhow::anyhow!("missing tag_id"))?;

                    // Read tag metadata (only name+address+dt needed for snapshot)
                    let (name, address, data_type_str): (String, String, String) = conn.query_row(
                        "SELECT name, address, data_type FROM tag WHERE id = ?1",
                        rusqlite::params![tag_id],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        },
                    )?;
                    let dt: DataType = data_type_str.parse().unwrap_or(DataType::I16);

                    let client_arc = {
                        let reg = plcs.lock();
                        reg.get(&plc_id).cloned().ok_or_else(|| {
                            anyhow::anyhow!("PLC client not available for {plc_id}")
                        })?
                    };

                    let result = db::validation::validate_tag(conn, &client_arc, tag_id)?;

                    if result.ok {
                        snapshot.update(
                            tag_id,
                            plc_id,
                            &name,
                            &address,
                            dt,
                            result.value_i,
                            result.value_f,
                            result.ts_us,
                        );
                    }

                    // Schedule config apply
                    let _ = db::config_apply::schedule(
                        conn,
                        ApplyScope::Tag,
                        Some(plc_id),
                        None,
                        None,
                        Some(tag_id),
                        &format!("tag_add id={tag_id}"),
                        db::config_apply::PLAN_COOLDOWN_MS,
                        None,
                    );

                    if result.ok {
                        let val_str = if let Some(v) = result.value_i {
                            if let Some(f) = result.value_f {
                                format!("value={v} float={f}")
                            } else {
                                format!("value={v}")
                            }
                        } else if let Some(f) = result.value_f {
                            format!("value={f}")
                        } else {
                            "value=null".into()
                        };
                        Ok((
                            true,
                            format!("{val_str} apply_state=pending tag_id={tag_id}"),
                        ))
                    } else {
                        let bc = result
                            .bad_class
                            .as_ref()
                            .map(|b| b.to_string())
                            .unwrap_or_default();
                        let det = result.detail.as_deref().unwrap_or("");
                        Ok((
                            false,
                            format!("bad bad_class={bc} detail=\"{det}\" tag_id={tag_id}"),
                        ))
                    }
                }

                "collection_start" => {
                    let plc_name = req["plc"].as_str().unwrap_or("?");
                    // Local CLI supplies plc_id; remote CLI cannot (its replica
                    // DB is empty), so resolve from the PLC name server-side.
                    let plc_id = match req["plc_id"].as_i64() {
                        Some(id) => id,
                        None => conn
                            .query_row(
                                "SELECT id FROM plc WHERE name = ?1",
                                rusqlite::params![plc_name],
                                |row| row.get(0),
                            )
                            .map_err(|_| anyhow::anyhow!("PLC '{plc_name}' not found"))?,
                    };
                    let dataset = req["dataset"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing dataset"))?;

                    // Check no active dataset
                    let active: i64 = conn.query_row(
                        "SELECT COUNT(*) FROM dataset WHERE plc_id = ?1 AND state = 'active'",
                        rusqlite::params![plc_id],
                        |row| row.get(0),
                    )?;
                    if active > 0 {
                        return Ok((
                            false,
                            format!("PLC {plc_name} already has an active dataset"),
                        ));
                    }

                    // Create dataset
                    let now = now_us();
                    let n = conn.execute(
                        "INSERT INTO dataset (plc_id, name, state, created_ts, updated_ts)
                         VALUES (?1, ?2, 'active', ?3, ?3)",
                        rusqlite::params![plc_id, dataset, now],
                    )?;
                    info!(
                        "collection_start: inserted dataset id=? plc_id={} name='{}' rows_affected={}",
                        plc_id, dataset, n
                    );
                    // Get dataset id
                    let dataset_id = conn.last_insert_rowid();

                    // Parse requested metric set names
                    let set_names: Vec<String> = req["sets"]
                        .as_array()
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect()
                        })
                        .unwrap_or_default();

                    if !set_names.is_empty() {
                        info!(
                            "collection_start {}: binding {} metric set(s)",
                            dataset,
                            set_names.len()
                        );
                        for set_name in &set_names {
                            // Find or create the metric set for this PLC
                            let ms_id: i64 = match conn.query_row(
                                "SELECT id FROM metric_set WHERE plc_id = ?1 AND name = ?2",
                                rusqlite::params![plc_id, set_name],
                                |row| row.get(0),
                            ) {
                                Ok(id) => {
                                    // Ensure it's enabled
                                    conn.execute(
                                        "UPDATE metric_set SET enabled = 1, updated_ts = ?1 WHERE id = ?2",
                                        rusqlite::params![now, id],
                                    )?;
                                    id
                                }
                                Err(_) => {
                                    // Auto-create the set
                                    conn.execute(
                                        "INSERT INTO metric_set (plc_id, name, enabled, created_ts, updated_ts)
                                         VALUES (?1, ?2, 1, ?3, ?3)",
                                        rusqlite::params![plc_id, set_name, now],
                                    )?;
                                    conn.last_insert_rowid()
                                }
                            };
                            // Bind the set to the dataset
                            conn.execute(
                                "INSERT OR IGNORE INTO dataset_metric_set (dataset_id, metric_set_id, plc_id, created_ts)
                                 VALUES (?1, ?2, ?3, ?4)",
                                rusqlite::params![dataset_id, ms_id, plc_id, now],
                            )?;
                        }
                    }

                    // Build set_id filter for the poll group query
                    let set_filter_clause: String = if set_names.is_empty() {
                        // No sets specified: use all enabled metric sets for this PLC
                        "ms.enabled = 1".to_string()
                    } else {
                        // Use only the requested sets (which we just enabled above)
                        let placeholders: Vec<String> = set_names
                            .iter()
                            .enumerate()
                            .map(|(i, _)| format!("?{}", i + 2)) // ?2, ?3, ...
                            .collect();
                        format!("ms.name IN ({})", placeholders.join(","))
                    };

                    // Find poll groups for this PLC from the selected metric sets
                    let pg_query = format!(
                        "SELECT DISTINCT pg.id, pg.name, pg.period_ms
                         FROM poll_group pg
                         JOIN tag t ON t.poll_group_id = pg.id
                         JOIN metric_set_tag mst ON mst.tag_id = t.id
                         JOIN metric_set ms ON ms.id = mst.metric_set_id
                         WHERE t.plc_id = ?1
                           AND t.enabled = 1
                           AND t.bad = 0
                           AND {set_filter_clause}
                         ORDER BY pg.id"
                    );
                    let pg_rows: Vec<(i64, String, u64)> = {
                        let mut stmt = conn.prepare(&pg_query)?;
                        let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(plc_id)];
                        for name in &set_names {
                            params.push(Box::new(name.clone()));
                        }
                        let param_refs: Vec<&dyn rusqlite::ToSql> =
                            params.iter().map(|b| b.as_ref()).collect();
                        let rows = stmt.query_map(
                            rusqlite::params_from_iter(param_refs.iter().copied()),
                            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                        )?;
                        rows.filter_map(|r| r.ok()).collect()
                    };

                    info!(
                        "found {} active poll groups for PLC {} (from {} metric set(s))",
                        pg_rows.len(),
                        plc_name,
                        if set_names.is_empty() {
                            "all enabled".to_string()
                        } else {
                            set_names.len().to_string()
                        },
                    );

                    if pg_rows.is_empty() {
                        warn!(
                            "collection_start {dataset}: no active poll groups for PLC {plc_name}"
                        );
                    }

                    for (pg_id, _pg_name, period_ms) in &pg_rows {
                        crate::engine::workers::spawn::spawn_one_poller(
                            plcs,
                            pollers,
                            pdu_cache,
                            sample_tx,
                            conn,
                            plc_id,
                            plc_name,
                            *pg_id,
                            *period_ms,
                            overrun_mode,
                        )?;
                    }

                    Ok((
                        true,
                        format!("collection started dataset={dataset} plc={plc_name}"),
                    ))
                }

                "collection_stop" => {
                    let plc_name = req["plc"].as_str().unwrap_or("?");
                    // Resolve from name when plc_id is absent (remote CLI).
                    let plc_id = match req["plc_id"].as_i64() {
                        Some(id) => id,
                        None => conn
                            .query_row(
                                "SELECT id FROM plc WHERE name = ?1",
                                rusqlite::params![plc_name],
                                |row| row.get(0),
                            )
                            .map_err(|_| anyhow::anyhow!("PLC '{plc_name}' not found"))?,
                    };

                    // Stop all pollers for this PLC
                    {
                        let mut reg = pollers.lock();
                        let keys: Vec<(i64, i64)> = reg
                            .keys()
                            .filter(|(pid, _)| *pid == plc_id)
                            .copied()
                            .collect();
                        for key in &keys {
                            if let Some(handle) = reg.remove(key) {
                                handle.stop();
                                info!("poller stopped for PLC {} group {}", plc_name, key.1);
                            }
                        }
                    }

                    // Mark dataset as stopped
                    conn.execute(
                        "UPDATE dataset SET state = 'stopped' WHERE plc_id = ?1 AND state = 'active'",
                        rusqlite::params![plc_id],
                    )?;

                    Ok((true, format!("collection stopped for PLC {plc_name}")))
                }

                "poll_group_update_period" => {
                    let pg_id = if let Some(id) = req["poll_group_id"].as_i64() {
                        id
                    } else if let Some(name) = req["poll_group"].as_str() {
                        conn.query_row(
                            "SELECT id FROM poll_group WHERE name = ?1",
                            rusqlite::params![name],
                            |row| row.get::<_, i64>(0),
                        )
                        .map_err(|_| anyhow::anyhow!("poll group '{name}' not found"))?
                    } else {
                        return Err(anyhow::anyhow!("missing poll_group_id or poll_group"));
                    };

                    let period_ms = req["period_ms"]
                        .as_i64()
                        .ok_or_else(|| anyhow::anyhow!("missing period_ms"))?;

                    if period_ms < 50 {
                        return Ok((false, "period_ms must be at least 50".into()));
                    }

                    let pg_name: String = conn
                        .query_row(
                            "SELECT name FROM poll_group WHERE id = ?1",
                            rusqlite::params![pg_id],
                            |row| row.get(0),
                        )
                        .unwrap_or_else(|_| format!("id={}", pg_id));

                    // Look up a plc_id that uses this poll group
                    let plc_id: Option<i64> = conn
                        .query_row(
                            "SELECT DISTINCT t.plc_id FROM tag t WHERE t.poll_group_id = ?1 LIMIT 1",
                            rusqlite::params![pg_id],
                            |row| row.get::<_, i64>(0),
                        )
                        .ok();

                    conn.execute(
                        "UPDATE poll_group SET period_ms = ?1 WHERE id = ?2",
                        rusqlite::params![period_ms, pg_id],
                    )?;

                    let reason = format!("poll_group_update_period id={pg_id}");
                    let _ = db::config_apply::schedule(
                        conn,
                        ApplyScope::PollGroup,
                        plc_id,
                        Some(pg_id),
                        None,
                        None,
                        &reason,
                        db::config_apply::PLAN_COOLDOWN_MS,
                        None,
                    );

                    Ok((
                        true,
                        format!("success poll_group={pg_name} period_ms={period_ms}"),
                    ))
                }

                "tag_import" => {
                    let plc_name = req["plc"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing plc"))?;
                    let yaml_content = req["yaml"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing yaml"))?;
                    let poll_group = req["poll_group"].as_str().unwrap_or("slow");
                    let store_mode = req["store_mode"].as_str().unwrap_or("on_change");
                    let storage_hook = req["storage_hook"].as_str().unwrap_or("on_change");
                    let value_role = req["value_role"].as_str().unwrap_or("state");
                    let do_validate = req["validate"].as_bool().unwrap_or(false);
                    let set_arg = req["set"].as_str();

                    // Validate store_mode and storage_hook
                    let _sm: StoreMode = store_mode
                        .parse()
                        .map_err(|e: String| anyhow::anyhow!("invalid store mode: {e}"))?;
                    let _sh: StorageHook = storage_hook
                        .parse()
                        .map_err(|e: String| anyhow::anyhow!("invalid storage hook: {e}"))?;

                    // Resolve PLC
                    let plc_id: i64 = conn
                        .query_row(
                            "SELECT id FROM plc WHERE name = ?1",
                            rusqlite::params![plc_name],
                            |row| row.get(0),
                        )
                        .map_err(|_| anyhow::anyhow!("PLC '{plc_name}' not found"))?;

                    // Parse signals
                    let signals = crate::cli::import::parse_signals(yaml_content)?;

                    let mut imported: u32 = 0;
                    let mut imported_tags: Vec<(i64, String)> = Vec::new();
                    let mut failures: Vec<(String, String)> = Vec::new();

                    for sig in &signals {
                        let dt = if let Some(ref type_str) = sig.data_type {
                            match type_str.parse::<DataType>() {
                                Ok(dt) => dt,
                                Err(e) => {
                                    failures.push((
                                        sig.name.clone(),
                                        format!("invalid type '{type_str}': {e}"),
                                    ));
                                    continue;
                                }
                            }
                        } else {
                            match crate::cli::import::infer_type(&sig.address) {
                                Ok(dt) => dt,
                                Err(e) => {
                                    failures.push((sig.name.clone(), e.to_string()));
                                    continue;
                                }
                            }
                        };

                        let addr = match S7Address::parse(&sig.address) {
                            Ok(a) => a,
                            Err(e) => {
                                failures.push((sig.name.clone(), format!("invalid address: {e}")));
                                continue;
                            }
                        };

                        match db::validation::insert_tag(
                            conn,
                            plc_name,
                            &sig.name,
                            &addr,
                            dt,
                            poll_group,
                            store_mode,
                            storage_hook,
                            value_role,
                            set_arg,
                        ) {
                            Ok(inserted) => {
                                imported += 1;
                                imported_tags.push((inserted.tag_id, sig.name.clone()));
                            }
                            Err(e) => {
                                failures.push((sig.name.clone(), e.to_string()));
                            }
                        }
                    }

                    // Schedule one PLC-scoped config apply if anything was imported
                    if imported > 0 {
                        let _ = db::config_apply::schedule(
                            conn,
                            ApplyScope::Plc,
                            Some(plc_id),
                            None,
                            None,
                            None,
                            "tag_import",
                            db::config_apply::PLAN_COOLDOWN_MS,
                            None,
                        );
                    }

                    // Optionally validate each imported tag synchronously
                    if do_validate && !imported_tags.is_empty() {
                        let client_opt = plcs.lock().get(&plc_id).cloned();
                        if let Some(client_arc) = client_opt {
                            for (tag_id, tag_name) in &imported_tags {
                                match db::validation::validate_tag(conn, &client_arc, *tag_id) {
                                    Ok(result) => {
                                        if result.ok {
                                            info!(
                                                "tag_import validate ok {tag_name} tag_id={tag_id}"
                                            );
                                        } else {
                                            let bc = result
                                                .bad_class
                                                .as_ref()
                                                .map(|b| b.to_string())
                                                .unwrap_or_default();
                                            warn!(
                                                "tag_import validate bad {tag_name} tag_id={tag_id} bad_class={bc}"
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "tag_import validate error {tag_name} tag_id={tag_id}: {e}"
                                        );
                                    }
                                }
                            }
                        } else {
                            warn!(
                                "tag_import: PLC client not available for {plc_name}, skipping validate"
                            );
                        }
                    }

                    let set_name = set_arg.unwrap_or("default");
                    Ok((
                        true,
                        format!(
                            "imported={imported} failed={} set={set_name}",
                            failures.len()
                        ),
                    ))
                }

                other => Ok((false, format!("unknown control action '{other}'"))),
            }
        }

        Query => Ok((
            false,
            "server cannot execute SQL queries, use CLI directly".into(),
        )),
    }
}

/// Runs in a dedicated thread: claims pending commands, dispatches to
/// `execute_command`, writes results.
pub(crate) fn command_worker(
    db: Arc<Mutex<Connection>>,
    running: Arc<AtomicBool>,
    snapshot: Arc<Snapshot>,
    plcs: PlcRegistry,
    pollers: PollerRegistry,
    pdu_cache: PduCache,
    sample_tx: Sender<SampleBatch>,
    overrun_mode: OverrunMode,
) {
    info!("cmd-worker started");

    while running.load(Ordering::Relaxed) {
        let claimed = {
            let conn = db.lock();
            db::command_queue::claim_next(&conn)
        };

        let cmd = match claimed {
            Ok(Some(cmd)) => cmd,
            Ok(None) => {
                std::thread::sleep(Duration::from_millis(200));
                continue;
            }
            Err(e) => {
                error!("cmd-worker: claim error: {e}");
                std::thread::sleep(Duration::from_millis(1000));
                continue;
            }
        };

        debug!(
            "cmd-worker: claimed command id={} kind={:?}",
            cmd.id, cmd.kind
        );

        // Execute under catch_unwind so a panic in one command (decode bug,
        // backend assertion, etc.) is converted to a failed command instead of
        // silently killing the worker thread and freezing the queue. (Bug B-7)
        // parking_lot mutexes don't poison, so the db lock stays usable after
        // an unwind; AssertUnwindSafe is sound here.
        let result = {
            let conn = db.lock();
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                execute_command(
                    &conn,
                    &plcs,
                    &pollers,
                    &pdu_cache,
                    &snapshot,
                    &sample_tx,
                    cmd.id,
                    cmd.kind,
                    &cmd.request_json,
                    overrun_mode,
                )
            }))
        };

        match result {
            Ok(Ok((ok, msg))) => {
                if ok {
                    let conn = db.lock();
                    if let Err(e) = db::command_queue::finish_ok(&conn, cmd.id, &msg) {
                        error!("cmd-worker: finish_ok id={} failed: {e}", cmd.id);
                    }
                } else {
                    let conn = db.lock();
                    if let Err(e) = db::command_queue::finish_bad(&conn, cmd.id, &msg) {
                        error!("cmd-worker: finish_bad id={} failed: {e}", cmd.id);
                    }
                }
            }
            Ok(Err(e)) => {
                error!("cmd-worker: command id={} execution error: {e}", cmd.id);
                let conn = db.lock();
                if let Err(e2) = db::command_queue::finish_bad(&conn, cmd.id, &e.to_string()) {
                    error!("cmd-worker: finish_bad id={} failed: {e2}", cmd.id);
                }
            }
            Err(panic) => {
                let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "<non-string panic payload>".to_string()
                };
                error!("cmd-worker: command id={} PANICKED: {msg}", cmd.id);
                let conn = db.lock();
                if let Err(e2) =
                    db::command_queue::finish_bad(&conn, cmd.id, &format!("panic: {msg}"))
                {
                    error!("cmd-worker: finish_bad id={} failed: {e2}", cmd.id);
                }
            }
        }
    }

    info!("cmd-worker stopped");
}
