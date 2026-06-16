/// s7trace server/daemon mode.
///
/// Orchestrates poller threads, command queue worker, config apply worker,
/// and sample writer. Runs until shutdown is requested.
///
/// ## Pipeline
///
/// ```text
/// CLI --enqueue--> command_request --claim--> cmd-worker --exec--> PLC / DB / Snapshot
///                                          +---> command_result
/// Poller --send--> sample-writer --batch--> SQLite
/// ```
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;

use crate::api;
use crate::db;
use crate::engine::plan::build_plan;
use crate::engine::poller::{spawn_poller, OverrunMode, PollResult, PollerHandle};
use crate::engine::snapshot::Snapshot;
use crate::plc::client::PlcClient;
use crate::plc::create_plc_client;
use crate::types::*;

/// Batch of samples from one poll cycle, sent through the channel.
pub type SampleBatch = PollResult;

/// Registry of PLC clients, keyed by plc_id.
type PlcRegistry = Arc<Mutex<HashMap<i64, Arc<Mutex<dyn PlcClient + Send>>>>>;

/// Registry of running poller handles, keyed by (plc_id, poll_group_id).
type PollerRegistry = Arc<Mutex<HashMap<(i64, i64), PollerHandle>>>;

/// Per-PLC PDU length cache. Populated on first successful connect.
type PduCache = Arc<Mutex<HashMap<i64, usize>>>;

// ---------------------------------------------------------------------------
// Config apply worker
// ---------------------------------------------------------------------------

fn apply_worker(
    db: Arc<Mutex<Connection>>,
    running: Arc<AtomicBool>,
    plcs: PlcRegistry,
    pollers: PollerRegistry,
    pdu_cache: PduCache,
    sample_tx: Sender<SampleBatch>,
    overrun_mode: OverrunMode,
) {
    info!("apply-worker started");

    while running.load(Ordering::Relaxed) {
        let due = {
            let conn = db.lock();
            match db::config_apply::claim_due(&conn) {
                Ok(Some(row)) => Some(row),
                Ok(None) => None,
                Err(e) => {
                    error!("apply-worker: claim_due error: {e}");
                    None
                }
            }
        };

        if let Some(row) = due {
            info!(
                "apply-worker: executing apply id={} reason={:?}",
                row.id, row.reason
            );
            let conn = db.lock();
            use crate::types::ApplyScope;
            let mut affected: Vec<(i64, i64)> = Vec::new(); // (plc_id, pg_id)
            match row.scope {
                ApplyScope::Tag => {
                    if let Some(tag_id) = row.tag_id {
                        let _ = conn.execute(
                            "UPDATE tag SET applied_version = desired_version, apply_state = 'applied'
                             WHERE id = ?1",
                            rusqlite::params![tag_id],
                        );
                        // Determine affected (plc, pg) for a plan rebuild
                        let plc_pg: Option<(i64, i64)> = conn
                            .query_row(
                                "SELECT plc_id, COALESCE(poll_group_id, 0) FROM tag WHERE id = ?1",
                                rusqlite::params![tag_id],
                                |r| Ok((r.get(0)?, r.get(1)?)),
                            )
                            .ok();
                        if let Some(pp) = plc_pg {
                            affected.push(pp);
                        }
                    }
                }
                ApplyScope::Plc => {
                    if let Some(plc_id) = row.plc_id {
                        let _ = conn.execute(
                            "UPDATE tag SET applied_version = desired_version, apply_state = 'applied'
                             WHERE plc_id = ?1",
                            rusqlite::params![plc_id],
                        );
                        // Rebuild all poll groups for this PLC
                        let pgs: Vec<i64> = conn
                            .prepare(
                                "SELECT DISTINCT COALESCE(poll_group_id, 0)
                             FROM tag WHERE plc_id = ?1 AND enabled = 1 AND bad = 0",
                            )
                            .ok()
                            .and_then(|mut s| {
                                s.query_map(rusqlite::params![plc_id], |r| r.get::<_, i64>(0))
                                    .ok()
                                    .map(|it| it.filter_map(|x| x.ok()).collect())
                            })
                            .unwrap_or_default();
                        for pg in pgs {
                            affected.push((plc_id, pg));
                        }
                    }
                }
                ApplyScope::PollGroup => {
                    if let (Some(plc_id), Some(pg_id)) = (row.plc_id, row.poll_group_id) {
                        let _ = conn.execute(
                            "UPDATE tag SET applied_version = desired_version, apply_state = 'applied'
                             WHERE plc_id = ?1 AND poll_group_id = ?2",
                            rusqlite::params![plc_id, pg_id],
                        );
                        affected.push((plc_id, pg_id));
                    }
                }
                ApplyScope::MetricSet => {
                    if let Some(plc_id) = row.plc_id {
                        let _ = conn.execute(
                            "UPDATE tag SET apply_state = 'applied'
                             WHERE plc_id = ?1
                               AND id IN (
                                 SELECT mst.tag_id FROM metric_set_tag mst
                                 WHERE mst.metric_set_id = ?2
                               )",
                            rusqlite::params![plc_id, row.metric_set_id],
                        );
                        // Rebuild all poll groups for this PLC that are
                        // affected by this metric set.
                        let pgs: Vec<i64> = conn
                            .prepare(
                                "SELECT DISTINCT COALESCE(t.poll_group_id, 0)
                             FROM tag t
                             JOIN metric_set_tag mst ON mst.tag_id = t.id
                             WHERE mst.metric_set_id = ?1 AND t.plc_id = ?2
                               AND t.enabled = 1 AND t.bad = 0",
                            )
                            .ok()
                            .and_then(|mut s| {
                                s.query_map(rusqlite::params![row.metric_set_id, plc_id], |r| {
                                    r.get::<_, i64>(0)
                                })
                                .ok()
                                .map(|it| it.filter_map(|x| x.ok()).collect())
                            })
                            .unwrap_or_default();
                        for pg in pgs {
                            affected.push((plc_id, pg));
                        }
                    }
                }
                _ => {}
            }

            // Rebuild running pollers for affected (plc, pg) pairs.
            // Only do this if there is an active dataset (collection running).
            for (plc_id, pg_id) in &affected {
                let has_active_dataset: bool = conn
                    .query_row(
                        "SELECT COUNT(*) FROM dataset WHERE plc_id = ?1 AND state = 'active'",
                        rusqlite::params![plc_id],
                        |r| r.get::<_, i64>(0),
                    )
                    .unwrap_or(0)
                    > 0;
                if !has_active_dataset {
                    continue;
                }

                // Stop the existing poller if running
                {
                    let mut reg = pollers.lock();
                    if let Some(handle) = reg.remove(&(*plc_id, *pg_id)) {
                        handle.stop();
                        info!(
                            "apply-worker: stopped poller for PLC {} group {}",
                            plc_id, pg_id
                        );
                    }
                }

                // Look up plc name
                let plc_name: String = match conn.query_row(
                    "SELECT name FROM plc WHERE id = ?1",
                    rusqlite::params![plc_id],
                    |r| r.get(0),
                ) {
                    Ok(n) => n,
                    Err(_) => continue,
                };

                // Spawn a fresh poller with the updated plan
                let period_ms: u64 = conn
                    .query_row(
                        "SELECT period_ms FROM poll_group WHERE id = ?1",
                        rusqlite::params![pg_id],
                        |r| r.get(0),
                    )
                    .unwrap_or(1000);

                if let Err(e) = spawn_one_poller(
                    &plcs,
                    &pollers,
                    &pdu_cache,
                    &sample_tx,
                    &conn,
                    *plc_id,
                    &plc_name,
                    *pg_id,
                    period_ms,
                    overrun_mode,
                ) {
                    error!(
                        "apply-worker: spawn_one_poller failed for PLC {} group {}: {}",
                        plc_id, pg_id, e
                    );
                }
            }

            if let Err(e) = db::config_apply::finish_apply(&conn, row.id) {
                error!("apply-worker: finish_apply id={} failed: {e}", row.id);
            }
        } else {
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    info!("apply-worker stopped");
}

// ---------------------------------------------------------------------------
// Command worker
// ---------------------------------------------------------------------------

/// Spawn a poller for one (plc, poll_group) pair.
fn spawn_one_poller(
    plcs: &PlcRegistry,
    pollers: &PollerRegistry,
    pdu_cache: &PduCache,
    sample_tx: &Sender<SampleBatch>,
    conn: &Connection,
    plc_id: i64,
    plc_name: &str,
    pg_id: i64,
    period_ms: u64,
    overrun_mode: OverrunMode,
) -> Result<(), anyhow::Error> {
    // Build plan
    let tags = load_active_tags(conn, plc_id, pg_id)?;
    if tags.is_empty() {
        debug!("PLC {} group {}: no active tags, skipping", plc_name, pg_id);
        return Ok(());
    }
    let pdu_payload = pdu_cache.lock().get(&plc_id).copied().unwrap_or(240);
    let max_gap = 8;
    let plan = build_plan(plc_id, pg_id, &tags, pdu_payload, max_gap);

    // Get client
    let client = {
        let reg = plcs.lock();
        reg.get(&plc_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("PLC {plc_id} not in registry"))?
    };

    // Spawn
    let handle = spawn_poller(
        plc_id,
        plc_name.to_string(),
        period_ms,
        plan,
        tags,
        client,
        sample_tx.clone(),
        overrun_mode,
    );

    // Store
    pollers.lock().insert((plc_id, pg_id), handle);
    info!(
        "poller started for PLC {} group {} (period={}ms)",
        plc_name, pg_id, period_ms
    );
    Ok(())
}

/// Load enabled, non-bad tags for a PLC + poll group from enabled metric sets.
fn load_active_tags(
    conn: &Connection,
    plc_id: i64,
    pg_id: i64,
) -> Result<Vec<TagRow>, anyhow::Error> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT t.id, t.plc_id, t.name, t.address,
                t.db_num, t.byte_offset, t.bit_offset,
                t.byte_size, t.data_type, t.poll_group_id,
                t.store_mode, t.storage_hook, t.hook_params_json,
                t.enabled, t.bad, t.bad_class, t.bad_detail
         FROM tag t
         JOIN metric_set_tag mst ON mst.tag_id = t.id
         JOIN metric_set ms ON ms.id = mst.metric_set_id
         WHERE t.plc_id = ?1
           AND t.poll_group_id = ?2
           AND t.enabled = 1
           AND t.bad = 0
           AND ms.enabled = 1
         ORDER BY t.db_num, t.byte_offset",
    )?;

    let tags: Vec<TagRow> = stmt
        .query_map(rusqlite::params![plc_id, pg_id], |row| {
            let data_type_str: String = row.get(8)?;
            let store_mode_str: String = row.get(10)?;
            let storage_hook_str: String = row.get(11)?;
            let bad_class_str: Option<String> = row.get(15)?;

            Ok(TagRow {
                id: row.get(0)?,
                plc_id: row.get(1)?,
                name: row.get(2)?,
                address: row.get(3)?,
                db_num: row.get(4)?,
                byte_offset: row.get(5)?,
                bit_offset: row.get(6)?,
                byte_size: row.get(7)?,
                data_type: data_type_str.parse::<DataType>().map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        8,
                        rusqlite::types::Type::Text,
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e).into(),
                    )
                })?,
                poll_group_id: row.get(9)?,
                store_mode: store_mode_str.parse::<StoreMode>().map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        10,
                        rusqlite::types::Type::Text,
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e).into(),
                    )
                })?,
                storage_hook: storage_hook_str.parse::<StorageHook>().map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        11,
                        rusqlite::types::Type::Text,
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e).into(),
                    )
                })?,
                hook_params_json: row.get(12)?,
                enabled: row.get(13)?,
                bad: row.get(14)?,
                bad_class: bad_class_str.map(|s| s.parse::<BadClass>().ok()).flatten(),
                bad_detail: row.get(16)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    Ok(tags)
}

/// Resolve a tag id from a command request: prefer an explicit `tag_id`,
/// otherwise look it up by (`plc` name, `name`). Remote CLIs cannot resolve
/// tag ids locally (their replica DB is empty), so they send plc+name instead.
fn resolve_tag_id(conn: &Connection, req: &serde_json::Value) -> Result<i64, anyhow::Error> {
    if let Some(id) = req["tag_id"].as_i64() {
        return Ok(id);
    }
    match (req["plc"].as_str(), req["name"].as_str()) {
        (Some(plc), Some(name)) => conn
            .query_row(
                "SELECT t.id FROM tag t JOIN plc p ON p.id = t.plc_id \
                 WHERE p.name = ?1 AND t.name = ?2",
                rusqlite::params![plc, name],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|_| anyhow::anyhow!("tag '{name}' not found on PLC '{plc}'")),
        _ => Err(anyhow::anyhow!("missing tag_id or (plc, name)")),
    }
}

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
                    let (vi, vf) = crate::engine::storage::decode_value(&data, dt, None);

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
                "tag_add_full" => {
                    // Full remote tag add: PLC + name + address + dt + ... — server
                    // does INSERT into tag + metric_set_tag, validates via PLC
                    // client, schedules apply. Returns tag_id, ok, value.
                    let plc_name = req["plc"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing plc"))?;
                    let name = req["name"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing name"))?;
                    let address_str = req["address"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing address"))?;
                    let data_type_str = req["data_type"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing data_type"))?;
                    let poll_group = req["poll_group"].as_str().unwrap_or("slow");
                    let store_mode = req["store_mode"].as_str().unwrap_or("on_change");
                    let storage_hook = req["storage_hook"].as_str().unwrap_or("on_change");
                    let value_role = req["value_role"].as_str().unwrap_or("state");
                    let metric_set = req["metric_set"].as_str();

                    let dt: DataType = data_type_str
                        .parse()
                        .map_err(|e: String| anyhow::anyhow!("invalid data_type: {e}"))?;
                    let _sm: StoreMode = store_mode.parse().map_err(|_e: String| {
                        anyhow::anyhow!("invalid store_mode: {store_mode}")
                    })?;
                    let _sh: StorageHook = storage_hook.parse().map_err(|_e: String| {
                        anyhow::anyhow!("invalid storage_hook: {storage_hook}")
                    })?;
                    let addr = S7Address::parse(address_str)
                        .map_err(|e| anyhow::anyhow!("invalid address: {e}"))?;

                    let inserted = db::validation::insert_tag(
                        conn,
                        plc_name,
                        name,
                        &addr,
                        dt,
                        poll_group,
                        store_mode,
                        storage_hook,
                        value_role,
                        metric_set,
                    )?;
                    let tag_id = inserted.tag_id;

                    // Validation
                    let client_arc = {
                        let reg = plcs.lock();
                        reg.get(&inserted.plc_id).cloned().ok_or_else(|| {
                            anyhow::anyhow!("PLC client not available for {}", inserted.plc_id)
                        })?
                    };
                    let result = db::validation::validate_tag(conn, &client_arc, tag_id)?;

                    if result.ok {
                        snapshot.update(
                            tag_id,
                            inserted.plc_id,
                            name,
                            address_str,
                            dt,
                            result.value_i,
                            result.value_f,
                            result.ts_us,
                        );
                    }

                    let _ = db::config_apply::schedule(
                        conn,
                        ApplyScope::Tag,
                        Some(inserted.plc_id),
                        None,
                        None,
                        Some(tag_id),
                        &format!("tag_add_full id={tag_id}"),
                        db::config_apply::PLAN_COOLDOWN_MS,
                        None,
                    );

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

                    if result.ok {
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

                "tag_disable" => {
                    let tag_id = resolve_tag_id(conn, &req)?;
                    {
                        let plc_id: Option<i64> = conn
                            .query_row(
                                "SELECT plc_id FROM tag WHERE id = ?1",
                                rusqlite::params![tag_id],
                                |row| row.get(0),
                            )
                            .ok();
                        conn.execute(
                            "UPDATE tag SET enabled = 0 WHERE id = ?1",
                            rusqlite::params![tag_id],
                        )?;
                        let reason = format!("tag_disable id={tag_id}");
                        let _ = db::config_apply::schedule(
                            conn,
                            ApplyScope::Tag,
                            plc_id,
                            None,
                            None,
                            Some(tag_id),
                            &reason,
                            db::config_apply::PLAN_COOLDOWN_MS,
                            None,
                        );
                        Ok((true, format!("tag {tag_id} disabled, apply scheduled")))
                    }
                }

                "tag_update" => {
                    let tag_id = resolve_tag_id(conn, &req)?;
                    let now = now_us();
                    if let Some(addr_str) = req["address"].as_str() {
                        let addr = S7Address::parse(addr_str)
                            .map_err(|e| anyhow::anyhow!("invalid address: {e}"))?;
                        conn.execute(
                            "UPDATE tag SET address = ?1, db_num = ?2, byte_offset = ?3, updated_ts = ?4, desired_version = desired_version + 1 WHERE id = ?5",
                            rusqlite::params![addr_str, addr.db_num, addr.byte_offset, now, tag_id],
                        )?;
                    }
                    if let Some(dt) = req["data_type"].as_str() {
                        let byte_size = dt
                            .parse::<DataType>()
                            .map_err(|e: String| anyhow::anyhow!("{e}"))?
                            .byte_size() as i64;
                        conn.execute(
                            "UPDATE tag SET data_type = ?1, byte_size = ?2, updated_ts = ?3, desired_version = desired_version + 1 WHERE id = ?4",
                            rusqlite::params![dt, byte_size, now, tag_id],
                        )?;
                    }
                    if let Some(pg) = req["poll_group"].as_str() {
                        let pg_id: i64 = conn
                            .query_row(
                                "SELECT id FROM poll_group WHERE name = ?1",
                                rusqlite::params![pg],
                                |row| row.get(0),
                            )
                            .map_err(|_| anyhow::anyhow!("poll group '{pg}' not found"))?;
                        conn.execute(
                            "UPDATE tag SET poll_group_id = ?1, updated_ts = ?2, desired_version = desired_version + 1 WHERE id = ?3",
                            rusqlite::params![pg_id, now, tag_id],
                        )?;
                    }
                    if let Some(sm) = req["store_mode"].as_str() {
                        conn.execute(
                            "UPDATE tag SET store_mode = ?1, updated_ts = ?2, desired_version = desired_version + 1 WHERE id = ?3",
                            rusqlite::params![sm, now, tag_id],
                        )?;
                    }
                    if let Some(sh) = req["storage_hook"].as_str() {
                        conn.execute(
                            "UPDATE tag SET storage_hook = ?1, updated_ts = ?2, desired_version = desired_version + 1 WHERE id = ?3",
                            rusqlite::params![sh, now, tag_id],
                        )?;
                    }
                    let _ = db::config_apply::schedule(
                        conn,
                        ApplyScope::Tag,
                        None,
                        None,
                        None,
                        Some(tag_id),
                        "tag_updated",
                        db::config_apply::PLAN_COOLDOWN_MS,
                        None,
                    );
                    Ok((true, format!("tag_id={tag_id} apply_state=pending")))
                }

                "tag_revalidate" => {
                    let tag_id = resolve_tag_id(conn, &req)?;

                    let (plc_id, name, address, data_type_str): (i64, String, String, String) =
                        conn.query_row(
                            "SELECT plc_id, name, address, data_type FROM tag WHERE id = ?1",
                            rusqlite::params![tag_id],
                            |row| {
                                Ok((
                                    row.get::<_, i64>(0)?,
                                    row.get::<_, String>(1)?,
                                    row.get::<_, String>(2)?,
                                    row.get::<_, String>(3)?,
                                ))
                            },
                        )?;
                    let dt: DataType = data_type_str.parse().unwrap_or(DataType::I16);

                    let client_arc = {
                        let reg = plcs.lock();
                        reg.get(&plc_id)
                            .cloned()
                            .ok_or_else(|| anyhow::anyhow!("PLC client not available"))?
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

                    let _ = db::config_apply::schedule(
                        conn,
                        ApplyScope::Tag,
                        Some(plc_id),
                        None,
                        None,
                        Some(tag_id),
                        &format!("tag_revalidate id={tag_id}"),
                        db::config_apply::PLAN_COOLDOWN_MS,
                        None,
                    );

                    if result.ok {
                        Ok((
                            true,
                            format!("tag {tag_id} revalidated OK value={:?}", result.value_i),
                        ))
                    } else {
                        let det = result.detail.as_deref().unwrap_or("");
                        Ok((false, format!("revalidate failed: {det}")))
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
                        spawn_one_poller(
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

                "plc_add" => {
                    let name = req["name"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing name"))?;
                    let host = req["host"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing host"))?;
                    let rack = req["rack"].as_i64().unwrap_or(0);
                    let slot = req["slot"].as_i64().unwrap_or(0);
                    let disabled = req["disabled"].as_bool().unwrap_or(false);

                    if name.trim().is_empty() {
                        return Ok((false, "name cannot be empty".into()));
                    }

                    let exists: bool = conn
                        .query_row(
                            "SELECT COUNT(*) FROM plc WHERE name = ?1",
                            rusqlite::params![name],
                            |row| row.get::<_, i64>(0),
                        )
                        .map(|c| c > 0)
                        .unwrap_or(false);
                    if exists {
                        return Ok((false, format!("PLC '{name}' already exists")));
                    }

                    let enabled = if disabled { 0 } else { 1 };
                    conn.execute(
                        "INSERT INTO plc (name, host, rack, slot, enabled)
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        rusqlite::params![name, host, rack, slot, enabled],
                    )?;
                    let id = conn.last_insert_rowid();

                    // Register PLC client at runtime (mirrors init_plc_clients)
                    let client = create_plc_client(name, host, rack as u16, slot as u16);
                    {
                        let mut c = client.lock();
                        if c.connect().is_ok() {
                            match c.pdu_len() {
                                Ok(n) => {
                                    info!("PLC {}: PDU length = {} bytes", name, n);
                                    pdu_cache.lock().insert(id, n);
                                }
                                Err(e) => {
                                    warn!("PLC {}: pdu_len() failed: {} (using 240)", name, e);
                                }
                            }
                        } else {
                            warn!(
                                "PLC {}: connect failed at startup; will retry in poller",
                                name
                            );
                            pdu_cache.lock().insert(id, 240);
                        }
                    }
                    plcs.lock().insert(id, client);
                    info!("PLC '{}' (id={}) registered at runtime", name, id);

                    Ok((true, format!("success plc_id={id} name={name} host={host}")))
                }

                "poll_group_add" => {
                    let name = req["name"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing name"))?;
                    let period_ms = req["period_ms"].as_i64().unwrap_or(0);

                    if name.trim().is_empty() {
                        return Ok((false, "name cannot be empty".into()));
                    }
                    if period_ms < 1 {
                        return Ok((false, "period_ms must be at least 1".into()));
                    }

                    let exists: bool = conn
                        .query_row(
                            "SELECT COUNT(*) FROM poll_group WHERE name = ?1",
                            rusqlite::params![name],
                            |row| row.get::<_, i64>(0),
                        )
                        .map(|c| c > 0)
                        .unwrap_or(false);
                    if exists {
                        return Ok((false, format!("poll group '{name}' already exists")));
                    }

                    conn.execute(
                        "INSERT INTO poll_group (name, period_ms) VALUES (?1, ?2)",
                        rusqlite::params![name, period_ms],
                    )?;
                    Ok((true, format!("success poll_group={name} period_ms={period_ms}")))
                }

                "set_create" => {
                    let plc_name = req["plc"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing plc"))?;
                    let name = req["name"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing name"))?;
                    let description = req["description"].as_str();

                    let plc_id: i64 = conn
                        .query_row(
                            "SELECT id FROM plc WHERE name = ?1",
                            rusqlite::params![plc_name],
                            |row| row.get(0),
                        )
                        .map_err(|_| anyhow::anyhow!("PLC '{plc_name}' not found"))?;

                    let now = now_us();
                    conn.execute(
                        "INSERT INTO metric_set (plc_id, name, enabled, description, created_ts, updated_ts)
                         VALUES (?1, ?2, 0, ?3, ?4, ?4)",
                        rusqlite::params![plc_id, name, description, now],
                    )?;
                    let id = conn.last_insert_rowid();
                    Ok((true, format!("success set_id={id} name={name} plc={plc_name}")))
                }

                "set_enable" => {
                    let set_name = req["set_name"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing set_name"))?;
                    let now = now_us();
                    conn.execute(
                        "UPDATE metric_set SET enabled = 1, updated_ts = ?1 WHERE name = ?2",
                        rusqlite::params![now, set_name],
                    )?;
                    if conn.changes() == 0 {
                        return Ok((false, format!("metric set '{set_name}' not found")));
                    }
                    let set_id: i64 = conn.query_row(
                        "SELECT id FROM metric_set WHERE name = ?1",
                        rusqlite::params![set_name],
                        |row| row.get(0),
                    )?;
                    let _ = db::config_apply::schedule(
                        conn,
                        ApplyScope::MetricSet,
                        None,
                        None,
                        Some(set_id),
                        None,
                        "metric_set_enabled",
                        db::config_apply::PLAN_COOLDOWN_MS,
                        None,
                    );
                    Ok((true, format!("success set={set_name} enabled=1 apply_state=pending")))
                }

                "set_disable" => {
                    let set_name = req["set_name"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing set_name"))?;
                    let now = now_us();
                    conn.execute(
                        "UPDATE metric_set SET enabled = 0, updated_ts = ?1 WHERE name = ?2",
                        rusqlite::params![now, set_name],
                    )?;
                    if conn.changes() == 0 {
                        return Ok((false, format!("metric set '{set_name}' not found")));
                    }
                    let set_id: i64 = conn.query_row(
                        "SELECT id FROM metric_set WHERE name = ?1",
                        rusqlite::params![set_name],
                        |row| row.get(0),
                    )?;
                    let _ = db::config_apply::schedule(
                        conn,
                        ApplyScope::MetricSet,
                        None,
                        None,
                        Some(set_id),
                        None,
                        "metric_set_disabled",
                        db::config_apply::PLAN_COOLDOWN_MS,
                        None,
                    );
                    Ok((true, format!("success set={set_name} enabled=0 apply_state=pending")))
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
fn command_worker(
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

// ---------------------------------------------------------------------------
// Sample writer
// ---------------------------------------------------------------------------

fn write_samples(conn: &Connection, batch: &SampleBatch) -> Result<(), anyhow::Error> {
    let ci = &batch.cycle_info;

    // Update PLC runtime status (last_ok_ts / last_error / connectivity)
    if ci.ok {
        conn.execute(
            "UPDATE plc SET last_ok_ts = ?1, last_error = NULL,
                            connectivity = 'online'
             WHERE id = ?2",
            rusqlite::params![ci.finished_ts, ci.plc_id],
        )?;
    } else if let Some(ref err) = ci.error {
        conn.execute(
            "UPDATE plc SET last_error = ?1, connectivity = 'offline'
             WHERE id = ?2",
            rusqlite::params![err, ci.plc_id],
        )?;
    }

    // Find the active dataset for this PLC. Datasets are not directly
    // linked to poll groups in the schema; the active dataset for the
    // PLC is the one to attach new cycles to. If there is no active
    // dataset, the sample-writer skips the write (pollers only produce
    // data while a collection is running).
    let dataset_id_opt: Option<i64> = conn
        .query_row(
            "SELECT id FROM dataset
             WHERE plc_id = ?1 AND state = 'active'
             ORDER BY id DESC LIMIT 1",
            rusqlite::params![ci.plc_id],
            |row| row.get(0),
        )
        .ok();
    let dataset_id = match dataset_id_opt {
        Some(id) => id,
        None => {
            debug!(
                "no active dataset for plc_id={} poll_group_id={}, skipping cycle write",
                ci.plc_id, ci.poll_group_id
            );
            return Ok(());
        }
    };

    conn.execute(
        "INSERT INTO poll_cycle
            (plc_id, poll_group_id, seq, started_ts, finished_ts,
             target_ms, actual_ms, ok, error, dataset_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        rusqlite::params![
            ci.plc_id,
            ci.poll_group_id,
            ci.seq as i64,
            ci.started_ts,
            ci.finished_ts,
            ci.target_ms as i64,
            ci.actual_ms,
            ci.ok as i32,
            ci.error,
            dataset_id,
        ],
    )?;
    let cycle_id = conn.last_insert_rowid();

    // Insert one read_span per physical S7 read, and remember the rowid
    // for each cycle-local span id.
    let mut span_rowids: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
    for s in &batch.spans {
        // read_span has (plc_id, poll_group_id, db_num, byte_offset,
        // byte_len, active) per docs/schema.sql. `ok` is implicit in
        // the parent poll_cycle row.
        conn.execute(
            "INSERT INTO read_span
                (plc_id, poll_group_id, db_num, byte_offset, byte_len, active)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                ci.plc_id,
                ci.poll_group_id,
                s.db_num as i64,
                s.byte_offset as i64,
                s.byte_len as i64,
                1i32,
            ],
        )?;
        span_rowids.insert(s.span_id, conn.last_insert_rowid());
    }

    if !batch.samples.is_empty() {
        let mut stmt = conn.prepare(
            "INSERT INTO sample
                (ts, tag_id, vi, vf, span_id, dataset_id, cycle_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;
        for s in &batch.samples {
            let span_rowid = s.span_id.and_then(|sid| span_rowids.get(&sid).copied());
            stmt.execute(rusqlite::params![
                s.ts, s.tag_id, s.vi, s.vf, span_rowid, dataset_id, cycle_id,
            ])?;
        }
    }

    // Rollup rows from min_max_rollup / delta_rollup hooks
    if !batch.rollups.is_empty() {
        let mut stmt = conn.prepare(
            "INSERT INTO sample_rollup
                (dataset_id, tag_id, plc_id, poll_group_id,
                 window_start_ts, window_end_ts, sample_count,
                 first_vi, last_vi, min_vi, max_vi, delta_vi,
                 first_vf, last_vf, min_vf, max_vf, delta_vf)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
        )?;
        for r in &batch.rollups {
            let p = &r.payload;
            stmt.execute(rusqlite::params![
                dataset_id,
                r.tag_id,
                ci.plc_id,
                ci.poll_group_id,
                p.window_start_ts,
                p.window_end_ts,
                p.sample_count,
                p.first_vi,
                p.last_vi,
                p.min_vi,
                p.max_vi,
                p.delta_vi,
                p.first_vf,
                p.last_vf,
                p.min_vf,
                p.max_vf,
                p.delta_vf,
            ])?;
        }
    }

    // State intervals from state_duration hook
    if !batch.state_intervals.is_empty() {
        let mut stmt = conn.prepare(
            "INSERT INTO event_marker
                (dataset_id, plc_id, cycle_id, ts, kind, severity, tag_id, value_i, details_json)
             VALUES (?1, ?2, ?3, ?4, 'state_duration', 'info', ?5, ?6, ?7)",
        )?;
        for iv in &batch.state_intervals {
            let details = serde_json::json!({
                "start_ts": iv.start_ts,
                "end_ts": iv.end_ts,
                "duration_ms": iv.duration_ms,
            });
            stmt.execute(rusqlite::params![
                dataset_id,
                ci.plc_id,
                cycle_id,
                iv.end_ts,
                iv.tag_id,
                iv.state,
                details.to_string(),
            ])?;
        }
    }

    // Mark localized bad tags (architecture.md: tag.bad = 1 with class+detail)
    // Then schedule a config apply so the plan is rebuilt without them.
    if !batch.bad_tags.is_empty() {
        for b in &batch.bad_tags {
            conn.execute(
                "UPDATE tag SET bad = 1, bad_class = ?1, bad_detail = ?2, last_error_ts = ?3
                 WHERE id = ?4",
                rusqlite::params![b.bad_class.to_string(), b.detail, ci.finished_ts, b.tag_id],
            )?;
            // Schedule plan rebuild for this tag's PLC + poll group
            let _ = crate::db::config_apply::schedule(
                conn,
                ApplyScope::Tag,
                Some(ci.plc_id),
                None,
                None,
                Some(b.tag_id),
                &format!("bad_tag id={}", b.tag_id),
                crate::db::config_apply::PLAN_COOLDOWN_MS,
                Some(
                    &serde_json::json!({
                        "bad_class": b.bad_class.to_string(),
                        "detail": b.detail,
                    })
                    .to_string(),
                ),
            );
        }
    }

    // Derived event markers
    if !batch.event_markers.is_empty() {
        let mut stmt = conn.prepare(
            "INSERT INTO event_marker
                (dataset_id, plc_id, cycle_id, ts, kind, severity, tag_id, value_i, details_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )?;
        for em in &batch.event_markers {
            stmt.execute(rusqlite::params![
                dataset_id,
                ci.plc_id,
                cycle_id,
                em.ts,
                em.kind,
                em.severity,
                em.tag_id,
                em.value_i,
                em.details_json,
            ])?;
        }
    }

    for s in &batch.samples {
        conn.execute(
            "UPDATE tag SET last_value_i = COALESCE(?1, last_value_i),
                            last_value_f = COALESCE(?2, last_value_f),
                            last_ts = COALESCE(?3, last_ts)
             WHERE id = ?4",
            rusqlite::params![s.vi, s.vf, s.ts, s.tag_id],
        )?;
    }

    Ok(())
}

fn sample_writer(
    db: Arc<Mutex<Connection>>,
    rx: Receiver<SampleBatch>,
    running: Arc<AtomicBool>,
    snapshot: Arc<Snapshot>,
) {
    info!("sample-writer started");

    // Cache tag metadata so we can update the in-memory snapshot without
    // hitting SQLite on every poll cycle. Populated lazily from the DB.
    // Map: tag_id -> (plc_id, name, address, data_type)
    use std::collections::HashMap;
    let mut tag_cache: HashMap<i64, (i64, String, String, DataType)> = HashMap::new();

    while running.load(Ordering::Relaxed) {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(batch) => {
                let conn = db.lock();
                if let Err(e) = write_samples(&conn, &batch) {
                    error!("sample-writer: write error: {e}");
                }
                // Update snapshot from every successful poll cycle, regardless
                // of whether the samples were stored (the snapshot is the live
                // view for status/API reads — per architecture.md D002).
                update_snapshot_from_batch(&snapshot, &conn, &mut tag_cache, &batch);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                warn!("sample-writer: channel closed, stopping");
                break;
            }
        }
    }

    while let Ok(batch) = rx.try_recv() {
        let conn = db.lock();
        if let Err(e) = write_samples(&conn, &batch) {
            error!("sample-writer: drain write error: {e}");
        }
        update_snapshot_from_batch(&snapshot, &conn, &mut tag_cache, &batch);
    }

    info!("sample-writer stopped");
}

/// Update the in-memory snapshot with tag values from a poll batch.
/// Populates the tag metadata cache lazily from the database.
fn update_snapshot_from_batch(
    snapshot: &Arc<Snapshot>,
    conn: &Connection,
    tag_cache: &mut HashMap<i64, (i64, String, String, DataType)>,
    batch: &SampleBatch,
) {
    let plc_id = batch.plc_id;
    for sample in &batch.samples {
        // Lazy-load tag metadata into cache
        if !tag_cache.contains_key(&sample.tag_id) {
            if let Ok((name, address, dt_str)) = conn.query_row(
                "SELECT name, address, data_type FROM tag WHERE id = ?1",
                rusqlite::params![sample.tag_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            ) {
                if let Ok(dt) = dt_str.parse::<DataType>() {
                    tag_cache.insert(sample.tag_id, (plc_id, name, address, dt));
                }
            }
        }
        if let Some((plc_id, name, address, dt)) = tag_cache.get(&sample.tag_id).cloned() {
            snapshot.update(
                sample.tag_id,
                plc_id,
                &name,
                &address,
                dt,
                sample.vi,
                sample.vf,
                sample.ts,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Server configuration.
pub struct ServerConfig {
    pub db_path: String,
    /// If set, start the HTTP API on this bind address (e.g. "127.0.0.1:8080").
    pub http_bind: Option<String>,
    /// If true, open the platform-native control window (Windows only).
    pub control_window: bool,
    /// Overrun behavior: SkipMissedTicks (default) or Strict.
    pub overrun_mode: OverrunMode,
}

/// The main s7trace server. Orchestrates all workers and pollers.
pub struct Server {
    db: Arc<Mutex<Connection>>,
    snapshot: Arc<Snapshot>,
    running: Arc<AtomicBool>,
    plcs: PlcRegistry,
    pollers: PollerRegistry,
    pdu_cache: PduCache,
    http_bind: Option<String>,
    control_window: bool,
    overrun_mode: OverrunMode,
}

impl Server {
    /// Create a new server instance.
    pub fn new(config: ServerConfig) -> Result<Self, anyhow::Error> {
        let conn = db::schema::open(&config.db_path)
            .map_err(|e| anyhow::anyhow!("opening database: {e}"))?;
        let db = Arc::new(Mutex::new(conn));
        let snapshot = Arc::new(Snapshot::new());
        let running = Arc::new(AtomicBool::new(true));

        Ok(Server {
            db,
            snapshot,
            running,
            plcs: Arc::new(Mutex::new(HashMap::new())),
            pollers: Arc::new(Mutex::new(HashMap::new())),
            pdu_cache: Arc::new(Mutex::new(HashMap::new())),
            http_bind: config.http_bind,
            control_window: config.control_window,
            overrun_mode: config.overrun_mode,
        })
    }

    /// Initialize PLC clients for all configured PLCs. Attempts to
    /// connect and discover the negotiated PDU length; falls back to a
    /// safe default (240) if the PLC is not reachable at startup.
    fn init_plc_clients(&self) -> Result<(), anyhow::Error> {
        let conn = self.db.lock();
        let mut stmt = conn.prepare(
            "SELECT id, name, host, COALESCE(rack, 0), COALESCE(slot, 0) FROM plc ORDER BY id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?;

        let mut reg = self.plcs.lock();
        let mut pdu = self.pdu_cache.lock();
        for row in rows {
            let (plc_id, plc_name, host, rack, slot): (i64, String, String, i64, i64) = row?;
            info!(
                "initializing PLC '{}' (id={}) at {} (rack={}, slot={})",
                plc_name, plc_id, host, rack, slot
            );
            let client = create_plc_client(&plc_name, &host, rack as u16, slot as u16);

            // Try to connect and discover PDU length. Failure is non-fatal
            // (PLC may come online later).
            {
                let mut c = client.lock();
                if c.connect().is_ok() {
                    match c.pdu_len() {
                        Ok(n) => {
                            info!("PLC {}: PDU length = {} bytes", plc_name, n);
                            pdu.insert(plc_id, n);
                        }
                        Err(e) => warn!("PLC {}: pdu_len() failed: {} (using 240)", plc_name, e),
                    }
                } else {
                    warn!(
                        "PLC {}: connect failed at startup; will retry in poller",
                        plc_name
                    );
                    pdu.insert(plc_id, 240);
                }
            }
            reg.insert(plc_id, client);
        }

        info!("initialized {} PLC client(s)", reg.len());
        Ok(())
    }

    /// Run the server main loop. Blocks until shutdown.
    pub fn run(&mut self) -> Result<(), anyhow::Error> {
        info!("s7trace server starting");

        // Init PLC clients
        self.init_plc_clients()?;

        // Startup revalidation of bad tags (per architecture.md:195)
        {
            let conn = self.db.lock();
            let bad_tags: Vec<(i64, i64)> = conn
                .prepare("SELECT id, plc_id FROM tag WHERE bad = 1")
                .ok()
                .and_then(|mut s| {
                    s.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))
                        .ok()
                        .map(|it| it.filter_map(|x| x.ok()).collect())
                })
                .unwrap_or_default();
            if !bad_tags.is_empty() {
                info!("startup: revalidating {} bad tag(s)", bad_tags.len());
                let reg = self.plcs.lock();
                for (tag_id, plc_id) in &bad_tags {
                    if let Some(client_arc) = reg.get(plc_id) {
                        match db::validation::validate_tag(&conn, client_arc, *tag_id) {
                            Ok(result) => {
                                if result.ok {
                                    info!(
                                        "startup: tag {} revalidated OK, clearing bad flag",
                                        tag_id
                                    );
                                    let _ = db::config_apply::schedule(
                                        &conn,
                                        ApplyScope::Tag,
                                        Some(*plc_id),
                                        None,
                                        None,
                                        Some(*tag_id),
                                        &format!("startup_revalidate tag_id={tag_id}"),
                                        db::config_apply::PLAN_COOLDOWN_MS,
                                        None,
                                    );
                                } else {
                                    info!(
                                        "startup: tag {} still bad (class={:?} detail={:?})",
                                        tag_id, result.bad_class, result.detail
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("startup: validate tag {} failed: {e}", tag_id);
                            }
                        }
                    } else {
                        warn!(
                            "startup: no PLC client for plc_id={} tag_id={}, skipping revalidation",
                            plc_id, tag_id
                        );
                    }
                }
                drop(reg);
            } else {
                info!("startup: no bad tags to revalidate");
            }
            drop(conn);
        }

        // Channel for poller samples
        let (sample_tx, sample_rx) = mpsc::channel::<SampleBatch>();

        // Start sample writer thread (with snapshot for live value updates)
        let db_writer = self.db.clone();
        let running_writer = self.running.clone();
        let snapshot_writer = self.snapshot.clone();
        std::thread::Builder::new()
            .name("sample-writer".into())
            .spawn(move || sample_writer(db_writer, sample_rx, running_writer, snapshot_writer))?;

        // Start command worker thread (with shared registries)
        let db_cmd = self.db.clone();
        let running_cmd = self.running.clone();
        let snapshot_cmd = self.snapshot.clone();
        let plcs_cmd = self.plcs.clone();
        let pollers_cmd = self.pollers.clone();
        let pdu_cache_cmd = self.pdu_cache.clone();
        let sample_tx_cmd = sample_tx.clone();
        let overrun_cmd = self.overrun_mode;
        std::thread::Builder::new()
            .name("cmd-worker".into())
            .spawn(move || {
                command_worker(
                    db_cmd,
                    running_cmd,
                    snapshot_cmd,
                    plcs_cmd,
                    pollers_cmd,
                    pdu_cache_cmd,
                    sample_tx_cmd,
                    overrun_cmd,
                );
            })?;

        // Start config apply worker thread (now with registries for plan rebuild)
        let db_apply = self.db.clone();
        let running_apply = self.running.clone();
        let plcs_apply = self.plcs.clone();
        let pollers_apply = self.pollers.clone();
        let pdu_cache_apply = self.pdu_cache.clone();
        let sample_tx_apply = sample_tx.clone();
        let overrun_apply = self.overrun_mode;
        std::thread::Builder::new()
            .name("apply-worker".into())
            .spawn(move || {
                apply_worker(
                    db_apply,
                    running_apply,
                    plcs_apply,
                    pollers_apply,
                    pdu_cache_apply,
                    sample_tx_apply,
                    overrun_apply,
                );
            })?;

        // Optionally start HTTP API
        if let Some(bind) = self.http_bind.clone() {
            let db_http = self.db.clone();
            std::thread::Builder::new()
                .name("http-api".into())
                .spawn(move || {
                    if let Err(e) = api::serve(db_http, bind) {
                        error!("HTTP API server failed: {e}");
                    }
                })?;
        }

        // Optionally open the platform control window (Windows only).
        #[cfg(windows)]
        if self.control_window {
            let db_win = self.db.clone();
            let running_win = self.running.clone();
            std::thread::Builder::new()
                .name("control-window".into())
                .spawn(move || {
                    if let Err(e) = crate::platform::run_control_window(db_win) {
                        error!("control window exited with error: {e}");
                    }
                    // Window returning means user chose Exit.
                    running_win.store(false, Ordering::Relaxed);
                })?;
        }

        info!("all workers started, monitoring...");

        // Monitor loop
        let mut tick: u64 = 0;
        while self.running.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(5000));
            tick += 1;

            if tick % 6 == 0 {
                let conn = self.db.lock();
                let tag_count: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM tag WHERE enabled = 1 AND bad = 0",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap_or(0);
                let bad_count: i64 = conn
                    .query_row("SELECT COUNT(*) FROM tag WHERE bad = 1", [], |r| r.get(0))
                    .unwrap_or(0);
                let queue_count: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM command_request WHERE state = 'pending'",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap_or(0);
                let active_ds: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM dataset WHERE state = 'active'",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap_or(0);
                let poller_count = self.pollers.lock().len();
                drop(conn);

                info!(
                    "status: {} PLC(s), {} active tag(s), {} bad, {} pending cmd(s), {} active dataset(s), {} poller(s)",
                    self.plcs.lock().len(), tag_count, bad_count, queue_count, active_ds, poller_count,
                );
            }
        }

        // Clean shutdown
        info!("shutting down...");
        // Stop all pollers
        let handles: Vec<PollerHandle> = self.pollers.lock().drain().map(|(_, h)| h).collect();
        for h in &handles {
            h.stop();
        }
        std::thread::sleep(Duration::from_millis(2000));

        info!("s7trace server stopped");
        Ok(())
    }

    /// Request graceful shutdown.
    #[allow(dead_code)]
    pub fn shutdown(&self) {
        info!("shutdown requested");
        self.running.store(false, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Top-level entry point
// ---------------------------------------------------------------------------

/// Run the server. Called from `main.rs`.
/// Start the server with explicit configuration (used when the CLI passes
/// extra options such as `--http-bind`).
pub fn run_server_with_config(config: ServerConfig) -> Result<(), anyhow::Error> {
    install_panic_logger();
    let mut server = Server::new(config)?;
    server.run()?;
    Ok(())
}

/// Install a panic hook that logs the thread name, source location, and message
/// of any panic before the default hook runs. Without this, a panic in a
/// background thread (poller, cmd-worker, sample-writer, apply-worker) just
/// kills that thread silently — from the outside the process appears to "stop
/// doing work" with nothing in the log. (Bug B-7)
fn install_panic_logger() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        let name = thread.name().unwrap_or("<unnamed>");
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let msg = if let Some(s) = info.payload().downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic payload>".to_string()
        };
        error!("PANIC in thread '{name}' at {location}: {msg}");
        default_hook(info);
    }));
}
